"""Web scraping and HTML parsing logic for Cook County Recorder of Deeds site"""

import re
import requests
import structlog
from bs4 import BeautifulSoup

from utils import make_snake_case, remove_duplicates, clean_pin


logger = structlog.get_logger(__name__)

BASE_URL: str = "https://crs.cookcountyclerkil.gov"
URL_TEMPLATES: list = [
    "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DateRecorded&direction=desc",
    "https://crs.cookcountyclerkil.gov/Search/SortResultByPin?id1={pin}&column=DateRecorded&direction=asc",
    "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=AlphaDocNumber&direction=desc",
    "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=AlphaDocNumber&direction=asc",
    "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DateExecuted&direction=desc",
    "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DateExecuted&direction=asc",
    "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DocTypeDescription&direction=desc",
    "https://crs.cookcountyclerkil.gov/search/SortResultByPin?id1={pin}&column=DocTypeDescription&direction=asc",
]


def retrieve_doc_page_urls(pin: str) -> list[str]:
    """
    Collects all of the available URls to document pages associated with a given PIN

    Parameters:
        pin (str): A pin value

    Returns:
        A list of url pathnames for documents associated with the given pin
    """

    url_pathnames = []
    for url in URL_TEMPLATES:
        logger.info("Querying url...", url=url.format(pin=pin))
        response = requests.get(url.format(pin=pin))
        soup = BeautifulSoup(response.text, features="lxml")
        url_pathnames += [x["href"] for x in soup.find_all("a", string="View")]
    return url_pathnames


def scrape_doc_page(url_pathname: str) -> dict[str] | None:
    """
    Given an url_pathname, pulls the metadata from the document page and returns it

    Parameters:
        url_pathname (str): The url path for a given document.

    Returns:
        Document metadata (dict) from the document page.
    """
    try:
        url = BASE_URL + url_pathname
        logger.info("Scraping url...", url=url)
        response = requests.get(url)
        assert response.status_code == 200, f"Document URL at {
            url
        } returned status code {response.status_code}, skipping..."

        soup = BeautifulSoup(response.text, features="lxml")

        doc_info = extract_info(soup)
        entities = extract_grantor_grantee(soup)
        prior_docs = extract_prior_documents(soup)
        related_pins = extract_related_pins(soup)
        pdf_url_pathname = soup.find("a", href=re.compile("/Document/DisplayPdf"))[
            "href"
        ]
        pdf_url = BASE_URL + pdf_url_pathname

        content = {
            "doc_info": doc_info,
            "entities": entities,
            "prior_docs": prior_docs,
            "related_pins": related_pins,
            "pdf_url": pdf_url,
        }

        return content

    except AssertionError as e:
        logger.error("Error scraping document page", error=e)
        return None


def extract_related_pins(soup: BeautifulSoup) -> list[str]:
    """
    Helper function for pulling related pins from a document page

    Parameters:
        A BeautifulSoup object containing the HTML content for a document page

    Returns:
        A list (list[str]) of the pins related to the pin described in the document, unduplicated
    """
    section = soup.find(True, string=re.compile("Legal Description"))
    if not section:
        return []

    table = section.find_next("table")
    if not table:
        return []

    tbody = table.find("tbody")
    if not tbody:
        return []

    related_pins = []
    rows = tbody.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        related_pin = cells[0].get_text(strip=True)
        related_pin_clean = clean_pin(related_pin)
        related_pins.append(related_pin_clean)

    related_pins = remove_duplicates(related_pins)
    return related_pins


def extract_prior_documents(soup: BeautifulSoup) -> list[str]:
    """
    Helper function for pulling prior documents from a document page

    Parameters:
        A BeautifulSoup object containing the HTML content for a document page

    Returns:
        A list (list[str]) of the prior documents associated with the given document
    """
    section_title = "Prior Documents"
    section_legend = soup.find("span", string=re.compile(section_title))
    if not section_legend:
        return []

    table = section_legend.find_next("table")
    if not table:
        return []

    tbody = table.find("tbody")
    if not tbody:
        return []

    prior_doc_nums = []
    rows = tbody.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        prior_doc_num = cells[1].get_text(strip=True)
        prior_doc_nums.append(prior_doc_num)

    return prior_doc_nums


def extract_info(soup: BeautifulSoup) -> dict[str] | list[None]:
    section_title = "Viewing Document"

    section_legend = soup.find(True, string=re.compile(section_title))
    if not section_legend:
        return []

    info_table = section_legend.find_next("table")
    if not info_table:
        return []

    labels = info_table.find_all("label")
    keys = [label.string.strip(": ") for label in labels]
    keys = [make_snake_case(key) for key in keys]

    info_table_values = info_table.find_all("td")
    values = [v.string for v in info_table_values]
    return dict(zip(keys, values))


def extract_grantor_grantee(soup: BeautifulSoup) -> dict[list[str], list[str]]:
    def extract_table_data(section_title):
        """Helper function to extract data from a specific table section"""
        section_span = soup.find(
            "span", string=re.compile(section_title), class_="fs-5"
        )
        if not section_span:
            return []

        table = section_span.find_next("table", class_="table")
        if not table:
            return []

        data = []
        tbody = table.find("tbody")
        if tbody:
            rows = tbody.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    name_cell = cells[0]
                    name_link = name_cell.find("a")
                    name = (
                        name_link.get_text(strip=True)
                        if name_link
                        else name_cell.get_text(strip=True)
                    )

                    trust_num = cells[1].get_text(strip=True)

                    data.append(
                        {"name": name, "trust_number": trust_num if trust_num else None}
                    )

            return data

    grantors = extract_table_data("Grantors")
    grantees = extract_table_data("Grantees")

    return {"grantors": grantors, "grantees": grantees}