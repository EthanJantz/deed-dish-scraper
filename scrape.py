"""Tool to scrape the Cook County Recorder of Deeds site for document metadata"""

import csv
import logging
import os
import re
from datetime import datetime

import requests
import structlog
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from models import Base, Document, Entity, Pin, PriorDoc

engine = create_engine(f"{os.environ.get('DB_URL')}", echo=True)

filename = os.path.curdir + "/logs/scrape.log"
logging.basicConfig(
    # format="%(asctime)s %(message)s",
    handlers=[logging.FileHandler(filename), logging.StreamHandler()],
    encoding="utf-8",
    level=logging.INFO,
)
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
        structlog.dev.ConsoleRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.make_filtering_bound_logger(logging.NOTSET),
    context_class=dict,
    cache_logger_on_first_use=False,
)
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


def make_snake_case(s: str) -> str:
    """Converts a given string to snake_case
    Parameters:
       s (str): A string value
    Returns:
       A string in snake_case
    """
    s = str(s)
    s = [char if char != " " else "_" for char in s]
    return "".join(s).lower()


def remove_duplicates(list_of_strings: list[str]) -> list[str]:
    """
    Remove duplicate values from a list, preserving order.

    Parameters:
        list_of_strings (List[str]): A list of strings.

    Returns:
        A list of unique strings.
    """
    return list(dict.fromkeys(list_of_strings))


def clean_pin(pin: str) -> str:
    """
    Cleans a Property Identification Number (PIN) value

    Parameters:
        pin (str): A formatted PIN

    Returns:
        A PIN without hyphens.
    """
    assert isinstance(pin, str), "pin must be of type str"
    pin = "".join(filter(str.isdigit, pin))
    assert len(pin) == 14, (
        f"pin value must evaluate to a string of  digits of length 14, instead have {pin}"
    )
    return pin


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


def create_tables() -> None:
    """
    Defines and creates the table schemas using SQLAlchemy's ORM Base.metadata

    Parameters:
       None (uses the global 'engine')

    Returns:
        None
    """
    Base.metadata.create_all(engine)


def insert_content(session: Session, pin: str, content: dict) -> None:
    """
    Inserts document metadata into database.

    Parameters:
        session (Session): A SQLAlchemy session
        pin (str): A pin value
        content (dict): The metadata for a single document

    Returns:
        None
    """
    doc_num = content["doc_info"]["document_number"]
    pin = clean_pin(pin)

    try:
        date_executed_str = content["doc_info"].get("date_executed")
        date_executed = (
            datetime.strptime(date_executed_str, "%m/%d/%Y").date()
            if date_executed_str
            else None
        )

        date_recorded_str = content["doc_info"].get("date_recorded")
        date_recorded = (
            datetime.strptime(date_recorded_str, "%m/%d/%Y").date()
            if date_recorded_str
            else None
        )

        document = Document(
            doc_num=doc_num,
            pin=pin,
            date_executed=date_executed,
            date_recorded=date_recorded,
            num_pages=int(content["doc_info"].get("#_of_pages", 0)),
            address=content["doc_info"].get("address"),
            doc_type=content["doc_info"]["document_type"],
            consideration_amount=content["doc_info"].get("consideration_amount"),
            pdf_url=content["pdf_url"],
        )

        session.add(document)

        for entity_data in content["entities"]["grantors"]:
            entity = Entity(
                doc_num=doc_num,
                pin=pin,
                entity_name=entity_data["name"],
                entity_status="grantor",
                trust_number=entity_data["trust_number"],
            )
            session.add(entity)

        for entity_data in content["entities"]["grantees"]:
            entity = Entity(
                doc_num=doc_num,
                pin=pin,
                entity_name=entity_data["name"],
                entity_status="grantee",
                trust_number=entity_data["trust_number"],
            )
            session.add(entity)

        for related_pin_str in content["related_pins"]:
            related_pin_obj = Pin(
                pin=pin,
                doc_num=doc_num,
                related_pin=related_pin_str,
            )
            session.add(related_pin_obj)

        for prior_doc_num_str in content["prior_docs"]:
            prior_doc_obj = PriorDoc(
                doc_num=doc_num,
                prior_doc_num=prior_doc_num_str,
            )
            session.add(prior_doc_obj)

        logger.info("Successfully added document data to session", content=content)

    except Exception as e:
        logger.error("Error inserting document", error=e, content=content)
        raise


def get_pins_to_scrape() -> list[str]:
    path = "data/pins.csv"
    if os.path.exists(path):
        pins = []
        with open(path, "r", newline="") as file:
            for row in csv.reader(file, delimiter=" "):
                pins.append("".join(row).strip(" "))
    else:
        pins = [
            "17-29-304-001-0000",  # Park
            "17-05-115-085-0000",  # Starsiak Clothing
            "16-10-421-053-0000",  # Hotel Guyon
        ]

    # remove pins that have already been scraped
    path = "data/completed_pins.csv"
    if os.path.exists(path):
        completed_pins = []
        with open(path, "r", newline="") as file:
            for row in csv.reader(file, delimiter=" "):
                completed_pins.append("".join(row).strip(" "))

        pins_to_scrape = [pin for pin in pins if pin not in completed_pins]
        return pins_to_scrape

    assert pins, "pins must have length > 0"
    return pins


def scrape_pin(session: Session, pin: str) -> None:
    logger.info("Querying PIN", pin=pin)

    cleaned_pin = clean_pin(pin)

    doc_pathnames = retrieve_doc_page_urls(cleaned_pin)
    doc_pathnames = remove_duplicates(doc_pathnames)

    db_session = session()

    try:
        for doc_pathname in doc_pathnames:
            try:
                doc_data = scrape_doc_page(doc_pathname)
                if doc_data:
                    insert_content(db_session, pin, doc_data)
            except Exception as e:
                # Log the error for the specific document and skip it
                logger.warning(
                    f"Skipping document {doc_pathname} for PIN {pin} due to error: {e}",
                    doc_pathname=doc_pathname,
                    pin=pin,
                    error=e,
                )
                continue  # Move to the next document

        # Commit all successfully processed documents
        db_session.commit()

        # Log overall success for the PIN
        logger.info("Successfully committed write for PIN.", pin=pin)

    except Exception as e:
        # Rollback the entire session only if a general error occurs outside the document processing loop
        db_session.rollback()
        logger.error(
            "Error processing PIN (overall session rollback)", error=e, pin=pin
        )
        return

    finally:
        db_session.close()


if __name__ == "__main__":
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    if not os.path.exists("data"):
        os.makedirs("data")
        logger.info("No data directory found, using default PINs")

    create_tables()

    pins = get_pins_to_scrape()

    for pin in pins:
        scrape_pin(SessionLocal, pin)
        with open("data/completed_pins.csv", "a", newline="") as file:
            file.write(f"{pin}\n")
            file.flush()
        logger.info("Finished scraping and loading data for PIN", pin=pin)
