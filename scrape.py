"""Tool to scrape the Cook County Recorder of Deeds site for document metadata"""

import os
import re
import csv
import sqlite3
import logging
from bs4 import BeautifulSoup
import requests

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=os.path.curdir + "/scrape.log",
    format="%(asctime)s %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)

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


def remove_duplicates(l: list[str]) -> list[str]:
    """
    Remove duplicate values from a list

    Parameters:
        l (List[str]): A list of strings.

    Returns:
        A list of unique strings.
    """
    return list(set(l))


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
    assert (
        len(pin) == 14
    ), f"pin value must evaluate to a string of  digits of length 14, instead have {pin}"
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
        logger.info(f"Querying {url.format(pin=pin)}")
        response = requests.get(url.format(pin=pin))
        soup = BeautifulSoup(response.text, features="lxml")
        url_pathnames += [x["href"] for x in soup.find_all("a", string="View")]
    return url_pathnames


def scrape_doc_page(url_pathname: str) -> dict[str]:
    """
    Given an url_pathname, pulls the metadata from the document page and returns it

    Parameters:
        url_pathname (str): The url path for a given document.

    Returns:
        Document metadata (dict) from the document page.
    """
    try:
        url = BASE_URL + url_pathname
        logger.info(f"Scraping {url} ...")
        response = requests.get(url)
        assert (
            response.status_code == 200
        ), f"Document URL at {
            url} returned status code {response.status_code}, skipping..."

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
        logger.error(e)
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


def extract_info(soup: BeautifulSoup) -> dict[str]:
    section_title = "Viewing Document"

    section_legend = soup.find(True, string=re.compile(section_title))
    if not section_legend:
        return

    info_table = section_legend.find_next("table")
    if not info_table:
        return []

    labels = info_table.find_all("label")
    keys = [label.string.strip(": ") for label in labels]
    keys = [make_snake_case(key) for key in keys]

    info_table_values = info_table.find_all("td")
    values = [v.string for v in info_table_values]
    return dict(zip(keys, values))


def extract_grantor_grantee(soup: BeautifulSoup) -> dict[list[str]]:
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


def create_tables(con: sqlite3.Connection):
    """
    Defines and creates the table schemas for the sqlite3 doc_relations_table

    Parameters:
        con (Connection): A connection to a sqlite3 database

    Returns:
        None
    """
    table_definitions = {
        "doc_table": """
                    CREATE TABLE IF NOT EXISTS documents (
                        doc_num VARCHAR(50) PRIMARY KEY,
                        pin VARCHAR(14) NOT NULL,
                        date_executed DATE,
                        date_recorded DATE NOT NULL,
                        num_pages INTEGER,
                        address VARCHAR(255),
                        doc_type VARCHAR(50) NOT NULL,
                        consideration_amount VARCHAR(50),
                        pdf_url VARCHAR(2048) NOT NULL
                    );
                """,
        "entities_table": """
            CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_num VARCHAR(50) NOT NULL,
                pin VARCHAR(50) NOT NULL,
                entity_name VARCHAR(255) NOT NULL,
                entity_status VARCHAR(7) NOT NULL,
                trust_number VARCHAR(50),
                CHECK (entity_status IN ('grantor', 'grantee')),
                CONSTRAINT unique_doc_pin_entity UNIQUE (doc_num, pin, entity_name),
                FOREIGN KEY (doc_num) REFERENCES documents(doc_num)
            );
        """,
        "pins_table": """
                CREATE TABLE IF NOT EXISTS pins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pin VARCHAR(14) NOT NULL,
                    doc_num VARCHAR(50) NOT NULL,
                    related_pin VARCHAR(14) NOT NULL,
                    FOREIGN KEY (doc_num) REFERENCES documents(doc_num)
                );
            """,
        "doc_relations_table": """
                CREATE TABLE IF NOT EXISTS doc_relations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_num VARCHAR(50) NOT NULL,
                    prior_doc_num VARCHAR(50) NOT NULL,
                    FOREIGN KEY (doc_num) REFERENCES documents(doc_num),
                    FOREIGN KEY (prior_doc_num) REFERENCES documents(doc_num)
                );
            """,
    }
    cur = con.cursor()

    for _, definition in table_definitions.items():
        cur.execute(definition)

    con.commit()
    cur.close()


def insert_content(con: sqlite3.Connection, pin: str, content: dict) -> None:
    """
    Inserts document metadata into a sqlite3 database.

    Parameters:
        con (Connection): A sqlite3 connection
        pin (str): A pin value
        content (dict): The metadata for a single document

    Returns:
        None
    """
    doc_num = content["doc_info"]["document_number"]
    cur = con.cursor()

    try:
        doc_data = """
            INSERT OR IGNORE INTO documents (
                doc_num, pin, date_executed, date_recorded,
                num_pages, address, doc_type, consideration_amount, pdf_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        cur.execute(
            doc_data,
            (
                doc_num,
                pin,
                content["doc_info"]["date_executed"],
                content["doc_info"]["date_recorded"],
                content["doc_info"]["#_of_pages"],
                content["doc_info"]["address"],
                content["doc_info"]["document_type"],
                content["doc_info"].get("consideration_amount", ""),
                content["pdf_url"],
            ),
        )

        entity_query = """
            INSERT OR IGNORE INTO entities (
                doc_num, pin, entity_name, entity_status, trust_number
            ) VALUES (?, ?, ?, ?, ?);
        """

        for entity in content["entities"]["grantors"]:
            entity_name = entity.get("name")
            trust_number = entity.get("trust_number", "")
            cur.execute(
                entity_query, (doc_num, pin, entity_name, "grantor", trust_number)
            )

        for entity in content["entities"]["grantees"]:
            entity_name = entity.get("name")
            trust_number = entity.get("trust_number", "")
            cur.execute(
                entity_query, (doc_num, pin, entity_name, "grantee", trust_number)
            )

        related_pin_query = """
            INSERT OR IGNORE INTO pins (pin, doc_num, related_pin) VALUES (?, ?, ?);
        """
        for related_pin in content["related_pins"]:
            cur.execute(related_pin_query, (pin, doc_num, related_pin))

        prior_doc_query = """
            INSERT OR IGNORE INTO doc_relations (doc_num, prior_doc_num) VALUES (?, ?);
        """
        for prior_doc in content["prior_docs"]:
            cur.execute(prior_doc_query, (doc_num, prior_doc))

        con.commit()
        logger.info(f"Successfully inserted document {doc_num}")

    except Exception as e:
        con.rollback()
        logger.error(f"Error inserting document {doc_num}: {e}")
        raise
    finally:
        cur.close()


def get_pins_to_scrape():
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


if __name__ == "__main__":
    con = sqlite3.connect("data/deeds.db")
    create_tables(con)

    pins = get_pins_to_scrape()

    for pin in pins:
        logger.info(f"Querying PIN: {pin}")

        cleaned_pin = clean_pin(pin)

        doc_pathnames = retrieve_doc_page_urls(cleaned_pin)
        doc_pathnames = remove_duplicates(doc_pathnames)

        try:
            for doc_pathname in doc_pathnames:
                doc_data = scrape_doc_page(doc_pathname)
                insert_content(con, pin, doc_data)

            with open("data/completed_pins.csv", "a", newline="") as file:
                file.write(f"{pin}\n")
                file.flush()

        except Exception as e:
            logger.error(f"Error processing PIN {pin}: {e}")
            continue
