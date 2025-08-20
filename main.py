"""Main orchestration script for Cook County Recorder of Deeds scraper"""

import csv
import logging
import os
from datetime import datetime

import structlog
from sqlalchemy.orm import Session

from database import create_engine_and_session, create_tables
from models import Document, Entity, Pin, PriorDoc
from scraper import retrieve_doc_page_urls, scrape_doc_page
from utils import clean_pin, remove_duplicates

engine, SessionLocal = create_engine_and_session()

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
    if not os.path.exists("data"):
        os.makedirs("data")
        logger.info("No data directory found, using default PINs")

    create_tables(engine)

    pins = get_pins_to_scrape()

    for pin in pins:
        scrape_pin(SessionLocal, pin)
        with open("data/completed_pins.csv", "a", newline="") as file:
            file.write(f"{pin}\n")
            file.flush()
        logger.info("Finished scraping and loading data for PIN", pin=pin)
