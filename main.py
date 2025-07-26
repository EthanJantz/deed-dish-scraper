"""
Contains the instructions for the entire scraper ETL process. 
"""
import os
import csv
from logger import logging
from sqlalchemy import create_engine
from scraper import Scraper
from initialize_postgres import create_database, create_tables
from load_scraped_data import verify_database, insert_data
from db_config import get_db_config

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=os.path.curdir + "/main.log",
    format="%(asctime)s %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)

def main():

    db_config = get_db_config()
    database_url = "postgresql://{username}:{password}@{host}:{port}/{database}".format(
        username=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port'],
        database=db_config['database']
    )
    engine = create_engine(database_url)

    logger.info("Initializing database...")
    create_database()
    create_tables(engine)
    logger.info(f"Database initialized at {database_url}")

    s = Scraper()
    pins = []
    path = 'data/vacant_bldg_pins.csv'

    with open(path, 'r', newline='') as file:
        for row in csv.reader(file, delimiter=' '):
            pins.append(''.join(row).strip(' '))

    logger.info(f"Scraping {len(pins)} pins...")
    s.scrape(pins)
    logger.info(f"Scrape completed on {len(pins)} pins")

    logger.info("Loading data to postgres...")
    verify_database()
    insert_data(engine, s.data)
    logger.info("Process completed")


if __name__ == "__main__":
    main()
