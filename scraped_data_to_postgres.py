'''
Creates a Postgres database and tables if they don't exist.
Loads the data from ~/data/metadata.json into the database.
'''
import sys
import json
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from logger import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=os.path.curdir + "/load.log",
    format="%(asctime)s %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)

DATABASE_NAME = "recordings"
DATABASE_URL = f"postgresql://ethan@localhost/{DATABASE_NAME}"


def create_database():
    """Create the database if it doesn't exist"""

    try:
        admin_url = "postgresql://ethan@localhost/postgres"
        engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            conn = conn.execution_options(autocommit=True)
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
                {"db_name": DATABASE_NAME}
            )
            exists = result.fetchone()
            if not exists:
                logger.info("Database not found, creating...")
                conn.execute(text(f"CREATE DATABASE {DATABASE_NAME}"))
                logger.info(f"Created database '{DATABASE_NAME}'")
            else:
                logger.info(f"Database '{DATABASE_NAME}' already exists")

        engine.dispose()

    except Exception as e:
        logger.error(f"Error creating database: {e}")
        sys.exit(1)


def create_tables(engine):
    """Create tables and set up the schema"""

    try:

        with engine.connect() as conn:
            docs_table = text("""
            CREATE TABLE IF NOT EXISTS documents (
                id SERIAL PRIMARY KEY,
                doc_num VARCHAR(50) UNIQUE NOT NULL,
                pin VARCHAR(50) NOT NULL,
                date_executed DATE,
                date_recorded DATE NOT NULL,
                num_pages INTEGER NOT NULL,
                address VARCHAR(255) NOT NULL,
                doc_type VARCHAR(50) NOT NULL,
                consideration_amount FLOAT,
                page_url VARCHAR(2048) NOT NULL,
                pdf_url VARCHAR(2048) NOT NULL,
                pdf_local_path VARCHAR(2048) NOT NULL
                )
                              """)
            conn.execute(docs_table)
            logger.info("Documents table created successfully")

            entities_table = text("""
                CREATE TABLE IF NOT EXISTS entities (
                    id SERIAL PRIMARY KEY,
                    doc_num VARCHAR(50) NOT NULL,
                    pin VARCHAR(50) NOT NULL,
                    entity_name VARCHAR(255) NOT NULL,
                    entity_status VARCHAR(7) NOT NULL,
                    trust_number VARCHAR(50),
                    CHECK (entity_status IN ('grantor', 'grantee')),
                    CONSTRAINT unique_doc_pin_entity UNIQUE (doc_num, pin, entity_name)
                );
                                  """)
            conn.execute(entities_table)
            logger.info("Entities table created successfully")

            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_pin ON documents(pin)",
                "CREATE INDEX IF NOT EXISTS idx_doc_num ON documents(doc_num)",
                "CREATE INDEX IF NOT EXISTS idx_pin ON entities(pin)",
                "CREATE INDEX IF NOT EXISTS idx_doc_num on entities(doc_num)",
                "CREATE INDEX IF NOT EXISTS idx_entity_name on entities(entity_name)",
            ]

            for index in indexes:
                conn.execute(text(index))
            logger.info("Indexes created successfully")

            conn.commit()

    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        sys.exit(1)


def read_data(path: str) -> dict:
    """Read scraped data into memory"""
    try:

        with open(path, 'r') as file:
            data = json.load(file)

        logger.info("Data loaded successfully")

        return data

    except FileNotFoundError:
        logger.error(f"Error: the file '{path}' was not found")
    except json.JSONDecodeError:
        logger.error(f"Error: Could not decode JSON from '{
                     path}'. Check if the file contains valid JSON.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


def insert_data(engine, data: dict):
    """Insert data into the tables"""

    def parse_date(date_str):
        """Parse date string in M/D/YYYY format to datetime object"""
        if not date_str or date_str.strip() == "":
            return None
        try:
            return datetime.strptime(date_str.strip(), "%m/%d/%Y").date()
        except ValueError:
            logger.warning(f"Could not parse date: {date_str}")
            return None

    def parse_consideration_amount(amount_str):
        """Parse consideration amount string to float"""
        if not amount_str or amount_str.strip() == "":
            return 0.0
        try:
            # Remove $ and whitespace, convert to float
            cleaned_amount = amount_str.replace(
                '$', '').replace(',', '').strip()
            return float(cleaned_amount)
        except ValueError:
            logger.warning(
                f"Could not parse consideration amount: {amount_str}")
            return 0.0

    with engine.connect() as conn:
        trans = conn.begin()
        try:

            for pin, pin_data in data.items():
                docs = pin_data.get('docs', {})

                for doc_num, doc_data, in docs.items():
                    date_executed = parse_date(doc_data.get('doc_executed'))
                    date_recorded = parse_date(doc_data.get('doc_recorded'))

                    if not date_recorded:
                        logger.warning(f"Skipping document {doc_num} \
                        - no value for date_recorded")

                    consideration_amount = parse_consideration_amount(
                        doc_data.get('doc_page_consideration_amount', "$0.00")
                    )

                    address = doc_data.get('doc_page_address')
                    doc_type = doc_data.get('doc_type')

                    # TODO: Does this need to be NOT NULL?
                    num_pages = int(doc_data.get('num_pages', '0'))

                    page_url = doc_data.get('url')
                    pdf_url = doc_data.get('pdf_url')
                    pdf_local_path = doc_data.get('pdf_path')

                    insert_doc_query = text("""
                    INSERT INTO documents (
                    doc_num, pin, date_executed, date_recorded,
                    num_pages, address, doc_type, consideration_amount,
                    page_url, pdf_url, pdf_local_path
                    ) VALUES (
                    :doc_num, :pin, :date_executed, :date_recorded,
                    :num_pages, :address, :doc_type, :consideration_amount,
                    :page_url, :pdf_url, :pdf_local_path
                    ) ON CONFLICT (doc_num) DO NOTHING
                                            """)

                    conn.execute(insert_doc_query, {
                                 'doc_num': doc_num,
                                 'pin': pin,
                                 'date_executed': date_executed,
                                 'date_recorded': date_recorded,
                                 'num_pages': num_pages,
                                 'address': address,
                                 'doc_type': doc_type,
                                 'consideration_amount': consideration_amount,
                                 'page_url': page_url,
                                 'pdf_url': pdf_url,
                                 'pdf_local_path': pdf_local_path
                                 })

                    entities_data = doc_data.get('entities', {})

                    for grantor in entities_data.get('grantors', []):
                        insert_entity_query = text("""
                        INSERT INTO entities (
                        doc_num, pin, entity_name, entity_status, trust_number
                        ) VALUES (
                        :doc_num, :pin, :entity_name, :entity_status, :trust_number
                        ) ON CONFLICT (doc_num, pin, entity_name) DO NOTHING
                                                   """)

                        conn.execute(insert_entity_query, {
                            'doc_num': doc_num,
                            'pin': pin,
                            'entity_name': grantor.get('name', ''),
                            'entity_status': 'grantor',
                            'trust_number': grantor.get('trust_number')
                        })

                    for grantee in entities_data.get('grantees', []):
                        insert_entity_query = text("""
                        INSERT INTO entities (
                        doc_num, pin, entity_name, entity_status, trust_number
                        ) VALUES (
                        :doc_num, :pin, :entity_name, :entity_status, :trust_number
                        ) ON CONFLICT (doc_num, pin, entity_name) DO NOTHING
                                                   """)

                        conn.execute(insert_entity_query, {
                            'doc_num': doc_num,
                            'pin': pin,
                            'entity_name': grantor.get('name', ''),
                            'entity_status': 'grantee',
                            'trust_number': grantor.get('trust_number')
                        })

            trans.commit()
            logger.info("Data insertion completed successfully")

        except Exception as e:
            trans.rollback()
            logger.error(f"Error inserting data: {str(e)}")
            raise


def main():
    data_path = os.getcwd() + '/data/metadata.json'
    engine = create_engine(f"{DATABASE_URL}")
    create_database()
    create_tables(engine)
    data = read_data(data_path)
    insert_data(engine, data)
    logger.info("Loading completed")


if __name__ == "__main__":
    main()
