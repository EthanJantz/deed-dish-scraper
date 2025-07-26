'''
Loads the data from ~/data/metadata.json into the database.
'''
import sys
import json
import os
from datetime import datetime
from sqlalchemy import create_engine, text, inspect
from logger import logging
from db_config import get_db_config

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=os.path.curdir + "/load.log",
    format="%(asctime)s %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)

db_config = get_db_config()
database_url = "postgresql://{username}:{password}@{host}:{port}/{database}".format(
    username=db_config['user'],
    password=db_config['password'],
    host=db_config['host'],
    port=db_config['port'],
    database=db_config['database']
)
EXPECTED_TABLES = ['documents', 'entities']

def _verify_table(conn, table_name, schema_name='public'):
    """
    Used by verify_database to ensure that the EXPECTED_TABLES exist
    """
    try:
        inspector = inspect(conn)
        tables = inspector.get_table_names(schema=schema_name)

        if table_name not in tables:
            logger.error(f"Table {schema_name}.{table_name} not found in {DATABASE_NAME}")
            sys.exit(1)

        logger.info(f"Table {schema_name}.{table_name} at {DATABASE_NAME} verified successfully")

    except Exception as e:
        logger.error(f"Error validating table existence: {e}")
        sys.exit(1)   

def verify_database():
    """
    Ensures that the database and tables exist before beginning the loading process.
    """
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn = conn.execution_options(autocommit=True)
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
            {'db_name': DATABASE_NAME}
        )
        exists = result.fetchone()
        if not exists:
            logger.error(f"{DATABASE_NAME} at {DATABASE_URL} doesn't exist. Exiting")
            sys.exit(1)

        for table in EXPECTED_TABLES:
            _verify_table(conn, table)


def read_data(path: str) -> dict:
    """Read scraped data into memory"""
    try:
        with open(path, 'r') as file:
            data = json.load(file)
        logger.info("Data loaded into memory successfully")
        return data

    except FileNotFoundError:
        logger.error(f"Error: the file '{path}' was not found")
        return
    except json.JSONDecodeError:
        logger.error(f"Error: Could not decode JSON from '{
                     path}'. Check if the file contains valid JSON.")
        return
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return


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

def insert_data(engine, data):
    """
    Insert data into postgres database
    """
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
                        doc_data.get('consideration_amount')
                        )

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
                                    'pin': pin, 'date_executed': date_executed,
                                    'date_recorded': date_recorded,
                                    'num_pages': doc_data.get('num_pages'),
                                    'address': doc_data.get('address'),
                                    'doc_type': doc_data.get('doc_type'),
                                    'consideration_amount': consideration_amount,
                                    'page_url': doc_data.get('url'),
                                    'pdf_url': doc_data.get('pdf_url'),
                                    'pdf_local_path': doc_data.get('pdf_path')
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
                            'entity_name': grantee.get('name', ''),
                            'entity_status': 'grantee',
                            'trust_number': grantee.get('trust_number')
                        })

            trans.commit()
            logger.info("Data insertion completed successfully")

        except Exception as e:
            trans.rollback()
            logger.error(f"Error inserting data: {str(e)}")
            raise

if __name__ == "__main__":
    data_path = os.getcwd() + '/data/metadata.json'
    engine = create_engine(f"{DATABASE_URL}")
    verify_database()
    data = read_data(data_path)
    insert_data(engine, data)
    logger.info("Loading completed")
