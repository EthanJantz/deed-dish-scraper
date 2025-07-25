"""
Ensures that the database and tables exist.
Creates them if they don't exist and exits. 
"""
import sys
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from logger import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=os.path.curdir + "/db_initialize.log",
    format="%(asctime)s %(message)s",
    encoding="utf-8",
    level=logging.INFO,
)

load_dotenv()

DATABASE_NAME = os.getenv("DB_NAME")
DATABASE_USER = os.getenv("DB_USER")
DATABASE_PASSWORD = os.getenv("DB_PASSWORD")

def set_host():
    """
    Sets hostname based on environment
    """
    if os.path.exists('/.dockerenv'):
        return 'postgres'  # Container name

    return 'localhost'  # Local development

DB_HOST = set_host()
DATABASE_URL = f"postgresql://{DATABASE_USER}:{DATABASE_PASSWORD}@{DB_HOST}/{DATABASE_NAME}"

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
                num_pages INTEGER,
                address VARCHAR(255),
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

if __name__ == "__main__":
    engine = create_engine(DATABASE_URL)
    create_database()
    create_tables(engine)
