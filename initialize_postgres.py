"""
Ensures that the database and tables exist.
Creates them if they don't exist and exits. 
"""
import sys
import os
from sqlalchemy import create_engine, text
from logger import logging
from db_config import get_db_config

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename=os.path.curdir + "/db_initialize.log",
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

def create_database():
    """Create the database if it doesn't exist"""

    try:
        admin_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}/postgres"
        engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")

        with engine.connect() as conn:
            conn = conn.execution_options(autocommit=True)
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
                {"db_name": db_config['database']}
            )
            exists = result.fetchone()
            if not exists:
                logger.info("Database not found, creating...")
                conn.execute(text(f"CREATE DATABASE {db_config['database']}"))
                logger.info(f"Created database '{db_config['database']}'")
            else:
                logger.info(f"Database '{db_config['database']}' already exists")

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
    engine = create_engine(database_url)
    create_database()
    create_tables(engine)
