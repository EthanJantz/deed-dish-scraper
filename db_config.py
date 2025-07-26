"""
Config for pipeline
"""
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_config():
    """Get database configuration based on environment"""
    if os.path.exists('/.dockerenv'):
        db_host = 'postgres'
        #db_port = 5432
    else:
        db_host = 'localhost'
        #db_port = 5433

    db_port = 5432
    return {
        'host': db_host,
        'port': db_port,
        'user': os.getenv('DB_USER', 'myuser'),
        'password': os.getenv('DB_PASSWORD', 'mypassword'),
        'database': os.getenv('DB_NAME', 'myapp')
    }
