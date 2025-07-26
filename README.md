# Cook County Deeds Scraper

This project is an ETL (Extract, Transform, Load) pipeline designed to scrape property deed information from the Cook County Clerk's website, download associated PDF documents, and load the structured data into a PostgreSQL database.

## Features

*   **Web Scraping**: Scrapes document metadata and associated PDFs for specified Property Identification Numbers (PINs).
*   **Data Extraction**: Extracts details like document numbers, dates, addresses, document types, consideration amounts, and entity (grantor/grantee) information.
*   **PDF Download**: Downloads the original PDF documents and stores them locally.
*   **Database Integration**: Loads the extracted data into a PostgreSQL database, with tables for documents and entities.
*   **Database Management**: Automates database and table creation if they don't exist.
*   **Containerized Environment**: Utilizes Docker and Docker Compose for easy setup and consistent execution.

## Prerequisites

Before you begin, ensure you have the following installed on your system:

*   [Docker](https://docs.docker.com/get-docker/)
*   [Docker Compose](https://docs.docker.com/compose/install/)

## Setup

1.  **Clone the repository:**

    ```bash
    git clone <repository-url>
    cd cook-county-deeds-scraper
    ```

2.  **Create a `.env` file:**
    Create a file named `.env` in the root directory of the project. This file will store your PostgreSQL database credentials.

    ```dotenv
    # .env
    DB_USER=myuser
    DB_PASSWORD=mypassword
    DB_NAME=myapp
    ```
    You can customize these values, but ensure they match your desired database configuration.

3.  **Prepare Input Data:**
    Place your `pins.csv` file inside the `data/` directory. This CSV should contain the PINs you wish to scrape, with one PIN per line. An example file is included.

4.  **Build and Run Docker Containers:**
    From the project root directory, run:

    ```bash
    docker-compose up --build -d
    ```
    This command will:
    *   Build the `scraper` service Docker image.
    *   Start the `postgres` database service.
    *   Start the `scraper` service.

    The PostgreSQL database will be accessible on `localhost:5433` from your host machine.

## Project Structure

*   `main.py`: The entry point for the ETL pipeline, orchestrating scraping, database initialization, and data loading.
*   `scraper.py`: Contains the `Scraper` class responsible for fetching data from the Cook County Clerk's website and downloading PDFs.
*   `initialize_postgres.py`: Handles the creation of the PostgreSQL database and necessary tables (`documents` and `entities`).
*   `load_scraped_data.py`: Manages reading scraped data (JSON) and inserting it into the PostgreSQL database.
*   `db_config.py`: Centralized configuration for database connection parameters.
*   `Dockerfile`: Defines the Docker image for the `scraper` service.
*   `docker-compose.yml`: Defines the multi-container Docker application (PostgreSQL and scraper).
*   `data/`: Directory for input CSVs (e.g., `pins.csv`) and output scraped data (PDFs, `metadata.json`).
*   `.env`: Environment variables for database configuration (not committed to VCS).
*   `logger.py`: (Assumed) A simple logging configuration module.
*   `pyproject.toml`, `uv.lock`: Dependency management files using `uv`.

## Usage

Once the Docker containers are up and running, you can execute the ETL pipeline within the `scraper` container.

1.  **Access the scraper container:**

    ```bash
    docker exec -it deeds-scraper bash
    ```

2.  **Run the main ETL process:**
    Inside the container, execute the `main.py` script:

    ```bash
    python main.py
    ```
    This will:
    *   Initialize the database and tables.
    *   Read PINs from `data/pins.csv`.
    *   Scrape data for each PIN.
    *   Download PDFs to `data/` subdirectories.
    *   Save scraped metadata to `data/metadata.json`.
    *   Load the scraped data into the PostgreSQL database.

3.  **Verify Data (Optional):**
    You can connect to your PostgreSQL database (e.g., via `psql` or a GUI client on `localhost:5433`) and query the `documents` and `entities` tables to verify data insertion.

To stop and remove the containers and volumes:
```bash
docker-compose down -v
