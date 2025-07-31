# Cook County Recorder of Deeds Scraper

This project provides a Python-based web scraper to collect public document metadata from the Cook County Recorder of Deeds website. The scraped data, including document details, associated entities (grantors/grantees), related Property Identification Numbers (PINs), and prior document relationships, is stored in a SQLite database.

## Features

*   **Document Scraping**: Extracts comprehensive metadata for legal documents (e.g., deeds, mortgages) from `crs.cookcountyclerkil.gov`.
*   **Data Models**: Defines SQLAlchemy ORM models (`Document`, `Entity`, `Pin`, `PriorDoc`) for structured storage.
*   **Database Storage**: Persists scraped data into a local SQLite database (`deeds.db`).
*   **PIN Management**: Allows scraping based on a predefined list of PINs or a `pins.csv` file, and tracks completed PINs to avoid re-scraping.
*   **Logging**: Detailed logging of scraping activities and errors to `scrape.log`.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine.

### Prerequisites

*   [Docker](https://docs.docker.com/get-docker/)
*   [Docker Compose](https://docs.docker.com/compose/install/)

### Installation

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/EthanJantz/cc-deeds-scraper.git
    cd cook-county-scraper
    ```

2.  **Build the Docker image:**

    ```bash
    docker-compose build
    ```

    This command will build the `scraper` service image based on the `Dockerfile`. It will install all necessary Python dependencies using `uv`.

### Usage

To run the scraper, execute the following command:

```bash
docker-compose up
```

This will start the `scraper` service, which runs the `scrape.py` script.

*   **Input PINs**:
    *   By default, the scraper will use a few hardcoded PINs for demonstration.
    *   To provide your own list of PINs, create a file named `pins.csv` in the `data/` directory (e.g., `./data/pins.csv`). Each PIN should be on a new line. The scraper will read from this file if it exists.
        ```
        17-29-304-001-0000
        17-05-115-085-0000
        ```
*   **Output Data**:
    *   The scraped data will be stored in a SQLite database located at `./data/deeds.db`.
    *   A log file, `./scrape.log`, will record the scraping process and any errors.
    *   A file `./data/completed_pins.csv` will be created to track PINs that have been successfully scraped, preventing duplicate efforts on subsequent runs.

## Project Structure

*   `models.py`: Defines the SQLAlchemy ORM models for `Document`, `Entity`, `Pin`, and `PriorDoc`. These correspond to the database tables.
*   `scrape.py`: Contains the core scraping logic, including functions to retrieve document URLs, parse HTML, extract metadata, and insert data into the SQLite database.
*   `Dockerfile`: Specifies how to build the Docker image for the scraping application, including dependencies and environment setup.
*   `docker-compose.yml`: Defines the Docker services (in this case, just the `scraper`) and how they interact, including volume mounts for data persistence.
*   `data/`: Directory mounted into the Docker container where the `deeds.db` database, `scrape.log`, `pins.csv`, and `completed_pins.csv` are stored.

## Data Storage

The scraped data is stored in `data/deeds.db`, an SQLite database. The schema is defined in `models.py` and includes the following tables:

*   `documents`: Stores primary document information (document number, PIN, dates, pages, address, type, consideration, PDF URL).
*   `entities`: Stores grantor and grantee information associated with documents (name, status, trust number).
*   `pins`: Stores relationships between the primary PIN of a document and any other related PINs found on the document page.
*   `prior_docs`: Stores relationships between a document and any prior documents referenced.

## Logging

All scraping activities, including successful operations and errors, are logged to `scrape.log` in the `data/` directory. This helps in monitoring the scraping process and debugging issues.
