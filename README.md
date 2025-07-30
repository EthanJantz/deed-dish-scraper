# Cook County Records Scraper

A Python tool designed to scrape document metadata from the Cook County Recorder of Deeds website and store it in a SQLite database.

## Features

*   **PIN Cleaning**: Standardizes Property Identification Numbers (PINs) by removing non-digit characters and validating their length.
*   **Dynamic URL Generation**: Constructs various URLs for a given PIN to maximize document discovery, sorting by different criteria (date recorded, document number, date executed, document type) in ascending and descending order.
*   **Document Metadata Extraction**: Scrapes detailed information from individual document pages, including:
    *   Document number, date executed, date recorded, number of pages, address, document type, consideration amount.
    *   Grantor and Grantee entities with their names and trust numbers.
    *   Related Property Identification Numbers (PINs).
    *   Prior document numbers.
    *   Direct URL to the PDF document.
*   **Data Storage**: Stores all extracted metadata into a structured SQLite database.
*   **Duplicate Handling**: Removes duplicate document URLs and ensures unique entries in the database tables.
*   **Progress Tracking**: Uses a `completed_pins.csv` file to track successfully scraped PINs, allowing for continuation of scraping tasks.
*   **Logging**: Provides detailed logging of scraping activities and errors to `scrape.log`.

## Installation

This project uses Docker for containerization, ensuring a consistent environment.

### Prerequisites

*   [Docker](https://www.docker.com/products/docker-desktop/) installed on your machine.

### Steps

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/EthanJantz/cc-deeds-scraper.git
    cd deeds-research
    ```

2.  **Build the Docker image:**
    ```bash
    docker-compose build
    ```
    This command will build the Docker image based on the `Dockerfile`, installing all necessary Python dependencies using `uv`.

## Usage

To run the scraper, you can use the `docker-compose` command. The scraper will attempt to read PINs from `data/pins.csv`. If this file does not exist, it will use a few hardcoded example PINs.

1.  **Prepare PINs (Optional):**
    You can create a `data/pins.csv` file in the root of the project (if it doesn't already exist) and list the Property Identification Numbers (PINs) you want to scrape, one PIN per line. For example:
    ```
    17-29-304-001-0000
    17-05-115-085-0000
    16-10-421-053-0000
    ```
    If `data/pins.csv` is not present, the script will use the default PINs defined within `scrape.py`.

2.  **Run the scraper:**
    ```bash
    docker-compose run scraper
    ```
    This command starts a new container based on the `scraper` service defined in `docker-compose.yml`, executes the `scrape.py` script, and then exits.

    *   Scraped data will be stored in `data/deeds.db`.
    *   Logs will be written to `scrape.log` inside the `data/` directory.
    *   Successfully processed PINs will be appended to `data/completed_pins.csv`.

## Project Structure

*   `scrape.py`: Contains the core Python logic for web scraping, data extraction, and database insertion.
*   `Dockerfile`: Defines the Docker image, including the Python version and dependencies.
*   `docker-compose.yml`: Orchestrates the Docker container for the scraper, mounting the `data` volume.
*   `pyproject.toml`: Specifies project metadata and Python dependencies.
*   `uv.lock`: A lock file managed by `uv` for reproducible dependency installations.
*   `data/`: A directory (created by the Dockerfile and mounted as a volume) where `pins.csv` can be placed, and `deeds.db` and `completed_pins.csv` will be saved.

## Dependencies

The Python dependencies are managed by `uv` and listed in `pyproject.toml`:

*   `bs4`
*   `lxml`
*   `requests`

## Database Schema

The `deeds.db` SQLite database created by the scraper contains the following tables:

### `documents`

| Column                | Type         | Description                                     |
| :-------------------- | :----------- | :---------------------------------------------- |
| `doc_num`             | `VARCHAR(50)`| Primary key, unique document number.            |
| `pin`                 | `VARCHAR(14)`| Property Identification Number.                 |
| `date_executed`       | `DATE`       | Date the document was executed.                 |
| `date_recorded`       | `DATE`       | Date the document was recorded.                 |
| `num_pages`           | `INTEGER`    | Number of pages in the document.                |
| `address`             | `VARCHAR(255)`| Associated property address.                    |
| `doc_type`            | `VARCHAR(50)`| Type of document (e.g., "Deed").                |
| `consideration_amount`| `VARCHAR(50)`| Financial consideration involved.               |
| `pdf_url`             | `VARCHAR(2048)`| URL to the PDF version of the document.         |

### `entities`

| Column         | Type         | Description                                       |
| :------------- | :----------- | :------------------------------------------------ |
| `id`           | `INTEGER`    | Primary key, auto-incrementing.                   |
| `doc_num`      | `VARCHAR(50)`| Foreign key referencing `documents.doc_num`.      |
| `pin`          | `VARCHAR(50)`| Associated Property Identification Number.        |
| `entity_name`  | `VARCHAR(255)`| Name of the grantor or grantee.                   |
| `entity_status`| `VARCHAR(7)` | Role of the entity (`'grantor'` or `'grantee'`). |
| `trust_number` | `VARCHAR(50)`| Trust number, if applicable.                      |

### `pins`

| Column      | Type         | Description                                      |
| :---------- | :----------- | :----------------------------------------------- |
| `id`        | `INTEGER`    | Primary key, auto-incrementing.                  |
| `pin`       | `VARCHAR(14)`| The main PIN being scraped.                      |
| `doc_num`   | `VARCHAR(50)`| Foreign key referencing `documents.doc_num`.     |
| `related_pin`| `VARCHAR(14)`| A PIN related to the document's legal description.|

### `doc_relations`

| Column         | Type         | Description                                       |
| :------------- | :----------- | :------------------------------------------------ |
| `id`           | `INTEGER`    | Primary key, auto-incrementing.                   |
| `doc_num`      | `VARCHAR(50)`| Foreign key referencing `documents.doc_num`.      |
| `prior_doc_num`| `VARCHAR(50)`| Foreign key referencing a prior `documents.doc_num`.|
