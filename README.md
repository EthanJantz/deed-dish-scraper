# Cook County Recorder of Deeds Scraper

This repository contains a Python script (`scrape.py`) designed to scrape document metadata from the Cook County Recorder of Deeds public access website. It automates the process of querying Property Identification Numbers (PINs), extracting relevant document details, and storing them in a local SQLite database.

## Features

*   **PIN Cleaning**: Standardizes PIN formats for consistent queries.
*   **Dynamic URL Retrieval**: Collects all available document page URLs for a given PIN.
*   **Comprehensive Data Extraction**: Scrapes document information (date executed, date recorded, document type, etc.), grantor/grantee details, prior document references, and related PINs.
*   **PDF URL Discovery**: Extracts the direct URL to the associated PDF document.
*   **SQLite Database Storage**: Persists the extracted data into a structured `deeds.db` file.

## Requirements

*   Python 3.12 or higher
*   `uv`

## Installation

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/your-username/cook-county-deeds-scraper.git
    cd cc-deeds-scraper
    ```

2.  **Create a virtual environment** (recommended, using `uv`):
    ```bash
    uv venv
    source .venv/bin/activate  # On Windows, use `.venv\Scripts\activate`
    ```

3.  **Install dependencies** using `uv`:
    ```bash
    uv pip install -e .
    ```

## Usage

To run the scraper, execute the `scrape.py` script from your terminal:

```bash
python scrape.py
```

The script will perform the following actions:

1.  **Database Creation**: It will create an SQLite database named `deeds.db` in the `data/` directory (if it doesn't already exist) and set up the necessary tables.
2.  **PIN Input**:
    *   If a `data/pins.csv` file exists, it will read PINs from this file. Each PIN should be on a new line or space-separated.
    *   If `data/pins.csv` does not exist, it will use a few hardcoded example PINs.
3.  **Scraping and Storage**: For each PIN, it will retrieve document links, scrape the details from each document page, and insert the extracted data into the `deeds.db` database.
4.  **Logging**: Detailed logs of the scraping process, including errors, will be written to `scraper.log`.

## Database Schema

The `deeds.db` SQLite database consists of the following tables:

### `documents` table

Stores core document information.

| Column                | Type        | Description                                  |
| :-------------------- | :---------- | :------------------------------------------- |
| `doc_num`             | `VARCHAR(50)` | Primary Key: Unique document number.         |
| `pin`                 | `VARCHAR(14)` | Property Identification Number.              |
| `date_executed`       | `DATE`      | Date the document was executed.              |
| `date_recorded`       | `DATE`      | Date the document was recorded.              |
| `num_pages`           | `INTEGER`   | Number of pages in the document.             |
| `address`             | `VARCHAR(255)`| Property address.                            |
| `doc_type`            | `VARCHAR(50)` | Type of document (e.g., Warranty Deed).      |
| `consideration_amount`| `VARCHAR(50)` | Amount of consideration, if available.       |
| `pdf_url`             | `VARCHAR(2048)`| URL to the PDF version of the document.      |

### `entities` table

Stores grantor and grantee information associated with documents.

| Column          | Type          | Description                                  |
| :-------------- | :------------ | :------------------------------------------- |
| `id`            | `INTEGER`     | Primary Key, Auto-incrementing.              |
| `doc_num`       | `VARCHAR(50)` | Foreign Key: References `documents.doc_num`. |
| `pin`           | `VARCHAR(50)` | The PIN associated with the document.        |
| `entity_name`   | `VARCHAR(255)`| Name of the grantor or grantee.              |
| `entity_status` | `VARCHAR(7)`  | 'grantor' or 'grantee'.                      |
| `trust_number`  | `VARCHAR(50)` | Trust number, if applicable.                 |

### `pins` table

Stores related PINs found within a document's legal description.

| Column        | Type          | Description                                  |
| :------------ | :------------ | :------------------------------------------- |
| `id`          | `INTEGER`     | Primary Key, Auto-incrementing.              |
| `pin`         | `VARCHAR(14)` | The primary PIN being queried.               |
| `doc_num`     | `VARCHAR(50)` | Foreign Key: References `documents.doc_num`. |
| `related_pin` | `VARCHAR(14)` | A PIN found related to the document.         |

### `doc_relations` table

Stores relationships between documents (e.g., prior documents).

| Column          | Type          | Description                                  |
| :-------------- | :------------ | :------------------------------------------- |
| `id`            | `INTEGER`     | Primary Key, Auto-incrementing.              |
| `doc_num`       | `VARCHAR(50)` | Foreign Key: References `documents.doc_num`. |
| `prior_doc_num` | `VARCHAR(50)` | Foreign Key: References another `documents.doc_num` that is a prior document. |

