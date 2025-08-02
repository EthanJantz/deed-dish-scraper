# Deeds Research Scraper

A Python tool for scraping document metadata from the Cook County Recorder of Deeds website and storing it in a database.

## Setup

This project uses `uv` for dependency management.

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/EthanJantz/cc-deeds-scraper.git
    cd deeds-research
    ```
2.  **Install `uv`**:
    ```bash
    pip install uv
    ```
3.  **Install dependencies**:
    ```bash
    uv sync
    ```

## Configuration

1.  **Database URL**: Create a `.env` file in the root directory and set your PostgreSQL database URL:
    ```
    DB_URL="postgresql://user:password@host:port/database"
    ```
2.  **PINs to Scrape**:
    -   By default, the scraper uses a few hardcoded PINs.
    -   To provide your own list of PINs, create a `data/pins.csv` file, with each PIN on a new line (e.g., `17-29-304-001-0000`).
    -   A `data` directory will be created automatically if it doesn't exist.
    -   `data/completed_pins.csv` will track successfully scraped PINs to avoid re-scraping.

## Usage

Run the scraper:

```bash
python scraper/scrape.py
```
