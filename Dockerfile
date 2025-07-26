FROM python:3.12
WORKDIR /app

COPY .env .
RUN mkdir -p ./data
COPY data/vacant_bldg_pins.csv ./data/
COPY db_config.py .
COPY scraper.py .
COPY initialize_postgres.py .
COPY load_scraped_data.py .
COPY main.py .
COPY pyproject.toml .
COPY uv.lock .

RUN pip install uv
RUN uv sync

