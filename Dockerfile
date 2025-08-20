FROM python:3.12
WORKDIR /app

RUN mkdir /app/data

COPY data/pins.csv ./data
COPY models.py .
COPY database.py .
COPY utils.py .
COPY scraper.py .
COPY main.py .
COPY pyproject.toml .
COPY uv.lock .


RUN pip install uv
RUN uv sync

