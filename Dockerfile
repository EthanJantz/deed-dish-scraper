FROM python:3.12
WORKDIR /app

COPY README.md .
COPY models.py .
COPY database.py .
COPY utils.py .
COPY scraper.py .
COPY main.py .
COPY pyproject.toml .
COPY uv.lock .

RUN mkdir /app/data

RUN pip install uv
RUN uv sync

