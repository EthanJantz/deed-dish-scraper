FROM python:3.12
WORKDIR /app

COPY README.md .
COPY models.py .
COPY scrape.py .
COPY pyproject.toml .
COPY uv.lock .

RUN mkdir /app/data

RUN pip install uv
RUN uv sync

