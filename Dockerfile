FROM python:3.12
WORKDIR /app

COPY scrape.py .
COPY pyproject.toml .
COPY uv.lock .

RUN pip install uv
RUN uv sync

