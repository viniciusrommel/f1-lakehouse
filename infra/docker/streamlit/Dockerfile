FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    streamlit==1.38.0 \
    fastf1==3.4.0 \
    pandas==2.2.2 \
    pyarrow==17.0.0 \
    psycopg2-binary==2.9.9 \
    sqlalchemy==2.0.32 \
    pydantic==2.7.4 \
    pydantic-settings==2.3.4 \
    structlog==24.1.0

# Reuses the SAME ingestion package the CLI producer uses — the Streamlit app
# shells out to `python -m ingestion.producers.fastf1_to_postgres` as a
# subprocess, it does not reimplement ingestion logic.
COPY ingestion/ ./ingestion/
COPY streamlit_app/ ./streamlit_app/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1

EXPOSE 8501

# fileWatcherType=none: this dev setup bind-mounts streamlit_app/ live; the
# watchdog auto-rerun-on-file-change can overlap with an in-flight script run
# (two threads doing native pyarrow/psycopg2 C-extension work concurrently),
# which segfaulted here. Restart the container manually after code edits.
CMD ["streamlit", "run", "streamlit_app/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--server.fileWatcherType=none"]
