FROM python:3.11-slim

WORKDIR /app

# This worker only writes to Postgres (ingestion.producers.fastf1_to_postgres) —
# Debezium captures the changes from the WAL, so no Kafka client library is
# needed here (no confluent-kafka/librdkafka build toolchain).
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    fastf1==3.4.0 \
    pandas==2.2.2 \
    psycopg2-binary==2.9.9 \
    pydantic==2.7.4 \
    pydantic-settings==2.3.4 \
    structlog==24.1.0 \
    python-dotenv==1.0.1

COPY ingestion/ ./ingestion/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

CMD ["sleep", "infinity"]
