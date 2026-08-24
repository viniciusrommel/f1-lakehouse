# ── Stage 1: download the S3 Sink connector and ALL runtime dependencies ─────
# Uses Maven to resolve every transitive JAR from packages.confluent.io/maven
# (the Confluent Hub CDN is geo-blocked; this avoids it entirely).
FROM maven:3.9.6-eclipse-temurin-17 AS plugin-downloader

COPY infra/docker/debezium/plugin-pom.xml /tmp/pom.xml
RUN mvn -f /tmp/pom.xml dependency:copy-dependencies \
        -DoutputDirectory=/tmp/s3-plugin \
        -DincludeScope=runtime \
        --no-transfer-progress

# ── Stage 2: Debezium Connect + S3 Sink plugin ───────────────────────────────
FROM debezium/connect:2.7.3.Final

# Copy all resolved JARs into the Debezium plugin directory.
# Kafka Connect scans /kafka/connect/** for connector classes at startup.
COPY --from=plugin-downloader /tmp/s3-plugin/ /kafka/connect/confluentinc-kafka-connect-s3/
