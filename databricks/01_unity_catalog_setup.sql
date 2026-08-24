CREATE CATALOG IF NOT EXISTS f1;
USE CATALOG f1;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

LIST 's3://f1-lakehouse-galgo/cdc/';

-- bronze/silver/gold são tabelas MANAGED (storage gerenciado pelo Databricks);
-- só a landing zone (.avro do CDC) fica no S3 externo.
