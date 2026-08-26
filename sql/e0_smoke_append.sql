-- End-to-end smoke: Kafka -> Flink -> Iceberg. Append mode, exactly-once.
SET 'execution.checkpointing.interval' = '10s';
SET 'pipeline.name' = 'e0-smoke-append';

CREATE CATALOG ice WITH (
  'type'='iceberg',
  'catalog-type'='rest',
  'uri'='http://iceberg-rest:8181',
  'warehouse'='s3://warehouse/',
  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'='http://minio:9000',
  's3.path-style-access'='true'
);

CREATE DATABASE IF NOT EXISTS ice.roster;

CREATE TABLE IF NOT EXISTS ice.roster.providers_append (
  npi STRING,
  source STRING,
  first_name STRING,
  last_name STRING,
  credential STRING,
  city STRING,
  state STRING,
  zip STRING,
  specialty STRING,
  network_status STRING,
  revision INT,
  seq BIGINT
);

CREATE TEMPORARY TABLE kafka_roster (
  npi STRING,
  source STRING,
  first_name STRING,
  last_name STRING,
  credential STRING,
  city STRING,
  state STRING,
  zip STRING,
  specialty STRING,
  network_status STRING,
  revision INT,
  seq BIGINT
) WITH (
  'connector'='kafka',
  'topic'='roster.updates',
  'properties.bootstrap.servers'='kafka:9092',
  'properties.group.id'='rig-e0-smoke',
  'scan.startup.mode'='earliest-offset',
  'format'='json',
  'json.fail-on-missing-field'='false',
  'json.ignore-parse-errors'='false'
);

INSERT INTO ice.roster.providers_append SELECT * FROM kafka_roster;
