"""The Flink SQL every experiment submits, built in one place.

Why this is a template and not a directory of .sql FILES. The four
configurations in experiment 1 and the three intervals in experiment 3 differ
by two or three settings each. Written out as files they would be eleven
near-identical documents, and the interesting differences, the checkpointing
mode, the upsert flag, the interval, would be the hardest thing in the
repository to see. Here they are arguments.

sql/e0_smoke_append.sql is the one exception and it stays: it is the
hand-runnable proof that the stack works, and it has to be readable without
running Python at all.
"""

COLUMNS = """  npi STRING,
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
  seq BIGINT"""

# The catalog is created fresh in every session because a SQL Client session
# starts empty. It is the same catalog Trino reads, which is what makes a
# Trino count evidence rather than a second opinion.
CATALOG = """CREATE CATALOG ice WITH (
  'type'='iceberg',
  'catalog-type'='rest',
  'uri'='http://iceberg-rest:8181',
  'warehouse'='s3://warehouse/',
  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'='http://minio:9000',
  's3.path-style-access'='true'
);

CREATE DATABASE IF NOT EXISTS ice.roster;
"""


def target(name, upsert=False, target_file_size=None):
    """The Iceberg table the job writes into.

    Upsert is a property of the table, not of the insert. The primary key
    declaration is what gives the sink an equality field to write deletes
    against; write.upsert.enabled is what makes it do so. Setting one without
    the other produces an append with extra ceremony.
    """
    pk = ",\n  PRIMARY KEY (npi) NOT ENFORCED" if upsert else ""
    props = ["'format-version'='2'"]
    if upsert:
        props.append("'write.upsert.enabled'='true'")
    if target_file_size is not None:
        props.append(f"'write.target-file-size-bytes'='{target_file_size}'")
    return (f"CREATE TABLE IF NOT EXISTS ice.roster.{name} (\n{COLUMNS}{pk}\n) "
            f"WITH (\n  " + ",\n  ".join(props) + "\n);\n")


def source(topic, group, startup="earliest-offset"):
    """The Kafka source, always TEMPORARY.

    A temporary table dies with the session, which is what we want: the source
    is a view onto a topic and belongs to the job, while the Iceberg table
    outlives every job that ever wrote to it.
    """
    return (f"CREATE TEMPORARY TABLE kafka_roster (\n{COLUMNS}\n) WITH (\n"
            f"  'connector'='kafka',\n"
            f"  'topic'='{topic}',\n"
            f"  'properties.bootstrap.servers'='kafka:9092',\n"
            f"  'properties.group.id'='{group}',\n"
            f"  'scan.startup.mode'='{startup}',\n"
            f"  'format'='json',\n"
            f"  'json.fail-on-missing-field'='false',\n"
            f"  'json.ignore-parse-errors'='false'\n);\n")


def settings(name, interval="10s", mode="EXACTLY_ONCE", parallelism=2,
             savepoint_path=None, extra=None):
    """SET statements, in the order the SQL Client applies them.

    Restart attempts are raised per job and that is deliberate. Three of these
    experiments kill the TaskManager on purpose. The cluster default gives up
    after ten attempts, and a job that gives up produces a FAILED status,
    which is the one outcome none of these experiments is about. Every
    interesting result here happens while the job is still RUNNING.
    """
    lines = [
        f"SET 'pipeline.name' = '{name}';",
        f"SET 'parallelism.default' = '{parallelism}';",
        f"SET 'execution.checkpointing.interval' = '{interval}';",
        f"SET 'execution.checkpointing.mode' = '{mode}';",
        "SET 'execution.checkpointing.externalized-checkpoint-retention' = "
        "'RETAIN_ON_CANCELLATION';",
        "SET 'restart-strategy.type' = 'fixed-delay';",
        "SET 'restart-strategy.fixed-delay.attempts' = '2147483647';",
        "SET 'restart-strategy.fixed-delay.delay' = '5s';",
    ]
    if savepoint_path:
        # The restore point is a set statement, not a command line flag. In the
        # SQL Client there is no `-s` to pass; the job picks this up when it is
        # submitted, and a typo in the path is not an error; it starts fresh.
        lines.append(f"SET 'execution.savepoint.path' = '{savepoint_path}';")
        lines.append("SET 'execution.savepoint.ignore-unclaimed-state' = 'true';")
    for k, v in (extra or {}).items():
        lines.append(f"SET '{k}' = '{v}';")
    return "\n".join(lines) + "\n\n"


def ingest(name, topic, group, table_name, upsert=False, interval="10s",
           mode="EXACTLY_ONCE", parallelism=2, startup="earliest-offset",
           savepoint_path=None, target_file_size=None, extra=None):
    """A whole Kafka -> Iceberg job as one SQL script."""
    return "\n".join([
        settings(name, interval, mode, parallelism, savepoint_path, extra),
        CATALOG,
        target(table_name, upsert, target_file_size),
        source(topic, group, startup),
        f"INSERT INTO ice.roster.{table_name} SELECT * FROM kafka_roster;\n",
    ])


def kafka_to_kafka(name, in_topic, out_topic, group, interval="30s",
                   transaction_timeout_ms=60000, parallelism=1):
    """A transactional Kafka -> Kafka job, for experiment 5 only.

    The roster pipeline does not need this shape. It is built because break
    point 1, transaction timeout against checkpoint interval, only applies to
    a job that commits a Kafka transaction, and a claim about it cannot be
    tested on a pipeline that never opens one.

    The transactional id prefix must differ per job. Two jobs sharing one
    prefix fence each other off the broker, and the second one fails with a
    ProducerFencedException that reads like a broker problem.
    """
    sink = (f"CREATE TEMPORARY TABLE kafka_out (\n{COLUMNS}\n) WITH (\n"
            f"  'connector'='kafka',\n"
            f"  'topic'='{out_topic}',\n"
            f"  'properties.bootstrap.servers'='kafka:9092',\n"
            f"  'properties.transaction.timeout.ms'='{transaction_timeout_ms}',\n"
            f"  'sink.delivery-guarantee'='exactly-once',\n"
            f"  'sink.transactional-id-prefix'='{name}',\n"
            f"  'format'='json'\n);\n")
    return "\n".join([
        settings(name, interval, "EXACTLY_ONCE", parallelism),
        source(in_topic, group),
        sink,
        "INSERT INTO kafka_out SELECT * FROM kafka_roster;\n",
    ])
