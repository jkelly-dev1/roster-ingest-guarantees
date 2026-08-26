# Sample run

Captured output, not retyped. Every command below was executed exactly as it
appears here and in the README, on 2026-08-21, on one Linux machine with Docker
Compose: Flink 1.20.5, Iceberg 1.10.0, Kafka 4.3.1, Trino 478, MinIO, all bound
to loopback. No cloud account and no paid API was involved in any of it.

What was altered: nothing. Two things need saying anyway, because both would
otherwise look like editing.

- The ANSI color codes the Flink SQL client writes around its own `[INFO]`
  lines are removed by `scripts/flink_sql.sh` and by the experiment harness AT
  THE SOURCE, not stripped out of this file afterward. The same is true of the
  jline terminal warning Trino prints to stderr on every invocation, which is
  filtered by pattern rather than by discarding stderr.
- The five experiments were run as five separate invocations, in the order
  shown. Each one creates and drops its own Kafka topics and Iceberg tables, so
  they do not depend on each other and can be run in any order.

The stack was torn down with `docker compose down -v` before the bring-up
below, so this is a capture of a cold start and not of a warm one.

## The offline suite

Nothing here needs Docker. It reads the shipped `results/*.json` and the pure
measurement functions.

```
$ ./envs/bin/pytest -q
........................................................................ [ 73%]
..........................                                               [100%]
102 passed in 0.20s

$ python3 scripts/check_readme_numbers.py

39 of 39 derived figures found verbatim in README.md (27 table rows, 12 in prose)
```

## Bringing the stack up

```
$ cd stack && docker compose up -d
 Network roster-ingest-guarantees_default Creating 
 Network roster-ingest-guarantees_default Creating 
 Volume roster-ingest-guarantees_flink-state Creating 
 Volume roster-ingest-guarantees_flink-state Creating 
 Volume roster-ingest-guarantees_kafka-data Creating 
 Volume roster-ingest-guarantees_kafka-data Creating 
 Volume roster-ingest-guarantees_minio-data Creating 
 Volume roster-ingest-guarantees_minio-data Creating 
 Volume roster-ingest-guarantees_flink-state Created 
 Volume roster-ingest-guarantees_flink-state Created 
 Volume roster-ingest-guarantees_kafka-data Created 
 Volume roster-ingest-guarantees_kafka-data Created 
 Volume roster-ingest-guarantees_minio-data Created 
 Volume roster-ingest-guarantees_minio-data Created 
 Network roster-ingest-guarantees_default Created 
 Network roster-ingest-guarantees_default Created 
 Container rig-minio Creating 
 Container rig-kafka Creating 
 Container rig-minio Created 
 Container rig-minio-init Creating 
 Container rig-kafka Created 
 Container rig-minio-init Created 
 Container rig-rest Creating 
 Container rig-rest Created 
 Container rig-trino Creating 
 Container rig-flink-jm Creating 
 Container rig-trino Created 
 Container rig-flink-jm Created 
 Container rig-flink-tm Creating 
 Container rig-flink-tm Created 
 Container rig-kafka Starting 
 Container rig-minio Starting 
 Container rig-kafka Started 
 Container rig-minio Started 
 Container rig-minio Waiting 
 Container rig-minio Healthy 
 Container rig-minio-init Starting 
 Container rig-minio-init Started 
 Container rig-minio-init Waiting 
 Container rig-minio-init Exited 
 Container rig-rest Starting 
 Container rig-rest Started 
 Container rig-kafka Waiting 
 Container rig-trino Starting 
 Container rig-trino Started 
 Container rig-kafka Healthy 
 Container rig-flink-jm Starting 
 Container rig-flink-jm Started 
 Container rig-flink-tm Starting 
 Container rig-flink-tm Started 

$ docker exec rig-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic roster.updates \
    --partitions 4 --replication-factor 1
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic roster.updates.
```

## The end-to-end smoke test

One thousand keyed roster events into Kafka, read by Flink, landed in Iceberg,
and read back through Trino. Reading it with a second engine is the check that
matters: a commit only Flink can parse is not an Iceberg commit.

```
$ ./scripts/produce.sh 1000
Warning: --property is deprecated and will be removed in a future version. Use --reader-property instead.
produced 1000 events

$ ./scripts/flink_sql.sh sql/e0_smoke_append.sql
WARNING: Unknown module: jdk.compiler specified to --add-exports
WARNING: Unknown module: jdk.compiler specified to --add-exports
WARNING: Unknown module: jdk.compiler specified to --add-exports
WARNING: Unknown module: jdk.compiler specified to --add-exports
WARNING: Unknown module: jdk.compiler specified to --add-exports
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/flink/lib/iceberg-aws-bundle-1.10.0.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/flink/lib/log4j-slf4j-impl-2.24.3.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
WARNING: sun.reflect.Reflection.getCallerClass is not supported. This will impact performance.
Aug 22, 2026 2:08:21 AM org.jline.utils.Log logr
WARNING: Unable to create a system terminal, creating a dumb terminal (enable debug logging for more information)
[INFO] Executing SQL from file.
> CREATE CATALOG ice WITH (
>   'type'='iceberg',
>   'catalog-type'='rest',
>   'uri'='http://iceberg-rest:8181',
>   'warehouse'='s3://warehouse/',
>   'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
>   's3.endpoint'='http://minio:9000',
>   's3.path-style-access'='true'
> )[INFO] Execute statement succeeded.
> CREATE TABLE IF NOT EXISTS ice.roster.providers_append (
>   npi STRING,
>   source STRING,
>   first_name STRING,
>   last_name STRING,
>   credential STRING,
>   city STRING,
>   state STRING,
>   zip STRING,
>   specialty STRING,
>   network_status STRING,
>   revision INT,
>   seq BIGINT
> )[INFO] Execute statement succeeded.
> CREATE TEMPORARY TABLE kafka_roster (
>   npi STRING,
>   source STRING,
>   first_name STRING,
>   last_name STRING,
>   credential STRING,
>   city STRING,
>   state STRING,
>   zip STRING,
>   specialty STRING,
>   network_status STRING,
>   revision INT,
>   seq BIGINT
> ) WITH (
>   'connector'='kafka',
>   'topic'='roster.updates',
>   'properties.bootstrap.servers'='kafka:9092',
>   'properties.group.id'='rig-e0-smoke',
>   'scan.startup.mode'='earliest-offset',
>   'format'='json',
>   'json.fail-on-missing-field'='false',
>   'json.ignore-parse-errors'='false'
> )[INFO] Execute statement succeeded.
[INFO] SQL update statement has been successfully submitted to the cluster:
Job ID: 73688d4ecc57e7eec132df82b2fcc4f4
Shutting down the session...
done.

$ ./scripts/q.sh "SELECT count(*) FROM iceberg.roster.providers_append"
1000

$ ./scripts/q.sh "SELECT count(DISTINCT npi), min(seq), max(seq) FROM iceberg.roster.providers_append"
774,1,1000
```

## Experiment 1: what a restart actually replays

Four configurations, one `docker kill` of the TaskManager in each while the
feed was still running, then the offsets rewound to the start of the log
behind the job's back.

```
$ PYTHONPATH=scripts ./envs/bin/python scripts/exp1_restart_replay.py
Part A: four configurations, one kill each
  --- at_least_once_append: AT_LEAST_ONCE, append
  submitted 8d6469ab6d8934cf8170f89a892ae44c
  before the kill: 6000 rows, watermark 2
  restored from checkpoint 2, job is RESTARTING
  settled at 12000 rows after 20.2s
  rows=12000 duplicate_seq_rows=0 duplicate_npi_rows=10002 prediction_held=False
  --- at_least_once_upsert: AT_LEAST_ONCE, upsert on npi
  submitted c1a22ad749d413f6f9e904c063f3ff41
  before the kill: 1832 rows, watermark 2
  restored from checkpoint 2, job is RUNNING
  settled at 1998 rows after 20.1s
  rows=1998 duplicate_seq_rows=0 duplicate_npi_rows=0 prediction_held=True
  --- exactly_once_append: EXACTLY_ONCE, append
  submitted 3a9c7d23712a75c2e4d3bf7a1a25c123
  before the kill: 6500 rows, watermark 2
  restored from checkpoint 2, job is RESTARTING
  settled at 12000 rows after 26.3s
  rows=12000 duplicate_seq_rows=0 duplicate_npi_rows=10002 prediction_held=True
  --- exactly_once_upsert: EXACTLY_ONCE, upsert on npi
  submitted 95d11c3a2140d9cc9fbae7fb2c25d9ba
  before the kill: 1787 rows, watermark 2
  restored from checkpoint 2, job is RUNNING
  settled at 1998 rows after 26.3s
  rows=1998 duplicate_seq_rows=0 duplicate_npi_rows=0 prediction_held=True
Part B: what the committed offsets are worth
  --- part B: are Kafka's committed offsets load bearing?
  first job landed 6000 rows, offsets {0: 1552, 1: 1500, 2: 1490, 3: 1458}
  rewound the committed offsets to {0: 0, 1: 0, 2: 0, 3: 0}
  after restoring from the checkpoint: 6000 rows, 0 duplicates
  after a job that trusted them: 12000 rows, 6000 duplicates
wrote results/exp1_restart_replay.json
```

## Experiment 2: what an old savepoint really does

```
$ PYTHONPATH=scripts ./envs/bin/python scripts/exp2_savepoint_loss.py
  01_first_batch_landed: 4000 rows, watermark 1, 1 snapshots, lag 0
  savepoint 5 written to file:/flink-state/savepoints/savepoint-bb32e9-791c490d3735
  02_savepoint_taken: 4000 rows, watermark 1, 1 snapshots, lag 0
  03_table_committed_past_the_savepoint: 16000 rows, watermark 19, 13 snapshots, lag 0
  04_job_stopped: 16000 rows, watermark 19, 13 snapshots, lag 0
  05_third_batch_produced_while_down: 16000 rows, watermark 19, 13 snapshots, lag 4000
  restored from checkpoint 5 (savepoint=True); the restored job's first completed checkpoint is 6 and the table's watermark is 19
  06_restored_job_cleared_the_watermark: 32000 rows, watermark 20, 14 snapshots, lag 0
  07_fourth_batch_landed: 36000 rows, watermark 22, 15 snapshots, lag 0
  lost 0, duplicated 12000, frozen for 58.1s at [16000] rows with minimum lag 0; prediction_held=False
wrote results/exp2_savepoint_loss.json
```

The line that matters is the last one: nothing lost, twelve thousand rows
duplicated, and the table frozen for a minute at zero consumer lag while the
job reported RUNNING.

## Experiment 3: the commit interval is the file-size knob

Five runs over the same sustained feed of sixty thousand events. The last two
are the control and the noise floor: the same interval with the target file
size raised to 1 GB, and the same configuration run again unchanged.

```
$ PYTHONPATH=scripts ./envs/bin/python scripts/exp3_commit_interval.py
  --- interval_5s: interval 5s, target file size Iceberg default
  39 commits, 78 data files, avg 13259 bytes, freshness 1.3s
  --- interval_30s: interval 30s, target file size Iceberg default
  8 commits, 16 data files, avg 34545 bytes, freshness 25.9s
  --- interval_120s: interval 120s, target file size Iceberg default
  3 commits, 6 data files, avg 74556 bytes, freshness 90.7s
  --- interval_5s_target_1gb: interval 5s, target file size 1073741824
  40 commits, 80 data files, avg 13041 bytes, freshness 1.4s
  --- interval_5s_repeat: interval 5s, target file size Iceberg default
  39 commits, 78 data files, avg 13249 bytes, freshness 1.2s
wrote results/exp3_commit_interval.json
```

## Experiment 4: maintenance against a live writer

```
$ PYTHONPATH=scripts ./envs/bin/python scripts/exp4_maintenance_live_writer.py
  --- part A: expire_snapshots against a live writer
  before: 5 snapshots, 6000 rows, watermark 5
  after expire_snapshots: 1 snapshots, job is RUNNING
  after writing on: 10000 rows, job is RUNNING
  restarted; job is RUNNING
  landed 12000 of 12000, 0 duplicates, survived=True
  --- part B: remove_orphan_files against a live writer
  one commit landed: 4000 rows
  remove_orphan_files ran with 4000 rows visible, 4000 in flight, and 0 unreferenced data objects on disk
  landed 8000 of 8000, lost 0, survived=True
wrote results/exp4_maintenance_live_writer.json
```

## Experiment 5: transaction timeout against checkpoint interval

```
$ PYTHONPATH=scripts ./envs/bin/python scripts/exp5_transaction_timeout.py
  --- timeout_below_interval: transaction.timeout.ms=5000 against a 120s interval
  job ended RUNNING; read_committed=0, read_uncommitted=8000, of 8000 in; transaction states seen ['CompleteAbort', 'Empty', 'Ongoing']
  --- timeout_above_interval: transaction.timeout.ms=300000 against a 120s interval
  job ended RUNNING; read_committed=8000, read_uncommitted=8000, of 8000 in; transaction states seen ['CompleteCommit', 'Empty', 'Ongoing']
wrote results/exp5_transaction_timeout.json
```

`read_committed=0` against `read_uncommitted=8000` is the whole result: every
record reached the topic and none of them is readable, while the job reports
RUNNING with no failure recorded.
