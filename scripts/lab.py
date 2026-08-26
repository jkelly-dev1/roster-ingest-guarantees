"""Shared plumbing for the five experiments: Kafka, Flink, Iceberg and Trino.

Everything here is a measurement helper. The experiments live in
exp1_restart_replay.py, exp2_savepoint_loss.py, exp3_commit_interval.py,
exp4_maintenance_live_writer.py and exp5_transaction_timeout.py.

Three rules shaped this file, and each one cost a debugging session.

  The table is read with TRINO, not with FLINK. A row Flink believes it wrote
  is not evidence. A row a second engine can read out of the catalog is. Every
  count in every result file comes back through Trino.

  A check that cannot run must refuse, never pass quietly. The wait helpers
  raise on timeout instead of returning whatever they happened to see. An
  experiment that reads an empty table and reports a clean run is the exact
  failure this repository is about, and it is silent by construction.

  The job status is part of the measurement. Several of these experiments end
  with a job that is RUNNING, healthy, and wrong. Status is recorded next to
  the row count everywhere, so the two can be compared.
"""

import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.request

KAFKA = "rig-kafka"
FLINK_JM = "rig-flink-jm"
FLINK_TM = "rig-flink-tm"
TRINO = "rig-trino"

FLINK_REST = "http://127.0.0.1:8091"
CATALOG = "iceberg"
SCHEMA = "roster"

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

# The SQL client colors its own output. The escape sequences are not ASCII and
# they end up in captured evidence if they are not removed at the source.
_ANSI = re.compile(r"\x1b\[[0-9;]*m")

# The jline warning is written to stderr on every Trino invocation and says
# nothing about the query. Filtered BY PATTERN, never by discarding the
# stream: stderr also carries the query failures, and dropping it makes a
# broken statement indistinguishable from an empty result.
_NOISE = ("org.jline", "dumb terminal", "WARNING:")


class LabError(RuntimeError):
    pass


def sh(*args, **kw):
    return subprocess.run(args, capture_output=True, text=True, **kw)


# ---------------------------------------------------------------- Trino ----

def _clean(text):
    return "\n".join(
        line for line in _ANSI.sub("", text).splitlines()
        if line.strip() and not any(n in line for n in _NOISE)
    )


def trino(statement, fmt="JSON"):
    """Run one statement against Trino. Trino VERIFIES here, it never writes
    into a table a Flink job owns; except in experiment 4, where writing is
    the whole question."""
    proc = sh("docker", "exec", TRINO, "trino", "--output-format", fmt,
              "--execute", statement)
    out = _clean(proc.stdout + proc.stderr)
    if proc.returncode != 0:
        raise LabError(f"{statement.splitlines()[0][:90]}\n{out}")
    if fmt != "JSON":
        return out
    return [json.loads(ln) for ln in out.splitlines() if ln.startswith("{")]


def trino_session(*statements):
    """Run several statements in ONE Trino session.

    Each `trino --execute` is a fresh session, so a SET SESSION in its own
    call is discarded before the next statement runs. A maintenance procedure
    that needs its retention guard lowered has to travel in the same call as
    the guard.
    """
    return trino("; ".join(statements), fmt="CSV")


def one(statement):
    rows = trino(statement)
    return next(iter(rows[0].values())) if rows else None


def table(name):
    return f"{CATALOG}.{SCHEMA}.{name}"


def meta(name, kind):
    return f'{CATALOG}.{SCHEMA}."{name}${kind}"'


def table_exists(name):
    return bool(trino(f"SHOW TABLES FROM {CATALOG}.{SCHEMA} LIKE '{name}'"))


def drop_table(name):
    trino(f"DROP TABLE IF EXISTS {table(name)}")


def rows_in(name):
    return int(one(f"SELECT count(*) FROM {table(name)}") or 0)


def seqs_in(name):
    """Every seq value in the table, as a sorted list, WITH duplicates kept.

    The duplicate count is the measurement in experiment 1 and the missing
    range is the measurement in experiment 2, so neither may be collapsed
    away by a DISTINCT here.
    """
    rows = trino(f"SELECT seq FROM {table(name)} ORDER BY seq")
    return [int(r["seq"]) for r in rows]


def snapshots(name):
    """Every snapshot, newest last, with the summary map parsed.

    Flink.max-committed-checkpoint-id lives in this map. It is the watermark
    the Iceberg committer compares against, and experiment 2 is entirely about
    what happens when a restored job's checkpoint ids fall at or below it.
    """
    # the summary is cast to JSON on purpose. Trino's JSON output format
    # cannot serialize a map column at all (it fails with "No ObjectCodec
    # defined for the generator", which names Jackson and not the query), so
    # the cast happens in SQL and the string is parsed here.
    rows = trino(
        f"SELECT snapshot_id, operation, CAST(summary AS JSON) AS summary "
        f"FROM {meta(name, 'snapshots')} ORDER BY committed_at")
    return [{"snapshot_id": str(r["snapshot_id"]),
             "operation": r["operation"],
             "summary": json.loads(r["summary"])} for r in rows]


def committed_checkpoint_id(name):
    """The watermark on the newest snapshot, or None if the table has none."""
    snaps = snapshots(name)
    if not snaps:
        return None
    val = snaps[-1]["summary"].get("flink.max-committed-checkpoint-id")
    return int(val) if val is not None else None


DATA, POSITION_DELETES, EQUALITY_DELETES = 0, 1, 2


def files_in(name):
    """Data and delete files the current snapshot references.

    Content is not optional. $files lists delete files beside data files, and
    the upsert path in experiment 1 writes equality deletes. Counting rows of
    $files without reading `content` reports an upsert commit as a pile of
    data files it is not.
    """
    rows = trino(f"SELECT file_path, file_size_in_bytes, record_count, content "
                 f"FROM {meta(name, 'files')}")
    data = [r for r in rows if r["content"] == DATA]
    deletes = [r for r in rows if r["content"] != DATA]
    return {
        "data_files": len(data),
        "delete_files": len(deletes),
        "data_bytes": sum(r["file_size_in_bytes"] for r in data),
        "delete_bytes": sum(r["file_size_in_bytes"] for r in deletes),
        "avg_data_file_bytes": (sum(r["file_size_in_bytes"] for r in data)
                                // len(data)) if data else 0,
    }


MINIO = "rig-minio"


def objects_under(prefix="warehouse"):
    """Every object actually stored in MinIO under a prefix: key -> size.

    The alias has to be set first, and it is set from the container's own
    environment. Mc ships with a `local` alias pointing at localhost:9000 with
    no credentials, and listing through it returns Access Denied as a JSON
    line with type "error". An unauthenticated listing therefore yields an
    empty dict, which is indistinguishable from an empty bucket, so the error
    RAISES here rather than being skipped. Reading the credentials from
    $MINIO_ROOT_USER inside the container keeps compose.yaml the single place
    they are written down.
    """
    script = (f'mc alias set rig http://localhost:9000 '
              f'"$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && '
              f'mc ls --recursive --json rig/{prefix}')
    proc = sh("docker", "exec", MINIO, "sh", "-c", script)
    found, errors = {}, []
    for line in proc.stdout.splitlines():
        if not line.startswith("{"):
            continue
        rec = json.loads(line)
        if rec.get("status") == "error" or rec.get("type") == "error":
            errors.append(str(rec.get("error", rec))[:200])
        elif rec.get("type") == "file":
            found[rec["key"]] = rec["size"]
    if errors:
        raise LabError("mc could not list the bucket: " + errors[0])
    return found


def storage_vs_catalog(name):
    """What is on disk for a table, against what its snapshot references.

    The difference is the orphan surface, the thing experiment 4 part B is
    really asking about. A streaming writer is only exposed to
    remove_orphan_files while a finished Parquet OBJECT exists that no
    snapshot points at yet, and an object that is still an open multipart
    upload is not an object. Counting both sides is how that gets answered
    instead of guessed.
    """
    # the trailing slash is required. This catalog lays a table out under
    # <schema>/<table>/ with no uuid in the path, so a prefix without the
    # slash also matches every table whose name merely starts with this one.
    on_disk = {k: v for k, v in objects_under().items()
               if k.startswith(f"{SCHEMA}/{name}/")}
    data_objects = {k: v for k, v in on_disk.items() if "/data/" in k}
    referenced = trino(f"SELECT file_path FROM {meta(name, 'files')}")
    referenced_names = {r["file_path"].rsplit("/", 1)[-1] for r in referenced}
    unreferenced = {k: v for k, v in data_objects.items()
                    if k.rsplit("/", 1)[-1] not in referenced_names}
    # Refuse rather than report zero. If the catalog says the table has data
    # files and the bucket listing found none of them, the listing is what is
    # broken, and "no unreferenced objects" would then be a statement about
    # this function rather than about the table.
    if referenced_names and not data_objects:
        raise LabError(
            f"the catalog references {len(referenced_names)} data files for "
            f"{name} and the bucket listing found none; the listing is wrong")
    return {
        "objects_on_disk": len(on_disk),
        "data_objects_on_disk": len(data_objects),
        "data_files_referenced_by_the_snapshot": len(referenced_names),
        "data_objects_no_snapshot_references": len(unreferenced),
        "unreferenced_bytes": sum(unreferenced.values()),
    }


# ---------------------------------------------------------------- Kafka ----

def kafka(*args):
    proc = sh("docker", "exec", KAFKA, *args)
    if proc.returncode != 0:
        raise LabError(" ".join(args) + "\n" + proc.stdout + proc.stderr)
    return proc.stdout


def create_topic(topic, partitions=4):
    delete_topic(topic)
    kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
          "localhost:9092", "--create", "--topic", topic,
          "--partitions", str(partitions), "--replication-factor", "1")
    # Topic creation is asynchronous on the broker even though the CLI has
    # returned. Producing into a topic whose metadata has not propagated fails
    # with UNKNOWN_TOPIC_OR_PARTITION, which reads like a typo in the name.
    for _ in range(30):
        if topic in kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                          "localhost:9092", "--list"):
            return
        time.sleep(0.5)
    raise LabError(f"topic {topic} never appeared")


def delete_topic(topic):
    proc = sh("docker", "exec", KAFKA, "/opt/kafka/bin/kafka-topics.sh",
              "--bootstrap-server", "localhost:9092", "--delete",
              "--topic", topic)
    if proc.returncode == 0:
        time.sleep(1)


def end_offsets(topic):
    """partition -> next offset the broker will assign. The end of the log."""
    out = kafka("/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server",
                "localhost:9092", "--topic", topic, "--time", "-1")
    found = {}
    for line in out.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[2].lstrip("-").isdigit():
            found[int(parts[1])] = int(parts[2])
    return found


def group_offsets(group):
    """partition -> COMMITTED offset for a consumer group, or {} if none.

    This number is the subject of experiment 1 part C. Flink commits it on
    every completed checkpoint and then never reads it back on recovery: the
    offsets that matter live in the checkpoint. The two are easy to confuse
    because a monitoring dashboard has nothing else to show.
    """
    proc = sh("docker", "exec", KAFKA,
              "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server",
              "localhost:9092", "--describe", "--group", group)
    found = {}
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 5 and parts[0] == group and parts[3].isdigit():
            found[int(parts[2])] = int(parts[3])
    return found


def reset_group_offsets(group, topic, to="earliest"):
    """Rewind a consumer group's committed offsets behind Flink's back."""
    proc = sh("docker", "exec", KAFKA,
              "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server",
              "localhost:9092", "--group", group, "--topic", topic,
              "--reset-offsets", f"--to-{to}", "--execute")
    if proc.returncode != 0:
        raise LabError("offset reset refused:\n" + proc.stdout + proc.stderr)
    return group_offsets(group)


def consume_count(topic, isolation="read_committed", timeout_ms=20000):
    """How many records a consumer can actually read out of a topic.

    Isolation level is the measurement in experiment 5. Read_uncommitted
    counts everything the producer wrote, including records inside a
    transaction that was never committed; read_committed counts only what a
    real downstream consumer would ever be allowed to see. The gap between
    the two is the loss.
    """
    proc = sh("docker", "exec", KAFKA,
              "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server",
              "localhost:9092", "--topic", topic, "--from-beginning",
              "--isolation-level", isolation, "--timeout-ms", str(timeout_ms))
    # The consumer exits non-zero on its own idle timeout, which is how it is
    # asked to stop, so the exit code says nothing. The records are on stdout.
    return sum(1 for ln in proc.stdout.splitlines() if ln.strip())


def transaction_states(prefix):
    """Every Kafka transaction whose id starts with prefix: id -> state.

    The coordinator is the only honest witness for experiment 5. Whether a
    producer's transaction.timeout.ms reached the broker at all, and how long
    a transaction actually sits in Ongoing, are both facts the broker holds
    and the job does not. Asking the job would only report what it intended.
    """
    listing = sh("docker", "exec", KAFKA, "/opt/kafka/bin/kafka-transactions.sh",
                 "--bootstrap-server", "localhost:9092", "list")
    ids = [ln.split()[0] for ln in listing.stdout.splitlines()[1:]
           if ln.strip() and ln.split()[0].startswith(prefix)]
    found = {}
    for tid in ids:
        desc = sh("docker", "exec", KAFKA,
                  "/opt/kafka/bin/kafka-transactions.sh", "--bootstrap-server",
                  "localhost:9092", "describe", "--transactional-id", tid)
        rows = [ln.split() for ln in desc.stdout.splitlines()[1:] if ln.strip()]
        if rows and len(rows[0]) >= 6:
            found[tid] = {"state": rows[0][4], "timeout_ms": int(rows[0][5])}
    return found


def produce(count, topic, start=1, providers=2000, corrections_from=None,
            chunk=None, pause=0.0):
    """Generate roster events and write them to Kafka. Returns the line count.

    The events go in as a file, not down a pipe. Feeding the generator into
    `docker exec -I kafka-console-producer` hangs: the producer never sees the
    EOF and the call does not return even though every record was written.
    Copying the file in and redirecting inside the container terminates.

    chunk and pause exist for experiment 3, which needs a SUSTAINED feed
    rather than a backlog. A whole backlog is drained inside a single
    checkpoint no matter how long the interval is, so a batch producer cannot
    show a commit interval doing anything at all.
    """
    args = ["python3", os.path.join(HERE, "generate_roster.py"), str(count),
            "--start", str(start), "--providers", str(providers)]
    if corrections_from is not None:
        args += ["--corrections-from", str(corrections_from)]
    gen = sh(*args)
    if gen.returncode != 0:
        raise LabError("generator failed:\n" + gen.stderr)

    tmp = f"/tmp/rig_events_{topic}.jsonl"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(gen.stdout)
    sh("docker", "cp", tmp, f"{KAFKA}:/tmp/events.jsonl")
    os.unlink(tmp)

    send = (f"/opt/kafka/bin/kafka-console-producer.sh "
            f"--bootstrap-server localhost:9092 --topic {topic} "
            f"--property parse.key=true --property key.separator='|'")
    if chunk:
        script = (f"cd /tmp && rm -f chunk_* && split -l {chunk} events.jsonl chunk_ && "
                  f"for f in chunk_*; do {send} < $f; sleep {pause}; done; "
                  f"rm -f chunk_* events.jsonl")
    else:
        script = f"{send} < /tmp/events.jsonl; rm -f /tmp/events.jsonl"

    before = sum(end_offsets(topic).values())
    proc = sh("docker", "exec", KAFKA, "bash", "-c", script)
    if proc.returncode != 0:
        raise LabError("produce failed:\n" + proc.stdout + proc.stderr)

    # The exit code is not the check: kafka-console-producer writes its
    # complaints to stderr and still exits 0, so the only honest proof that
    # the records reached the log is that the log got longer by exactly as
    # many. A restart experiment fed by a producer that quietly wrote nothing
    # reads an empty topic and reports a clean run.
    landed = sum(end_offsets(topic).values()) - before
    if landed != count:
        raise LabError(f"produced {landed} records into {topic}, expected {count}")
    return count


# ---------------------------------------------------------------- Flink ----

def rest(path, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{FLINK_REST}{path}", data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode()
    except urllib.error.HTTPError as exc:
        raise LabError(f"{method} {path}: {exc.code} {exc.read().decode()[:300]}")
    return json.loads(text) if text.strip() else {}


def submit(sql_text, label):
    """Run a SQL file in the SQL client and return the job id it submitted.

    The whole file runs in one session. Catalogs, temporary tables and SET
    statements do not survive between invocations, so a statement that depends
    on a catalog has to travel in the same file that creates it.
    """
    path = f"/tmp/rig_{label}.sql"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(sql_text)
    sh("docker", "cp", path, f"{FLINK_JM}:/tmp/job.sql")
    os.unlink(path)
    proc = sh("docker", "exec", FLINK_JM, "/opt/flink/bin/sql-client.sh",
              "-f", "/tmp/job.sql")
    out = _ANSI.sub("", proc.stdout + proc.stderr)
    match = re.search(r"Job ID: ([0-9a-f]{32})", out)
    if not match:
        raise LabError(f"no job id from {label}:\n{out[-2000:]}")
    return match.group(1)


def job(job_id):
    return rest(f"/jobs/{job_id}")


def job_state(job_id):
    return job(job_id)["state"]


def checkpoint_counts(job_id):
    c = rest(f"/jobs/{job_id}/checkpoints")["counts"]
    return {"completed": c["completed"], "failed": c["failed"],
            "in_progress": c["in_progress"], "restored": c["restored"]}


def latest_checkpoint_id(job_id):
    latest = rest(f"/jobs/{job_id}/checkpoints")["latest"]["completed"]
    return latest["id"] if latest else None


def wait_for_checkpoints(job_id, n, timeout=300):
    """Wait until n checkpoints have COMPLETED. Refuses on timeout.

    Counting completed checkpoints is not the same as counting Iceberg
    commits: a checkpoint whose commit the committer skips still completes.
    Experiment 2 turns on exactly that difference.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = job_state(job_id)
        if st in ("FAILED", "CANCELED", "FINISHED"):
            raise LabError(f"job {job_id} went {st} while waiting for {n} checkpoints")
        if checkpoint_counts(job_id)["completed"] >= n:
            return checkpoint_counts(job_id)
        time.sleep(2)
    raise LabError(f"job {job_id} did not complete {n} checkpoints in {timeout}s")


def wait_for_rows(name, expected, timeout=300, poll=3):
    """Wait until Trino can see `expected` rows. Refuses on timeout.

    This is the freshness clock in experiment 3 and the barrier everywhere
    else. It reads through Trino on purpose: waiting on a Flink metric would
    only prove that Flink thinks it is finished.
    """
    start = time.time()
    deadline = start + timeout
    while time.time() < deadline:
        if table_exists(name) and rows_in(name) >= expected:
            return round(time.time() - start, 1)
        time.sleep(poll)
    got = rows_in(name) if table_exists(name) else "no table"
    raise LabError(f"{name} reached {got}, not {expected}, in {timeout}s")


def wait_for_restore(job_id, prior_restored, timeout=300):
    """Wait until the job has restored from a checkpoint one more time.

    The job state is not the signal. A TaskManager kill takes the TASKS down
    and the JOB stays RUNNING throughout in every observation here, so polling
    for a RESTARTING state finds nothing and returns instantly with the wrong
    answer. The restored counter under /checkpoints is the fact.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        counts = checkpoint_counts(job_id)
        if counts["restored"] > prior_restored:
            return counts
        if job_state(job_id) in ("FAILED", "CANCELED", "FINISHED"):
            raise LabError(f"job {job_id} went {job_state(job_id)} instead of restoring")
        time.sleep(2)
    raise LabError(f"job {job_id} never restored within {timeout}s")


def restore_point(job_id):
    """What the job last restored from, or None if it started clean."""
    latest = rest(f"/jobs/{job_id}/checkpoints")["latest"].get("restored")
    if not latest:
        return None
    return {"checkpoint_id": latest["id"],
            "is_savepoint": latest["is_savepoint"],
            "path": latest["external_path"]}


def failure_causes(job_id):
    """The exception history, trimmed to the first line of each entry.

    Evidence that the induced failure actually happened. A restart experiment
    whose kill missed looks exactly like a restart experiment that passed.
    """
    entries = rest(f"/jobs/{job_id}/exceptions")["exceptionHistory"]["entries"]
    return [e["exceptionName"] or e["stacktrace"].splitlines()[0]
            for e in entries][:5]


def wait_until_stable(name, quiet_polls=3, poll=5, timeout=420,
                      commit_interval_seconds=10):
    """Wait until the row count stops moving. Returns (rows, seconds waited).

    A streaming job has no "done". The table is settled when the same count
    comes back several polls running, and the count is read through Trino so
    that settling means settled IN THE CATALOG, not in a Flink buffer.

    The quiet window must be longer than the commit interval, and this refuses
    when it is not. A streaming table does not move BETWEEN commits, so a
    quiet window shorter than the interval settles on a table that is merely
    waiting for its next commit, and reports rows not yet committed as rows
    that never arrived.
    """
    if quiet_polls * poll <= commit_interval_seconds:
        raise LabError(
            f"a {quiet_polls * poll}s quiet window cannot settle a table that "
            f"commits every {commit_interval_seconds}s")
    start = time.time()
    last, same = None, 0
    while time.time() - start < timeout:
        now = rows_in(name) if table_exists(name) else 0
        same = same + 1 if now == last else 0
        last = now
        if same >= quiet_polls:
            return last, round(time.time() - start, 1)
        time.sleep(poll)
    raise LabError(f"{name} never settled: still moving at {last} rows")


def wait_for_state(job_id, states, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = job_state(job_id)
        if st in states:
            return st
        time.sleep(2)
    raise LabError(f"job {job_id} stayed {job_state(job_id)}, never {states}")


def cancel(job_id):
    if job_state(job_id) in ("CANCELED", "FAILED", "FINISHED"):
        return job_state(job_id)
    rest(f"/jobs/{job_id}", method="PATCH")
    return wait_for_state(job_id, ("CANCELED", "FAILED", "FINISHED"))


def savepoint(job_id, cancel_job=False, timeout=180):
    """Trigger a savepoint and return the path it was written to.

    The REST call is ASYNCHRONOUS: it returns a request id, and the path only
    exists once the request reports COMPLETED. Reading the response of the
    trigger call and moving on gives you a directory that is not there yet.
    """
    req = rest(f"/jobs/{job_id}/savepoints", method="POST",
               body={"target-directory": "file:///flink-state/savepoints",
                     "cancel-job": cancel_job})
    rid = req["request-id"]
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = rest(f"/jobs/{job_id}/savepoints/{rid}")
        if status["status"]["id"] == "COMPLETED":
            op = status["operation"]
            if "failure-cause" in op:
                raise LabError(f"savepoint failed: {op['failure-cause']}")
            return op["location"]
        time.sleep(2)
    raise LabError(f"savepoint on {job_id} did not complete in {timeout}s")


def latest_savepoint_id(job_id, timeout=60):
    """The checkpoint id the last savepoint was given.

    A savepoint takes a number from the same counter as the checkpoints, so it
    is not the id of the last completed checkpoint before it; it is that
    plus one. Reading the wrong one puts every later comparison against
    flink.max-committed-checkpoint-id off by one in the direction that hides
    the effect.
    """
    # the field appears late. The savepoint REST request reports COMPLETED
    # before the job's checkpoint statistics carry it, so a single read
    # straight after the trigger returns None. This polls until it is there
    # and refuses, rather than handing back a None for callers to compare.
    deadline = time.time() + timeout
    while time.time() < deadline:
        latest = rest(f"/jobs/{job_id}/checkpoints")["latest"].get("savepoint")
        if latest:
            return latest["id"]
        time.sleep(1)
    raise LabError(f"job {job_id} reports no savepoint in its statistics")


def wait_for_watermark(name, target, timeout=420, poll=4):
    """Wait until the TABLE's committed-checkpoint watermark reaches target.

    Not the same as waiting for checkpoints. The watermark only moves when a
    commit happens, and a commit only happens when a checkpoint had data. On
    an idle feed the job's counter climbs and the table's watermark stands
    still, so waiting on the counter opens no gap at all.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        now = committed_checkpoint_id(name) if table_exists(name) else None
        if now is not None and now >= target:
            return now
        time.sleep(poll)
    raise LabError(f"{name} watermark stalled at {committed_checkpoint_id(name)}, "
                   f"never reached {target}")


def latest_retained_checkpoint(job_id):
    """The externalized checkpoint directory a canceled job left behind."""
    latest = rest(f"/jobs/{job_id}/checkpoints")["latest"]["completed"]
    if not latest:
        raise LabError(f"job {job_id} completed no checkpoint to restore from")
    return latest["external_path"]


def kill_taskmanager(restart_after=2):
    """SIGKILL the TaskManager, then bring it back.

    `docker kill` and not `docker stop`: a graceful stop lets Flink shut the
    tasks down cleanly, which is a different event from the one every restart
    guarantee is written for. The kill is the failure worth testing.
    """
    sh("docker", "kill", FLINK_TM)
    time.sleep(restart_after)
    sh("docker", "start", FLINK_TM)


def wait_for_taskmanager(slots=1, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if rest("/overview")["slots-total"] >= slots:
                return True
        except LabError:
            pass
        time.sleep(2)
    raise LabError("the TaskManager never came back")


# ------------------------------------------------------------- reporting ----

def duplicates(seqs):
    """How many rows are copies of a seq already present.

    Not the number of DISTINCT values that repeat: the number of EXTRA rows,
    which is the number a reader of the table would have to deduplicate away.
    """
    return len(seqs) - len(set(seqs))


def missing_range(seqs, lo, hi):
    """Which of lo..hi never arrived, as (start, end) runs.

    Reported as runs and not as a count because the SHAPE is the finding in
    experiment 2: a silent loss caused by a skipped commit is one contiguous
    window, not scattered rows.
    """
    present = set(seqs)
    gaps, run = [], None
    for i in range(lo, hi + 1):
        if i not in present:
            run = (i, i) if run is None else (run[0], i)
        elif run is not None:
            gaps.append(run)
            run = None
    if run is not None:
        gaps.append(run)
    return gaps


def write_result(name, payload, merge=True):
    """Write a results file, MERGING with what is already there by default.

    Every experiment here can be run one part at a time, because a part that
    takes ten minutes should not have to be repeated to re-measure the part
    that takes two. Overwriting the file on a partial run silently deletes the
    other part's evidence and the tests then fail on a missing key, which
    points at the tests rather than at the run.
    """
    path = os.path.join(ROOT, "results", name)
    if merge and os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            existing = json.load(fh)
        existing.update(payload)
        payload = existing
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print(f"wrote results/{name}")


def say(msg):
    print(f"  {msg}", flush=True)
