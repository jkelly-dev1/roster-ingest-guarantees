"""Experiment 4: table maintenance run against a job that is still writing.

Iceberg maintenance is presented as housekeeping. expire_snapshots drops old
snapshots, remove_orphan_files deletes files no snapshot references, and both
are things a nightly job runs without asking anybody. Neither of them knows
that a Flink job is in the middle of writing to the table.

Two things could go wrong and they are not the same kind of wrong.

Part a: expiring the snapshot that carries the watermark. The Iceberg
committer does not keep flink.max-committed-checkpoint-id anywhere durable of
its own: it reads it back out of the table's snapshot history, matching on its
own flink.job-id. A running job holds the value in memory, so expiring the
snapshot changes nothing WHILE IT RUNS. The exposure is the next restart, when
the job goes looking for a watermark that has been deleted. Then the job kill
goes in on purpose, after the expiry, and the question is whether the replay
it does on recovery is suppressed or committed twice.

Part b; deleting files the writer has not committed yet. A streaming writer
has open and closed Parquet files that no snapshot references, because the
snapshot that will reference them has not been written. That is precisely the
definition remove_orphan_files uses, and its retention threshold is the only
thing standing between the two. Run it with the threshold at zero and the
question is whether it takes the pending files with it.

Both procedures are run from TRINO, on a table Flink owns, exactly as a
scheduled maintenance job would. If nothing breaks, that is the result and it
is published as one with the reason it did not reproduce.
"""

import time

import lab
import jobs

PROVIDERS = 2000
CHUNK = 500
PAUSE = 0.5
INTERVAL = "5s"
PARALLELISM = 2

PREDICTION_A = ("expiring snapshots under a live writer is survivable while "
                "it runs, and the next restart re-commits its pending state "
                "because the watermark it looks for has been deleted")
PREDICTION_B = ("remove_orphan_files with the retention threshold at zero "
                "deletes Parquet files the writer has written but not yet "
                "committed, and the loss shows up as missing rows")


def expire_snapshots(table_name):
    """Expire everything, guard lowered in the same session.

    TRINO refuses this by default. It enforces a seven day minimum retention,
    so expiring anything recent fails until iceberg.expire_snapshots_min_
    retention is lowered, and a SET SESSION in a separate invocation is
    discarded before the statement it configures ever runs.
    """
    return lab.trino_session(
        "SET SESSION iceberg.expire_snapshots_min_retention = '0s'",
        f"ALTER TABLE {lab.table(table_name)} EXECUTE expire_snapshots("
        f"retention_threshold => '0s')")


def remove_orphan_files(table_name):
    return lab.trino_session(
        "SET SESSION iceberg.remove_orphan_files_min_retention = '0s'",
        f"ALTER TABLE {lab.table(table_name)} EXECUTE remove_orphan_files("
        f"retention_threshold => '0s')")


def readable(table_name):
    """Can the table still be read at all, and what does it say?

    A maintenance procedure that removed a live file does not announce it. It
    shows up the next time somebody scans the table and a Parquet file is not
    where the manifest says it is, so the read itself is the check.
    """
    try:
        return {"readable": True, "rows": lab.rows_in(table_name)}
    except lab.LabError as exc:
        return {"readable": False,
                "error": str(exc).splitlines()[-1][:240]}


def part_a():
    """Expire the snapshots, then force the restart that has to read them."""
    key = "expire_under_writer"
    topic, group, table_name = f"e4.{key}", f"rig-e4-{key}", f"e4_{key}"
    lab.say("--- part A: expire_snapshots against a live writer")
    lab.drop_table(table_name)
    lab.create_topic(topic, partitions=4)

    job_id = lab.submit(
        jobs.ingest(f"e4-{key}", topic, group, table_name, interval=INTERVAL,
                    parallelism=PARALLELISM), key)
    lab.wait_for_state(job_id, ("RUNNING",), timeout=180)

    first = 6000
    lab.produce(first, topic, start=1, providers=PROVIDERS,
                chunk=CHUNK, pause=PAUSE)
    lab.wait_for_rows(table_name, first, timeout=420)
    before = {"snapshots": len(lab.snapshots(table_name)),
              "rows": lab.rows_in(table_name),
              "watermark": lab.committed_checkpoint_id(table_name),
              "job_state": lab.job_state(job_id)}
    lab.say(f"before: {before['snapshots']} snapshots, {before['rows']} rows, "
            f"watermark {before['watermark']}")

    expiry_error = None
    try:
        expire_snapshots(table_name)
    except lab.LabError as exc:
        expiry_error = str(exc).splitlines()[-1][:240]
    after_expiry = {"snapshots": len(lab.snapshots(table_name)),
                    "watermark": lab.committed_checkpoint_id(table_name),
                    "job_state": lab.job_state(job_id),
                    "table": readable(table_name),
                    "error": expiry_error}
    lab.say(f"after expire_snapshots: {after_expiry['snapshots']} snapshots, "
            f"job is {after_expiry['job_state']}")

    # Keep writing after the expiry. A job that only survives because nothing
    # asked it to do anything has not been tested.
    second = 4000
    lab.produce(second, topic, start=first + 1, providers=PROVIDERS,
                chunk=CHUNK, pause=PAUSE)
    lab.wait_for_rows(table_name, first + second, timeout=420)
    still_writing = {"rows": lab.rows_in(table_name),
                     "snapshots": len(lab.snapshots(table_name)),
                     "job_state": lab.job_state(job_id)}
    lab.say(f"after writing on: {still_writing['rows']} rows, job is "
            f"{still_writing['job_state']}")

    # The exposure is the restart, not the expiry.
    prior = lab.checkpoint_counts(job_id)["restored"]
    lab.kill_taskmanager()
    lab.wait_for_taskmanager(1)
    lab.wait_for_restore(job_id, prior, timeout=300)
    lab.say(f"restarted; job is {lab.job_state(job_id)}")

    third = 2000
    lab.produce(third, topic, start=first + second + 1, providers=PROVIDERS,
                chunk=CHUNK, pause=PAUSE)
    settled, _ = lab.wait_until_stable(table_name, quiet_polls=4, timeout=420,
                                       commit_interval_seconds=5)
    seqs = lab.seqs_in(table_name)
    total = first + second + third
    result = {
        "prediction": PREDICTION_A,
        "job_id": job_id,
        "events_produced": total,
        "before_expiry": before,
        "after_expiry": after_expiry,
        # The reason part A came out the way it did: expire_snapshots keeps
        # the CURRENT snapshot by definition, and the current snapshot is the
        # one carrying flink.max-committed-checkpoint-id. The watermark cannot
        # be expired away while the table still has a newest commit.
        "the_watermark_survived_because_the_newest_snapshot_did":
            after_expiry["watermark"] == before["watermark"],
        "kept_writing_after_expiry": still_writing,
        "after_the_restart": {
            "job_state": lab.job_state(job_id),
            "checkpoints": lab.checkpoint_counts(job_id),
            "failure_causes": lab.failure_causes(job_id),
            "snapshots": len(lab.snapshots(table_name)),
            "watermark": lab.committed_checkpoint_id(table_name),
        },
        "landed": {
            "rows": settled,
            "rows_expected": total,
            "duplicate_rows": lab.duplicates(seqs),
            "missing_seq_runs": lab.missing_range(seqs, 1, total),
            "files": lab.files_in(table_name),
        },
    }
    result["survived"] = (result["landed"]["rows"] == total
                          and result["landed"]["duplicate_rows"] == 0
                          and not result["landed"]["missing_seq_runs"])
    result["prediction_held"] = result["landed"]["duplicate_rows"] > 0
    lab.say(f"landed {settled} of {total}, "
            f"{result['landed']['duplicate_rows']} duplicates, "
            f"survived={result['survived']}")
    lab.cancel(job_id)
    lab.delete_topic(topic)
    return result


def part_b():
    """Delete the writer's uncommitted files out from under it."""
    key = "orphans_under_writer"
    topic, group, table_name = f"e4.{key}", f"rig-e4-{key}", f"e4_{key}"
    lab.say("--- part B: remove_orphan_files against a live writer")
    lab.drop_table(table_name)
    lab.create_topic(topic, partitions=4)

    # A long interval is the condition under test here. The window in which
    # written files are not yet referenced by any snapshot IS the checkpoint
    # interval, so a short one gives the procedure almost nothing to land in.
    # Sixty seconds makes the window wide enough to aim at.
    job_id = lab.submit(
        jobs.ingest(f"e4-{key}", topic, group, table_name, interval="60s",
                    parallelism=PARALLELISM), key)
    lab.wait_for_state(job_id, ("RUNNING",), timeout=180)

    first = 4000
    lab.produce(first, topic, start=1, providers=PROVIDERS)
    lab.wait_for_rows(table_name, first, timeout=300)
    committed_before = lab.rows_in(table_name)
    lab.say(f"one commit landed: {committed_before} rows")

    # Now feed a second batch and run the procedure INSIDE the interval, while
    # those records are in Parquet files no snapshot references yet.
    second = 4000
    lab.produce(second, topic, start=first + 1, providers=PROVIDERS)
    time.sleep(10)
    at_run_time = {"rows_visible": lab.rows_in(table_name),
                   "snapshots": len(lab.snapshots(table_name)),
                   "job_state": lab.job_state(job_id)}
    # Count the objects, not only the rows. "Nothing broke" is not a result
    # until it says what the procedure had to work with. If there is no
    # finished Parquet object that no snapshot references, then there was
    # never anything for remove_orphan_files to take, and the experiment
    # measured an empty window rather than a safe one.
    at_run_time["storage"] = lab.storage_vs_catalog(table_name)
    orphan_error = None
    try:
        remove_orphan_files(table_name)
    except lab.LabError as exc:
        orphan_error = str(exc).splitlines()[-1][:240]
    after_run_storage = lab.storage_vs_catalog(table_name)
    lab.say(f"remove_orphan_files ran with {at_run_time['rows_visible']} rows "
            f"visible, {second} in flight, and "
            f"{at_run_time['storage']['data_objects_no_snapshot_references']} "
            f"unreferenced data objects on disk")

    # Nine polls of ten seconds, against a sixty second checkpoint interval.
    # The quiet window has to outlast the interval: a shorter one settles on a
    # table that is merely between commits, and reports rows that have not
    # been committed yet as rows the procedure removed.
    settled, _ = lab.wait_until_stable(table_name, quiet_polls=9, poll=10,
                                       timeout=600, commit_interval_seconds=60)
    total = first + second
    seqs = lab.seqs_in(table_name) if settled else []
    result = {
        "prediction": PREDICTION_B,
        "job_id": job_id,
        "checkpoint_interval": "60s",
        "events_produced": total,
        "rows_committed_before": committed_before,
        "state_when_the_procedure_ran": at_run_time,
        "storage_immediately_after_the_procedure": after_run_storage,
        "rows_in_flight_when_it_ran": second,
        "error": orphan_error,
        "after": {
            "job_state": lab.job_state(job_id),
            "checkpoints": lab.checkpoint_counts(job_id),
            "failure_causes": lab.failure_causes(job_id),
            "table": readable(table_name),
        },
        "landed": {
            "rows": settled,
            "rows_expected": total,
            "rows_lost": total - len(set(seqs)),
            "duplicate_rows": lab.duplicates(seqs),
            "missing_seq_runs": lab.missing_range(seqs, 1, total),
            "files": lab.files_in(table_name),
        },
    }
    result["survived"] = (result["landed"]["rows"] == total
                          and not result["landed"]["missing_seq_runs"])
    result["prediction_held"] = result["landed"]["rows_lost"] > 0
    # The reason it did not reproduce, stated as a measurement rather than as
    # a guess: an in-flight Parquet file is an OPEN MULTIPART UPLOAD, not an
    # object. There is nothing in the bucket for the procedure to classify as
    # an orphan until the checkpoint closes it, and closing it is the same
    # event that commits it.
    result["nothing_was_exposed_to_the_procedure"] = (
        at_run_time["storage"]["data_objects_no_snapshot_references"] == 0)
    lab.say(f"landed {settled} of {total}, lost "
            f"{result['landed']['rows_lost']}, survived={result['survived']}")
    lab.cancel(job_id)
    lab.delete_topic(topic)
    return result


def main():
    import sys
    part = sys.argv[1] if len(sys.argv) > 1 else "all"
    payload = {
        "experiment": "maintenance_live_writer",
        "question": "what do expire_snapshots and remove_orphan_files do to a "
                    "job that is still writing to the table",
    }
    if part in ("all", "a"):
        payload["expire_snapshots"] = part_a()
    if part in ("all", "b"):
        payload["remove_orphan_files"] = part_b()
    lab.write_result("exp4_maintenance_live_writer.json", payload)


if __name__ == "__main__":
    main()
