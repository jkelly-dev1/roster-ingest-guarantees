"""Experiment 1: what a restart actually replays.

Every tutorial shows Kafka -> Flink -> Iceberg working. This asks what the
pipeline does when a TaskManager dies mid-feed, and whether the two settings a
reader would reach for, the checkpointing mode and the write mode, are the
ones that decide the answer.

PART A runs four configurations through the same failure:

    at-least-once + append        predicted: DUPLICATE ROWS
    at-least-once + upsert        predicted: no duplicates, idempotent by key
    exactly-once  + append        predicted: no duplicates
    exactly-once  + upsert        predicted: no duplicates

PART B asks a different question, the one that matters more in practice:
Kafka's committed offsets are for MONITORING, NOT CORRECTNESS. The way to
prove that is not to read the two numbers and compare them; they agree most
of the time, which proves nothing. It is to rewind the committed offsets to
zero behind the job's back and show that recovery does not care, then start a
second job that trusts those offsets and watch it duplicate everything.

The row count comes back through TRINO in every case. A row only Flink can
see is not a committed Iceberg row.
"""

import sys
import threading

import lab
import jobs
import generate_roster as gen

EVENTS = 12000
PROVIDERS = 2000
CHUNK = 500          # events per producer invocation
PAUSE = 0.4          # seconds between invocations

# The feed has to still be running when the TaskManager dies. A backlog
# produced up front is drained inside one checkpoint however long the feed
# was, so the kill lands on an idle job and measures nothing.
CONFIGS = [
    {"key": "at_least_once_append", "mode": "AT_LEAST_ONCE", "upsert": False,
     "prediction": "duplicate rows"},
    {"key": "at_least_once_upsert", "mode": "AT_LEAST_ONCE", "upsert": True,
     "prediction": "no duplicates, idempotent by key"},
    {"key": "exactly_once_append", "mode": "EXACTLY_ONCE", "upsert": False,
     "prediction": "no duplicates"},
    {"key": "exactly_once_upsert", "mode": "EXACTLY_ONCE", "upsert": True,
     "prediction": "no duplicates"},
]


def expected_state(count, providers=PROVIDERS, start=1):
    """What a correct pipeline must end up holding, computed from the seed.

    The generator is a pure function of the record number, so the answer is
    known before anything runs. This is what makes "no duplicates" a
    measurement rather than a comparison against whatever showed up.
    """
    latest = {}
    for i in range(start, start + count):
        npi, _ = gen.event(i, providers)
        latest[npi] = i
    return {"events": count, "distinct_npi": len(latest), "latest_seq": latest}


def measure(table_name, exp, upsert):
    """Everything worth knowing about the landed table."""
    seqs = lab.seqs_in(table_name)
    rows = len(seqs)
    distinct_npi = int(lab.one(
        f"SELECT count(DISTINCT npi) FROM {lab.table(table_name)}"))
    out = {
        "rows": rows,
        "distinct_npi": distinct_npi,
        "duplicate_seq_rows": lab.duplicates(seqs),
        "duplicate_npi_rows": rows - distinct_npi,
        "files": lab.files_in(table_name),
    }
    if upsert:
        # The upsert table is not checked by row count. It is checked against
        # the seed: every provider present exactly once, each carrying the
        # LAST assertion made about it. A table with the right number of rows
        # and the wrong revision in them is worse than one that is short.
        held = {r["npi"]: int(r["seq"]) for r in lab.trino(
            f"SELECT npi, seq FROM {lab.table(table_name)}")}
        wrong = {k: v for k, v in held.items() if exp["latest_seq"].get(k) != v}
        out["providers_expected"] = exp["distinct_npi"]
        out["providers_held"] = len(held)
        out["providers_not_carrying_the_latest_record"] = len(wrong)
        out["complete"] = (len(held) == exp["distinct_npi"] and not wrong)
        # No SEQ gap is reported for an upsert table. An upsert table is
        # SUPPOSED to hold one row per provider, so almost every seq is
        # legitimately absent and a gap list over them means nothing. A
        # completeness measure has to be the one the table's contract implies,
        # which for upsert is the check above and not this one.
        out["missing_seq_runs"] = None
    else:
        out["events_expected"] = exp["events"]
        out["missing_seq_runs"] = lab.missing_range(seqs, 1, exp["events"])
        out["complete"] = (rows == exp["events"]
                           and out["duplicate_seq_rows"] == 0
                           and not out["missing_seq_runs"])
    return out


def run_config(cfg, exp):
    key = cfg["key"]
    topic, group, table_name = f"e1.{key}", f"rig-e1-{key}", f"e1_{key}"
    lab.say(f"--- {key}: {cfg['mode']}, "
            f"{'upsert on npi' if cfg['upsert'] else 'append'}")

    lab.drop_table(table_name)
    lab.create_topic(topic, partitions=4)

    feeder = threading.Thread(
        target=lab.produce,
        args=(EVENTS, topic),
        kwargs={"providers": PROVIDERS, "chunk": CHUNK, "pause": PAUSE},
        daemon=True)
    feeder.start()

    sql = jobs.ingest(f"e1-{key}", topic, group, table_name,
                      upsert=cfg["upsert"], interval="10s", mode=cfg["mode"],
                      parallelism=2)
    job_id = lab.submit(sql, key)
    lab.say(f"submitted {job_id}")

    # Two completed checkpoints means the table has real commits to lose and
    # the source has real offsets in state. Killing before that measures a
    # cold start, not a restart.
    lab.wait_for_checkpoints(job_id, 2, timeout=240)
    before_kill = {
        "rows": lab.rows_in(table_name) if lab.table_exists(table_name) else 0,
        "group_offsets": lab.group_offsets(group),
        "topic_end_offsets": lab.end_offsets(topic),
        "committed_checkpoint_id": lab.committed_checkpoint_id(table_name)
            if lab.table_exists(table_name) else None,
        "checkpoints": lab.checkpoint_counts(job_id),
    }
    lab.say(f"before the kill: {before_kill['rows']} rows, watermark "
            f"{before_kill['committed_checkpoint_id']}")

    prior = before_kill["checkpoints"]["restored"]
    lab.kill_taskmanager()
    lab.wait_for_taskmanager(1)
    after = lab.wait_for_restore(job_id, prior, timeout=300)
    restored_from = lab.restore_point(job_id)
    lab.say(f"restored from checkpoint {restored_from['checkpoint_id']}, "
            f"job is {lab.job_state(job_id)}")

    feeder.join(timeout=600)
    rows, settled_in = lab.wait_until_stable(table_name,
                                             commit_interval_seconds=10)
    lab.say(f"settled at {rows} rows after {settled_in}s")

    result = {
        "configuration": {"checkpointing_mode": cfg["mode"],
                          "write_mode": "upsert on npi" if cfg["upsert"]
                                        else "append",
                          "parallelism": 2, "checkpoint_interval": "10s"},
        "prediction": cfg["prediction"],
        "job_id": job_id,
        "events_produced": EVENTS,
        "before_kill": before_kill,
        "restart": {
            "restored_from": restored_from,
            "checkpoints_after": after,
            "failure_causes": lab.failure_causes(job_id),
            "job_state_after_restart": lab.job_state(job_id),
        },
        "after": {
            "group_offsets": lab.group_offsets(group),
            "topic_end_offsets": lab.end_offsets(topic),
            "committed_checkpoint_id": lab.committed_checkpoint_id(table_name),
        },
        "landed": measure(table_name, exp, cfg["upsert"]),
    }
    result["prediction_held"] = _prediction_held(cfg, result["landed"])
    lab.say(f"rows={result['landed']['rows']} "
            f"duplicate_seq_rows={result['landed']['duplicate_seq_rows']} "
            f"duplicate_npi_rows={result['landed']['duplicate_npi_rows']} "
            f"prediction_held={result['prediction_held']}")
    lab.cancel(job_id)
    lab.delete_topic(topic)
    return result


def _prediction_held(cfg, landed):
    """Was the prediction right? Recorded either way, and never corrected.

    A refuted prediction is the only part of a result a reader cannot get from
    the documentation, so it stays in the file exactly as it fell out.
    """
    # the duplicate measure differs by write mode. An APPEND table is
    # duplicate-free when no seq repeats; a provider appearing six times there
    # is the feed doing its job, so counting repeated NPIs would call a
    # correct table broken. An UPSERT table is duplicate-free when no NPI
    # repeats, and seq is expected to be sparse.
    dupes = (landed["duplicate_npi_rows"] if cfg["upsert"]
             else landed["duplicate_seq_rows"])
    if cfg["prediction"] == "duplicate rows":
        return dupes > 0
    return dupes == 0


def part_b():
    """The committed offsets are for you. The checkpoint is for the job."""
    topic, group, table_name = "e1.offsets", "rig-e1-offsets", "e1_offsets"
    count = 6000
    lab.say("--- part B: are Kafka's committed offsets load bearing?")
    lab.drop_table(table_name)
    lab.create_topic(topic, partitions=4)
    lab.produce(count, topic, providers=PROVIDERS)

    sql = jobs.ingest("e1-offsets-a", topic, group, table_name,
                      interval="10s", parallelism=2)
    job_id = lab.submit(sql, "e1_offsets_a")
    lab.wait_for_rows(table_name, count, timeout=300)
    lab.wait_for_checkpoints(job_id, 2, timeout=240)
    retained = lab.latest_retained_checkpoint(job_id)
    lab.cancel(job_id)
    landed_first = lab.rows_in(table_name)
    offsets_before = lab.group_offsets(group)
    lab.say(f"first job landed {landed_first} rows, offsets {offsets_before}")

    # Rewind the committed offsets to the start of the log. Nothing about the
    # job's state changes: the checkpoint still holds the real position.
    rewound = lab.reset_group_offsets(group, topic, "earliest")
    lab.say(f"rewound the committed offsets to {rewound}")

    sql = jobs.ingest("e1-offsets-restored", topic, group, table_name,
                      interval="10s", parallelism=2, savepoint_path=retained)
    restored_job = lab.submit(sql, "e1_offsets_restored")
    lab.wait_for_checkpoints(restored_job, 2, timeout=240)
    rows_after_restore, _ = lab.wait_until_stable(
        table_name, commit_interval_seconds=10)
    seqs = lab.seqs_in(table_name)
    # Read the offsets before canceling. A canceled job's consumer group has
    # no members, and kafka-consumer-groups then prints a describe block this
    # parser reads as no offsets at all, which is indistinguishable from the
    # offsets having been wiped.
    offsets_after_restore = lab.group_offsets(group)
    lab.cancel(restored_job)
    lab.say(f"after restoring from the checkpoint: {rows_after_restore} rows, "
            f"{lab.duplicates(seqs)} duplicates")

    # Now the same rewind, but with a job that TRUSTS the offsets: no restore
    # point, startup mode group-offsets. This is what an operator does when a
    # job will not restart and they resubmit it from scratch.
    rewound_again = lab.reset_group_offsets(group, topic, "earliest")
    sql = jobs.ingest("e1-offsets-trusting", topic, group, table_name,
                      interval="10s", parallelism=2, startup="group-offsets")
    trusting_job = lab.submit(sql, "e1_offsets_trusting")
    lab.wait_for_checkpoints(trusting_job, 2, timeout=240)
    rows_trusting, _ = lab.wait_until_stable(
        table_name, commit_interval_seconds=10)
    seqs_trusting = lab.seqs_in(table_name)
    lab.cancel(trusting_job)
    lab.say(f"after a job that trusted them: {rows_trusting} rows, "
            f"{lab.duplicates(seqs_trusting)} duplicates")
    lab.delete_topic(topic)

    return {
        "events_produced": count,
        "first_job": {"job_id": job_id, "rows": landed_first,
                      "committed_offsets": offsets_before,
                      "retained_checkpoint": retained},
        "offsets_rewound_to": rewound,
        "restored_from_checkpoint": {
            "job_id": restored_job,
            "restore_point": "the retained checkpoint, not the offsets",
            "rows": rows_after_restore,
            "duplicate_rows": lab.duplicates(seqs),
            "committed_offsets_afterward": offsets_after_restore,
        },
        "offsets_rewound_again_to": rewound_again,
        "job_that_trusted_the_offsets": {
            "job_id": trusting_job,
            "startup_mode": "group-offsets",
            "rows": rows_trusting,
            "duplicate_rows": lab.duplicates(seqs_trusting),
        },
    }


def main():
    part = sys.argv[1] if len(sys.argv) > 1 else "all"
    exp = expected_state(EVENTS)
    payload = {
        "experiment": "restart_replay",
        "question": "what does a restart actually replay, and which setting "
                    "decides it",
        "workload": {"events": EVENTS, "providers_in_the_feed": PROVIDERS,
                     "distinct_npi_expected": exp["distinct_npi"],
                     "partitions": 4, "produced_in_chunks_of": CHUNK,
                     "seconds_between_chunks": PAUSE},
        "failure_injected": "docker kill of the TaskManager, mid-feed, once "
                            "per configuration",
    }
    if part in ("all", "a"):
        print("Part A: four configurations, one kill each", flush=True)
        payload["configurations"] = [run_config(c, exp) for c in CONFIGS]
    if part in ("all", "b"):
        print("Part B: what the committed offsets are worth", flush=True)
        payload["offsets_are_monitoring"] = part_b()
    lab.write_result("exp1_restart_replay.json", payload)


if __name__ == "__main__":
    main()
