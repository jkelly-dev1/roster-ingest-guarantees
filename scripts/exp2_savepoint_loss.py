"""Experiment 2: what an old savepoint really does to an Iceberg table.

The prediction, written before the first run and kept after it was refuted:
restoring from a savepoint older than the table's newest commit loses every
record read while the restored job's checkpoint ids are at or below
flink.max-committed-checkpoint-id, because the Iceberg committer skips any
checkpoint at or below that watermark. The job reports RUNNING throughout and
the data never appears.

The watermark is real, the skip is real, and the job does stay running. The
loss is not real, and the reason is worth more than the prediction was. When
the committer is asked to commit a checkpoint at or below the watermark it
does NOTHING AT ALL; it does not commit and it does not discard. The files
stay in the committer's state. The first checkpoint that clears the watermark
commits the whole accumulation in one lump.

So the hazard is not loss. It is these two, and both are measured here:

  The table freezes while everything says it is healthy. For as long as the
  restored job's checkpoint counter is climbing back to the watermark, the
  table does not move. The job is RUNNING, checkpoints complete, no exception
  is thrown, and the KAFKA consumer lag goes to zero, because the records
  really were consumed. Every dashboard is green and the table is stale.

  The replay is committed, so the rows duplicate. The savepoint rewinds the
  source to an older offset. Everything between that offset and the table's
  newest commit is read a second time and committed a second time.

Where the skip does work is an internal restart, which is the case experiment
1 measures: a TaskManager kill keeps the job id, the watermark matches, and
the replay is suppressed. Same rule, opposite outcome, and the difference is
which kind of restart happened.
"""

import time

import lab
import jobs

TOPIC = "e2.savepoint"
GROUP = "rig-e2"
TABLE = "e2_savepoint"
BATCH = 4000
SUSTAINED = 12000    # the second batch, fed slowly so every checkpoint commits
CHUNK = 1000
PAUSE = 4.0
PROVIDERS = 2000
INTERVAL = "5s"

# The gap between the savepoint and the table's newest commit is what decides
# how long the restored job runs before it can commit anything. It is made
# WIDE ON PURPOSE: a one or two checkpoint gap closes before the replayed
# records are even read, and then nothing is observable at all.
CHECKPOINT_GAP = 12

PREDICTION = ("restoring from a savepoint older than the table's newest "
              "commit LOSES every record read while the restored job's "
              "checkpoint ids are at or below "
              "flink.max-committed-checkpoint-id, with the job reporting "
              "RUNNING throughout")


def sample(job_id=None):
    """One observation of the job and the table at the same moment."""
    snaps = lab.snapshots(TABLE) if lab.table_exists(TABLE) else []
    committed = lab.group_offsets(GROUP)
    end = lab.end_offsets(TOPIC)
    rec = {
        "rows_visible_to_trino": lab.rows_in(TABLE) if snaps else 0,
        "snapshots": len(snaps),
        "max_committed_checkpoint_id": (
            int(snaps[-1]["summary"]["flink.max-committed-checkpoint-id"])
            if snaps else None),
        "writing_job_id_on_newest_snapshot": (
            snaps[-1]["summary"].get("flink.job-id") if snaps else None),
        # Consumer lag is part of the measurement, not background detail. It
        # is the number an on-call dashboard shows, and during the frozen
        # window it reads zero while the table is thousands of rows behind.
        "consumer_lag": sum(end.get(p, 0) - o for p, o in committed.items()),
    }
    if job_id:
        rec["job_state"] = lab.job_state(job_id)
        rec["latest_completed_checkpoint_id"] = lab.latest_checkpoint_id(job_id)
        rec["checkpoints_failed"] = lab.checkpoint_counts(job_id)["failed"]
    return rec


def stage(label, job_id=None):
    rec = {"stage": label}
    rec.update(sample(job_id))
    lab.say(f"{label}: {rec['rows_visible_to_trino']} rows, watermark "
            f"{rec['max_committed_checkpoint_id']}, "
            f"{rec['snapshots']} snapshots, lag {rec['consumer_lag']}")
    return rec


def watch(job_id, watermark, timeout=600, poll=3):
    """Sample until the restored job's checkpoint id clears the watermark.

    This timeline is the whole finding. Read down the checkpoint id column
    against the rows column: the table does not move until the first id
    greater than the watermark, and everything else looks healthy the entire
    time.
    """
    timeline, start = [], time.time()
    while time.time() - start < timeout:
        rec = {"seconds_since_restore": round(time.time() - start, 1)}
        rec.update(sample(job_id))
        timeline.append(rec)
        cp = rec["latest_completed_checkpoint_id"]
        if cp is not None and cp > watermark:
            return timeline
        time.sleep(poll)
    raise lab.LabError(
        f"the restored job never passed checkpoint {watermark} in {timeout}s")


def main():
    lab.drop_table(TABLE)
    lab.create_topic(TOPIC, partitions=4)
    stages = []

    lab.produce(BATCH, TOPIC, start=1, providers=PROVIDERS)
    job_a = lab.submit(jobs.ingest("e2-original", TOPIC, GROUP, TABLE,
                                   interval=INTERVAL, parallelism=2),
                       "e2_original")
    lab.wait_for_rows(TABLE, BATCH, timeout=300)
    lab.wait_for_checkpoints(job_a, 3, timeout=240)
    stages.append(stage("01_first_batch_landed", job_a))

    # The savepoint an operator takes before a deploy. The job keeps running.
    savepoint_path = lab.savepoint(job_a, cancel_job=False)
    savepoint_id = lab.latest_savepoint_id(job_a)
    lab.say(f"savepoint {savepoint_id} written to {savepoint_path}")
    stages.append(stage("02_savepoint_taken", job_a))

    # The table commits past the savepoint. This is not an unusual state: it
    # is every streaming table between one savepoint and the next.
    #
    # The feed is sustained here and that is not cosmetic. The watermark only
    # advances on a commit and a commit needs data, so a batch dropped in at
    # once advances it two or three and then stops however long the job runs.
    # That gap has to be opened by feeding the job, not by waiting.
    lab.produce(SUSTAINED, TOPIC, start=BATCH + 1, providers=PROVIDERS,
                chunk=CHUNK, pause=PAUSE)
    lab.wait_for_rows(TABLE, BATCH + SUSTAINED, timeout=420)
    watermark = lab.wait_for_watermark(TABLE, savepoint_id + CHECKPOINT_GAP,
                                       timeout=420)
    stages.append(stage("03_table_committed_past_the_savepoint", job_a))

    lab.cancel(job_a)
    stages.append(stage("04_job_stopped"))

    # New data arrives while the job is down. There is nothing wrong with it.
    lab.produce(BATCH, TOPIC, start=BATCH + SUSTAINED + 1, providers=PROVIDERS)
    stages.append(stage("05_third_batch_produced_while_down"))

    job_b = lab.submit(jobs.ingest("e2-restored", TOPIC, GROUP, TABLE,
                                   interval=INTERVAL, parallelism=2,
                                   savepoint_path=savepoint_path),
                       "e2_restored")
    lab.wait_for_checkpoints(job_b, 1, timeout=240)
    restored_from = lab.restore_point(job_b)
    first_ckpt = lab.latest_checkpoint_id(job_b)
    lab.say(f"restored from checkpoint {restored_from['checkpoint_id']} "
            f"(savepoint={restored_from['is_savepoint']}); the restored job's "
            f"first completed checkpoint is {first_ckpt} and the table's "
            f"watermark is {watermark}")

    timeline = watch(job_b, watermark, timeout=600, poll=3)
    stages.append(stage("06_restored_job_cleared_the_watermark", job_b))

    # A fourth batch AFTER the watermark is cleared. It has to land, or the
    # finding is "the job broke" rather than "the job froze and caught up".
    lab.produce(BATCH, TOPIC, start=2 * BATCH + SUSTAINED + 1,
                providers=PROVIDERS)
    settled, _ = lab.wait_until_stable(TABLE, quiet_polls=4, poll=5, timeout=420,
                                       commit_interval_seconds=5)
    stages.append(stage("07_fourth_batch_landed", job_b))

    seqs = lab.seqs_in(TABLE)
    total = 3 * BATCH + SUSTAINED
    gaps = lab.missing_range(seqs, 1, total)

    frozen = [t for t in timeline
              if t["latest_completed_checkpoint_id"] is not None
              and t["latest_completed_checkpoint_id"] <= watermark]
    freeze = {
        "samples_while_the_checkpoint_id_was_at_or_below_the_watermark":
            len(frozen),
        # A censored lower bound, and named as one. This is the last SAMPLE
        # that still showed the pre-freeze row count, so the freeze ended
        # somewhere between it and the next sample. The clock also starts at
        # the restored job's first completed checkpoint rather than at
        # restore, so the true freeze is longer than this by an unmeasured
        # amount. Publishing it as a stopwatch reading would overstate the
        # resolution of a 7-second poll.
        "seconds_frozen": round(frozen[-1]["seconds_since_restore"], 1)
            if frozen else 0.0,
        "rows_while_frozen": sorted({t["rows_visible_to_trino"] for t in frozen}),
        "job_states_while_frozen": sorted({t["job_state"] for t in frozen}),
        "checkpoints_failed_while_frozen": max(
            (t["checkpoints_failed"] for t in frozen), default=0),
        "min_consumer_lag_while_frozen": min(
            (t["consumer_lag"] for t in frozen), default=None),
        "rows_at_the_first_commit_after_the_watermark_cleared":
            timeline[-1]["rows_visible_to_trino"],
    }
    freeze["the_table_never_moved_while_frozen"] = len(freeze["rows_while_frozen"]) == 1
    freeze["rows_committed_in_one_lump"] = (
        freeze["rows_at_the_first_commit_after_the_watermark_cleared"]
        - freeze["rows_while_frozen"][0]) if frozen else 0

    result = {
        "experiment": "savepoint_loss",
        "question": "what does restoring from a savepoint older than the "
                    "table's newest commit actually do",
        "prediction": PREDICTION,
        "workload": {"first_batch": BATCH, "sustained_batch": SUSTAINED,
                     "third_batch": BATCH, "fourth_batch": BATCH,
                     "events_total": total, "partitions": 4,
                     "checkpoint_interval": INTERVAL, "parallelism": 2,
                     "checkpoint_gap_requested": CHECKPOINT_GAP},
        "savepoint": {"path": savepoint_path, "checkpoint_id": savepoint_id},
        "watermark_before_restore": watermark,
        "restored_job": {
            "job_id": job_b,
            "restored_from": restored_from,
            "first_completed_checkpoint_id": first_ckpt,
            "checkpoints_below_the_watermark": max(
                0, watermark - first_ckpt + 1),
            "final_state": lab.job_state(job_b),
            "checkpoints": lab.checkpoint_counts(job_b),
            "failure_causes": lab.failure_causes(job_b),
        },
        "frozen_window": freeze,
        "timeline_after_restore": timeline,
        "stages": stages,
        "landed": {
            "rows": len(seqs),
            "rows_expected": total,
            "rows_lost": max(0, total - len(set(seqs))),
            "duplicate_rows": lab.duplicates(seqs),
            "missing_seq_runs": gaps,
            "files": lab.files_in(TABLE),
            "settled_at": settled,
        },
    }
    result["prediction_held"] = (result["landed"]["rows_lost"] > 0
                                 and len(gaps) == 1)
    lab.say(f"lost {result['landed']['rows_lost']}, duplicated "
            f"{result['landed']['duplicate_rows']}, frozen for "
            f"{freeze['seconds_frozen']}s at {freeze['rows_while_frozen']} rows "
            f"with minimum lag {freeze['min_consumer_lag_while_frozen']}; "
            f"prediction_held={result['prediction_held']}")
    lab.cancel(job_b)
    lab.delete_topic(TOPIC)
    lab.write_result("exp2_savepoint_loss.json", result, merge=False)


if __name__ == "__main__":
    main()
