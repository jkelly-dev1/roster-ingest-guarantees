"""Experiment 3: the checkpoint interval is the file-size knob.

Iceberg has a property called write.target-file-size-bytes, it defaults to
512 MB, and anyone whose streaming table grows a million tiny Parquet files
reaches for it first. It does not help, and the reason is mechanical rather
than subtle: target file size makes a writer roll over to a new file when the
current one gets too big. It has nothing to say about a writer that is told
to close and commit long before it gets there.

What decides file size in a streaming write is the CHECKPOINT INTERVAL, because
every checkpoint closes every open file and commits them. So the number of
files per commit is roughly the number of writer subtasks times the number of
partitions they touched, and the size of each is however much arrived in one
interval.

That makes file size and freshness the same dial. A short interval gives a
fresh table made of small files; a long one gives large files a reader sees
late. There is no setting that gives both, and the only escape is compaction
downstream, which is not free either.

Measured here: three intervals over the same sustained feed, then TWO more
runs at the shortest interval: one repeating it unchanged, one with the
target file size raised to 1 GB.

The repeat is not padding. "Raising the target changed nothing" is a claim
about a difference being small, and a difference is only small compared to
something. Judging the control against a fixed threshold would only be
comparing it to a number chosen by hand. The repeat run measures what two runs
of the SAME configuration differ by, and the control is judged against that.

Freshness is measured from KAFKA to TRINO. The clock starts when the last
record is written to the log and stops when a Trino query can see it. It
therefore includes the commit and the catalog round trip, which is what a
downstream reader actually waits for.
"""

import time

import lab
import jobs

PROVIDERS = 2000
CHUNK = 500
PAUSE = 0.5
CHUNKS = 120
EVENTS = CHUNK * CHUNKS
PARALLELISM = 2

GB = 1024 ** 3

RUNS = [
    {"key": "interval_5s", "interval": "5s", "target_file_size": None},
    {"key": "interval_30s", "interval": "30s", "target_file_size": None},
    {"key": "interval_120s", "interval": "120s", "target_file_size": None},
    # The control. Same interval as the first run, target file size raised to
    # 1 GB, which is twice the Iceberg default and far past anything this
    # workload could reach.
    {"key": "interval_5s_target_1gb", "interval": "5s", "target_file_size": GB},
    # The noise floor: the first configuration, run again, unchanged.
    {"key": "interval_5s_repeat", "interval": "5s", "target_file_size": None},
]

PREDICTION = ("files per commit tracks the writer subtasks and not the target "
              "file size; a longer interval gives fewer and larger files and a "
              "staler table; raising write.target-file-size-bytes changes "
              "nothing at all")


def run_one(cfg):
    key = cfg["key"]
    topic, group, table_name = f"e3.{key}", f"rig-e3-{key}", f"e3_{key}"
    lab.say(f"--- {key}: interval {cfg['interval']}, target file size "
            f"{cfg['target_file_size'] or 'Iceberg default'}")
    lab.drop_table(table_name)
    lab.create_topic(topic, partitions=4)

    job_id = lab.submit(
        jobs.ingest(f"e3-{key}", topic, group, table_name,
                    interval=cfg["interval"], parallelism=PARALLELISM,
                    target_file_size=cfg["target_file_size"]), key)
    lab.wait_for_state(job_id, ("RUNNING",), timeout=180)

    # The feed is sustained and it has to be. A backlog produced up front is
    # drained inside a single checkpoint however long the interval is, so
    # every interval would produce one commit and the experiment would report
    # that the interval does nothing.
    feed_start = time.time()
    lab.produce(EVENTS, topic, providers=PROVIDERS, chunk=CHUNK, pause=PAUSE)
    feed_seconds = round(time.time() - feed_start, 1)
    last_record_at = time.time()

    lab.wait_for_rows(table_name, EVENTS, timeout=600, poll=1)
    freshness = round(time.time() - last_record_at, 1)
    lab.cancel(job_id)

    snaps = lab.snapshots(table_name)
    files = lab.files_in(table_name)
    added = [int(s["summary"].get("added-data-files", 0)) for s in snaps]
    result = {
        "checkpoint_interval": cfg["interval"],
        "target_file_size_bytes": cfg["target_file_size"],
        "parallelism": PARALLELISM,
        "events": EVENTS,
        "feed_seconds": feed_seconds,
        "events_per_second": round(EVENTS / feed_seconds, 1),
        "commits": len(snaps),
        "data_files": files["data_files"],
        "data_bytes": files["data_bytes"],
        "avg_data_file_bytes": files["avg_data_file_bytes"],
        "data_files_per_commit": round(files["data_files"] / len(snaps), 2),
        "max_data_files_in_one_commit": max(added),
        "min_data_files_in_one_commit": min(added),
        "rows": lab.rows_in(table_name),
        # The clock starts at the last record written to Kafka and stops when
        # a Trino query can see it, so it is an END TO END figure and not a
        # commit latency.
        "seconds_from_last_record_to_visible_in_trino": freshness,
        "job_id": job_id,
    }
    lab.say(f"{result['commits']} commits, {result['data_files']} data files, "
            f"avg {result['avg_data_file_bytes']} bytes, freshness "
            f"{freshness}s")
    lab.delete_topic(topic)
    return result


def findings(runs, parallelism=PARALLELISM):
    """Judge the claims against the runs, comparing like with like."""
    base = runs["interval_5s"]
    repeat = runs["interval_5s_repeat"]
    control = runs["interval_5s_target_1gb"]
    slow = runs["interval_120s"]

    def gap(a, b, field):
        return abs(a[field] - b[field])

    noise_files = gap(base, repeat, "data_files")
    noise_size = gap(base, repeat, "avg_data_file_bytes")
    control_files = gap(base, control, "data_files")
    control_size = gap(base, control, "avg_data_file_bytes")
    return {
        "repeat_of_the_same_configuration_differed_by_files": noise_files,
        "repeat_of_the_same_configuration_differed_by_avg_bytes": noise_size,
        "raising_the_target_to_1gb_differed_by_files": control_files,
        "raising_the_target_to_1gb_differed_by_avg_bytes": control_size,
        # The claim, stated so it can fail: raising the target moved the
        # numbers no further than simply running the same thing twice did.
        "raising_the_target_moved_nothing_beyond_run_to_run_variation":
            control_files <= noise_files and control_size <= noise_size,
        "target_file_size_bytes_requested": control["target_file_size_bytes"],
        "largest_average_file_size_observed": max(
            r["avg_data_file_bytes"] for r in runs.values()),
        "every_file_is_orders_of_magnitude_below_the_target": all(
            r["avg_data_file_bytes"] * 1000 < GB for r in runs.values()),
        "longer_interval_gave_fewer_files": slow["data_files"] < base["data_files"],
        "longer_interval_gave_larger_files":
            slow["avg_data_file_bytes"] > base["avg_data_file_bytes"],
        "longer_interval_cost_freshness_seconds": round(
            slow["seconds_from_last_record_to_visible_in_trino"]
            - base["seconds_from_last_record_to_visible_in_trino"], 1),
        "files_per_commit_never_exceeded_the_writer_count": all(
            r["max_data_files_in_one_commit"] <= parallelism
            for r in runs.values()),
    }


def main():
    import sys
    wanted = sys.argv[1:] or [c["key"] for c in RUNS]
    payload = {
        "experiment": "commit_interval",
        "question": "what decides the size of the files a streaming writer "
                    "produces, and what does it cost to make them bigger",
        "prediction": PREDICTION,
        "workload": {"events": EVENTS, "chunk": CHUNK,
                     "seconds_between_chunks": PAUSE, "partitions": 4,
                     "providers_in_the_feed": PROVIDERS,
                     "parallelism": PARALLELISM},
    }
    runs = {}
    for cfg in RUNS:
        if cfg["key"] in wanted:
            runs[cfg["key"]] = run_one(cfg)
    payload["runs"] = runs
    if set(runs) == {c["key"] for c in RUNS}:
        payload["findings"] = findings(runs)
        payload["prediction_held"] = (
            payload["findings"]["raising_the_target_moved_nothing_beyond_run_to_run_variation"]
            and payload["findings"]["longer_interval_gave_larger_files"]
            and payload["findings"]["files_per_commit_never_exceeded_the_writer_count"])
    lab.write_result("exp3_commit_interval.json", payload, merge=False)


if __name__ == "__main__":
    main()
