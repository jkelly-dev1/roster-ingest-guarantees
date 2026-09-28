"""Re-derive the experiments' figures in README.md from results/*.json.

A README is prose and drifts; results/*.json is evidence and does not. This
script rebuilds each table row from the JSON and asserts the exact string is
present in the README, so a re-run that shifts a figure fails loudly instead
of leaving the document silently wrong.

    python3 scripts/check_readme_numbers.py            check
    python3 scripts/check_readme_numbers.py --emit     print the rows

The emit mode is how the tables were written in the first place, which is the
only reason the two agree at all. A table typed by hand and checked afterward
is a table that gets edited until the checker stops complaining.

The README numbers this script does not derive, because no experiment
measured them: the engine versions (Flink 1.20 and 1.20.5, Iceberg 1.10 and
1.10.0, Kafka 4.3 and 4.3.1, Trino 478, JDK 11.0.31 and 11.0.32), Iceberg's
documented 512 MB default target file size, the pytest install bound below
10, the stack's host ports 8091, 8092, 8182, 9010 and 9011, Kafka's
in-network listener 9092, the loopback address 127.0.0.1, Flink's uid 9999,
the 1000 events the smoke test produces, and the 20,000,000 rows of the
sibling repository's table.
"""

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load(name):
    with open(os.path.join(ROOT, "results", name), encoding="utf-8") as fh:
        return json.load(fh)


def n(value):
    return f"{value:,}"


CONFIG_LABEL = {
    ("AT_LEAST_ONCE", "append"): "`at-least-once` + append",
    ("AT_LEAST_ONCE", "upsert on npi"): "`at-least-once` + upsert on npi",
    ("EXACTLY_ONCE", "append"): "`exactly-once` + append",
    ("EXACTLY_ONCE", "upsert on npi"): "`exactly-once` + upsert on npi",
}

INTERVAL_LABEL = {
    "interval_5s": ("`5s`", "Iceberg default"),
    "interval_30s": ("`30s`", "Iceberg default"),
    "interval_120s": ("`120s`", "Iceberg default"),
    "interval_5s_target_1gb": ("`5s`", "1 GB"),
    "interval_5s_repeat": ("`5s`, run again", "Iceberg default"),
}


WORDS = {0: "zero", 1: "one", 2: "two", 3: "three", 4: "four", 5: "five"}


def rows_exp1(e1):
    out = []
    for c in e1["configurations"]:
        cfg, landed = c["configuration"], c["landed"]
        label = CONFIG_LABEL[(cfg["checkpointing_mode"], cfg["write_mode"])]
        out.append((f"exp1:{label}",
                    f"| {label} | {n(landed['rows'])} | "
                    f"{n(landed['duplicate_seq_rows'])} | "
                    f"{n(landed['duplicate_npi_rows'])} | "
                    f"{landed['files']['delete_files']} | "
                    f"{c['prediction']} | "
                    f"{'held' if c['prediction_held'] else 'REFUTED'} |"))

    b = e1["offsets_are_monitoring"]
    steps = [
        # The first job's table was counted, not checked for duplicates, so
        # its two rows say so instead of printing a 0 nothing measured.
        ("the first job, all events landed",
         sum(b["first_job"]["committed_offsets"].values()),
         b["first_job"]["rows"], None),
        ("committed offsets rewound to the start of the log",
         sum(b["offsets_rewound_to"].values()),
         b["first_job"]["rows"], None),
        ("restored from the retained checkpoint",
         sum(b["restored_from_checkpoint"]["committed_offsets_afterward"].values()),
         b["restored_from_checkpoint"]["rows"],
         b["restored_from_checkpoint"]["duplicate_rows"]),
        ("rewound again, restarted from `group-offsets`",
         sum(b["offsets_rewound_again_to"].values()),
         b["job_that_trusted_the_offsets"]["rows"],
         b["job_that_trusted_the_offsets"]["duplicate_rows"]),
    ]
    for label, offsets, rows, dupes in steps:
        out.append((f"exp1b:{label}",
                    f"| {label} | {n(offsets)} | {n(rows)} | "
                    f"{'not measured' if dupes is None else n(dupes)} |"))
    return out


def rows_exp2(e2):
    out = []
    for t in e2["timeline_after_restore"]:
        # A sample with no committed offsets has no lag, and the row says so
        # instead of printing a 0 nothing read.
        lag = ("not read" if t["consumer_lag"] is None
               else n(t["consumer_lag"]))
        out.append((f"exp2:{t['seconds_since_restore']}",
                    f"| {t['seconds_since_restore']} | "
                    f"{t['latest_completed_checkpoint_id']} | "
                    f"{t['job_state']} | {lag} | "
                    f"{n(t['rows_visible_to_trino'])} |"))
    return out


def rows_exp3(e3):
    out = []
    for key, run in e3["runs"].items():
        interval, target = INTERVAL_LABEL[key]
        out.append((f"exp3:{key}",
                    f"| {interval} | {target} | {run['commits']} | "
                    f"{run['data_files']} | {run['data_files_per_commit']} | "
                    f"{n(run['avg_data_file_bytes'])} | "
                    f"{run['seconds_from_last_record_to_visible_in_trino']} s |"))
    return out


def rows_exp4(e4):
    a, b = e4["expire_snapshots"], e4["remove_orphan_files"]
    return [
        ("exp4:expire_snapshots",
         f"| `expire_snapshots` | {a['before_expiry']['snapshots']} snapshots "
         f"to {a['after_expiry']['snapshots']} | "
         f"{a['after_expiry']['job_state']} | "
         f"{n(a['landed']['rows'])} of {n(a['events_produced'])} | "
         f"{n(a['landed']['duplicate_rows'])} | "
         f"{n(_rows_lost(a['landed']))} |"),
        ("exp4:remove_orphan_files",
         f"| `remove_orphan_files` | "
         f"{b['state_when_the_procedure_ran']['storage']['data_objects_no_snapshot_references']}"
         f" unreferenced objects to take | {b['after']['job_state']} | "
         f"{n(b['landed']['rows'])} of {n(b['events_produced'])} | "
         f"{n(b['landed']['duplicate_rows'])} | {n(b['landed']['rows_lost'])} |"),
    ]


def _rows_lost(landed):
    """Expected rows that are not in the table: rows_expected less the rows
    that landed once. The expire_snapshots run did not record rows_lost, so
    it is derived here from the counts it did record."""
    once = landed["rows"] - landed["duplicate_rows"]
    return landed["rows_expected"] - once


def rows_exp5(e5):
    out = []
    for key in ("timeout_below_interval", "timeout_above_interval"):
        r = e5["runs"][key]
        out.append((f"exp5:{key}",
                    f"| {n(r['transaction_timeout_ms'])} ms | "
                    f"{r['checkpoint_interval']} | {r['final_job_state']} | "
                    f"{n(r['records_written_uncommitted'])} | "
                    f"{n(r['records_readable_committed'])} | "
                    f"{n(r['records_a_downstream_consumer_never_sees'])} |"))
    return out


def _feed_order(e3):
    """The three five-second runs, in the order their feeds actually ran.

    This is the sentence that carries the whole explanation of experiment 3's
    refutation, so it is built from the measurements rather than typed.
    """
    runs = sorted((r for r in e3["runs"].values()
                   if r["checkpoint_interval"] == "5s"),
                  key=lambda r: r["feed_seconds"])
    a, b, c = runs
    return (f"{a['feed_seconds']} seconds gave {a['commits']} commits, "
            f"{b['feed_seconds']} gave {b['commits']}, and "
            f"{c['feed_seconds']} gave {c['commits']}")


def prose_facts():
    """Numbers that live in SENTENCES, not in tables, re-derived the same way.

    The tables were never the whole risk. A table row is obviously a figure and
    gets checked; a sentence that says the table froze for sixty seconds reads
    like prose and drifts silently the next time the experiment is re-run. Each
    string below is built from the results and has to appear verbatim.
    """
    e1 = load("exp1_restart_replay.json")
    e2 = load("exp2_savepoint_loss.json")
    e3 = load("exp3_commit_interval.json")
    e4 = load("exp4_maintenance_live_writer.json")
    e5 = load("exp5_transaction_timeout.json")

    w = e1["workload"]
    frozen = e2["frozen_window"]
    watermark = e2["watermark_before_restore"]
    first_above = min(t["latest_completed_checkpoint_id"]
                      for t in e2["timeline_after_restore"]
                      if t["latest_completed_checkpoint_id"] > watermark)
    short = e5["runs"]["timeout_below_interval"]
    runs3 = e3["runs"]
    size_factor = (runs3["interval_120s"]["avg_data_file_bytes"]
                   / runs3["interval_5s"]["avg_data_file_bytes"])
    interval_ms = int(short["checkpoint_interval"].rstrip("s")) * 1000

    configs = e1["configurations"]
    at_least_once_append = next(c for c in configs
                        if c["configuration"]["write_mode"] == "append"
                        and c["configuration"]["checkpointing_mode"]
                        == "AT_LEAST_ONCE")
    first_upsert = next(c for c in configs
                        if c["configuration"]["write_mode"] != "append")
    # Each cell is one run with one kill; the restored-checkpoint count is
    # what says so. The bound is the exact one-sided binomial bound for zero
    # events in that many trials, at a chosen confidence.
    kills = min(c["restart"]["checkpoints_after"]["restored"] for c in configs)
    worst = max(c["landed"]["duplicate_seq_rows"] for c in configs)
    confidence = 0.95
    bound = 1 - (1 - confidence) ** (1 / kills)
    timeline = e2["timeline_after_restore"]
    stamps = [t["seconds_since_restore"] for t in timeline]
    gaps = sorted(round(b - a, 1) for a, b in zip(stamps, stamps[1:]))
    poll = gaps[len(gaps) // 2]
    last_frozen = max(t["seconds_since_restore"] for t in timeline
                      if t["latest_completed_checkpoint_id"] <= watermark)
    first_clear = min(t["seconds_since_restore"] for t in timeline
                      if t["latest_completed_checkpoint_id"] > watermark)
    repeats = at_least_once_append["landed"]["duplicate_seq_rows"]
    expire = e4["expire_snapshots"]
    expire_dupes = expire["landed"]["duplicate_rows"]
    registered = sorted(set(short["broker_registered_the_requested_timeout"]))

    facts = [
        # The two figures the README opens with.
        ("lead:exp2_duplicates",
         f"then committed {n(e2['landed']['duplicate_rows'])} duplicate rows"),
        ("lead:exp5_records",
         f"every one of "
         f"{n(short['records_a_downstream_consumer_never_sees'])} "
         f"written records unreadable"),
        ("lead:exp1_feed",
         f"so {n(w['providers_in_the_feed'])} providers generate "
         f"{n(w['events'])} assertions"),
        ("exp1:events",
         f"one workload of {n(w['events'])} roster events"),
        ("exp1:upsert_rows",
         f"instead: {n(first_upsert['landed']['rows'])} rows, one per provider"),
        ("exp1:at_least_once_rows",
         f"It landed exactly {n(at_least_once_append['landed']['rows'])} with "
         f"{WORDS.get(repeats, n(repeats)).replace('zero', 'no')} repeated "
         f"sequence number"),
        ("exp1:resolution",
         f"a single repeat in {n(w['events'])} rows "
         f"({100 / w['events']:.3f}%)"),
        ("exp1:bound",
         f"{WORDS.get(kills, n(kills))} kill producing "
         f"{WORDS.get(worst, n(worst))} duplicates puts the "
         f"one-sided {confidence:.0%} bound on per-kill duplication "
         f"probability at about {bound:.2f}"),
        ("exp1:workload",
         f"{n(w['events'])} assertions about "
         f"{n(w['distinct_npi_expected'])} distinct providers"),
        ("exp2:savepoint",
         f"A savepoint was taken at checkpoint {e2['savepoint']['checkpoint_id']}"),
        ("exp2:watermark",
         f"the table's watermark reached {watermark}"),
        ("exp2:freeze_seconds",
         f"did not move for\nAT LEAST {frozen['seconds_frozen']} seconds"),
        ("exp2:lower_bound",
         f"READ {frozen['seconds_frozen']} as a lower bound"),
        ("exp2:poll",
         f"The sampler polls about every {poll} seconds, so "
         f"{frozen['seconds_frozen']} is simply the last sample that still "
         f"showed {n(frozen['rows_while_frozen'][0])} rows; the freeze ended "
         f"somewhere in [{last_frozen}, {first_clear}]"),
        ("exp2:longer_than",
         f"the true freeze is longer than {frozen['seconds_frozen']} by an"),
        ("exp2:frozen_rows",
         f"the table was {n(frozen['rows_while_frozen'][0])} rows behind"),
        ("exp2:clearing_checkpoint",
         f"Then checkpoint {first_above} cleared the watermark of {watermark} "
         f"and {n(frozen['rows_committed_in_one_lump'])} rows"),
        ("exp2:duplicates",
         f"and {n(e2['landed']['duplicate_rows'])} rows landed twice"),
        ("exp3:events",
         f"sustained feed of {n(e3['workload']['events'])} events"),
        ("exp3:control_delta",
         f"{n(e3['findings']['raising_the_target_to_1gb_differed_by_avg_bytes'])}"
         f" bytes of average file size"),
        ("exp3:repeat_delta",
         f"differed by zero commits and "
         f"{n(e3['findings']['repeat_of_the_same_configuration_differed_by_avg_bytes'])}"
         f" bytes"),
        ("exp3:control_residual",
         f"WHAT THE "
         f"{n(e3['findings']['raising_the_target_to_1gb_differed_by_avg_bytes'])}"
         f" bytes are made of is feed duration"),
        ("exp3:feed_order", _feed_order(e3)),
        ("exp3:size_factor",
         f"the interval from "
         f"{runs3['interval_5s']['checkpoint_interval'].rstrip('s')} seconds "
         f"to {runs3['interval_120s']['checkpoint_interval'].rstrip('s')} "
         f"changed the average file size by a factor of {size_factor:.1f}"),
        ("exp4:expired",
         f"took the table from {expire['before_expiry']['snapshots']} "
         f"snapshots to {expire['after_expiry']['snapshots']}"),
        ("exp4:restart_landed",
         f"landed all {n(expire['landed']['rows'])} rows with "
         + ("no duplicate" if expire_dupes == 0
            else f"{n(expire_dupes)} duplicates")),
        ("exp4:in_flight",
         f"while {n(e4['remove_orphan_files']['rows_in_flight_when_it_ran'])} "
         f"records were in flight"),
        ("exp5:setup",
         f"exactly-once, {n(short['events_in'])} records, and one setting"),
        ("exp5:registered",
         f"The short run registered "
         f"{' and '.join(n(r) for r in registered)} ms, the value the job set"),
        ("exp5:margin",
         f"The margin is "
         f"{interval_ms // short['transaction_timeout_ms']} to one"),
        ("exp5:records",
         f"Every one of the {n(short['events_in'])} records reached the output "
         f"topic"),
    ]
    return facts


def expected_rows():
    return (rows_exp1(load("exp1_restart_replay.json"))
            + rows_exp2(load("exp2_savepoint_loss.json"))
            + rows_exp3(load("exp3_commit_interval.json"))
            + rows_exp4(load("exp4_maintenance_live_writer.json"))
            + rows_exp5(load("exp5_transaction_timeout.json")))


def main(readme_path=None):
    """Compare, and return 0 only if every derived string was found.

    `readme_path` is here so the comparison can be tested against a document
    with a figure altered. The number printed is len(checked) - len(missing),
    so a comparison that never ran would print the maximum; a gate that
    cannot be pointed at a document it should reject cannot be shown to be
    running at all.
    """
    rows = expected_rows()
    facts = prose_facts()
    if readme_path is None and "--emit" in sys.argv:
        for tag, row in rows + facts:
            print(f"{tag}\n{row}")
        return 0
    with open(readme_path or os.path.join(ROOT, "README.md"),
              encoding="utf-8") as fh:
        readme = fh.read()
    # Whitespace is normalized on both sides. A prose figure lands wherever the
    # line wrap puts it, so matching raw text would make this script fail on a
    # reflowed paragraph: a false alarm that trains a reader to ignore it.
    flat = " ".join(readme.split())
    checked = rows + facts
    missing = [(tag, row) for tag, row in checked
               if " ".join(row.split()) not in flat]
    for tag, row in missing:
        print(f"MISSING [{tag}]\n  {row}")
    # Count what ran, not only what passed. The number of derived strings is
    # printed whether or not any are missing, so a version of this script that
    # silently stopped deriving half of them is visible instead of clean.
    print(f"\n{len(checked) - len(missing)} of {len(checked)} derived figures "
          f"found verbatim in README.md "
          f"({len(rows)} table rows, {len(facts)} in prose)")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
