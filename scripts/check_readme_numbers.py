"""Re-derive every number in README.md from results/*.json and diff them.

A README is prose and drifts; results/*.json is evidence and does not. This
script rebuilds each table row from the JSON and asserts the exact string is
present in the README, so a re-run that shifts a figure fails loudly instead
of leaving the document quietly wrong.

    python3 scripts/check_readme_numbers.py            check
    python3 scripts/check_readme_numbers.py --emit     print the rows

The emit mode is how the tables were written in the first place, which is the
only reason the two agree at all. A table typed by hand and checked afterward
is a table that gets edited until the checker stops complaining.
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
        ("the first job, all events landed",
         sum(b["first_job"]["committed_offsets"].values()),
         b["first_job"]["rows"], 0),
        ("committed offsets rewound to the start of the log",
         sum(b["offsets_rewound_to"].values()),
         b["first_job"]["rows"], 0),
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
                    f"| {label} | {n(offsets)} | {n(rows)} | {n(dupes)} |"))
    return out


def rows_exp2(e2):
    out = []
    for t in e2["timeline_after_restore"]:
        out.append((f"exp2:{t['seconds_since_restore']}",
                    f"| {t['seconds_since_restore']} | "
                    f"{t['latest_completed_checkpoint_id']} | "
                    f"{t['job_state']} | {n(t['consumer_lag'])} | "
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
         f"{n(a['landed']['duplicate_rows'])} | 0 |"),
        ("exp4:remove_orphan_files",
         f"| `remove_orphan_files` | "
         f"{b['state_when_the_procedure_ran']['storage']['data_objects_no_snapshot_references']}"
         f" unreferenced objects to take | {b['after']['job_state']} | "
         f"{n(b['landed']['rows'])} of {n(b['events_produced'])} | "
         f"{n(b['landed']['duplicate_rows'])} | {n(b['landed']['rows_lost'])} |"),
    ]


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
    e5 = load("exp5_transaction_timeout.json")

    w = e1["workload"]
    frozen = e2["frozen_window"]
    watermark = e2["watermark_before_restore"]
    first_above = min(t["latest_completed_checkpoint_id"]
                      for t in e2["timeline_after_restore"]
                      if t["latest_completed_checkpoint_id"] > watermark)
    short = e5["runs"]["timeout_below_interval"]

    facts = [
        ("exp1:workload",
         f"{n(w['events'])} assertions about "
         f"{n(w['distinct_npi_expected'])} distinct providers"),
        ("exp2:savepoint",
         f"A savepoint was taken at checkpoint {e2['savepoint']['checkpoint_id']}"),
        ("exp2:watermark",
         f"the table's watermark reached {watermark}"),
        ("exp2:freeze_seconds",
         f"did not move for\nAT LEAST {frozen['seconds_frozen']} seconds"),
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
        ("exp3:feed_order", _feed_order(e3)),
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


def main():
    rows = expected_rows()
    facts = prose_facts()
    if "--emit" in sys.argv:
        for tag, row in rows + facts:
            print(f"{tag}\n{row}")
        return 0
    with open(os.path.join(ROOT, "README.md"), encoding="utf-8") as fh:
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
