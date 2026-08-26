# roster-ingest-guarantees

[![CI](https://github.com/jkelly-dev1/roster-ingest-guarantees/actions/workflows/ci.yml/badge.svg)](https://github.com/jkelly-dev1/roster-ingest-guarantees/actions/workflows/ci.yml)

Where a Kafka to Iceberg pipeline stops being exactly-once, measured on Apache
Flink 1.20, Iceberg 1.10, Kafka 4.3 and Trino 478 in Docker Compose on one
machine. No cloud account, no managed service, and no corpus to download: the
roster feed is a pure function of a seed, so a clone reproduces the same bytes.

A personal learning project. The numbers come out of the shipped results files,
each claim names a test, and the predictions were recorded first. The refuted
ones were left where they were.

Its payload is a provider roster. Records arrive from several sources, out of
order, and corrections land after the row they correct, so 2,000 providers
generate 12,000 assertions about themselves. That makes
upsert-on-a-natural-key against plain append a real design question rather
than a stylistic one.

Five questions, each answered with numbers:

1. What does a restart actually replay, and which setting decides it?
2. What does restoring from a savepoint older than the table do?
3. What decides the size of the files a streaming writer produces?
4. What does table maintenance do to a job that is still writing?
5. What does a Kafka transaction timeout shorter than the checkpoint interval
   cost the pipeline's output?

What the four configurations showed. Turning off exactly-once checkpointing
changed nothing: all four configurations survived a TaskManager kill with every
event landed once. What did cost something was an old savepoint, which froze the
table for a minute while the job reported RUNNING and Kafka consumer lag read
zero and then committed 12,000 duplicate rows; a job that believed Kafka's
committed offsets, which replayed the whole log; and a Kafka transaction timeout
under the checkpoint interval, which made every one of 8,000 written records
unreadable while nothing reported an error.

## 1. What a restart actually replays

Four job configurations, one workload of 12,000 roster events across four
partitions, and one `docker kill` of the TaskManager while the feed was still
running. The table is read back through Trino, never through Flink. A commit
only the engine that wrote it can read is not an Iceberg commit.

| Configuration | Rows landed | Duplicate seq rows | Rows sharing an NPI | Delete files | Predicted | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| `at-least-once` + append | 12,000 | 0 | 10,002 | 0 | duplicate rows | REFUTED |
| `at-least-once` + upsert on npi | 1,998 | 0 | 0 | 12 | no duplicates, idempotent by key | held |
| `exactly-once` + append | 12,000 | 0 | 10,002 | 0 | no duplicates | held |
| `exactly-once` + upsert on npi | 1,998 | 0 | 0 | 12 | no duplicates | held |

`Rows sharing an NPI` is not an error in the append rows. The feed makes 12,000
assertions about 1,998 distinct providers, so an append table is supposed to
hold about six rows for the same doctor. The column is there to show what upsert
on the natural key does instead: 1,998 rows, one per provider, each carrying
that provider's LATEST record: checked against the generator's own answer rather
than against whatever arrived.

The prediction in the first row was wrong and it is still in the code.
At-least-once checkpointing was predicted to duplicate rows. It landed exactly
12,000 with no repeated sequence number at all, the same as exactly-once.
`execution.checkpointing.mode` is not the knob that decides duplicates against
an Iceberg sink. The sink commits at checkpoint boundaries through its own
two-phase protocol and stamps `flink.max-committed-checkpoint-id` into the
snapshot summary; a job that restarts internally keeps its job id, matches that
watermark, and suppresses the replay before barrier alignment gets a chance to
matter.

How strong is this null? Weaker than it reads. Each of the four cells is ONE run
with ONE kill, always after exactly two completed checkpoints and at about half
the feed. Within a run the resolution is excellent. Duplicate sequence numbers
are counted exactly, so a single repeat in 12,000 rows (0.008%) would have
shown, so "this kill produced no duplicates" is fully supported. What is NOT
supported is the general reading, "the mode does not matter": one kill producing
zero duplicates puts the one-sided 95% bound on per-kill duplication probability
at about 0.95, which cannot separate "impossible" from "usually does not
happen". Kill timing, kill target, parallelism, checkpoint interval and
partition count were never varied.

And the setting is not verified as in force. The only evidence that
`AT_LEAST_ONCE` actually applied is the SQL string sent to the job and the
results echoing the value that was REQUESTED. Experiment 5 does this properly;
it asks the broker what timeout it registered, precisely because "a setting that
never reached the component" and "a rule that does not bite" are
indistinguishable from the outside. That guard exists three sections later and
is missing from the repository's headline null.

Idempotence is not free. Both upsert runs wrote equality delete files beside
their data files, in the column above, and both append runs wrote none. Every
reader of that table pays for them afterward.

### What the committed Kafka offsets are worth

Flink commits the consumer group's offsets on every completed checkpoint, which
makes them the number a monitoring dashboard has to show. They are not the
number the job recovers from. Reading both and comparing them proves nothing,
because they normally agree, so the offsets were rewound to the start of the log
behind the job's back instead.

| Step | Committed offsets, summed | Rows in the table | Duplicate rows |
| --- | --- | --- | --- |
| the first job, all events landed | 6,000 | 6,000 | 0 |
| committed offsets rewound to the start of the log | 0 | 6,000 | 0 |
| restored from the retained checkpoint | 6,000 | 6,000 | 0 |
| rewound again, restarted from `group-offsets` | 0 | 12,000 | 6,000 |

The third row is the finding. Every committed offset was set to zero and the
restored job replayed nothing, because its position came out of the checkpoint;
the offsets were simply overwritten at its next one. The fourth row is the same
rewind believed: a fresh job with `scan.startup.mode = group-offsets`, which is
what an operator gets when a job will not restart and they resubmit it from
scratch. It read the whole log a second time and doubled the table.

## 2. What an old savepoint really does

The prediction, written before the run and kept after it was refuted: restoring
from a savepoint older than the table's newest commit LOSES every record read
while the restored job's checkpoint ids are at or below
`flink.max-committed-checkpoint-id`, because the Iceberg committer skips any
checkpoint at or below that watermark.

The watermark is real, the skip is real, and the job does stay RUNNING. The
loss is not real. A savepoint was taken at checkpoint 5, the job ran on until
the table's watermark reached 19, the job was stopped, a fresh batch of
records was written to Kafka, and then the job was restored from that old
savepoint:

| Seconds after the restored job's first checkpoint | Job's newest completed checkpoint | Job state | Kafka consumer lag | Rows visible in Trino |
| --- | --- | --- | --- | --- |
| 0.0 | 7 | RUNNING | 0 | 16,000 |
| 7.2 | 9 | RUNNING | 0 | 16,000 |
| 14.5 | 10 | RUNNING | 0 | 16,000 |
| 21.7 | 12 | RUNNING | 0 | 16,000 |
| 29.2 | 13 | RUNNING | 0 | 16,000 |
| 36.4 | 14 | RUNNING | 0 | 16,000 |
| 43.6 | 16 | RUNNING | 0 | 16,000 |
| 50.9 | 17 | RUNNING | 0 | 16,000 |
| 58.1 | 19 | RUNNING | 0 | 16,000 |
| 65.4 | 20 | RUNNING | 0 | 32,000 |

Read the checkpoint column against the last one. The table did not move for
AT LEAST 58.1 seconds while the restored job completed checkpoint after
checkpoint.

READ 58.1 as a lower bound, not a stopwatch reading. The sampler polls about
every 7.2 seconds, so 58.1 is simply the last sample that still showed 16,000
rows; the freeze ended somewhere in [58.1, 65.4]. The clock also starts when the
restored job completes its FIRST checkpoint, not at restore, that is why this
column is labeled the way it is, so the true freeze is longer than 58.1 by an
unmeasured amount. "About a minute" is the honest summary and the figure to
quote is the bound, not the point. No exception was recorded, the job never left
RUNNING, not one further checkpoint failed, and the Kafka consumer lag went to
zero, because the records really had been consumed. Every signal an on-call
rotation watches was green while the table was 16,000 rows behind. Then
checkpoint 20 cleared the watermark of 19 and 16,000 rows arrived in a single
commit.

The one blemish is honest and is not the signal either: a checkpoint can fail at
the moment of restore, and in one recorded run one did. It had already happened
before the first sample, and the counter does not move again for the whole
freeze, so the thing an operator might notice fires at the restart everybody
expects to be noisy, and stays quiet through the minute that actually matters.

What the committer does with a checkpoint at or below the watermark is nothing,
and nothing is not discarding. The files stay in its state and the first
checkpoint that clears the watermark commits the whole accumulation at once. So
the hazard is not loss. It is these two:

- **The table freezes while everything reports healthy.** For as long as the
  restored job's counter is climbing back to the watermark, a downstream
  reader sees stale data and no instrument says so.
- **The replay is committed, so rows duplicate.** The savepoint rewound the
  source, everything between that offset and the newest commit was read again,
  and 12,000 rows landed twice. Zero rows were lost and there is no gap in the
  sequence at all.

Where the skip does work is the internal restart in section 1: a TaskManager
kill keeps the job id, the watermark matches, and the replay is suppressed.
Same rule, opposite outcome, and the difference is which kind of restart
happened.

## 3. The checkpoint interval is the file-size knob

`write.target-file-size-bytes` defaults to 512 MB and is the first thing anyone
reaches for when a streaming table grows a million tiny Parquet files. It does
not help. Target size makes a writer ROLL OVER to a new file when the current
one gets too big; it has nothing to say about a writer that is told to close
and commit long before it gets there. What decides file size in a streaming
write is the checkpoint interval, because every checkpoint closes every open
file and commits it.

The same sustained feed of 60,000 events at three checkpoint intervals, plus
two more runs at the shortest interval: one with the target raised to 1 GB,
and one repeating the first configuration unchanged.

| Checkpoint interval | Target file size | Commits | Data files | Files per commit | Average data file bytes | Last record to visible |
| --- | --- | --- | --- | --- | --- | --- |
| `120s` | Iceberg default | 3 | 6 | 2.0 | 74,556 | 90.7 s |
| `30s` | Iceberg default | 8 | 16 | 2.0 | 34,545 | 25.9 s |
| `5s` | Iceberg default | 39 | 78 | 2.0 | 13,259 | 1.3 s |
| `5s`, run again | Iceberg default | 39 | 78 | 2.0 | 13,249 | 1.2 s |
| `5s` | 1 GB | 40 | 80 | 2.0 | 13,041 | 1.4 s |

Every commit produced exactly one file per writer subtask, at every interval
and both target sizes, and a 1 GB target produced nothing remotely like 1 GB
files: the largest average in any run is four orders of magnitude below it.

The prediction said "changes nothing at all", and in that strict form it is
wrong. The 1 GB run differed from its baseline by one commit and 218 bytes of
average file size. That is exactly why the fifth run exists; it repeats the
first configuration UNCHANGED, and it differed by zero commits and 10 bytes. The
control moved further than a repeat did, so the verdict is recorded as refuted
and stays there.

WHAT THE 218 bytes are made of is feed duration, not target size. All three
five-second runs commit once per interval, so the commit count is set by how
long the producer happened to run: 197.3 seconds gave 39 commits, 198.1 gave 39,
and 198.6 gave 40. The 1 GB run is simply the longest feed of the three, and one
extra commit spreads the same data over two more files. A test asserts that
ordering. The conclusion itself is not asserted.

Put beside the effect that is real, the residual is a rounding error: moving
the interval from 5 seconds to 120 changed the average file size by a factor of
five and a half, and raising the target changed it by under two percent. That
comparison is only available because the repeat run gives the residual
something to be measured against.

File size and freshness are the same dial. A short interval gives a fresh table
made of small files; a long one gives larger files a reader sees late. There is
no setting that gives both. The only escape is compaction downstream, which is
not free either.

## 4. Maintenance against a live writer

Iceberg maintenance is presented as housekeeping, and neither procedure knows
that a Flink job is in the middle of writing to the table. Both were run from
Trino against a table a job still owned.

| Procedure | What it had to work with | Job afterward | Rows landed | Duplicates | Rows lost |
| --- | --- | --- | --- | --- | --- |
| `expire_snapshots` | 5 snapshots to 1 | RUNNING | 12,000 of 12,000 | 0 | 0 |
| `remove_orphan_files` | 0 unreferenced objects to take | RUNNING | 8,000 of 8,000 | 0 | 0 |

Both predictions were wrong, and in both cases the measurement says why. A null
result that does not say what the procedure had to work with is not a result, it
is a shrug.

- `expire_snapshots` took the table from five snapshots to one and the
  watermark did not move. IT CANNOT. `flink.max-committed-checkpoint-id` lives
  in the summary of the newest snapshot, and expiring snapshots keeps the
  newest one by definition. The real exposure was supposed to be the next
  restart, so the TaskManager was killed after the expiry: the job restored,
  kept its job id, found the watermark exactly where it had left it, and
  landed all 12,000 rows with no duplicate.
- `remove_orphan_files` ran with its retention threshold at zero while 4,000
  records were in flight, and there were ZERO unreferenced data objects in the
  bucket for it to take. An in-flight Parquet file is an open multipart upload,
  not an object. Nothing exists to classify as an orphan until the checkpoint
  closes it, and closing it is the same event that commits it.

The honest form of this section is that the window these procedures are
dangerous in is far narrower than it looks from the documentation, not that
they are safe. Neither result says anything about a table with several
writers, a failed job that left files behind, or an object store that
materializes partial uploads.

## 5. Transaction timeout against checkpoint interval

The rule under test, and the only one of the five that does not apply to the
roster pipeline as built:

```
checkpoint interval + worst-case checkpoint duration + restart time
  < transaction.timeout.ms <= broker transaction.max.timeout.ms
```

It applies to a job that writes to Kafka TRANSACTIONALLY, and the Kafka to
Iceberg path never opens a Kafka transaction: its exactly-once comes from
Iceberg's own two-phase commit against the catalog. So a second pipeline was
built to test it. Kafka in, Kafka out, `sink.delivery-guarantee` set to
exactly-once, 8,000 records, and one setting different between the two runs.

| `transaction.timeout.ms` | Checkpoint interval | Final job state | Records written | Records a consumer can read | Records never visible |
| --- | --- | --- | --- | --- | --- |
| 5,000 ms | 120s | RUNNING | 8,000 | 0 | 8,000 |
| 300,000 ms | 120s | RUNNING | 8,000 | 8,000 | 0 |

With the timeout under the interval, a downstream consumer saw nothing at all.
Every one of the 8,000 records reached the output topic, a `read_uncommitted`
consumer counts all of them, and every transaction carrying them was aborted by
the broker before Flink asked it to commit. `read_committed`, which is what a
real consumer uses, returned zero. The job stayed RUNNING with no failure
recorded and no failed checkpoint. This is the one prediction of the five that
held outright, and it is also the most complete failure in the repository: a
pipeline that loses one hundred percent of its output while every instrument
says it is fine.

Two things make that a result rather than a coincidence. The margin is
twenty-four to one: a narrow one cannot distinguish a rule that does not apply
from a test too gentle to trigger it, and there is no way to tell those apart
from the record afterward. And the broker is asked directly rather than trusted:
`kafka-transactions.sh describe` reports the timeout a coordinator actually
registered against each transactional id, and the states it moved through. The
short run registered 5,000 ms, exactly as requested, and its transactions ended
in `CompleteAbort`; the long run's ended in `CompleteCommit`.

## Claims backed by tests

Run `pytest -q`. The suite is offline: it tests the measurement logic as pure
functions and the shipped `results/*.json` as evidence. It never needs Docker,
Kafka or Flink.

| Claim | Test |
| --- | --- |
| Every configuration really did restart from a checkpoint | `tests/test_results_invariants.py::TestTheFailureWasReallyInduced::test_every_configuration_actually_restarted` |
| Every configuration recorded a real failure behind that restart | `tests/test_results_invariants.py::TestTheFailureWasReallyInduced::test_every_configuration_recorded_a_real_failure` |
| The kill landed while data was still arriving | `tests/test_results_invariants.py::TestTheFailureWasReallyInduced::test_the_kill_landed_while_data_was_still_arriving` (mutation-checked: record the kill after the feed drained and it fails) |
| An append table holds every event exactly once after the kill | `tests/test_results_invariants.py::TestRestartReplay::test_an_append_table_holds_every_event_exactly_once` |
| An upsert table holds one row per provider | `tests/test_results_invariants.py::TestRestartReplay::test_an_upsert_table_holds_one_row_per_provider` |
| Every provider carries its latest record, checked against the seed | `tests/test_results_invariants.py::TestRestartReplay::test_every_provider_carries_its_latest_record` (mutation-checked: keep one older revision and it fails) |
| The checkpointing mode changed nothing at all | `tests/test_results_invariants.py::TestRestartReplay::test_the_checkpointing_mode_changed_nothing` |
| The generator still produces the bytes the results describe, pinned by digest rather than by comparing two runs of the same code | `tests/test_measurement_logic.py::TestGenerator::test_the_generator_still_produces_the_bytes_the_results_describe` (mutation-checked: change `SALT` and it fails) |
| The README checker still derives every published figure, so it cannot pass by checking nothing | `tests/test_results_invariants.py::TestTheReadmeCheckerStillChecks::test_it_still_derives_a_row_for_every_published_table_row`, `::TestTheReadmeCheckerStillChecks::test_every_headline_number_is_among_the_derived_strings` (mutation-checked: empty `expected_rows()` or `prose_facts()` and it fails) |
| The at-least-once prediction stays recorded as refuted | `tests/test_results_invariants.py::TestRestartReplay::test_the_at_least_once_prediction_is_recorded_as_refuted` (mutation-checked: "correct" it in the results and it fails) |
| Upsert pays for idempotence in equality delete files | `tests/test_results_invariants.py::TestRestartReplay::test_upsert_pays_for_itself_in_delete_files` |
| Append writes no delete files at all | `tests/test_results_invariants.py::TestRestartReplay::test_append_writes_no_delete_files_at_all` |
| The committed offsets really were rewound to the start | `tests/test_results_invariants.py::TestOffsetsAreMonitoring::test_the_offsets_were_really_rewound_to_the_start` |
| Recovery ignored the rewound offsets entirely | `tests/test_results_invariants.py::TestOffsetsAreMonitoring::test_recovery_ignored_the_rewound_offsets_entirely` (mutation-checked: record a single duplicate and it fails) |
| A job that trusted the offsets replayed the whole log | `tests/test_results_invariants.py::TestOffsetsAreMonitoring::test_a_job_that_trusted_the_offsets_replayed_the_whole_log` |
| The savepoint really was older than the table's newest commit | `tests/test_results_invariants.py::TestSavepointRestore::test_the_savepoint_was_really_older_than_the_table` |
| The predicted loss did not happen and stays recorded as refuted | `tests/test_results_invariants.py::TestSavepointRestore::test_the_predicted_loss_did_not_happen_and_stays_recorded` (mutation-checked: record any lost row, or flip the verdict, and it fails) |
| The replay was committed a second time | `tests/test_results_invariants.py::TestSavepointRestore::test_the_replay_was_committed_a_second_time` |
| The table did not move while the job was healthy | `tests/test_results_invariants.py::TestSavepointRestore::test_the_table_did_not_move_while_the_job_was_healthy` (mutation-checked: let it move during the freeze and it fails) |
| Not one further checkpoint failed during the freeze | `tests/test_results_invariants.py::TestSavepointRestore::test_no_further_checkpoint_failed_during_the_freeze` |
| Consumer lag reached zero while the table was stale | `tests/test_results_invariants.py::TestSavepointRestore::test_the_consumer_lag_reached_zero_while_the_table_was_stale` (mutation-checked: raise the minimum lag above zero and it fails) |
| The freeze ended exactly at the first checkpoint above the watermark | `tests/test_results_invariants.py::TestSavepointRestore::test_the_freeze_ended_at_the_first_checkpoint_above_the_watermark` (mutation-checked: move one sample across the boundary and it fails) |
| Nothing was discarded: the backlog committed in one lump | `tests/test_results_invariants.py::TestSavepointRestore::test_the_backlog_committed_in_one_lump` |
| The job never reported anything wrong | `tests/test_results_invariants.py::TestSavepointRestore::test_the_job_never_reported_anything_wrong` |
| Data written after the watermark cleared still lands | `tests/test_results_invariants.py::TestSavepointRestore::test_data_written_after_the_watermark_cleared_still_lands` |
| Every commit-interval run landed the whole feed | `tests/test_results_invariants.py::TestCommitInterval::test_every_run_landed_the_whole_feed` |
| A longer interval gives fewer and larger files | `tests/test_results_invariants.py::TestCommitInterval::test_a_longer_interval_gives_fewer_and_larger_files` |
| Files per commit tracks the writer subtasks, not the data | `tests/test_results_invariants.py::TestCommitInterval::test_files_per_commit_tracks_the_writers_not_the_data` (mutation-checked: record one commit above the writer count and it fails) |
| The target-size prediction stays recorded as refuted | `tests/test_results_invariants.py::TestCommitInterval::test_the_target_size_prediction_is_recorded_as_refuted` (mutation-checked: flip the verdict, or claim the control matched the repeat, and it fails) |
| The residual difference tracks feed duration, not the target | `tests/test_results_invariants.py::TestCommitInterval::test_the_residual_tracks_feed_duration_and_not_the_target` |
| A 1 GB target still left every file four orders of magnitude below it | `tests/test_results_invariants.py::TestCommitInterval::test_raising_the_target_left_every_file_four_orders_below_it` |
| The interval moves file size hundreds of times further than the target does | `tests/test_results_invariants.py::TestCommitInterval::test_the_interval_moves_file_size_hundreds_of_times_further` |
| The repeat run exists, so the comparison has a baseline | `tests/test_results_invariants.py::TestCommitInterval::test_the_repeat_run_exists_so_the_comparison_has_a_baseline` |
| Freshness is what the larger files cost | `tests/test_results_invariants.py::TestCommitInterval::test_freshness_is_the_price_of_the_larger_files` |
| `expire_snapshots` really expired something | `tests/test_results_invariants.py::TestMaintenanceAgainstALiveWriter::test_expire_snapshots_really_expired_something` |
| The watermark survived the expiry, because the newest snapshot did | `tests/test_results_invariants.py::TestMaintenanceAgainstALiveWriter::test_the_watermark_survived_the_expiry` (mutation-checked: move the watermark across the expiry and it fails) |
| The writer kept going and the restart afterward lost nothing | `tests/test_results_invariants.py::TestMaintenanceAgainstALiveWriter::test_the_writer_kept_going_and_the_restart_lost_nothing` |
| The bucket listing actually saw the bucket before concluding it was empty | `tests/test_results_invariants.py::TestMaintenanceAgainstALiveWriter::test_the_bucket_listing_actually_saw_the_bucket` |
| `remove_orphan_files` had nothing to take | `tests/test_results_invariants.py::TestMaintenanceAgainstALiveWriter::test_remove_orphan_files_had_nothing_to_take` (mutation-checked: record one unreferenced object and the null result stops being explained) |
| The in-flight rows still arrived | `tests/test_results_invariants.py::TestMaintenanceAgainstALiveWriter::test_the_in_flight_rows_still_arrived` |
| The two transaction runs differ only in the timeout | `tests/test_results_invariants.py::TestTransactionTimeout::test_the_two_runs_differ_only_in_the_timeout` |
| The broker really registered the 5 second timeout | `tests/test_results_invariants.py::TestTransactionTimeout::test_the_broker_really_registered_the_short_timeout` |
| A timeout under the interval cost the reader everything | `tests/test_results_invariants.py::TestTransactionTimeout::test_a_timeout_under_the_interval_costs_the_reader_everything` (mutation-checked: count the topic at read_uncommitted and it fails) |
| The transactions were aborted, not merely slow | `tests/test_results_invariants.py::TestTransactionTimeout::test_the_transactions_were_aborted_and_not_merely_slow` (mutation-checked: drop `CompleteAbort` from the observed states and it fails) |
| The job reported nothing wrong while losing everything | `tests/test_results_invariants.py::TestTransactionTimeout::test_the_job_reported_nothing_wrong_while_losing_everything` |
| Raising the timeout recovers every record | `tests/test_results_invariants.py::TestTransactionTimeout::test_raising_the_timeout_recovers_every_record` |
| Every experiment recorded a prediction and a verdict | `tests/test_results_invariants.py::TestEveryExperimentRecordedItsPrediction::test_a_prediction_and_a_verdict_are_on_record` |
| Duplicates count extra rows, not values that repeat | `tests/test_measurement_logic.py::TestDuplicateCounting::test_counts_extra_rows_not_repeated_values` |
| Missing rows are reported as runs, not as a count | `tests/test_measurement_logic.py::TestMissingRange::test_reports_runs_and_not_a_count` (mutation-checked: return the count and the shape of a loss disappears) |
| A gap that runs to the end of the feed is still closed | `tests/test_measurement_logic.py::TestMissingRange::test_a_gap_that_runs_to_the_end_is_still_closed` |
| A real error survives output cleaning | `tests/test_measurement_logic.py::TestOutputCleaning::test_a_real_error_survives_cleaning` (mutation-checked: discard stderr wholesale and a failed statement looks like an empty result) |
| Upsert needs the primary key and the flag together | `tests/test_measurement_logic.py::TestJobSql::test_upsert_needs_the_key_and_the_flag_together` (mutation-checked: drop either one and it fails) |
| Restart attempts are raised, so a kill does not end the job | `tests/test_measurement_logic.py::TestJobSql::test_restart_attempts_are_raised_so_a_kill_does_not_end_the_job` |
| The same seed gives the same bytes | `tests/test_measurement_logic.py::TestGenerator::test_the_same_seed_gives_the_same_bytes` |
| Providers recur, which is what makes upsert a real alternative | `tests/test_measurement_logic.py::TestGenerator::test_providers_recur_which_is_what_makes_upsert_a_real_choice` |
| The expected table is computed from the seed, not from the run | `tests/test_measurement_logic.py::TestExpectedState::test_the_expected_table_is_computed_from_the_seed_not_the_run` |
| Every number in this README is derived from `results/*.json` | `scripts/check_readme_numbers.py`, run in CI (mutation-checked: change one figure in the README and it fails) |

## Running it

The tests need nothing but pytest. Reproducing the measurements needs Docker
and about 6 GB of free memory.

```
python3 -m venv envs && ./envs/bin/pip install "pytest>=8,<10"
./envs/bin/pytest -q
python3 scripts/check_readme_numbers.py
```

```
cd stack && docker compose up -d
docker exec rig-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic roster.updates \
  --partitions 4 --replication-factor 1
```

Then, from the repository root, the end-to-end smoke test:

```
./scripts/produce.sh 1000
./scripts/flink_sql.sh sql/e0_smoke_append.sql
./scripts/q.sh "SELECT count(*) FROM iceberg.roster.providers_append"
```

And the experiments, each of which creates and drops its own topics and
tables:

```
PYTHONPATH=scripts ./envs/bin/python scripts/exp1_restart_replay.py
PYTHONPATH=scripts ./envs/bin/python scripts/exp2_savepoint_loss.py
PYTHONPATH=scripts ./envs/bin/python scripts/exp3_commit_interval.py
PYTHONPATH=scripts ./envs/bin/python scripts/exp4_maintenance_live_writer.py
PYTHONPATH=scripts ./envs/bin/python scripts/exp5_transaction_timeout.py
```

On the machine they were recorded on they took roughly eight, four, twenty,
six and eleven minutes. `SAMPLE_RUN.md` is the captured output of exactly
those commands.

The stack binds every port to loopback and picks unusual ones, the Flink UI on
8091, Trino on 8092, the catalog on 8182, MinIO on 9010 and 9011, so it does not
collide with anything already running. The MinIO credentials in
`stack/compose.yaml` are fixtures, not secrets: MinIO needs a root user to start
and the whole stack is bound to 127.0.0.1.

### Five traps in this stack

All five are handled in the files and commented there. They are collected here
because every one of them presents as something other than what it is, so a
reader building the same stack loses time to them in the same way.

- **Hadoop is required even though nothing uses HDFS.** Iceberg's Flink
  catalog factory constructs an `org.apache.hadoop.conf.Configuration`
  unconditionally, so `CREATE CATALOG` dies with a bare
  `ClassNotFoundException` naming Hadoop while every byte is going to S3FileIO
  against MinIO.
- **Kafka needs a named volume, and not for convenience.** Rebuilding the Flink
  image recreated the broker and wiped the topic. A replay experiment against an
  empty log reads nothing and reports a clean run.
- **The checkpoint volume mounts as root** while Flink runs as uid 9999. It
  presents as `Failed to create directory for shared state`, which points at
  the state backend rather than at permissions.
- **`kafka-console-producer` never returns** when fed through `docker exec -i`
  from a host pipe. Copy the file into the container and redirect there.
- **Compose derives the project name from the containing directory.** This
  file lives in one called `stack/`, so two unrelated projects that both do
  that share one project namespace: bringing one up treats the other's
  containers as orphans, and taking one down removes them. `compose.yaml` sets
  an explicit `name:` for that reason.

## What this does not measure

- **One machine, one broker, four partitions.** Nothing here says what happens
  across a real cluster. Every timing and freshness figure carries that
  caveat, and none of them is a benchmark.
- **One version of each engine.** Flink 1.20 (the image is `flink:1.20`, a
  moving tag. The exact patch was not captured), Iceberg 1.10.0, Kafka 4.3.1,
  Trino 478. Section 2's result in particular is a fact about how Iceberg 1.10's
  committer treats a checkpoint at or below the watermark, and another minor
  version could behave differently.
- **Not a throughput benchmark.** The measurements are about CORRECTNESS UNDER
  FAILURE. Any number that looks like a benchmark should be read as the first
  caveat says to read it.
- **No equality-delete read cost.** The upsert path writes equality deletes
  and this repository counts them; what they cost a reader afterward is not
  measured here.
- **Two null results, and their limits.** Neither maintenance procedure broke
  a live writer, and section 4 says what each one had to work with. That is
  evidence about this shape of pipeline, not a general safety claim.
- **Synthetic roster.** Generated from a seed, no corpus. It is a plausible
  shape, not real provider data, and no claim here depends on the values.
- **Byte totals move slightly between runs.** Parquet file sizes vary with
  write parallelism. The qualitative results reproduce; the byte figures in
  the tables come from the recorded run in `results/`, and section 3 measures
  that variation rather than assuming it away.

## Related repositories

[iceberg-evolution-cost](https://github.com/jkelly-dev1/iceberg-evolution-cost)
is the one to read directly against this repository, and the two were built to
be read that way. It measures what Iceberg table changes cost on a static
20,000,000 row table with Trino: which changes are metadata-only, what
partition evolution costs every query afterward, and that `expire_snapshots`
reclaims exactly zero bytes while `remove_orphan_files` is what frees the
space. That last pair is the fact section 4 here puts a live writer next to.

[roster-entity-resolution](https://github.com/jkelly-dev1/roster-entity-resolution)
takes the other half of the same problem. This repository measures getting
a roster feed in exactly once; that one measures deciding which of the rows
in four disagreeing rosters are the same provider, and where to draw the
matching line once a false match is priced above a missed one. It needs a
single Postgres container and none of the stack here.

All three follow the same rules: no claim without a test, mutation checks on the
tests that matter, every number re-derived from the shipped results by script,
and predictions recorded before the run so the refuted ones survive.

## License

MIT. See `LICENSE`.
