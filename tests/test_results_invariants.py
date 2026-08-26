"""Tests over the RESULTS THAT SHIP, not over a fresh run.

results/*.json is evidence committed to the repository. These tests assert
three kinds of thing about it:

  The failure really happened. Three of these experiments kill a TaskManager
  or restore from a stale savepoint on purpose, and an experiment whose
  induced failure MISSED looks exactly like one that passed. Every claim about
  surviving a restart is guarded by an assertion that the restart occurred.

  The findings themselves. The qualitative results the README claims, in the
  run-invariant form. Exact byte totals are not asserted: Parquet file sizes
  move between runs with write parallelism, and a test that pinned them would
  fail for a reason that means nothing.

  The refuted predictions stay refuted. Two of the five predictions written
  down before the runs were wrong. Those are the only parts of this repository
  a reader could not have got from the documentation, and a later tidy-up must
  not be able to quietly correct them.
"""

import json
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load(name):
    with open(os.path.join(ROOT, "results", name), encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="module")
def exp1():
    return load("exp1_restart_replay.json")


@pytest.fixture(scope="module")
def exp2():
    return load("exp2_savepoint_loss.json")


def cfg(exp1, mode, write_mode):
    return next(c for c in exp1["configurations"]
                if c["configuration"]["checkpointing_mode"] == mode
                and c["configuration"]["write_mode"] == write_mode)


APPEND_CONFIGS = [("AT_LEAST_ONCE", "append"), ("EXACTLY_ONCE", "append")]
UPSERT_CONFIGS = [("AT_LEAST_ONCE", "upsert on npi"),
                  ("EXACTLY_ONCE", "upsert on npi")]
ALL_CONFIGS = APPEND_CONFIGS + UPSERT_CONFIGS


class TestTheFailureWasReallyInduced:
    """Guards. Every claim below them depends on the kill having landed."""

    @pytest.mark.parametrize("mode,write_mode", ALL_CONFIGS)
    def test_every_configuration_actually_restarted(self, exp1, mode, write_mode):
        c = cfg(exp1, mode, write_mode)
        assert c["restart"]["restored_from"] is not None
        assert c["restart"]["restored_from"]["checkpoint_id"] >= 1
        assert c["restart"]["checkpoints_after"]["restored"] >= 1

    @pytest.mark.parametrize("mode,write_mode", ALL_CONFIGS)
    def test_every_configuration_recorded_a_real_failure(self, exp1, mode,
                                                         write_mode):
        # A restart with no exception behind it is a job that was restarted
        # by something other than the kill, and proves nothing about recovery.
        assert cfg(exp1, mode, write_mode)["restart"]["failure_causes"]

    @pytest.mark.parametrize("mode,write_mode", ALL_CONFIGS)
    def test_the_kill_landed_while_data_was_still_arriving(self, exp1, mode,
                                                           write_mode):
        # Mutation check anchor. If the kill lands after the feed has drained,
        # the experiment measures a cold start on an idle job and every
        # configuration passes for the wrong reason. Rows at the moment of the
        # kill have to be a real fraction of the total, and short of it.
        c = cfg(exp1, mode, write_mode)
        landed_at_kill = c["before_kill"]["rows"]
        assert 0 < landed_at_kill < c["events_produced"]

    def test_the_table_was_already_committed_when_the_kill_landed(self, exp1):
        for c in exp1["configurations"]:
            assert c["before_kill"]["committed_checkpoint_id"] >= 1


class TestRestartReplay:
    @pytest.mark.parametrize("mode,write_mode", APPEND_CONFIGS)
    def test_an_append_table_holds_every_event_exactly_once(self, exp1, mode,
                                                            write_mode):
        c = cfg(exp1, mode, write_mode)
        assert c["landed"]["rows"] == c["events_produced"]
        assert c["landed"]["duplicate_seq_rows"] == 0
        assert c["landed"]["missing_seq_runs"] == []
        assert c["landed"]["complete"] is True

    @pytest.mark.parametrize("mode,write_mode", UPSERT_CONFIGS)
    def test_an_upsert_table_holds_one_row_per_provider(self, exp1, mode,
                                                        write_mode):
        c = cfg(exp1, mode, write_mode)
        assert c["landed"]["duplicate_npi_rows"] == 0
        assert c["landed"]["providers_held"] == c["landed"]["providers_expected"]

    @pytest.mark.parametrize("mode,write_mode", UPSERT_CONFIGS)
    def test_every_provider_carries_its_latest_record(self, exp1, mode,
                                                      write_mode):
        # Mutation check anchor. Row count alone cannot tell a correct upsert
        # from one that kept an older revision. This is checked against the
        # generator's own answer, not against whatever arrived.
        c = cfg(exp1, mode, write_mode)
        assert c["landed"]["providers_not_carrying_the_latest_record"] == 0
        assert c["landed"]["complete"] is True

    def test_the_checkpointing_mode_changed_nothing(self, exp1):
        # The headline of experiment 1. Both append runs landed the identical
        # result and so did both upsert runs, which is what refutes the
        # prediction: the knob a reader would reach for is not the one that
        # decides duplicates on this sink.
        for write_mode in ("append", "upsert on npi"):
            at_least, exactly = (cfg(exp1, "AT_LEAST_ONCE", write_mode),
                                 cfg(exp1, "EXACTLY_ONCE", write_mode))
            assert at_least["landed"]["rows"] == exactly["landed"]["rows"]
            assert (at_least["landed"]["duplicate_seq_rows"]
                    == exactly["landed"]["duplicate_seq_rows"] == 0)

    def test_the_at_least_once_prediction_is_recorded_as_refuted(self, exp1):
        # Mutation check anchor. Set prediction_held true, or delete the
        # prediction, and this fails. The refuted prediction is the finding.
        c = cfg(exp1, "AT_LEAST_ONCE", "append")
        assert c["prediction"] == "duplicate rows"
        assert c["prediction_held"] is False
        assert c["landed"]["duplicate_seq_rows"] == 0

    def test_the_other_three_predictions_held(self, exp1):
        for mode, write_mode in ALL_CONFIGS[1:]:
            assert cfg(exp1, mode, write_mode)["prediction_held"] is True

    @pytest.mark.parametrize("mode,write_mode", UPSERT_CONFIGS)
    def test_upsert_pays_for_itself_in_delete_files(self, exp1, mode,
                                                    write_mode):
        # Idempotence on a natural key is not free: the sink writes equality
        # deletes beside the data. An append table writes none.
        assert cfg(exp1, mode, write_mode)["landed"]["files"]["delete_files"] > 0

    @pytest.mark.parametrize("mode,write_mode", APPEND_CONFIGS)
    def test_append_writes_no_delete_files_at_all(self, exp1, mode, write_mode):
        assert cfg(exp1, mode, write_mode)["landed"]["files"]["delete_files"] == 0


class TestOffsetsAreMonitoring:
    def test_the_offsets_were_really_rewound_to_the_start(self, exp1):
        # The guard again: a rewind that did not happen proves nothing.
        b = exp1["offsets_are_monitoring"]
        assert b["offsets_rewound_to"]
        assert all(v == 0 for v in b["offsets_rewound_to"].values())
        assert any(v > 0 for v in b["first_job"]["committed_offsets"].values())

    def test_recovery_ignored_the_rewound_offsets_entirely(self, exp1):
        # Mutation check anchor. Every committed offset was set to zero and
        # the restored job replayed nothing: it took its position from the
        # checkpoint. Record a single duplicate here and it fails.
        b = exp1["offsets_are_monitoring"]
        r = b["restored_from_checkpoint"]
        assert r["rows"] == b["events_produced"]
        assert r["duplicate_rows"] == 0

    def test_a_job_that_trusted_the_offsets_replayed_the_whole_log(self, exp1):
        # The same rewind, believed. This is what the committed offsets would
        # have cost if they had been load bearing.
        b = exp1["offsets_are_monitoring"]
        t = b["job_that_trusted_the_offsets"]
        assert t["startup_mode"] == "group-offsets"
        assert t["duplicate_rows"] == b["events_produced"]
        assert t["rows"] == 2 * b["events_produced"]

    def test_the_difference_between_the_two_is_the_restore_point(self, exp1):
        b = exp1["offsets_are_monitoring"]
        assert (b["restored_from_checkpoint"]["duplicate_rows"]
                < b["job_that_trusted_the_offsets"]["duplicate_rows"])


class TestSavepointRestore:
    def test_the_savepoint_was_really_older_than_the_table(self, exp2):
        # The precondition. Without it there is nothing to observe.
        assert exp2["savepoint"]["checkpoint_id"] < exp2["watermark_before_restore"]
        assert exp2["restored_job"]["restored_from"]["is_savepoint"] is True
        assert (exp2["restored_job"]["restored_from"]["checkpoint_id"]
                == exp2["savepoint"]["checkpoint_id"])
        assert exp2["restored_job"]["checkpoints_below_the_watermark"] > 1

    def test_the_predicted_loss_did_not_happen_and_stays_recorded(self, exp2):
        # Mutation check anchor. This is the repository's most surprising
        # result and the easiest one to "fix" later. Setting prediction_held
        # true, or recording any lost row, fails here.
        assert "LOSES" in exp2["prediction"]
        assert exp2["prediction_held"] is False
        assert exp2["landed"]["rows_lost"] == 0
        assert exp2["landed"]["missing_seq_runs"] == []

    def test_the_replay_was_committed_a_second_time(self, exp2):
        # What actually goes wrong: duplication, not loss.
        assert exp2["landed"]["duplicate_rows"] > 0
        assert (exp2["landed"]["rows"]
                == exp2["landed"]["rows_expected"] + exp2["landed"]["duplicate_rows"])

    def test_the_table_did_not_move_while_the_job_was_healthy(self, exp2):
        # Mutation check anchor. The freeze is the finding. Let the table move
        # during it, record two different row counts, and this fails.
        f = exp2["frozen_window"]
        assert f["the_table_never_moved_while_frozen"] is True
        assert len(f["rows_while_frozen"]) == 1
        assert f["seconds_frozen"] > 0
        assert f["job_states_while_frozen"] == ["RUNNING"]

    def test_no_further_checkpoint_failed_during_the_freeze(self, exp2):
        # The delta, not the total, and the difference matters. One run had a
        # checkpoint fail at the moment of restore, which is an ordinary
        # transient and had already happened before the first sample. What
        # would mean something is a failure DURING the freeze, and across the
        # whole window the counter does not move.
        watermark = exp2["watermark_before_restore"]
        frozen = [t["checkpoints_failed"] for t in exp2["timeline_after_restore"]
                  if t["latest_completed_checkpoint_id"] <= watermark]
        assert frozen
        assert max(frozen) - min(frozen) == 0

    def test_the_consumer_lag_reached_zero_while_the_table_was_stale(self, exp2):
        # MUTATION CHECK ANCHOR, and the operational point of the whole
        # experiment. The records really were consumed, so the number an
        # on-call dashboard watches goes green while the table is thousands of
        # rows behind. Raise the minimum lag above zero and it fails.
        f = exp2["frozen_window"]
        assert f["min_consumer_lag_while_frozen"] == 0

    def test_the_freeze_ended_at_the_first_checkpoint_above_the_watermark(self, exp2):
        # Mutation check anchor. Not "eventually caught up": the boundary is
        # exact and sits at the watermark. Every sample at or below it shows
        # the frozen row count, and the first one above it does not.
        watermark = exp2["watermark_before_restore"]
        frozen_rows = exp2["frozen_window"]["rows_while_frozen"][0]
        below = [t for t in exp2["timeline_after_restore"]
                 if t["latest_completed_checkpoint_id"] <= watermark]
        above = [t for t in exp2["timeline_after_restore"]
                 if t["latest_completed_checkpoint_id"] > watermark]
        assert below and above
        assert all(t["rows_visible_to_trino"] == frozen_rows for t in below)
        assert all(t["rows_visible_to_trino"] > frozen_rows for t in above)

    def test_the_backlog_committed_in_one_lump(self, exp2):
        # Nothing was discarded while the watermark was in the way. It was
        # held, and it all arrived at once, so there is no gap.
        f = exp2["frozen_window"]
        assert f["rows_committed_in_one_lump"] > 0

    def test_the_job_never_reported_anything_wrong(self, exp2):
        # No exception and no state change. The absolute count of failed
        # checkpoints is deliberately NOT asserted here: it is zero or one
        # depending on whether the restore itself produced a transient, and
        # pinning it would fail for a reason that has nothing to do with the
        # finding. What the freeze is about is checked one test above.
        assert exp2["restored_job"]["final_state"] == "RUNNING"
        assert exp2["restored_job"]["failure_causes"] == []
        assert exp2["restored_job"]["checkpoints"]["restored"] == 1

    def test_data_written_after_the_watermark_cleared_still_lands(self, exp2):
        # Without this the finding would be "the job broke", not "the job
        # froze and caught up".
        last = exp2["stages"][-1]
        assert last["stage"] == "07_fourth_batch_landed"
        assert last["job_state"] == "RUNNING"
        assert last["rows_visible_to_trino"] == exp2["landed"]["rows"]


@pytest.fixture(scope="module")
def exp3():
    return load("exp3_commit_interval.json")


@pytest.fixture(scope="module")
def exp4():
    return load("exp4_maintenance_live_writer.json")


@pytest.fixture(scope="module")
def exp5():
    return load("exp5_transaction_timeout.json")


INTERVALS = ["interval_5s", "interval_30s", "interval_120s"]


class TestCommitInterval:
    def test_every_run_landed_the_whole_feed(self, exp3):
        # The guard. A run that dropped records would make every file-size
        # comparison below meaningless, and smaller files would look like a
        # result rather than a shortfall.
        for run in exp3["runs"].values():
            assert run["rows"] == run["events"]

    def test_a_longer_interval_gives_fewer_and_larger_files(self, exp3):
        runs = [exp3["runs"][k] for k in INTERVALS]
        files = [r["data_files"] for r in runs]
        sizes = [r["avg_data_file_bytes"] for r in runs]
        assert files == sorted(files, reverse=True)
        assert sizes == sorted(sizes)

    def test_files_per_commit_tracks_the_writers_not_the_data(self, exp3):
        # Mutation check anchor. Every commit produced at most one file per
        # writer subtask, at every interval and every target size. That is the
        # mechanism the whole section rests on: a checkpoint closes every open
        # file, so the count is decided by the topology and not by volume.
        for run in exp3["runs"].values():
            assert run["max_data_files_in_one_commit"] <= run["parallelism"]
            assert run["data_files"] <= run["commits"] * run["parallelism"]

    def test_the_target_size_prediction_is_recorded_as_refuted(self, exp3):
        # Mutation check anchor. The prediction said raising
        # write.target-file-size-bytes "changes nothing at all". In its strict
        # form it is WRONG and the verdict stays wrong: the 1 GB run differed
        # from its baseline by one commit and 218 bytes of average file size,
        # where a repeat of the same configuration differed by zero commits
        # and ten bytes. Flipping prediction_held, or claiming the control
        # matched, fails here.
        f = exp3["findings"]
        assert exp3["prediction_held"] is False
        assert f["raising_the_target_moved_nothing_beyond_run_to_run_variation"] is False
        assert (f["raising_the_target_to_1gb_differed_by_files"]
                > f["repeat_of_the_same_configuration_differed_by_files"])

    def test_the_residual_tracks_feed_duration_and_not_the_target(self, exp3):
        # What the refutation is actually made of. All three five-second runs
        # committed once per interval, so their commit counts are set by how
        # long the feed happened to run. Ordered by feed duration, the commit
        # counts are non-decreasing, and the 1 GB run is simply the longest
        # feed of the three. That is a boundary effect, not a property.
        five_second = sorted(
            (r for r in exp3["runs"].values() if r["checkpoint_interval"] == "5s"),
            key=lambda r: r["feed_seconds"])
        assert len(five_second) == 3
        commits = [r["commits"] for r in five_second]
        assert commits == sorted(commits)
        # And the spread is one commit across the three, not a step change.
        assert max(commits) - min(commits) <= 1

    def test_raising_the_target_left_every_file_four_orders_below_it(self, exp3):
        # The claim that survives, and the one the property is reached for.
        # A 1 GB target did not produce anything remotely like 1 GB files: the
        # largest average in any run is smaller than the target by four orders
        # of magnitude, at every interval and both target sizes.
        control = exp3["runs"]["interval_5s_target_1gb"]
        assert control["target_file_size_bytes"] == 1024 ** 3
        assert control["avg_data_file_bytes"] * 10_000 < control["target_file_size_bytes"]
        assert exp3["findings"]["every_file_is_orders_of_magnitude_below_the_target"]

    def test_the_interval_moves_file_size_hundreds_of_times_further(self, exp3):
        # The comparison that puts the refutation in proportion: changing the
        # interval moved the average file size by a factor, and changing the
        # target moved it by a rounding error.
        base = exp3["runs"]["interval_5s"]
        slow = exp3["runs"]["interval_120s"]
        control = exp3["runs"]["interval_5s_target_1gb"]
        by_interval = abs(slow["avg_data_file_bytes"] - base["avg_data_file_bytes"])
        by_target = abs(control["avg_data_file_bytes"] - base["avg_data_file_bytes"])
        assert by_interval > 100 * by_target

    def test_every_file_is_orders_of_magnitude_under_the_target(self, exp3):
        assert exp3["findings"]["every_file_is_orders_of_magnitude_below_the_target"]
        assert exp3["findings"]["largest_average_file_size_observed"] < 1_000_000

    def test_the_repeat_run_exists_so_the_comparison_has_a_baseline(self, exp3):
        # Without this run the claim above is a difference with nothing to be
        # small compared to. Deleting it must fail here, not silently weaken
        # the argument.
        assert "interval_5s_repeat" in exp3["runs"]
        base = exp3["runs"]["interval_5s"]
        repeat = exp3["runs"]["interval_5s_repeat"]
        assert base["checkpoint_interval"] == repeat["checkpoint_interval"]
        assert base["target_file_size_bytes"] == repeat["target_file_size_bytes"] is None

    def test_freshness_is_the_price_of_the_larger_files(self, exp3):
        # The tradeoff is the finding: the same dial sets both.
        base = exp3["runs"]["interval_5s"]
        slow = exp3["runs"]["interval_120s"]
        assert (slow["seconds_from_last_record_to_visible_in_trino"]
                > base["seconds_from_last_record_to_visible_in_trino"])
        assert exp3["findings"]["longer_interval_cost_freshness_seconds"] > 0


class TestMaintenanceAgainstALiveWriter:
    def test_expire_snapshots_really_expired_something(self, exp4):
        # The guard. A procedure that refused, or found nothing to do, tests
        # nothing at all.
        a = exp4["expire_snapshots"]
        assert a["after_expiry"]["error"] is None
        assert a["before_expiry"]["snapshots"] > a["after_expiry"]["snapshots"]

    def test_the_watermark_survived_the_expiry(self, exp4):
        # MUTATION CHECK ANCHOR, and the reason part A came out safe.
        # expire_snapshots keeps the CURRENT snapshot by definition, and the
        # current snapshot is the one carrying flink.max-committed-checkpoint-
        # id. The value a restart needs cannot be expired away.
        a = exp4["expire_snapshots"]
        assert a["the_watermark_survived_because_the_newest_snapshot_did"] is True
        assert a["after_expiry"]["watermark"] == a["before_expiry"]["watermark"]

    def test_the_writer_kept_going_and_the_restart_lost_nothing(self, exp4):
        a = exp4["expire_snapshots"]
        assert a["after_expiry"]["job_state"] == "RUNNING"
        assert a["kept_writing_after_expiry"]["rows"] > a["before_expiry"]["rows"]
        assert a["after_the_restart"]["checkpoints"]["restored"] >= 1
        assert a["landed"]["rows"] == a["events_produced"]
        assert a["landed"]["duplicate_rows"] == 0
        assert a["landed"]["missing_seq_runs"] == []
        assert a["survived"] is True

    def test_the_expiry_prediction_is_recorded_as_refuted(self, exp4):
        a = exp4["expire_snapshots"]
        assert a["prediction_held"] is False

    def test_the_bucket_listing_actually_saw_the_bucket(self, exp4):
        # Zero out of zero is not a measurement. An unauthenticated bucket
        # listing returns Access Denied as a JSON error line and yields an
        # empty result, which is indistinguishable from an empty bucket, so
        # "no unreferenced objects" would be a statement about the instrument
        # rather than about the table. The count below only means something
        # once the listing is known to have seen the bucket at all.
        storage = exp4["remove_orphan_files"]["state_when_the_procedure_ran"]["storage"]
        assert storage["objects_on_disk"] > 0
        assert storage["data_objects_on_disk"] > 0
        assert storage["data_files_referenced_by_the_snapshot"] > 0

    def test_remove_orphan_files_had_nothing_to_take(self, exp4):
        # Mutation check anchor. "Nothing broke" is only a result once it says
        # what the procedure had to work with, and the guard above is what
        # makes this line mean anything at all.
        b = exp4["remove_orphan_files"]
        assert b["state_when_the_procedure_ran"]["storage"][
            "data_objects_no_snapshot_references"] == 0
        assert b["nothing_was_exposed_to_the_procedure"] is True

    def test_the_in_flight_rows_still_arrived(self, exp4):
        b = exp4["remove_orphan_files"]
        assert b["rows_in_flight_when_it_ran"] > 0
        assert b["landed"]["rows"] == b["events_produced"]
        assert b["landed"]["rows_lost"] == 0
        assert b["after"]["job_state"] == "RUNNING"
        assert b["survived"] is True

    def test_the_orphan_prediction_is_recorded_as_refuted(self, exp4):
        assert exp4["remove_orphan_files"]["prediction_held"] is False


class TestTransactionTimeout:
    def test_the_two_runs_differ_only_in_the_timeout(self, exp5):
        short = exp5["runs"]["timeout_below_interval"]
        long_ = exp5["runs"]["timeout_above_interval"]
        assert short["checkpoint_interval"] == long_["checkpoint_interval"]
        assert short["transaction_timeout_ms"] < long_["transaction_timeout_ms"]
        assert short["timeout_is_below_the_interval"] is True
        assert long_["timeout_is_below_the_interval"] is False

    def test_the_broker_really_registered_the_short_timeout(self, exp5):
        # THE GUARD, and the one this experiment needed most. The first
        # version of it found no loss at a six-to-one margin, and there was no
        # way to tell a rule that does not bite from a setting that never
        # reached the producer. Now the coordinator is asked directly.
        short = exp5["runs"]["timeout_below_interval"]
        assert short["broker_registered_the_requested_timeout"] == [5000]
        assert exp5["findings"]["the_broker_registered_the_five_second_timeout"]

    def test_a_timeout_under_the_interval_costs_the_reader_everything(self, exp5):
        # Mutation check anchor. The measurement is what a read_committed
        # consumer can see, not what the producer wrote. Counting the topic at
        # read_uncommitted instead reports this pipeline as perfectly healthy
        # while its entire output is unreadable.
        short = exp5["runs"]["timeout_below_interval"]
        assert short["records_written_uncommitted"] == short["events_in"]
        assert short["records_readable_committed"] == 0
        assert short["records_a_downstream_consumer_never_sees"] == short["events_in"]

    def test_the_transactions_were_aborted_and_not_merely_slow(self, exp5):
        # Mutation check anchor. CompleteAbort on the short run and never on
        # the long one is the mechanism, in the broker's own words. Without it
        # "nothing was readable" could be a consumer that gave up early.
        assert "CompleteAbort" in exp5["runs"]["timeout_below_interval"][
            "transaction_states_observed"]
        assert "CompleteAbort" not in exp5["runs"]["timeout_above_interval"][
            "transaction_states_observed"]
        assert "CompleteCommit" in exp5["runs"]["timeout_above_interval"][
            "transaction_states_observed"]

    def test_the_job_reported_nothing_wrong_while_losing_everything(self, exp5):
        short = exp5["runs"]["timeout_below_interval"]
        assert short["final_job_state"] == "RUNNING"
        assert short["failure_causes"] == []
        assert short["checkpoints"]["failed"] == 0

    def test_raising_the_timeout_recovers_every_record(self, exp5):
        long_ = exp5["runs"]["timeout_above_interval"]
        assert long_["records_readable_committed"] == long_["events_in"]
        assert long_["records_a_downstream_consumer_never_sees"] == 0

    def test_the_rule_is_the_only_difference(self, exp5):
        assert exp5["findings"]["raising_the_timeout_recovered"] > 0
        assert (exp5["findings"]["the_only_difference_between_the_two_runs"]
                == "transaction.timeout.ms")


class TestEveryExperimentRecordedItsPrediction:
    @pytest.mark.parametrize("name", [
        "exp1_restart_replay.json", "exp2_savepoint_loss.json",
        "exp3_commit_interval.json", "exp4_maintenance_live_writer.json",
        "exp5_transaction_timeout.json"])
    def test_a_prediction_and_a_verdict_are_on_record(self, name):
        # The house rule, enforced. A prediction written after the run is not
        # a prediction. Every results file has to carry one and say whether it
        # held, at the top level or inside each part.
        blob = json.dumps(load(name))
        assert "prediction" in blob
        assert "prediction_held" in blob


class TestTheReadmeCheckerStillChecks:
    """The README checker is the second CI gate, and nothing guarded IT.

    `check_readme_numbers.py` prints how many strings it derived so that "a
    version of this script that silently stopped deriving half of them is
    visible instead of clean"; its own words. Visible to a human reading
    stdout, yes. Nothing failed: emptying `expected_rows()` left the script
    printing a smaller count and exiting 0, and pytest never imports it, so
    both CI gates stayed green with every table row unchecked.

    A printed count is a report. These are the assertion.
    """

    @staticmethod
    def _checker():
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "_chk", os.path.join(ROOT, "scripts", "check_readme_numbers.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_it_still_derives_a_row_for_every_published_table_row(self):
        rows = self._checker().expected_rows()
        assert len(rows) >= 27, (
            f"the deriver produces {len(rows)} table rows; it stopped covering "
            "figures the README publishes")

    def test_it_still_derives_the_prose_figures(self):
        facts = self._checker().prose_facts()
        assert len(facts) >= 12, (
            f"the deriver produces {len(facts)} prose facts; the headline "
            "numbers live in prose, not in the tables")

    def test_every_headline_number_is_among_the_derived_strings(self):
        """The four figures the README leads with must each be derived from
        results/*.json rather than typed. Without this the duplicate count
        could be any positive integer: 1 or 900,000 both passed."""
        chk = self._checker()
        derived = " ".join(row for _, row in chk.expected_rows() + chk.prose_facts())
        for needle in ("12,000", "58.1", "8,000"):
            assert needle in derived, (
                f"{needle} appears in the README but is not derived from the "
                "results, so nothing ties it to the run that produced it")
