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
import re

import pytest

import lab

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
        # Mutation check anchor, and the operational point of the whole
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
        # checkpoints is left unasserted here: it is zero or one
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
        # Mutation check anchor, and the reason part A came out safe.
        # expire_snapshots keeps the current snapshot by definition, and the
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
        # The guard, and the one this experiment needs most. Without it there
        # is no way to tell a rule that does not bite from a setting that never
        # reached the producer, so the coordinator is asked directly.
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


class _ReplayLab:
    """A stand-in `lab` that answers only what it was given.

    `__getattr__` raising is the whole design. A mock that invents a return
    value for any call would let these replays pass while the function under
    test did something entirely different, with the fake agreeing with
    itself. Every method the code reaches for has to be supplied by name,
    and anything else fails loudly and says which call it was.

    An answer is a plain value, a callable, or a `_Seq([...])` served one per
    call in order, so a before-and-after pair is written as a pair.

    The sequence is an explicit wrapper, not "a list means several answers":
    `lab.snapshots()` returns a list, so the type alone cannot say whether a
    list is the answer or a queue of them, and a fake that guesses hands the
    code the first snapshot where it wanted the snapshot list. Dispatching on
    type to infer intent is the same mistake in miniature as a fake that
    invents return values.

    `duplicates`, `missing_range` and `table` are delegated to the real module:
    they are pure arithmetic and string-building, they are part of what these
    replays are checking, and faking them would hollow the test out.
    """

    _REAL = ("duplicates", "missing_range", "table")

    class Seq:
        """Several answers for one method, served in call order."""

        def __init__(self, values):
            self.values = list(values)

    def __init__(self, **answers):
        self._answers = answers
        self._used = {}
        self.calls = []
        self.LabError = lab.LabError

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name in self._REAL:
            return getattr(lab, name)
        if name not in self._answers:
            raise AssertionError(
                f"the code called lab.{name}(), which this replay was not "
                f"given an answer for. Add it, or the test is exercising "
                f"something other than what it claims.")

        def answer(*args, **kwargs):
            self.calls.append(name)
            value = self._answers[name]
            if isinstance(value, _ReplayLab.Seq):
                i = self._used.get(name, 0)
                assert i < len(value.values), (
                    f"lab.{name}() was called {i + 1} times and this replay "
                    f"has {len(value.values)} answer(s) for it")
                self._used[name] = i + 1
                return value.values[i]
            if callable(value):
                return value(*args, **kwargs)
            return value
        return answer

    def unused(self):
        """Answers that were supplied and never reached. A replay that does
        not exercise what it set up is describing a run that did not happen."""
        return sorted(k for k, v in self._answers.items()
                      if isinstance(v, _ReplayLab.Seq)
                      and self._used.get(k, 0) < len(v.values))


def _offsets_with_lag(lag):
    """(committed, end) whose difference is exactly `lag`, as sample() sums it."""
    committed, end = {0: 0}, {0: lag}
    assert sum(end.get(p, 0) - o for p, o in committed.items()) == lag
    return committed, end


def _snapshots_for(record):
    """Snapshots whose count and newest summary derive back to `record`."""
    n = record["snapshots"]
    if not n:
        return []
    snaps = [{"summary": {"flink.max-committed-checkpoint-id": "0"}}
             for _ in range(n - 1)]
    snaps.append({"summary": {
        "flink.max-committed-checkpoint-id":
            str(record["max_committed_checkpoint_id"]),
        "flink.job-id": record["writing_job_id_on_newest_snapshot"]}})
    assert len(snaps) == n
    return snaps


def _lab_for_sample(record, extra=None):
    committed, end = _offsets_with_lag(record["consumer_lag"])
    answers = dict(
        snapshots=_snapshots_for(record),
        table_exists=True,
        rows_in=record["rows_visible_to_trino"],
        group_offsets=committed,
        end_offsets=end,
        job_state=record.get("job_state"),
        latest_checkpoint_id=record.get("latest_completed_checkpoint_id"),
        checkpoint_counts={"failed": record.get("checkpoints_failed", 0)},
        say=None,
    )
    answers.update(extra or {})
    return _ReplayLab(**answers)


def _table_state_for(landed, exp, upsert):
    """(seqs, held) that `measure` derives the recorded `landed` block from.

    Like every other reconstruction here, this is checked: if it stops
    reproducing the record the test must die at this line rather than exercise
    a table the run never had.
    """
    if upsert:
        held = dict(list(exp["latest_seq"].items())[:landed["providers_held"]])
        seqs = sorted(held.values())
        assert len(held) == landed["providers_held"]
        return seqs, held
    missing = {i for run in (landed["missing_seq_runs"] or [])
               for i in range(run[0], run[1] + 1)}
    events = landed["events_expected"]
    seqs = [s for s in range(1, events + 1) if s not in missing]
    seqs = seqs[:landed["rows"] - landed["duplicate_seq_rows"]]
    seqs += seqs[:landed["duplicate_seq_rows"]]
    assert len(seqs) == landed["rows"]
    assert lab.duplicates(seqs) == landed["duplicate_seq_rows"]
    assert lab.missing_range(seqs, 1, events) == [
        tuple(r) for r in (landed["missing_seq_runs"] or [])]
    return seqs, {}


class _JobsStub:
    """`jobs` builds SQL and is tested on its own terms in TestJobSql. Here it
    only has to hand back something a submit can take, and record that it was
    asked, so a replay cannot pass with the job never built."""

    def __init__(self):
        self.built = []

    def ingest(self, name, *args, **kwargs):
        self.built.append((name, kwargs))
        return f"-- SQL for {name}"

    def kafka_to_kafka(self, name, *args, **kwargs):
        self.built.append((name, kwargs))
        return f"-- SQL for {name}"


def _seqs_with(rows, duplicate_rows):
    """A seq list of `rows` entries carrying exactly `duplicate_rows` copies."""
    base = list(range(1, rows - duplicate_rows + 1))
    seqs = base + base[:duplicate_rows]
    assert len(seqs) == rows
    assert lab.duplicates(seqs) == duplicate_rows
    return seqs


def _seqs_for_landed_block(landed):
    """Seqs deriving back to a landed block's rows / duplicates / gaps."""
    total = landed["rows_expected"]
    missing = {i for run in (landed["missing_seq_runs"] or [])
               for i in range(run[0], run[1] + 1)}
    present = [s for s in range(1, total + 1) if s not in missing]
    present = present[:landed["rows"] - landed["duplicate_rows"]]
    seqs = present + present[:landed["duplicate_rows"]]
    assert len(seqs) == landed["rows"]
    assert lab.duplicates(seqs) == landed["duplicate_rows"]
    assert lab.missing_range(seqs, 1, total) == [
        tuple(r) for r in (landed["missing_seq_runs"] or [])]
    if "rows_lost" in landed:
        assert total - len(set(seqs)) == landed["rows_lost"]
    return seqs


class TestTheMaintenanceRunsReplay:
    """exp4's two parts, each rebuilt from the block it recorded.

    These are the runs where a maintenance procedure is pointed at a table a
    Flink job is actively writing. Both results are negative, since the
    writer survived, and a negative result is the easiest kind to fake by
    accident, so the block carries `nothing_was_exposed_to_the_procedure` and
    the storage counts behind it.
    Part B's prediction is recorded as refuted and has to stay that way.
    """

    def test_part_a_rebuilds_the_expire_snapshots_run(self, monkeypatch):
        import exp4_maintenance_live_writer as exp4

        record = load("exp4_maintenance_live_writer.json")["expire_snapshots"]
        before, after_x = record["before_expiry"], record["after_expiry"]
        writing, after_r = record["kept_writing_after_expiry"], record["after_the_restart"]
        landed = record["landed"]
        seqs = _seqs_for_landed_block(landed)

        Seq = _ReplayLab.Seq
        fake = _ReplayLab(
            say=None, drop_table=None, create_topic=None, produce=0,
            submit=record["job_id"], wait_for_state="RUNNING",
            wait_for_rows=None,
            snapshots=Seq([[None] * before["snapshots"],
                           [None] * after_x["snapshots"],
                           [None] * writing["snapshots"],
                           [None] * after_r["snapshots"]]),
            rows_in=Seq([before["rows"], after_x["table"]["rows"],
                         writing["rows"]]),
            committed_checkpoint_id=Seq([before["watermark"],
                                         after_x["watermark"],
                                         after_r["watermark"]]),
            job_state=Seq([before["job_state"], after_x["job_state"],
                           writing["job_state"], after_r["job_state"],
                           after_r["job_state"]]),
            trino_session="ok",
            checkpoint_counts=Seq([{"restored": 0}, after_r["checkpoints"]]),
            kill_taskmanager=None, wait_for_taskmanager=None,
            wait_for_restore=None,
            failure_causes=after_r["failure_causes"],
            wait_until_stable=(landed["rows"], 40.0),
            seqs_in=seqs, files_in=landed["files"],
            cancel=None, delete_topic=None)
        monkeypatch.setattr(exp4, "lab", fake)
        monkeypatch.setattr(exp4, "jobs", _JobsStub())

        rebuilt = exp4.part_a()

        assert set(rebuilt) == set(record), sorted(set(rebuilt) ^ set(record))
        for field in sorted(record):
            want = record[field]
            if field == "landed" and want.get("missing_seq_runs"):
                want = dict(want, missing_seq_runs=[tuple(r) for r in
                                                    want["missing_seq_runs"]])
            assert rebuilt[field] == want, f"{field}: {rebuilt[field]!r} != {want!r}"
        assert fake.unused() == [], fake.unused()
        # The exposure is the restart, not the expiry: part A's whole point.
        assert "kill_taskmanager" in fake.calls

    def test_part_b_rebuilds_the_remove_orphan_files_run(self, monkeypatch):
        import exp4_maintenance_live_writer as exp4

        record = load("exp4_maintenance_live_writer.json")["remove_orphan_files"]
        at_run, after = record["state_when_the_procedure_ran"], record["after"]
        landed = record["landed"]
        seqs = _seqs_for_landed_block(landed)

        Seq = _ReplayLab.Seq
        fake = _ReplayLab(
            say=None, drop_table=None, create_topic=None, produce=0,
            submit=record["job_id"], wait_for_state="RUNNING",
            wait_for_rows=None,
            rows_in=Seq([record["rows_committed_before"],
                         at_run["rows_visible"],
                         after["table"]["rows"]]),
            snapshots=[None] * at_run["snapshots"],
            job_state=Seq([at_run["job_state"], after["job_state"]]),
            storage_vs_catalog=Seq([at_run["storage"],
                                    record["storage_immediately_after_the_procedure"]]),
            trino_session="ok",
            wait_until_stable=(landed["rows"], 90.0),
            seqs_in=seqs, files_in=landed["files"],
            checkpoint_counts=after["checkpoints"],
            failure_causes=after["failure_causes"],
            cancel=None, delete_topic=None)
        monkeypatch.setattr(exp4, "lab", fake)
        monkeypatch.setattr(exp4, "jobs", _JobsStub())
        monkeypatch.setattr(exp4.time, "sleep", lambda s: None)

        rebuilt = exp4.part_b()

        assert set(rebuilt) == set(record), sorted(set(rebuilt) ^ set(record))
        for field in sorted(record):
            want = record[field]
            if field == "landed" and want.get("missing_seq_runs"):
                want = dict(want, missing_seq_runs=[tuple(r) for r in
                                                    want["missing_seq_runs"]])
            assert rebuilt[field] == want, f"{field}: {rebuilt[field]!r} != {want!r}"
        assert fake.unused() == [], fake.unused()
        # The refuted prediction stays refuted.
        assert rebuilt["prediction_held"] is False
        assert rebuilt["nothing_was_exposed_to_the_procedure"] is True


class TestTheOffsetsAreMonitoringRunReplays:
    """exp1::part_b, the part that shows committed offsets are not the
    recovery mechanism.

    It runs three jobs: one that lands the feed, one restored from the
    retained checkpoint after the offsets are rewound to zero, and one that
    trusts those offsets. The finding is the contrast between the last two,
    0 duplicates against 6,000, and it lives entirely in the block this
    function returns.
    """

    def test_part_b_rebuilds_the_offsets_block(self, monkeypatch):
        import exp1_restart_replay as exp1

        record = load("exp1_restart_replay.json")["offsets_are_monitoring"]
        first, restored = record["first_job"], record["restored_from_checkpoint"]
        trusting = record["job_that_trusted_the_offsets"]

        Seq = _ReplayLab.Seq
        fake = _ReplayLab(
            say=None, drop_table=None, create_topic=None, produce=0,
            submit=Seq([first["job_id"], restored["job_id"],
                        trusting["job_id"]]),
            wait_for_rows=None,
            wait_for_checkpoints=None,
            latest_retained_checkpoint=first["retained_checkpoint"],
            cancel=None,
            rows_in=first["rows"],
            group_offsets=Seq([first["committed_offsets"],
                               restored["committed_offsets_afterward"]]),
            reset_group_offsets=Seq([record["offsets_rewound_to"],
                                     record["offsets_rewound_again_to"]]),
            wait_until_stable=Seq([(restored["rows"], 30.0),
                                   (trusting["rows"], 30.0)]),
            seqs_in=Seq([_seqs_with(restored["rows"],
                                    restored["duplicate_rows"]),
                         _seqs_with(trusting["rows"],
                                    trusting["duplicate_rows"])]),
            delete_topic=None)
        monkeypatch.setattr(exp1, "lab", fake)
        monkeypatch.setattr(exp1, "jobs", _JobsStub())

        rebuilt = exp1.part_b()

        assert rebuilt == record, (
            f"differs at: "
            f"{[k for k in record if rebuilt.get(k) != record[k]]}")
        assert fake.unused() == [], fake.unused()
        # The rewind is the experiment, twice: once behind a job that restores
        # from its checkpoint and once in front of a job that trusts them.
        assert fake.calls.count("reset_group_offsets") == 2
        assert fake.calls.count("submit") == 3

    def test_the_offsets_are_read_before_the_job_is_canceled(self,
                                                              monkeypatch):
        """The ordering the code has a comment about, made mechanical.

        A canceled job's consumer group has no members, and
        kafka-consumer-groups then prints a block the parser reads as no
        offsets at all, indistinguishable from the offsets having been wiped,
        which is the very thing this experiment is measuring. The read
        therefore has to come first.
        """
        import exp1_restart_replay as exp1

        record = load("exp1_restart_replay.json")["offsets_are_monitoring"]
        first, restored = record["first_job"], record["restored_from_checkpoint"]
        trusting = record["job_that_trusted_the_offsets"]
        Seq = _ReplayLab.Seq
        fake = _ReplayLab(
            say=None, drop_table=None, create_topic=None, produce=0,
            submit=Seq([first["job_id"], restored["job_id"], trusting["job_id"]]),
            wait_for_rows=None, wait_for_checkpoints=None,
            latest_retained_checkpoint=first["retained_checkpoint"],
            cancel=None, rows_in=first["rows"],
            group_offsets=Seq([first["committed_offsets"],
                               restored["committed_offsets_afterward"]]),
            reset_group_offsets=Seq([record["offsets_rewound_to"],
                                     record["offsets_rewound_again_to"]]),
            wait_until_stable=Seq([(restored["rows"], 30.0),
                                   (trusting["rows"], 30.0)]),
            seqs_in=Seq([_seqs_with(restored["rows"], restored["duplicate_rows"]),
                         _seqs_with(trusting["rows"], trusting["duplicate_rows"])]),
            delete_topic=None)
        monkeypatch.setattr(exp1, "lab", fake)
        monkeypatch.setattr(exp1, "jobs", _JobsStub())
        exp1.part_b()

        order = [c for c in fake.calls if c in ("group_offsets", "cancel")]
        # first job: cancel, then read. restored job: read, then cancel.
        assert order == ["cancel", "group_offsets", "group_offsets", "cancel",
                         "cancel"], order


class TestTheRestartRunsReplay:
    """exp1::run_config rebuilt each of the four configurations it recorded.

    This is the longest orchestrator in the repository: it feeds a topic on
    a thread, waits for two checkpoints, kills a TaskManager, waits for the
    restore, waits for the table to settle, and only then measures. Every one
    of those steps is a lab call, and the block it returns is
    `configurations[i]` of the shipped results.
    """

    @pytest.mark.parametrize("index", [0, 1, 2, 3])
    def test_run_config_rebuilds_each_configuration(self, index, monkeypatch):
        import exp1_restart_replay as exp1

        shipped = load("exp1_restart_replay.json")
        record = shipped["configurations"][index]
        landed, before, restart, after = (record["landed"],
                                          record["before_kill"],
                                          record["restart"], record["after"])
        cfg = next(c for c in exp1.CONFIGS
                   if ("upsert on npi" if c["upsert"] else "append")
                   == record["configuration"]["write_mode"]
                   and c["mode"] == record["configuration"]["checkpointing_mode"])
        exp = exp1.expected_state(shipped["workload"]["events"],
                                  shipped["workload"]["providers_in_the_feed"])
        seqs, held = _table_state_for(landed, exp, cfg["upsert"])

        Seq = _ReplayLab.Seq
        fake = _ReplayLab(
            say=None, drop_table=None, create_topic=None, produce=0,
            submit=record["job_id"],
            wait_for_checkpoints=None,
            rows_in=before["rows"],
            table_exists=True,
            group_offsets=Seq([before["group_offsets"], after["group_offsets"]]),
            end_offsets=Seq([before["topic_end_offsets"],
                             after["topic_end_offsets"]]),
            committed_checkpoint_id=Seq([before["committed_checkpoint_id"],
                                         after["committed_checkpoint_id"]]),
            checkpoint_counts=before["checkpoints"],
            kill_taskmanager=None, wait_for_taskmanager=None,
            wait_for_restore=restart["checkpoints_after"],
            restore_point=restart["restored_from"],
            job_state=restart["job_state_after_restart"],
            wait_until_stable=(landed["rows"], 12.5),
            failure_causes=restart["failure_causes"],
            # what `measure` reads
            seqs_in=seqs,
            one=landed["distinct_npi"],
            trino=[{"npi": k, "seq": v} for k, v in held.items()],
            files_in=landed["files"],
            cancel=None, delete_topic=None)
        monkeypatch.setattr(exp1, "lab", fake)
        monkeypatch.setattr(exp1, "jobs", _JobsStub())

        rebuilt = exp1.run_config(cfg, exp)

        assert set(rebuilt) == set(record), (
            f"fields differ: {sorted(set(rebuilt) ^ set(record))}")
        for field in sorted(record):
            want = record[field]
            if field == "landed" and want.get("missing_seq_runs"):
                want = dict(want, missing_seq_runs=[tuple(r) for r in
                                                    want["missing_seq_runs"]])
            assert rebuilt[field] == want, (
                f"config {index} field {field}:\n  rebuilt {rebuilt[field]!r}"
                f"\n  shipped {record[field]!r}")
        assert fake.unused() == [], (
            f"the replay set up answers the run never reached: {fake.unused()}")
        # The kill is the experiment. A run_config that never killed anything
        # would measure a cold start and look identical in the results.
        assert "kill_taskmanager" in fake.calls
        assert "wait_for_restore" in fake.calls


class TestTheSavepointTimelineReplays:
    """exp2's three observation functions, against the timeline they recorded.

    `sample` is where experiment 2's finding is actually made: it reads the
    table, the snapshots and the consumer lag at one moment, and the lag is
    what an on-call dashboard shows. During the frozen window it reads zero
    while the table is thousands of rows behind, and that juxtaposition is the
    result.
    """

    def test_sample_rebuilds_each_recorded_stage(self):
        import exp2_savepoint_loss as exp2
        stages = load("exp2_savepoint_loss.json")["stages"]
        # Two of the seven carry no job fields, 04_job_stopped and
        # 05_third_batch_produced_while_down, because at those moments there
        # was no job to ask. `sample` adds them only when given a job id, and
        # a replay that passed one anyway would be testing a stage the run
        # never took.
        assert sum("job_state" in s for s in stages) == 5, stages
        for record in stages:
            want = {k: v for k, v in record.items() if k != "stage"}
            job_id = "a-job" if "job_state" in record else None
            fake = _lab_for_sample(record)
            exp2.lab, original = fake, exp2.lab
            try:
                got = exp2.sample(job_id=job_id)
            finally:
                exp2.lab = original
            assert got == want, f"{record['stage']}: {got} != {want}"

    def test_a_table_with_no_snapshots_reports_no_rows(self):
        # The first stage of any run, and the guard that stops `snapshots[-1]`
        # raising on an empty table.
        import exp2_savepoint_loss as exp2
        fake = _ReplayLab(snapshots=[], table_exists=False,
                          group_offsets={}, end_offsets={}, say=None)
        exp2.lab, original = fake, exp2.lab
        try:
            got = exp2.sample()
        finally:
            exp2.lab = original
        assert got["rows_visible_to_trino"] == 0
        assert got["snapshots"] == 0
        assert got["max_committed_checkpoint_id"] is None
        assert "job_state" not in got, (
            "no job id was given, so no job fields belong in the record")

    def test_stage_is_the_sample_plus_its_label(self):
        import exp2_savepoint_loss as exp2
        record = next(s for s in load("exp2_savepoint_loss.json")["stages"]
                      if "job_state" in s)
        fake = _lab_for_sample(record)
        exp2.lab, original = fake, exp2.lab
        try:
            got = exp2.stage(record["stage"], job_id="a-job")
        finally:
            exp2.lab = original
        assert got == record

    def test_watch_rebuilds_the_frozen_window_and_stops_when_it_clears(self,
                                                                      monkeypatch):
        """The timeline is the whole finding, and it is a stopping rule.

        `watch` samples until the restored job's checkpoint id passes the
        watermark. Read down the recorded timeline: the id starts at 7 against
        a watermark of 19, the table sits at 16,000 rows looking healthy, and
        the moment the id reaches 20 the table jumps to 32,000. A `watch` that
        stopped early would report the freeze as shorter than it was, and a
        `watch` that never stopped would hang.
        """
        import exp2_savepoint_loss as exp2
        shipped = load("exp2_savepoint_loss.json")
        timeline = shipped["timeline_after_restore"]
        watermark = shipped["watermark_before_restore"]

        class Clock:
            def __init__(self):
                self.t = 900.0
                self.i = 0

            def time(self):
                return self.t

            def sleep(self, seconds):
                # advance to the moment the next sample was recorded
                self.i += 1
                if self.i < len(timeline):
                    self.t = 900.0 + timeline[self.i]["seconds_since_restore"]

        clock = Clock()
        Seq = _ReplayLab.Seq
        fake = _ReplayLab(
            snapshots=Seq(_snapshots_for(r) for r in timeline),
            table_exists=True,
            rows_in=Seq(r["rows_visible_to_trino"] for r in timeline),
            group_offsets=Seq(_offsets_with_lag(r["consumer_lag"])[0]
                              for r in timeline),
            end_offsets=Seq(_offsets_with_lag(r["consumer_lag"])[1]
                            for r in timeline),
            job_state=Seq(r["job_state"] for r in timeline),
            latest_checkpoint_id=Seq(r["latest_completed_checkpoint_id"]
                                     for r in timeline),
            checkpoint_counts=Seq({"failed": r["checkpoints_failed"]}
                                  for r in timeline),
            say=None)
        monkeypatch.setattr(exp2, "lab", fake)
        monkeypatch.setattr(exp2, "time", clock)

        got = exp2.watch("restored-job", watermark)

        assert got == timeline, "the replayed timeline is not the recorded one"
        assert len(got) == len(timeline)
        assert got[-1]["latest_completed_checkpoint_id"] > watermark
        assert all(r["latest_completed_checkpoint_id"] <= watermark
                   for r in got[:-1]), (
            "it stopped at the FIRST id above the watermark, which is the "
            "boundary the finding is about")
        assert fake.unused() == [], fake.unused()

    def test_watch_refuses_a_job_that_never_clears_the_watermark(self,
                                                                monkeypatch):
        """The falsifier. Returning a short timeline instead of raising is
        exactly the silent-loss shape this experiment exists to expose."""
        import exp2_savepoint_loss as exp2
        stuck = {"rows_visible_to_trino": 16000, "snapshots": 13,
                 "max_committed_checkpoint_id": 19,
                 "writing_job_id_on_newest_snapshot": "old-job",
                 "consumer_lag": 0, "job_state": "RUNNING",
                 "latest_completed_checkpoint_id": 7, "checkpoints_failed": 1}

        class Clock:
            def __init__(self):
                self.t = 0.0

            def time(self):
                return self.t

            def sleep(self, seconds):
                self.t += seconds

        monkeypatch.setattr(exp2, "lab", _lab_for_sample(stuck))
        monkeypatch.setattr(exp2, "time", Clock())
        with pytest.raises(lab.LabError) as raised:
            exp2.watch("stuck-job", watermark=19, timeout=30, poll=3)
        assert "never passed checkpoint 19" in str(raised.value)


class TestTheShippedRunsReplayThroughTheCode:
    """Drive the experiment's own function with the evidence it produced.

    `exp5.run_one` starts a Flink job, watches the Kafka coordinator for five
    minutes and then derives the result, including `prediction_held`, which is
    the verdict. Nothing offline can run a Flink job, so the recorded run is
    replayed through the unchanged function: a stand-in `lab` answers every
    query out of results/exp5_transaction_timeout.json, and the function has
    to rebuild that file's stored result from them. The scripts produced the
    shipped results, so they are exercised as they are rather than
    rearranged to be convenient to test.

    This is not a test that the code agrees with itself. It is the claim that
    the shipped evidence is reproducible by the shipped code, which is
    stronger than the internal-consistency checks elsewhere in this file.

    What is not replayed: `job_id` comes from the cluster and
    `transaction_samples[i]["seconds"]` is wall clock. The clock is driven
    from the recording, with each stand-in query advancing it to the second
    that sample was taken, so the timeline is reproduced rather than re-timed,
    and the loop stops where the recording stops instead of running the real
    300 seconds.
    """

    class _Clock:
        """time.time()/time.sleep() driven by the recorded timeline."""

        def __init__(self):
            self.t = 1_000_000.0
            self.jump_to = None

        def time(self):
            return self.t

        def sleep(self, seconds):
            # After the last recorded sample, step past the run window so the
            # loop ends exactly where the recording does instead of asking for
            # a sample that was never taken.
            self.t = self.jump_to if self.jump_to is not None else self.t + seconds

    class _FakeLab:
        def __init__(self, record, clock, started):
            self.record = record
            self.clock = clock
            self.started = started
            self.samples = record["transaction_samples"]
            self.taken = 0
            self.cancelled = []
            self.topics = []

        # -- the calls run_one makes, in the order it makes them -------------
        def say(self, *args, **kwargs):
            pass

        def create_topic(self, topic, partitions=1):
            self.topics.append((topic, partitions))

        def submit(self, sql, key):
            assert isinstance(sql, str) and sql.strip(), (
                "run_one submitted an empty job")
            return self.record["job_id"]

        def job_state(self, job_id):
            assert job_id == self.record["job_id"]
            return self.record["final_job_state"]

        def transaction_states(self, name):
            sample = self.samples[self.taken]
            self.taken += 1
            # advance to the moment this sample was actually taken
            self.clock.t = self.started + sample["seconds"]
            if self.taken == len(self.samples):
                self.clock.jump_to = self.started + self.record["run_seconds"] + 1
            return _txns_that_reproduce(sample)

        def checkpoint_counts(self, job_id):
            return self.record["checkpoints"]

        def failure_causes(self, job_id):
            return self.record["failure_causes"]

        def consume_count(self, topic, isolation):
            key = ("records_readable_committed" if isolation == "read_committed"
                   else "records_written_uncommitted")
            return self.record[key]

        def cancel(self, job_id):
            self.cancelled.append(job_id)

    @pytest.mark.parametrize("run_key", ["timeout_below_interval",
                                         "timeout_above_interval"])
    def test_exp5_run_one_rebuilds_the_run_it_recorded(self, run_key,
                                                       monkeypatch):
        import exp5_transaction_timeout as exp5

        shipped = load("exp5_transaction_timeout.json")
        record = shipped["runs"][run_key]
        cfg = next(c for c in exp5.RUNS if c["key"] == run_key)

        clock = self._Clock()
        started = clock.t
        fake = self._FakeLab(record, clock, started)
        monkeypatch.setattr(exp5, "lab", fake)
        monkeypatch.setattr(exp5, "time", clock)

        rebuilt = exp5.run_one(cfg)

        # the loop ran exactly as long as the recording, not 300 real seconds
        assert fake.taken == len(record["transaction_samples"])
        assert rebuilt["samples_taken"] == record["samples_taken"]
        # and it cleaned up after itself
        assert fake.cancelled == [record["job_id"]]

        # Every derived field, checked by name so a new one cannot be added
        # without this test either covering it or saying it does not.
        run_local = {"job_id", "transaction_samples"}
        for field in sorted(set(record) - run_local):
            assert rebuilt[field] == record[field], (
                f"{run_key}: run_one rebuilt {field}={rebuilt[field]!r} from "
                f"the recorded observations; the shipped file says "
                f"{record[field]!r}")

        # the per-sample derivation too, minus the wall clock
        for got, want in zip(rebuilt["transaction_samples"],
                             record["transaction_samples"]):
            for field in ("states", "ongoing",
                          "timeouts_registered_on_the_broker"):
                assert got[field] == want[field]

        # and nothing the shipped record carries was quietly dropped
        assert set(record) - set(rebuilt) == set(), (
            f"the shipped record carries fields run_one no longer produces: "
            f"{sorted(set(record) - set(rebuilt))}")


class TestTheCommitIntervalRunsReplay:
    """`exp3.run_one` derives the file-size block the README publishes.

    `max_data_files_in_one_commit` is what backs the claim that files per
    commit tracks the writer subtasks and not the data. `findings()` reads
    it, and this pins the number it reads: run_one counts `added-data-files`
    per snapshot.

    Replayed the same way as exp5: a stand-in `lab` answers out of the shipped
    results, and the clock is driven from the two durations the file records,
    so `feed_seconds`, `events_per_second` and the freshness figure come out
    exactly rather than approximately.
    """

    class _Clock:
        def __init__(self, record):
            self.t = 500_000.0
            self.feed = record["feed_seconds"]
            self.fresh = record["seconds_from_last_record_to_visible_in_trino"]

        def time(self):
            return self.t

        def sleep(self, seconds):
            pass

    class _FakeLab:
        def __init__(self, record, clock):
            self.record, self.clock = record, clock
            self.cancelled, self.dropped = [], []

        def say(self, *a, **k):
            pass

        def drop_table(self, name):
            self.dropped.append(name)

        def create_topic(self, topic, partitions=1):
            pass

        def submit(self, sql, key):
            return self.record["job_id"]

        def wait_for_state(self, job_id, states, timeout=0):
            return "RUNNING"

        def produce(self, count, topic, providers=None, chunk=None, pause=0):
            # the feed is what took feed_seconds
            self.clock.t += self.clock.feed
            return count

        def wait_for_rows(self, name, rows, timeout=0, poll=1):
            # and this is the gap the freshness figure measures
            self.clock.t += self.clock.fresh
            return rows

        def cancel(self, job_id):
            self.cancelled.append(job_id)

        def snapshots(self, name):
            return _snapshots_that_reproduce(self.record)

        def files_in(self, name):
            return {"data_files": self.record["data_files"],
                    "data_bytes": self.record["data_bytes"],
                    "avg_data_file_bytes": self.record["avg_data_file_bytes"]}

        def rows_in(self, name):
            return self.record["rows"]

        def delete_topic(self, topic):
            pass

    @pytest.mark.parametrize("run_key", ["interval_5s", "interval_30s",
                                         "interval_120s",
                                         "interval_5s_target_1gb",
                                         "interval_5s_repeat"])
    def test_run_one_rebuilds_the_run_it_recorded(self, run_key, monkeypatch):
        import exp3_commit_interval as exp3

        record = load("exp3_commit_interval.json")["runs"][run_key]
        cfg = next(c for c in exp3.RUNS if c["key"] == run_key)
        clock = self._Clock(record)
        fake = self._FakeLab(record, clock)
        monkeypatch.setattr(exp3, "lab", fake)
        monkeypatch.setattr(exp3, "time", clock)

        rebuilt = exp3.run_one(cfg)

        assert set(rebuilt) == set(record)
        for field in sorted(record):
            if field == "job_id":
                continue
            assert rebuilt[field] == record[field], (
                f"{run_key} field {field}: rebuilt {rebuilt[field]!r}, "
                f"shipped {record[field]!r}")
        assert fake.cancelled == [record["job_id"]]


def _snapshots_that_reproduce(record):
    """Snapshots whose added-data-files derive back to the recorded counts.

    Checked for the same reason as the exp5 helper: a shape this does not
    know must fail here rather than quietly feed run_one something other than
    what the test claims.
    """
    commits = record["commits"]
    hi, lo = record["max_data_files_in_one_commit"], record["min_data_files_in_one_commit"]
    added = [lo] * commits
    added[0] = hi
    # the total has to be the file count the table actually holds
    short = record["data_files"] - sum(added)
    i = 1
    while short > 0 and i < commits:
        room = hi - added[i]
        step = min(room, short)
        added[i] += step
        short -= step
        i += 1
    assert len(added) == commits, "commit count"
    assert max(added) == hi and min(added) == lo, "per-commit extremes"
    assert sum(added) == record["data_files"], (
        f"reconstructed {sum(added)} data files, the run recorded "
        f"{record['data_files']}")
    return [{"summary": {"added-data-files": str(n)}} for n in added]


class TestTheExactlyOnceVerdictIsComputed:
    """`exp1.measure` decides `complete`.

    This is the verdict the whole repository is about: whether the table that
    landed is the table the seed says should have landed. It is computed in
    `exp1.measure`, which reads the table through `lab` and then judges it.

    All four shipped configurations record `complete: true`, so replaying them
    alone would not pin the verdict: a `measure` that hard-coded True would
    satisfy every one of them. The falsifiers below are what make the replay
    mean anything: a stale revision, a missing provider, a duplicate and a
    gap.

    The stand-in `lab` answers the four queries `measure` makes and delegates
    `duplicates` and `missing_range` to the real module, because those two are
    the arithmetic under test rather than the I/O around it.
    """

    class _FakeLab:
        def __init__(self, seqs, distinct_npi, files, held=None):
            self.seqs, self.distinct_npi = seqs, distinct_npi
            self.files, self.held = files, held or {}
            self.duplicates = lab.duplicates          # the real arithmetic
            self.missing_range = lab.missing_range

        def seqs_in(self, table_name):
            return self.seqs

        def one(self, statement):
            assert "count(DISTINCT npi)" in statement, statement
            return self.distinct_npi

        def table(self, name):
            return f"iceberg.roster.{name}"

        def trino(self, statement):
            assert "SELECT npi, seq" in statement, statement
            return [{"npi": k, "seq": v} for k, v in self.held.items()]

        def files_in(self, table_name):
            return self.files

    @staticmethod
    def _append_seqs(rows, duplicate_seq_rows, missing_seq_runs, events):
        """Seqs that derive back to the recorded three, checked as built."""
        missing = {i for run in (missing_seq_runs or []) for i in range(run[0], run[1] + 1)}
        seqs = [s for s in range(1, events + 1) if s not in missing]
        seqs = seqs[:rows - duplicate_seq_rows]
        seqs += seqs[:duplicate_seq_rows]
        assert len(seqs) == rows
        assert lab.duplicates(seqs) == duplicate_seq_rows
        assert lab.missing_range(seqs, 1, events) == [tuple(r) for r in (missing_seq_runs or [])]
        return seqs

    @pytest.mark.parametrize("index", [0, 1, 2, 3])
    def test_measure_rebuilds_each_landed_block(self, index, monkeypatch):
        import exp1_restart_replay as exp1

        shipped = load("exp1_restart_replay.json")
        config = shipped["configurations"][index]
        landed = config["landed"]
        upsert = config["configuration"]["write_mode"] == "upsert on npi"
        exp = exp1.expected_state(shipped["workload"]["events"],
                                  shipped["workload"]["providers_in_the_feed"])
        assert exp["distinct_npi"] == shipped["workload"]["distinct_npi_expected"]

        if upsert:
            held = dict(list(exp["latest_seq"].items())[:landed["providers_held"]])
            seqs = sorted(held.values())
            fake = self._FakeLab(seqs, landed["distinct_npi"], landed["files"], held)
        else:
            seqs = self._append_seqs(landed["rows"], landed["duplicate_seq_rows"],
                                     landed["missing_seq_runs"],
                                     landed["events_expected"])
            fake = self._FakeLab(seqs, landed["distinct_npi"], landed["files"])

        monkeypatch.setattr(exp1, "lab", fake)
        rebuilt = exp1.measure("e1_table", exp, upsert)

        assert set(rebuilt) == set(landed), (
            f"measure produces {sorted(set(rebuilt) ^ set(landed))} that the "
            "shipped landed block does not, or the other way round")
        for field in sorted(landed):
            want = landed[field]
            if field == "missing_seq_runs" and want:
                want = [tuple(r) for r in want]
            assert rebuilt[field] == want, (
                f"config {index} field {field}: rebuilt {rebuilt[field]!r}, "
                f"shipped {landed[field]!r}")
        assert rebuilt["complete"] is True

    def test_a_duplicate_row_is_not_a_complete_append_table(self, monkeypatch):
        import exp1_restart_replay as exp1
        exp = exp1.expected_state(100, 20)
        seqs = list(range(1, 100)) + [99]            # 100 rows, one a copy
        monkeypatch.setattr(exp1, "lab", self._FakeLab(seqs, 20, {}))
        got = exp1.measure("t", exp, upsert=False)
        assert got["duplicate_seq_rows"] == 1
        assert got["complete"] is False

    def test_a_gap_is_not_a_complete_append_table(self, monkeypatch):
        import exp1_restart_replay as exp1
        exp = exp1.expected_state(100, 20)
        seqs = [s for s in range(1, 101) if s not in (40, 41, 42)]
        monkeypatch.setattr(exp1, "lab", self._FakeLab(seqs, 20, {}))
        got = exp1.measure("t", exp, upsert=False)
        assert got["missing_seq_runs"] == [(40, 42)], (
            "the loss is reported as a run, which is the shape of a skipped "
            "commit")
        assert got["complete"] is False

    def test_a_missing_provider_is_not_a_complete_upsert_table(self, monkeypatch):
        import exp1_restart_replay as exp1
        exp = exp1.expected_state(100, 20)
        held = dict(list(exp["latest_seq"].items())[:-1])   # one provider short
        monkeypatch.setattr(exp1, "lab",
                            self._FakeLab(sorted(held.values()), len(held), {}, held))
        got = exp1.measure("t", exp, upsert=True)
        assert got["providers_held"] == exp["distinct_npi"] - 1
        assert got["complete"] is False

    def test_a_stale_revision_is_not_a_complete_upsert_table(self, monkeypatch):
        import exp1_restart_replay as exp1
        exp = exp1.expected_state(100, 20)
        held = dict(exp["latest_seq"])
        stale = next(iter(held))
        held[stale] = held[stale] - 1      # right row count, wrong revision
        monkeypatch.setattr(exp1, "lab",
                            self._FakeLab(sorted(held.values()), len(held), {}, held))
        got = exp1.measure("t", exp, upsert=True)
        assert got["providers_held"] == exp["distinct_npi"], (
            "the table is the right SIZE, which is why a row count cannot be "
            "the completeness measure for an upsert table")
        assert got["providers_not_carrying_the_latest_record"] == 1
        assert got["complete"] is False


def _txns_that_reproduce(sample):
    """A coordinator answer that derives back to `sample`.

    Checked as built. If this reconstruction ever stops reproducing the
    recorded sample (a new field, a shape this does not know), the test must
    fail here rather than quietly exercise a different input than it claims.
    """
    states = list(sample["states"])
    timeouts = list(sample["timeouts_registered_on_the_broker"])
    txns, n = {}, 0
    for state in states:
        repeats = max(1, sample["ongoing"]) if state == "Ongoing" else 1
        for _ in range(repeats):
            txns[f"txn-{n}"] = {"state": state,
                                "timeout_ms": timeouts[n % len(timeouts)]}
            n += 1
    while len({t["timeout_ms"] for t in txns.values()}) != len(set(timeouts)):
        # more timeouts than transactions built: pad with a state already seen
        filler = next(s for s in states if s != "Ongoing")
        txns[f"txn-{n}"] = {"state": filler, "timeout_ms": timeouts[n % len(timeouts)]}
        n += 1

    assert sorted({t["state"] for t in txns.values()}) == sorted(states)
    assert sum(1 for t in txns.values() if t["state"] == "Ongoing") == sample["ongoing"]
    assert sorted({t["timeout_ms"] for t in txns.values()}) == sorted(timeouts)
    return txns


class TestTheEvidenceMatchesTheCodeThatMadeIt:
    """A results file records the writer count. So does the code that ran.

    Both experiments write `workload.parallelism` into their results, and both
    start their Flink job with the same number. Both places read one constant,
    so the evidence cannot record a writer count no job ran at, and this test
    is what says so.
    """

    def test_the_recorded_writer_count_is_the_one_the_code_starts(self):
        import exp2_savepoint_loss as exp2
        import exp5_transaction_timeout as exp5
        for module, name in ((exp2, "exp2_savepoint_loss.json"),
                             (exp5, "exp5_transaction_timeout.json")):
            recorded = load(name)["workload"]["parallelism"]
            assert recorded == module.PARALLELISM, (
                f"{name} records parallelism {recorded} and the code starts "
                f"its job at {module.PARALLELISM}; the evidence describes a "
                "run this code does not produce")


class TestTheRepositoryDescriptionIsAlsoDerived:
    """GITHUB_DESCRIPTION.txt is a published surface too.

    `check_readme_numbers.py` re-derives every figure in README.md. The one
    sentence GitHub shows above the file list carries both a measured figure
    and the four engine versions: the same claims, on the surface most
    readers see first and the one nobody edits when a result moves.

    These do not check the prose, which is allowed to be phrased any way. They
    check that each NUMBER in it is the number the repository can still show.
    """

    @staticmethod
    def _description():
        with open(os.path.join(ROOT, "GITHUB_DESCRIPTION.txt"),
                  encoding="utf-8") as fh:
            return fh.read()

    def test_the_duplicate_count_is_the_one_the_run_recorded(self):
        landed = load("exp2_savepoint_loss.json")["landed"]["duplicate_rows"]
        assert f"{landed:,} duplicates" in self._description(), (
            f"the description does not say {landed:,} duplicates, which is "
            "what exp2 recorded")

    def test_the_engine_versions_are_the_ones_the_stack_runs(self):
        # The compose file is the only place that decides these. A description
        # naming a version the stack does not run is the cheapest possible
        # false claim and the least likely to be noticed.
        with open(os.path.join(ROOT, "stack", "compose.yaml"),
                  encoding="utf-8") as fh:
            compose = fh.read()
        text = self._description()
        for named, in_compose in (("Flink 1.20", "rig-flink:1.20-"),
                                  ("Iceberg 1.10", "iceberg-rest-fixture:1.10"),
                                  ("Kafka 4.3", "apache/kafka:4.3"),
                                  ("Trino 478", "trinodb/trino:478")):
            assert named in text, f"the description no longer names {named}"
            assert in_compose in compose, (
                f"the description says {named} and compose.yaml no longer "
                f"runs it ({in_compose} is gone)")

    def test_the_flink_base_image_is_pinned_by_patch_and_digest(self):
        # A moving tag builds a different image on a different day. The base
        # must name its patch and its digest, and the patch must be the one
        # the README says the runs used.
        with open(os.path.join(ROOT, "stack", "flink", "Dockerfile"),
                  encoding="utf-8") as fh:
            froms = [line.split()[1] for line in fh
                     if line.startswith("FROM ")]
        assert len(froms) == 1, froms
        m = re.fullmatch(r"flink:(1\.20\.\d+)@sha256:[0-9a-f]{64}", froms[0])
        assert m, f"the Flink base is not pinned by patch and digest: {froms[0]}"
        with open(os.path.join(ROOT, "README.md"), encoding="utf-8") as fh:
            assert f"Flink {m.group(1)}" in fh.read(), (
                f"the Dockerfile pins Flink {m.group(1)} and the README does "
                f"not say so")


class TestTheReadmeCheckerStillChecks:
    """The README checker is the second CI gate, and these guard it.

    `check_readme_numbers.py` prints how many strings it derived, which makes
    a version that stopped deriving half of them visible to a human reading
    stdout. A printed count is a report; these are the assertion.
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
        """The three figures the README leads with must each be derived from
        results/*.json rather than typed, so the duplicate count cannot be
        any positive integer the prose happens to say."""
        chk = self._checker()
        derived = " ".join(row for _, row in chk.expected_rows() + chk.prose_facts())
        for needle in ("12,000", "58.1", "8,000"):
            assert needle in derived, (
                f"{needle} appears in the README but is not derived from the "
                "results, so nothing ties it to the run that produced it")

    # The three tests above pin the deriver. These two pin the comparison,
    # which is a different half of the same script: what main() prints is
    # len(checked) - len(missing), so a comparison that never ran would print
    # the maximum count and exit 0.
    #
    # They come as a pair. A gate that always failed would satisfy the
    # altered-README test on its own, and a gate that always passed would
    # satisfy the shipped-README test on its own. Only both together say the
    # comparison looked.

    def test_the_shipped_readme_passes_the_comparison(self):
        assert self._checker().main() == 0, (
            "the shipped README no longer matches the figures derived from "
            "results/*.json")

    def test_one_altered_figure_makes_the_comparison_fail(self, tmp_path):
        chk = self._checker()
        with open(os.path.join(ROOT, "README.md"), encoding="utf-8") as fh:
            readme = fh.read()
        # A table row is one line in the source and is not reflowed, so it
        # appears in the README exactly as the deriver builds it. If that ever
        # stops being true this raises StopIteration, so a test that examined
        # nothing cannot pass.
        row = next(r for _, r in chk.expected_rows() if r in readme)
        at = next(i for i, ch in enumerate(row) if ch.isdigit())
        altered = row[:at] + ("8" if row[at] == "9" else "9") + row[at + 1:]
        assert altered != row
        mutated = tmp_path / "README.md"
        mutated.write_text(readme.replace(row, altered, 1), encoding="utf-8")
        assert chk.main(readme_path=str(mutated)) == 1, (
            "one figure was changed and the checker still reported a clean "
            "README, so the comparison is not running")
