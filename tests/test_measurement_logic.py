"""The measurement logic, tested without Kafka, Flink, Iceberg or Trino.

Every function here is pure. The experiments are what need a running stack;
the reasoning that turns a list of sequence numbers into "twelve thousand
duplicates" or "one contiguous window" does not.

That reasoning is where a wrong answer is most dangerous, because it produces
a plausible NUMBER rather than a crash, and a plausible number that points
the opposite way from the truth reads exactly like a finding. The tests below
pin the distinctions that decide which way it points.
"""

import json
import hashlib
import os
import subprocess
import sys

import pytest

import exp1_restart_replay as exp1
import exp3_commit_interval as exp3
import generate_roster as gen
import jobs
import lab

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestDuplicateCounting:
    def test_counts_extra_rows_not_repeated_values(self):
        # The number a reader of the table cares about is how many rows they
        # would have to throw away, not how many values happen to repeat.
        # Three copies of one seq is TWO duplicates, not one.
        assert lab.duplicates([1, 1, 1, 2]) == 2

    def test_no_duplicates_is_zero_not_falsy_guesswork(self):
        assert lab.duplicates([1, 2, 3]) == 0
        assert lab.duplicates([]) == 0

    def test_a_full_replay_duplicates_every_row(self):
        # Experiment 1 part B: a job that trusted the committed offsets read
        # the whole log a second time. Six thousand rows in, six thousand
        # duplicates out.
        assert lab.duplicates(list(range(1, 6001)) * 2) == 6000


class TestMissingRange:
    def test_reports_runs_and_not_a_count(self):
        # Mutation check anchor. Returning len(missing) instead of the runs
        # passes every "is anything missing" assertion and destroys the only
        # thing that distinguishes a skipped commit from scattered loss.
        assert lab.missing_range([1, 2, 6, 7], 1, 7) == [(3, 5)]

    def test_one_contiguous_window_is_one_run(self):
        seqs = list(range(1, 8001)) + list(range(12001, 16001))
        assert lab.missing_range(seqs, 1, 16000) == [(8001, 12000)]

    def test_scattered_loss_is_many_runs_and_reads_differently(self):
        assert lab.missing_range([1, 3, 5], 1, 5) == [(2, 2), (4, 4)]

    def test_nothing_missing_is_an_empty_list(self):
        assert lab.missing_range([1, 2, 3], 1, 3) == []

    def test_a_gap_that_runs_to_the_end_is_still_closed(self):
        # The loop closes the open run after it finishes. Without that, a
        # pipeline that stopped writing halfway reports NO missing rows.
        assert lab.missing_range([1, 2], 1, 5) == [(3, 5)]

    def test_duplicates_do_not_hide_a_gap(self):
        assert lab.missing_range([1, 1, 1, 4], 1, 4) == [(2, 3)]


class TestOutputCleaning:
    def test_ansi_color_codes_are_stripped(self):
        # The Flink SQL client colors its own output. Those escape sequences
        # are not ASCII and they end up in captured evidence otherwise.
        assert lab._ANSI.sub("", "\x1b[34;1m[INFO] done\x1b[0m") == "[INFO] done"

    def test_the_jline_warning_goes_and_the_query_result_stays(self):
        noisy = ("WARNING: Unable to create a system terminal\n"
                 "org.jline.utils.Log logr\n"
                 "1000\n")
        assert lab._clean(noisy) == "1000"

    def test_a_real_error_survives_cleaning(self):
        # Mutation check anchor, for the filter: widen the noise list to
        # anything that also matches a Trino error ("Query", say, or every
        # line carrying a colon) and the error is cleaned away with the
        # terminal warnings.
        broken = "org.jline noise\nQuery failed: Table does not exist\n"
        assert "Query failed: Table does not exist" in lab._clean(broken)

    def test_a_failed_statement_raises_with_its_error_text(self, monkeypatch):
        """The guard that stops a failed statement becoming an empty result.

        `_clean` does not do this. Cleaning is a display concern; what keeps a
        broken measurement from reading as a clean one is the return code
        check in `trino()`. Both halves are asserted here, because each fails
        a different way:

          drop `if proc.returncode != 0: raise` and a failed statement returns
          an empty row list, which every caller reads as "the table is empty"

          clean only `proc.stdout` and the exception is raised with an empty
          message, naming the statement and saying nothing about why

        `lab.sh` is patched, not `subprocess.run`, because `trino()` resolves
        `sh` through this module's own globals.
        """
        class Proc:
            returncode = 1
            stdout = ""
            stderr = ("WARNING: Unable to create a system terminal\n"
                      "Query failed: line 1:15: Table 'iceberg.roster.nope' "
                      "does not exist\n")

        monkeypatch.setattr(lab, "sh", lambda *a, **k: Proc())
        with pytest.raises(lab.LabError) as raised:
            lab.trino("SELECT count(*) FROM iceberg.roster.nope")
        message = str(raised.value)
        assert "Table 'iceberg.roster.nope' does not exist" in message, (
            "the failure was raised without the reason it failed")
        # and the noise it was filtered for is still gone
        assert "Unable to create a system terminal" not in message


class TestTheMaintenanceProceduresCarryTheirOwnGuard:
    """exp4's two procedures are SQL builders wearing a procedure's name.

    Their whole contract is the statement they build. Trino enforces a seven
    day minimum retention, so expiring anything recent fails until the guard
    is lowered, and each `trino --execute` is a fresh session, so a
    `SET SESSION` sent on its own is discarded before the statement it
    configures ever runs. The guard has to travel in the same call.

    That is a contract two lines of code can get wrong, and no stack is
    needed to check it.
    """

    class _Recorder:
        def __init__(self):
            self.calls = []

        def trino_session(self, *statements):
            self.calls.append(statements)
            return "ok"

        @staticmethod
        def table(name):
            return f"iceberg.roster.{name}"

    def test_expire_snapshots_lowers_the_guard_in_the_same_call(self,
                                                               monkeypatch):
        import exp4_maintenance_live_writer as exp4
        rec = self._Recorder()
        monkeypatch.setattr(exp4, "lab", rec)
        exp4.expire_snapshots("e4_expire")

        assert len(rec.calls) == 1, (
            "the guard and the procedure went in separate calls, which is the "
            "one thing that makes the guard useless")
        guard, procedure = rec.calls[0]
        assert guard == ("SET SESSION iceberg.expire_snapshots_min_retention "
                         "= '0s'")
        assert "EXECUTE expire_snapshots(retention_threshold => '0s')" in procedure
        assert "iceberg.roster.e4_expire" in procedure

    def test_remove_orphan_files_lowers_the_guard_in_the_same_call(self,
                                                                  monkeypatch):
        import exp4_maintenance_live_writer as exp4
        rec = self._Recorder()
        monkeypatch.setattr(exp4, "lab", rec)
        exp4.remove_orphan_files("e4_orphans")

        assert len(rec.calls) == 1
        guard, procedure = rec.calls[0]
        assert guard == ("SET SESSION iceberg.remove_orphan_files_min_retention"
                         " = '0s'")
        assert ("EXECUTE remove_orphan_files(retention_threshold => '0s')"
                in procedure)
        assert "iceberg.roster.e4_orphans" in procedure

    def test_each_procedure_lowers_its_OWN_guard(self, monkeypatch):
        # The two session variables have different names and are not
        # interchangeable. Lowering the expire guard before remove_orphan_files
        # leaves the orphan guard at seven days, and the procedure then refuses
        # the very files the experiment is about, which reads as "nothing was
        # exposed", the result exp4 part B actually reports.
        import exp4_maintenance_live_writer as exp4
        rec = self._Recorder()
        monkeypatch.setattr(exp4, "lab", rec)
        exp4.expire_snapshots("t")
        exp4.remove_orphan_files("t")
        first, second = rec.calls[0][0], rec.calls[1][0]
        assert "expire_snapshots_min_retention" in first
        assert "remove_orphan_files_min_retention" in second
        assert first != second


class TestReadableIsTheCheck:
    """exp4::readable. A maintenance procedure that removed a live file does
    not announce it. The next scan of the table is the announcement, so the
    read itself is the check and its failure is the finding.
    """

    def test_a_readable_table_reports_its_rows(self, monkeypatch):
        import exp4_maintenance_live_writer as exp4

        class Fine:
            LabError = lab.LabError

            @staticmethod
            def rows_in(name):
                return 12000

        monkeypatch.setattr(exp4, "lab", Fine)
        assert exp4.readable("t") == {"readable": True, "rows": 12000}

    def test_a_table_that_cannot_be_read_reports_why(self, monkeypatch):
        import exp4_maintenance_live_writer as exp4

        class Broken:
            LabError = lab.LabError

            @staticmethod
            def rows_in(name):
                raise lab.LabError(
                    "SELECT count(*)\nQuery failed: Error opening Iceberg "
                    "split s3://warehouse/data/x.parquet: File does not exist")

        monkeypatch.setattr(exp4, "lab", Broken)
        got = exp4.readable("t")
        assert got["readable"] is False
        # The reason must survive. A missing Parquet file is the exact failure
        # this experiment exists to detect, and "readable: False" without it
        # is indistinguishable from any other broken query.
        assert "File does not exist" in got["error"]
        assert "rows" not in got


class TestProduceScript:
    """scripts/produce.sh, driven against a stand-in broker.

    The script is the README's first instruction, and the count it prints has
    to be one the broker confirmed: the generator's line count is known before
    Kafka has been reached at all.

    What the script needs from a broker is two numbers, so a stand-in that
    answers `kafka-get-offsets.sh` and counts what it was asked to accept
    pins the arithmetic without Kafka, in CI, on the failure paths that
    matter: fewer records landed than were fed, and a producer that failed.
    """

    # Answers the three `docker` calls produce.sh makes: the offset read, the
    # copy, and the producer. STUB_LANDED is how many records this broker
    # admits to having accepted, and it is allowed to disagree with what it
    # was sent.
    STUB = """#!/usr/bin/env bash
case "$1 $3" in
  "exec /opt/kafka/bin/kafka-get-offsets.sh")
      echo "roster.updates:0:$(cat "$STUB_STATE")"
      echo "roster.updates:1:0"
      echo "roster.updates:2:0"
      echo "roster.updates:3:0" ;;
  "exec bash")
      echo $(( $(cat "$STUB_STATE") + ${STUB_LANDED} )) > "$STUB_STATE"
      exit "${STUB_PRODUCER_RC:-0}" ;;
  *) case "$1" in cp) exit 0 ;; *) echo "stub: $*" >&2; exit 99 ;; esac ;;
esac
"""

    def _run(self, tmp_path, landed, producer_rc=0, asked=5):
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        stub = bin_dir / "docker"
        stub.write_text(self.STUB)
        stub.chmod(0o755)
        state = tmp_path / "end_offset"
        state.write_text("0\n")
        env = dict(os.environ,
                   PATH=f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
                   STUB_STATE=str(state),
                   STUB_LANDED=str(landed),
                   STUB_PRODUCER_RC=str(producer_rc))
        return subprocess.run(
            ["bash", os.path.join(ROOT, "scripts", "produce.sh"), str(asked)],
            capture_output=True, text=True, env=env)

    def test_it_reports_the_count_the_broker_took(self, tmp_path):
        done = self._run(tmp_path, landed=5)
        assert done.returncode == 0, done.stderr
        # the wording is the one SAMPLE_RUN.md captured
        assert done.stdout.strip() == "produced 5 events"

    def test_records_that_never_landed_are_a_failure(self, tmp_path):
        # The expensive case: the producer exits 0 and writes its complaints
        # to stderr, so only the log getting longer proves anything.
        done = self._run(tmp_path, landed=3)
        assert done.returncode != 0
        assert "produced 5 events" not in done.stdout
        assert "expected 5" in done.stderr

    def test_a_producer_that_failed_is_a_failure(self, tmp_path):
        done = self._run(tmp_path, landed=0, producer_rc=1)
        assert done.returncode != 0
        assert "events" not in done.stdout

    def test_a_generator_that_failed_is_not_a_feed_of_zero_events(
            self, tmp_path):
        # Why the script needs errexit. Without `set -e` the generator's
        # failure leaves an empty file, and then every number in the script
        # agrees: nothing was fed, nothing landed, zero equals zero, and it
        # reports "produced 0 events" and exits 0. The script cannot tell
        # "the feed was empty" from "the feed never ran" by comparing its own
        # two numbers, because both are zero either way.
        done = self._run(tmp_path, landed=0, asked="notanumber")
        assert done.returncode != 0
        assert "produced" not in done.stdout


class TestTheVerdictsAreComputed:
    """Three judgments, each pinned against the mutation that quietly
    re-corrects it.

    Every one of these is pure, offline and importable. A verdict-preserving
    mutant agrees with the shipped results/*.json because it was written to
    agree with them, so a judgment checked only against the file it produced
    is not checked; these assert it on constructed inputs.
    """

    def test_a_predicted_duplicate_is_the_prediction_holding(self):
        # exp1. `return dupes == 0` would read as "the table is clean", which
        # is the opposite verdict for the one configuration this experiment
        # expected to fail: at-least-once + append was predicted to duplicate,
        # and duplicating is the prediction holding.
        cfg = {"upsert": False, "prediction": "duplicate rows"}
        held = exp1._prediction_held(
            cfg, {"duplicate_seq_rows": 10002, "duplicate_npi_rows": 0})
        assert held is True
        refuted = exp1._prediction_held(
            cfg, {"duplicate_seq_rows": 0, "duplicate_npi_rows": 0})
        assert refuted is False

    def test_the_duplicate_measure_follows_the_write_mode(self):
        # The other half: an append table is duplicate-free when no seq
        # repeats (a provider appearing six times is the feed working), and
        # an upsert table is duplicate-free when no NPI repeats (seq is
        # expected to be sparse). Read the wrong column and a correct table is
        # called broken.
        clean_append = {"duplicate_seq_rows": 0, "duplicate_npi_rows": 10002}
        clean_upsert = {"duplicate_seq_rows": 10002, "duplicate_npi_rows": 0}
        append = {"upsert": False, "prediction": "no duplicates"}
        upsert = {"upsert": True, "prediction": "no duplicates, idempotent"}
        assert exp1._prediction_held(append, clean_append) is True
        assert exp1._prediction_held(upsert, clean_upsert) is True
        # and swapped, each one is judged by the column that does not apply
        assert exp1._prediction_held(append, clean_upsert) is False
        assert exp1._prediction_held(upsert, clean_append) is False

    def test_the_control_comparison_can_come_out_either_way(self):
        """exp3, and one of the two refuted predictions.

        The prediction was that raising `write.target-file-size-bytes` changes
        nothing at all. The fifth run exists to say what "nothing" is, by
        running the same configuration twice and seeing how far the numbers
        move on their own. Against that floor the control moved further, 2
        files and 218 bytes against 0 files and 10 bytes, so the shipped
        verdict is False and the prediction is refuted. Hard-coding the flag to
        True would quietly correct that.

        Both directions are asserted because either one alone is satisfied by a
        constant.
        """
        with open(os.path.join(ROOT, "results", "exp3_commit_interval.json"),
                  encoding="utf-8") as fh:
            shipped = json.load(fh)
        recomputed = exp3.findings(shipped["runs"])
        assert recomputed[
            "raising_the_target_moved_nothing_beyond_run_to_run_variation"
        ] is False, ("the control moved further than the repeat did and the "
                     "claim was still reported as holding")
        assert recomputed[
            "raising_the_target_moved_nothing_beyond_run_to_run_variation"
        ] == shipped["findings"][
            "raising_the_target_moved_nothing_beyond_run_to_run_variation"]

        # and the other way: a control that stayed inside the noise floor
        def run(files, avg):
            return {"data_files": files, "avg_data_file_bytes": avg,
                    "target_file_size_bytes": 1073741824,
                    "max_data_files_in_one_commit": 2,
                    "seconds_from_last_record_to_visible_in_trino": 1.0}

        quiet = {"interval_5s": run(78, 13259),
                 "interval_5s_repeat": run(70, 13000),      # noise: 8 files
                 "interval_5s_target_1gb": run(77, 13200),  # control: 1 file
                 "interval_120s": run(8, 120000),
                 "interval_30s": run(20, 50000)}
        assert exp3.findings(quiet)[
            "raising_the_target_moved_nothing_beyond_run_to_run_variation"
        ] is True

    def test_a_quiet_window_shorter_than_the_commit_interval_is_refused(
            self, monkeypatch):
        # lab.py. A streaming table does not move between commits, so a quiet
        # window shorter than the commit interval settles on a table that is
        # merely waiting, and reports rows not yet committed as rows that
        # never arrived.
        def looked(*a, **kw):
            raise AssertionError(
                "wait_until_stable polled the table instead of refusing: the "
                "quiet window is shorter than the commit interval")

        monkeypatch.setattr(lab, "table_exists", looked)
        monkeypatch.setattr(lab, "rows_in", looked)
        with pytest.raises(lab.LabError) as raised:
            lab.wait_until_stable("e1_append", quiet_polls=3, poll=3,
                                  commit_interval_seconds=10)
        assert "cannot settle" in str(raised.value)

    def test_a_quiet_window_longer_than_the_interval_is_allowed(self,
                                                                monkeypatch):
        # The pair: the refusal has to let a legitimate window through, or it
        # would be satisfied by a function that refused everything.
        monkeypatch.setattr(lab, "table_exists", lambda name: True)
        monkeypatch.setattr(lab, "rows_in", lambda name: 12000)
        rows, waited = lab.wait_until_stable(
            "e1_append", quiet_polls=3, poll=0, commit_interval_seconds=-1)
        assert rows == 12000
        assert waited >= 0


class TestJobSql:
    def test_upsert_needs_the_key_and_the_flag_together(self):
        # Mutation check anchor. A primary key without write.upsert.enabled
        # is an append with extra ceremony, and the flag without the key has
        # no equality field to write deletes against. Either one alone
        # produces a table that looks configured and behaves like the other
        # mode, which would have made every upsert row in experiment 1 a lie.
        sql = jobs.target("t", upsert=True)
        assert "PRIMARY KEY (npi) NOT ENFORCED" in sql
        assert "'write.upsert.enabled'='true'" in sql

    def test_append_declares_neither(self):
        sql = jobs.target("t", upsert=False)
        assert "PRIMARY KEY" not in sql
        assert "write.upsert.enabled" not in sql

    def test_the_restore_point_appears_only_when_one_is_given(self):
        assert "execution.savepoint.path" not in jobs.settings("j")
        assert ("SET 'execution.savepoint.path' = 'file:/x';"
                in jobs.settings("j", savepoint_path="file:/x"))

    def test_the_checkpointing_mode_reaches_the_job(self):
        assert ("SET 'execution.checkpointing.mode' = 'AT_LEAST_ONCE';"
                in jobs.settings("j", mode="AT_LEAST_ONCE"))

    def test_restart_attempts_are_raised_so_a_kill_does_not_end_the_job(self):
        # Three experiments kill the TaskManager. A job that gives up and goes
        # FAILED is the one outcome none of them is about.
        assert ("SET 'restart-strategy.fixed-delay.attempts' = '2147483647';"
                in jobs.settings("j"))

    def test_target_file_size_is_only_set_when_asked_for(self):
        assert "write.target-file-size-bytes" not in jobs.target("t")
        assert ("'write.target-file-size-bytes'='1073741824'"
                in jobs.target("t", target_file_size=1024 ** 3))

    def test_a_whole_ingest_job_carries_the_catalog_and_the_insert(self):
        sql = jobs.ingest("j", "topic", "group", "tbl")
        assert "CREATE CATALOG ice" in sql
        assert "'topic'='topic'" in sql
        assert "INSERT INTO ice.roster.tbl SELECT * FROM kafka_roster;" in sql

    def test_the_transactional_sink_carries_its_own_timeout(self):
        sql = jobs.kafka_to_kafka("j", "in", "out", "g",
                                  transaction_timeout_ms=5000)
        assert "'sink.delivery-guarantee'='exactly-once'" in sql
        assert "'properties.transaction.timeout.ms'='5000'" in sql
        assert "'sink.transactional-id-prefix'='j'" in sql


class TestGenerator:
    def test_the_same_seed_gives_the_same_bytes(self):
        # No corpus is shipped, so the generator IS the input. If it drifted,
        # every result file would describe a workload nobody could rebuild.
        runs = [subprocess.run(
            [sys.executable, gen.__file__, "50", "--providers", "10"],
            capture_output=True, text=True).stdout for _ in range(2)]
        assert runs[0] == runs[1]
        assert len(runs[0].splitlines()) == 50

    def test_the_generator_still_produces_the_bytes_the_results_describe(self):
        """Two runs of one version agreeing is not the property that matters.

        The test above runs the generator twice in the same process tree and
        compares the outputs to each other, so it detects nondeterminism
        within a version. It cannot detect drift between versions, and that
        is the failure that matters, because no corpus ships and every results
        file describes a workload that only this code can rebuild.

        A digest is what pins it. If this fails, either the generator changed
        deliberately, in which case every results file describes a workload
        that no longer exists and the experiments must be re-run, or it
        changed by accident.
        """
        cases = [
            ("50", "10",
             "6521342db1ea47a76e48446ea125ac51be1fb3eb49ce229216904925c6191066"),
            ("1000", "200",
             "c2fae09330cf7109cb64c24626c7e4a1f26c5723f635c4e7a67626838549f8b5"),
        ]
        for events, providers, want in cases:
            out = subprocess.run(
                [sys.executable, gen.__file__, events, "--providers", providers],
                capture_output=True, text=True).stdout
            got = hashlib.sha256(out.encode()).hexdigest()
            assert got == want, (
                f"generator output changed for {events} events / {providers} "
                f"providers: {got} != {want}. Every results/*.json describes a "
                "workload this code no longer produces.")

    def test_the_key_is_the_npi_and_it_prefixes_the_line(self):
        out = subprocess.run(
            [sys.executable, gen.__file__, "5", "--providers", "10"],
            capture_output=True, text=True).stdout
        for line in out.splitlines():
            key, payload = line.split("|", 1)
            assert json.loads(payload)["npi"] == key
            assert len(key) == 10 and key.isdigit()

    def test_providers_recur_which_is_what_makes_upsert_a_real_choice(self):
        # A feed of distinct entities would make upsert and append identical
        # and experiment 1 meaningless. Providers have to repeat.
        npis = [gen.event(i, 200)[0] for i in range(1, 2001)]
        assert len(set(npis)) < len(npis) / 2
        assert max(npis.count(n) for n in set(npis)) > 1

    def test_distinct_providers_get_distinct_npis(self):
        assert len({gen.npi_for(i) for i in range(500)}) == 500

    def test_a_correction_carries_a_later_revision_for_the_same_provider(self):
        plain_npi, plain = gen.event(7, 200, correcting=False)
        fixed_npi, fixed = gen.event(7, 200, correcting=True)
        assert plain_npi == fixed_npi
        assert plain["revision"] == 1 and fixed["revision"] == 2


class TestExpectedState:
    def test_the_expected_table_is_computed_from_the_seed_not_the_run(self):
        # Experiment 1 checks an upsert table against what the generator says
        # must be there, not against whatever arrived. A table with the right
        # row count and the wrong revision in it passes the second check and
        # fails this one.
        import exp1_restart_replay as exp1
        exp = exp1.expected_state(500, providers=50)
        assert exp["distinct_npi"] == len(exp["latest_seq"])
        for npi, seq in exp["latest_seq"].items():
            assert gen.event(seq, 50)[0] == npi
            later = [i for i in range(seq + 1, 501)
                     if gen.event(i, 50)[0] == npi]
            assert later == []


@pytest.mark.parametrize("seqs,lo,hi", [([], 1, 3), ([1], 1, 1)])
def test_missing_range_handles_the_degenerate_inputs(seqs, lo, hi):
    lab.missing_range(seqs, lo, hi)
