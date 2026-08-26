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
import subprocess
import sys

import pytest

import generate_roster as gen
import jobs
import lab


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
        # Mutation check anchor. Discarding stderr wholesale instead of
        # filtering by pattern makes a failed statement look like an empty
        # result, which is the most expensive way for a measurement to be
        # wrong: it reports a clean run.
        broken = "org.jline noise\nQuery failed: Table does not exist\n"
        assert "Query failed: Table does not exist" in lab._clean(broken)


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
        # Three experiments kill the TaskManager on purpose. A job that gives
        # up and goes FAILED is the one outcome none of them is about.
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
        WITHIN a version. It cannot detect drift BETWEEN versions, and that
        is the failure its own comment describes, because no corpus ships and
        every results file describes a workload that only this code can
        rebuild. Changing SALT left the whole suite green.

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
