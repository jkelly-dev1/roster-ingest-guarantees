"""Experiment 5: transaction timeout against checkpoint interval.

The rule under test, and the only one of the five questions in this
repository that does not apply to the roster pipeline as built:

    checkpoint interval + worst case checkpoint duration + restart time
      < transaction.timeout.ms <= broker transaction.max.timeout.ms

It applies to a job that writes to Kafka TRANSACTIONALLY, which the Kafka to
Iceberg pipeline never does. Iceberg's exactly-once comes from its own
two-phase commit against the catalog, not from a Kafka transaction. So this
experiment builds the shape the rule is about: Kafka in, Kafka out, sink
delivery guarantee exactly-once.

A Flink Kafka sink opens a transaction and holds it open across the whole
checkpoint interval, committing when the checkpoint completes. Set the timeout
below the interval and the broker aborts the transaction before Flink ever
asks it to commit. The records were written. They are in the log. They are
inside an aborted transaction, so a read_committed consumer will never see
them, and the writing job may not notice at all.

Measured both ways on purpose. Counting the output topic at read_uncommitted
shows what the producer wrote; counting it at read_committed shows what a
downstream consumer is allowed to read. Only the second number is the
pipeline's output, and the difference between them is the whole finding.
"""

import time

import lab
import jobs

IN_TOPIC = "e5.in"
GROUP = "rig-e5"
EVENTS = 8000
PROVIDERS = 2000
# A 120 second interval against a 5 second timeout is a margin of twenty-four
# to one. The margin has to be wide: a thin one cannot distinguish a rule that
# does not apply from a test too weak to trigger it, and the transaction state
# sampled from the broker below is what tells the two apart.
INTERVAL = "120s"
RUN_SECONDS = 300

RUNS = [
    {"key": "timeout_below_interval", "out_topic": "e5.out.short",
     "transaction_timeout_ms": 5000,
     "prediction": "the broker aborts before the commit; a read_committed "
                   "consumer sees fewer records than were written"},
    {"key": "timeout_above_interval", "out_topic": "e5.out.long",
     "transaction_timeout_ms": 300000,
     "prediction": "every record is committed and visible"},
]


def run_one(cfg):
    key = cfg["key"]
    lab.say(f"--- {key}: transaction.timeout.ms="
            f"{cfg['transaction_timeout_ms']} against a {INTERVAL} interval")
    lab.create_topic(cfg["out_topic"], partitions=4)

    # The prefix carries the interval, and it has to. Kafka keeps a
    # transactional id for seven days after its last use, so a prefix reused
    # between two shapes of this experiment matches the previous shape's
    # finished transactions and reports their states as this run's.
    name = f"e5-{key}-i{INTERVAL}"
    job_id = lab.submit(
        jobs.kafka_to_kafka(name, IN_TOPIC, cfg["out_topic"],
                            f"{GROUP}-{key}", interval=INTERVAL,
                            transaction_timeout_ms=cfg["transaction_timeout_ms"],
                            parallelism=1), key)
    started = time.time()
    state_seen = []
    txn_samples = []
    while time.time() - started < RUN_SECONDS:
        state_seen.append(lab.job_state(job_id))
        # Watch the coordinator, not only the job. If a transaction is held
        # open across the whole interval it spends that interval in Ongoing,
        # and a timeout shorter than the interval has to bite. If it is barely
        # ever Ongoing, the rule cannot bite however the numbers are set, and
        # that is a different result entirely.
        txns = lab.transaction_states(name)
        txn_samples.append({
            "seconds": round(time.time() - started, 1),
            "states": sorted({t["state"] for t in txns.values()}),
            "ongoing": sum(1 for t in txns.values() if t["state"] == "Ongoing"),
            "timeouts_registered_on_the_broker":
                sorted({t["timeout_ms"] for t in txns.values()}),
        })
        if state_seen[-1] in ("FAILED", "FINISHED"):
            break
        time.sleep(5)

    final_state = lab.job_state(job_id)
    counts = lab.checkpoint_counts(job_id) if final_state != "FAILED" else {}
    causes = lab.failure_causes(job_id)
    committed = lab.consume_count(cfg["out_topic"], "read_committed")
    uncommitted = lab.consume_count(cfg["out_topic"], "read_uncommitted")
    lab.say(f"job ended {final_state}; read_committed={committed}, "
            f"read_uncommitted={uncommitted}, of {EVENTS} in; transaction "
            f"states seen "
            f"{sorted({st for sm in txn_samples for st in sm['states']})}")

    result = {
        "transaction_timeout_ms": cfg["transaction_timeout_ms"],
        "checkpoint_interval": INTERVAL,
        "timeout_is_below_the_interval":
            cfg["transaction_timeout_ms"] < int(INTERVAL.rstrip("s")) * 1000,
        "prediction": cfg["prediction"],
        "job_id": job_id,
        "run_seconds": RUN_SECONDS,
        "events_in": EVENTS,
        "job_states_observed": sorted(set(state_seen + [final_state])),
        "broker_registered_the_requested_timeout": sorted(
            {t for sample in txn_samples
             for t in sample["timeouts_registered_on_the_broker"]}),
        "samples_with_a_transaction_ongoing": sum(
            1 for sample in txn_samples if sample["ongoing"]),
        "samples_taken": len(txn_samples),
        "transaction_states_observed": sorted(
            {st for sample in txn_samples for st in sample["states"]}),
        "transaction_samples": txn_samples,
        "final_job_state": final_state,
        "checkpoints": counts,
        "failure_causes": causes,
        "records_readable_committed": committed,
        "records_written_uncommitted": uncommitted,
        "records_written_but_never_committed": uncommitted - committed,
        "records_a_downstream_consumer_never_sees": EVENTS - committed,
    }
    result["prediction_held"] = (
        result["records_a_downstream_consumer_never_sees"] > 0
        if result["timeout_is_below_the_interval"]
        else committed == EVENTS)
    lab.cancel(job_id)
    return result


def main():
    lab.create_topic(IN_TOPIC, partitions=4)
    lab.produce(EVENTS, IN_TOPIC, providers=PROVIDERS)
    payload = {
        "experiment": "transaction_timeout",
        "question": "what does a Kafka transaction timeout shorter than the "
                    "checkpoint interval cost the pipeline's output",
        "rule_under_test": "checkpoint interval + checkpoint duration + "
                           "restart time < transaction.timeout.ms <= broker "
                           "transaction.max.timeout.ms",
        "why_this_pipeline_needed_building": "the Kafka to Iceberg path never "
                                             "opens a Kafka transaction, so "
                                             "the rule cannot be tested on it",
        "workload": {"events": EVENTS, "partitions": 4,
                     "checkpoint_interval": INTERVAL, "parallelism": 1},
        "runs": {},
    }
    for cfg in RUNS:
        payload["runs"][cfg["key"]] = run_one(cfg)
        lab.delete_topic(cfg["out_topic"])
    lab.delete_topic(IN_TOPIC)

    short = payload["runs"]["timeout_below_interval"]
    long_ = payload["runs"]["timeout_above_interval"]
    payload["findings"] = {
        "records_lost_with_the_short_timeout":
            short["records_a_downstream_consumer_never_sees"],
        "records_lost_with_the_long_timeout":
            long_["records_a_downstream_consumer_never_sees"],
        "the_only_difference_between_the_two_runs":
            "transaction.timeout.ms",
        "raising_the_timeout_recovered":
            short["records_a_downstream_consumer_never_sees"]
            - long_["records_a_downstream_consumer_never_sees"],
        # The reason, measured rather than reasoned about: how much of the run
        # a transaction was actually open for. A timeout can only expire a
        # transaction that is Ongoing when the clock runs out.
        "samples_with_a_transaction_ongoing_short":
            short["samples_with_a_transaction_ongoing"],
        "samples_taken_short": short["samples_taken"],
        "the_broker_registered_the_five_second_timeout":
            5000 in short["broker_registered_the_requested_timeout"],
    }
    lab.write_result("exp5_transaction_timeout.json", payload, merge=False)


if __name__ == "__main__":
    main()
