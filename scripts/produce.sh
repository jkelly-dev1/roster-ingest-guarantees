#!/usr/bin/env bash
# Produce roster events into Kafka, keyed by NPI.
#
# Keyed on purpose. Kafka orders within a partition, not across them. A roster
# feed whose corrections can overtake the record they correct is a bug that
# only shows up once the topic has more than one partition, which is exactly
# when nobody is looking for it.
#
# The events go in as a file, not down a pipe. Piping the generator straight
# into `docker exec -I kafka-console-producer` hangs: the producer does not
# see EOF the way an interactive terminal delivers it, and the call never
# returns even though every record was written. Copying the file in and
# redirecting inside the container terminates cleanly and is reproducible.
#
# The count this prints is the broker's, not the generator's. The number of
# lines generated is known before Kafka has been reached at all, so it cannot
# say anything landed. So end offsets are read before and after, and the
# difference must be exactly the number of records fed in, which is what
# `lab.produce()` does for the experiments. The exit code alone is not enough
# either, because kafka-console-producer writes its complaints to stderr and
# still exits 0, so both are checked.
set -euo pipefail

COUNT="${1:-1000}"
shift || true

HERE="$(cd "$(dirname "$0")" && pwd)"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

TOPIC="roster.updates"

# partition offsets summed: `kafka-get-offsets.sh --time -1` prints one
# `topic:partition:offset` per line, and the end of the log is where the next
# record will go. A partition with no offset to report prints a non-numeric
# field, which is skipped rather than counted as zero.
end_offsets() {
  docker exec rig-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$TOPIC" --time -1 \
    | awk -F: '$3 ~ /^[0-9]+$/ { total += $3 } END { print total + 0 }'
}

python3 "$HERE/generate_roster.py" "$COUNT" "$@" > "$TMP"
LINES=$(wc -l < "$TMP")

BEFORE="$(end_offsets)"

docker cp "$TMP" rig-kafka:/tmp/events.jsonl >/dev/null
# The producer's own status is kept and returned. Its output goes to a file
# first so that the noise filter runs on the file: `producer | grep -v` makes
# the PIPELINE's status grep's, and grep exits 1 when it selects nothing, so
# the filter would report failure on a clean run and success on a failed one.
docker exec rig-kafka bash -c '
  /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic roster.updates \
    --property parse.key=true \
    --property "key.separator=|" \
    < /tmp/events.jsonl > /tmp/produce.log 2>&1
  rc=$?
  grep -v "^\[" /tmp/produce.log || true
  rm -f /tmp/events.jsonl /tmp/produce.log
  exit $rc'

AFTER="$(end_offsets)"
LANDED=$(( AFTER - BEFORE ))

if [ "$LANDED" -ne "$LINES" ]; then
  echo "produced $LANDED records into $TOPIC, expected $LINES" >&2
  exit 1
fi

echo "produced $LANDED events"
