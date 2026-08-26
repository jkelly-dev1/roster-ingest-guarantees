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
set -uo pipefail

COUNT="${1:-1000}"
shift || true

HERE="$(cd "$(dirname "$0")" && pwd)"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

python3 "$HERE/generate_roster.py" "$COUNT" "$@" > "$TMP"
LINES=$(wc -l < "$TMP")

docker cp "$TMP" rig-kafka:/tmp/events.jsonl >/dev/null
docker exec rig-kafka bash -c "
  /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic roster.updates \
    --property parse.key=true \
    --property key.separator='|' \
    < /tmp/events.jsonl 2>&1 | grep -v '^\[' || true
  rm -f /tmp/events.jsonl"

echo "produced $LINES events"
