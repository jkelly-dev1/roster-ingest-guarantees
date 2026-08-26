#!/usr/bin/env bash
# Run one SQL statement against Trino. Trino is here to VERIFY, never to write.
#
# The jline "unable to create a system terminal" warning goes to stderr on every
# invocation and says nothing about the query. It is filtered BY PATTERN, not by
# discarding stderr, because dropping the whole stream hides real failures.
set -uo pipefail
FMT="${FMT:-CSV_UNQUOTED}"
out=$(docker exec rig-trino trino --output-format="$FMT" --execute "$*" 2>&1)
rc=$?
printf '%s\n' "$out" | grep -v -e 'org.jline' -e 'dumb terminal' -e '^WARNING:'
exit $rc
