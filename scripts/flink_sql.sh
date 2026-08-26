#!/usr/bin/env bash
# Run a Flink SQL file inside the JobManager container.
#
# The whole file runs in one SQL client session. Catalogs, temporary tables and
# SET statements do not survive between invocations, so anything that depends
# on a catalog has to travel in the same file that creates it. This is the same
# trap any per-statement CLI has, in a different tool.
set -uo pipefail
[ $# -eq 1 ] || { echo "usage: $0 <sql-file>" >&2; exit 2; }
#
# The color codes are stripped here, at the source. The SQL client writes ANSI
# escape sequences around its own [INFO] lines. They are invisible on a
# terminal and they are still there in anything that captures this output, so
# removing them afterward means editing evidence rather than not producing it.
docker cp "$1" rig-flink-jm:/tmp/job.sql >/dev/null
docker exec rig-flink-jm /opt/flink/bin/sql-client.sh -f /tmp/job.sql 2>&1 \
  | sed -e 's/\x1b\[[0-9;]*m//g' \
  | grep -v -e '^$' -e 'Command history' -e 'Flink SQL>' -e '^ *$'
