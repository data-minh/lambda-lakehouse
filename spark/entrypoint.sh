#!/usr/bin/env bash
set -e
ROLE=${1:-}
export SPARK_NO_DAEMONIZE=true

case "$ROLE" in
  master)
    exec "$SPARK_HOME/sbin/start-master.sh" -h 0.0.0.0 -p 7077 ;;
  worker)
    exec "$SPARK_HOME/sbin/start-worker.sh" spark://spark-master:7077 ;;
  *)
    echo "Usage: ./entrypoint.sh {master|worker}" >&2; exit 1 ;;
esac