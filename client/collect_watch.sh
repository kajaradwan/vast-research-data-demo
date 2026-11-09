#!/usr/bin/env bash
# Restart the panel collector if live/state.json stops changing (it hung once, 5 Nov, after a broker blip).
# Started by ./live.sh start; read-only like the collector itself.
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
while sleep 30; do
  age=$(( $(date +%s) - $(stat -c %Y live/state.json 2>/dev/null || echo 0) ))
  [ "$age" -lt 90 ] && continue
  echo "$(date '+%F %T') collect_watch: state.json ${age}s old, restarting the collector"
  [ -f .live-collect.pid ] && kill "$(cat .live-collect.pid)" 2>/dev/null; sleep 2
  PYTHONPATH=lib LD_LIBRARY_PATH="$RLU_SRV/env/lib" nohup ./env/bin/python live/collect.py >> logs/live-collect.log 2>&1 < /dev/null &
  echo $! > .live-collect.pid
done
