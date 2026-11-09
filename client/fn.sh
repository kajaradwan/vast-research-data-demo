#!/bin/bash
# The function stand-ins (the DataEngine runtime's part): ./client/fn.sh start|stop|status [genomics|hep|all]
# Each runs functions/runner.py as rlu-svc (uid base+1) from $RLU_SRV/app, logs to $RLU_SRV/run/<fn>.log.
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
UID_SVC=$((RLU_UID_BASE + 1))
what=${2:-all}; [ "$what" = all ] && what="genomics hep"
sync_code() {
  rsync -a --delete --exclude stage --exclude state --exclude env --exclude logs --exclude results --exclude bin \
    --exclude .env --exclude 'live/.live*' --exclude .venv ./ "$RLU_SRV/app/"
  cp .env "$RLU_SRV/app/.env"; mkdir -p "$RLU_SRV/app/state"; chmod -R a+rX "$RLU_SRV/app"
}
pid() { pgrep -u "$UID_SVC" -f "[r]unner.py $1" | head -1; }
case "${1:-status}" in
  start)
    sync_code
    for f in $what; do
      [ -n "$(pid $f)" ] && { echo "$f: running (pid $(pid $f))"; continue; }
      [ "$f" = hep ] && [ ! -f functions/adfilter/handler.py ] && { echo "hep: no handler yet"; continue; }
      before=$(grep -c '"runner": "'$f'"' "$RLU_SRV/run/$f.log" 2>/dev/null || true)
      sudo nohup setpriv --reuid="$UID_SVC" --regid="$UID_SVC" --clear-groups env PYTHONPATH="$RLU_SRV/app/lib" LD_LIBRARY_PATH="$RLU_SRV/env/lib" HOME=/tmp \
        "$RLU_SRV/env/bin/python" -u "$RLU_SRV/app/functions/runner.py" $f >> "$RLU_SRV/run/$f.log" 2>&1 < /dev/null &
      for i in $(seq 60); do   # wait for THIS start's runner line, not the previous one still in the log
        [ "$(grep -c '"runner": "'$f'"' "$RLU_SRV/run/$f.log")" -gt "${before:-0}" ] && break; sleep 0.5; done
      sleep 1; [ -n "$(pid $f)" ] && echo "$f: up (uid $UID_SVC) $(tail -1 "$RLU_SRV/run/$f.log")" \
        || { echo "$f: FAILED to start:"; tail -4 "$RLU_SRV/run/$f.log"; exit 1; }
    done ;;
  stop)
    for f in $what; do p=$(pid $f); [ -n "$p" ] && sudo kill "$p" && echo "$f: stopped" || echo "$f: not running"; done ;;
  status)
    for f in $what; do p=$(pid $f); [ -n "$p" ] && echo "$f: running (pid $p, uid $UID_SVC)" || echo "$f: not running"; done ;;
  log) tail -n 30 "$RLU_SRV/run/${2:-genomics}.log" ;;
esac
