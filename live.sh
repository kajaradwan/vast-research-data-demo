#!/usr/bin/env bash
# The live deck: slides with Run buttons that execute the demo on this host.
#   ./live.sh start | stop | status | url
# Listens on the management network (RLU_LIVE_BIND, default all interfaces), port $RLU_LIVE_PORT. No tunnel: open the
# printed URL from the presenting laptop. The URL carries a token (kept in .live.token, same across restarts) because
# the buttons change the cluster. live/collect.py runs beside the server and feeds the live panels.
cd "$(dirname "$0")"
set -a; . ./.env; set +a
port="${RLU_LIVE_PORT:-8097}"
pidf=.live.pid; cpidf=.live-collect.pid; wpidf=.live-watch.pid; tokf=.live.token
host="${RLU_LIVE_HOST:-$(echo "${SSH_CONNECTION:-}" | awk '{print $3}')}"
[ -n "$host" ] || host="$(hostname -I 2>/dev/null | awk '{print $1}')"
url() { echo "http://$host:$port/?k=$(cat $tokf)"; }
alive() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }
case "${1:-status}" in
  start)
    if alive $pidf; then echo "live: already running (pid $(cat $pidf)): $(url)"; exit 0; fi
    [ -s $tokf ] || { (umask 077; python3 -c 'import secrets; print(secrets.token_hex(16))' > $tokf); }
    mkdir -p logs
    alive $cpidf || { PYTHONPATH=lib LD_LIBRARY_PATH="$RLU_SRV/env/lib" nohup ./env/bin/python live/collect.py >> logs/live-collect.log 2>&1 < /dev/null &
                      echo $! > $cpidf; }
    alive $wpidf || { nohup ./client/collect_watch.sh >> logs/live-collect.log 2>&1 < /dev/null & echo $! > $wpidf; }
    RLU_LIVE_PORT=$port RLU_LIVE_TOKEN="$(cat $tokf)" nohup python3 live/server.py >> logs/live.log 2>&1 < /dev/null &
    echo $! > $pidf; sleep 1
    code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/api/steps")
    [ "$code" = 403 ] && echo "live: up (pid $(cat $pidf)). Open: $(url)" \
      || { echo "live: failed to start (got $code), see logs/live.log" >&2; exit 1; } ;;
  stop)
    for f in $wpidf $pidf $cpidf; do [ -f $f ] && kill "$(cat $f)" 2>/dev/null; rm -f $f; done; echo "live: stopped" ;;
  status)
    alive $pidf && echo "live: running (pid $(cat $pidf)): $(url)" || echo "live: not running"
    alive $cpidf && echo "panels: collector running (pid $(cat $cpidf))" || echo "panels: collector not running" ;;
  url)
    [ -s $tokf ] && url || echo "live: no token yet; run ./live.sh start" ;;
  *) sed -n '2,6p' "$0"; exit 2 ;;
esac
