#!/bin/bash
# Start the dashboard ON the data client, beside Trino, bound to loopback only (not a service: stops on reboot).
# VMS access uses the least-privilege logins in .vms-dash.json (build_dash_manager.py), never the cluster admin.
# From the Mac:  ssh -N -L 8095:127.0.0.1:8095 vastdata@192.0.2.111   then open http://localhost:8095
set -euo pipefail
cd "$(dirname "$0")"
chmod 600 .vms-dash.json
pkill -f "[a]pp/server.py" || true
RLE_LOCAL=1 RLE_VMS_CREDS="$PWD/.vms-dash.json" setsid nohup ./env/bin/python app/server.py > logs/dashboard.log 2>&1 < /dev/null &
for i in $(seq 30); do curl -s -o /dev/null http://127.0.0.1:8095/ && { echo DASH_UP; exit 0; }; sleep 1; done
echo DASH_TIMEOUT; tail -20 logs/dashboard.log; exit 1
