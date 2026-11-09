#!/usr/bin/env bash
# vast-research-data-demo: one entry point. Run on the Mac (management network + the build login); it drives the client
# over ssh for everything on the data path.
#   ./deploy.sh              build or converge to demo-start (idempotent; ~75 min cold incl. the public-data fetch)
#   ./deploy.sh --reset      back to demo-start between audiences (~25 s)
#   ./deploy.sh --teardown [--purge]   delete everything, sweep for orphans, clean the client (--purge: staged data too)
#   ./deploy.sh --status     what is standing
set -euo pipefail
cd "$(dirname "$0")"
[ -f .env ] || { echo "no .env: cp site.env.example .env and edit it" >&2; exit 1; }
set -a; . ./.env; set +a
PY=.venv/bin/python
[ -x $PY ] || { python3 -m venv .venv && .venv/bin/pip install -q vastpy requests; }
C="$RLU_CLIENT"; D="$RLU_CLIENT_DIR"
RUN="cd $D && LD_LIBRARY_PATH=$RLU_SRV/env/lib PYTHONPATH=lib ./env/bin/python"
t0=$(date +%s); el() { echo "[$(( $(date +%s) - t0 ))s] $*"; }
sync() {
  rsync -a --exclude .git --exclude 'vendor/genomics/.git' --exclude .venv --exclude stage --exclude results \
    --exclude 'live/.live*' --exclude hepenv --exclude env --exclude bin --exclude logs ./ "$C:$D/"
  ssh "$C" "chmod 600 $D/state/*.json 2>/dev/null; true"
}

case "${1:-}" in
  "")
    el "preflight (read-only)"; $PY mgmt/preflight.py
    el "VMS build (tenant, pools, users, keys, view + identity policies, views)"; $PY mgmt/build.py | tail -3
    el "deck logins (read-only, tenant admin, delete-only)"; $PY mgmt/logins.py | tail -4
    el "sync to the client"; sync
    el "client: tools env"; ssh "$C" "cd $D && ./client/env.sh" | tail -1
    el "client: public data (skips what is staged)"
    ssh "$C" "cd $D && ./client/fetch.sh >> logs/fetch.log 2>&1 && ./client/slice.sh >> logs/slice.log 2>&1 && ./client/slice_vcf.sh >> logs/slice.log 2>&1 && ./client/fetch_hep.sh >> logs/fetch_hep.log 2>&1; tail -1 logs/fetch.log"
    ssh "$C" "cd $D && [ -n \"\$(ls stage/hep/chunks 2>/dev/null)\" ] || for f in stage/hep/mc_410000.ttbar_lep.1lep.root stage/hep/data_A.1lep.root; do $RLU_SRV/env/bin/python client/split_hep.py \$f; done"
    ssh "$C" "cd $D/stage/hep && mkdir -p landing && cd landing && [ -n \"\$(ls)\" ] || { for n in 2 3; do ln -sf ../chunks/\$(ls ../chunks | grep ttbar | sed -n \${n}p) .; done; for f in ../mc_3013*.root; do ln -sf \$f .; done; }"
    el "client: keys, mounts, Kafka topics, Trino"; ssh "$C" "cd $D && ./client/bootstrap.sh" | tail -3
    el "DataEngine: enable, topics, triggers"; $PY mgmt/de_setup.py | tail -3
    el "function stand-ins"; ssh "$C" "cd $D && ./client/fn.sh start all"
    el "beat 1 tables through the trigger, then the Parquet copy"; ssh "$C" "$RUN client/baseline.py" | tail -2
    el "beat 5 homes + curated copy"; ssh "$C" "$RUN beats/seed_homes.py" | tail -1
    el "reset to demo-start"; ssh "$C" "$RUN client/reset.py" | tail -1
    el "deck"; ssh "$C" "cd $D && ./live.sh start"
    el "DEPLOY_DONE" ;;
  --reset)
    sync; ssh "$C" "$RUN client/reset.py" ;;
  --teardown)
    el "deck down"; ssh "$C" "cd $D && ./live.sh stop" || true
    el "data plane"; ssh "$C" "$RUN client/teardown_data.py" || true
    el "VMS"; $PY mgmt/teardown.py build
    ssh "$C" "$RUN client/teardown_data.py --final" || ssh "$C" "sudo rm -rf $RLU_SRV"
    if [ "${2:-}" = "--purge" ]; then
      ssh "$C" "rm -rf $D" && el "removed $C:$D, staged public data included"
    else  # keep the ~45 GB of staged public data, so a rebuild skips the fetch
      ssh "$C" "cd $D && find . -mindepth 1 -maxdepth 1 ! -name stage ! -name hepenv -exec rm -rf {} +" && el "removed $C:$D except stage/ and hepenv/ (--purge removes those too)"
    fi
    el "TEARDOWN_DONE" ;;
  --status)
    $PY - <<'PY'
import sys; sys.path.insert(0, "lib")
from env import TENANT, ids
from vms import client, L
v = client("build"); d = ids()
t = [x for x in L(v.tenants.get()) if x["name"] == TENANT]
print("tenant", TENANT, t[0]["id"] if t else "absent")
if t:
    for x in L(v.views.get()):
        if x.get("tenant_id") == t[0]["id"]: print(" view", x["id"], x["path"], x.get("protocols"), x.get("bucket") or "")
PY
    ssh "$C" "cd $D && ./client/fn.sh status; ./live.sh status; docker ps --filter name=$RLU_TENANT --format '{{.Names}} {{.Status}}'" ;;
  *) sed -n '2,7p' "$0"; exit 2 ;;
esac
