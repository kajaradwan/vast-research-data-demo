#!/bin/bash
# Client side of deploy, after mgmt/build.py: run on the client from the repo dir (deploy.sh does it over ssh).
#   1 $RLU_SRV/app (function code) and $RLU_SRV/keys/<user>.json: mode 600, owned by that identity's uid
#   2 NFS mounts of the data, home and datasets views (not in fstab: nothing persistent)
#   3 Kafka topics on the events view (DataEngine needs them before the triggers)
#   4 Trino with two catalogs over the analyst's key: vast (VAST DB) and lake (Hive over Parquet on the same view)
# Idempotent. Needs sudo and docker. Prints no secret.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
R=$(pwd); PY=$R/env/bin/python
export PYTHONPATH=$R/lib
[ -s state/keys.json ] || { echo "no state/keys.json: run mgmt/build.py on the Mac and sync state/ first" >&2; exit 1; }
chmod 600 state/keys.json state/vms-logins.json 2>/dev/null || true

# 1 code + per-uid keys (the function stand-ins run as rlu-svc and can read only rlu-svc's key)
sudo install -d -o root -g root -m 755 "$RLU_SRV/keys"
rsync -a --delete --exclude stage --exclude state --exclude env --exclude logs --exclude results --exclude bin \
  --exclude .env --exclude 'live/.live*' "$R/" "$RLU_SRV/app/"
cp .env "$RLU_SRV/app/.env"; mkdir -p "$RLU_SRV/app/state"; chmod -R a+rX "$RLU_SRV/app"
$PY - <<'PY'
import json, os, subprocess
from env import USERS, KEYDIR
k = json.load(open("state/keys.json"))
for user, uid in USERS.items():
    p = f"/tmp/.{user}.json"
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600); os.write(fd, json.dumps({user: k[user]}).encode()); os.close(fd)
    subprocess.run(["sudo", "install", "-o", str(uid), "-g", str(uid), "-m", "600", p, f"{KEYDIR}/{user}.json"], check=True)
    os.remove(p)
print("keys installed for", ", ".join(USERS))
PY
sudo install -d -m 1777 "$RLU_SRV/run"

# 2 mounts (NFSv3; the view policies allow only this client's data NICs)
POOL0=${RLU_POOL_RANGE%%-*}
for v in data home datasets; do
  sudo mkdir -p "$RLU_MNT/$v"
  mountpoint -q "$RLU_MNT/$v" || sudo mount -t nfs -o vers=3,nconnect=8,rsize=1048576,wsize=1048576 \
    "$POOL0:/$RLU_TENANT/$v" "$RLU_MNT/$v"
done
echo "mounts: $(findmnt -rn -o TARGET | grep -c "^$RLU_MNT/") under $RLU_MNT"

# 3 Kafka topics on the events view
$PY - <<'PY'
from confluent_kafka.admin import AdminClient, NewTopic
from env import KAFKA, TOPICS
a = AdminClient({"bootstrap.servers": KAFKA})
have = a.list_topics(timeout=20).topics
new = [NewTopic(t, num_partitions=1 if t.endswith("dlq") else 4, replication_factor=1) for t in TOPICS.values() if t not in have]
for t, f in (a.create_topics(new).items() if new else []):
    f.result(); print("created topic", t)
print("topics:", sorted(t for t in a.list_topics(timeout=20).topics if not t.startswith("__")))
PY

# 4 Trino (loopback only), catalogs written mode 600 from the analyst's key
D=$RLU_SRV/trino/catalog; sudo install -d -o "$(id -u)" -m 700 "$RLU_SRV/trino" "$D" "$RLU_SRV/trino/metastore"
before=$(cat "$D"/*.properties 2>/dev/null | sha256sum || true)
$PY - <<PY
import json, os
from env import POOL, DB_EP, S3_EP, P
from env import write_secret
k = json.load(open("state/keys.json"))["%s-analyst" % P]
def w(name, lines):
    p = "$D/%s.properties" % name
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600); os.write(fd, ("\n".join(lines) + "\n").encode()); os.close(fd)
w("vast", ["connector.name=vast", "endpoint=" + DB_EP, "region=us-east-1", "access_key_id=" + k["access_key"],
           "secret_access_key=" + k["secret_key"], "data_endpoints=" + ",".join("http://" + i for i in POOL),
           "num_of_splits=64", "num_of_subsplits=10", "vast.http-client.request-timeout=100h",
           "vast.http-client.idle-timeout=290s", "vast.http-client.max-connections-per-server=50"])
# the strongest realistic alternative: Parquet files on the same view, Hive connector over S3, file metastore
w("lake", ["connector.name=hive", "hive.metastore=file", "hive.metastore.catalog.dir=local:///",
           "fs.native-local.enabled=true", "local.location=/data/metastore", "fs.native-s3.enabled=true",
           "s3.endpoint=" + S3_EP, "s3.path-style-access=true", "s3.region=us-east-1",
           "s3.aws-access-key=" + k["access_key"], "s3.aws-secret-key=" + k["secret_key"],
           "hive.non-managed-table-writes-enabled=true", "hive.collect-column-statistics-on-write=true"])
print("trino catalogs: vast, lake")
PY
NAME=$RLU_TENANT-trino
# a running Trino keeps the catalogs it started with: restart it when they changed (new keys after a rebuild)
if [ "$before" != "$(cat "$D"/*.properties | sha256sum)" ] && docker ps --format '{{.Names}}' | grep -qx "$NAME"; then
  docker restart "$NAME" >/dev/null && echo "trino restarted: catalogs changed"
fi
if ! docker ps --format '{{.Names}}' | grep -qx "$NAME"; then
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run -d --name "$NAME" -p 127.0.0.1:$RLU_TRINO_PORT:8080 -m 96g \
    -v "$D":/etc/trino/catalog:ro -v "$RLU_SRV/trino/metastore":/data/metastore \
    "$RLU_TRINO_IMAGE" >/dev/null
fi
for i in $(seq 80); do docker exec "$NAME" trino --execute "select 1" >/dev/null 2>&1 && { echo "trino up on 127.0.0.1:$RLU_TRINO_PORT"; break; }; sleep 3; done
echo BOOTSTRAP_DONE
