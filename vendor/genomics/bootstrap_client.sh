#!/bin/bash
# Prepare the data-network client (client-n5) for the enclave demo. Run from the repo dir on the client, after the
# Mac has run build.py / build_auditor.py / build_catalog_reader.py and copied keys.json (mode 600) and ids.json here.
#   1 micromamba + two envs: ./env (vastdata) and /srv/rad-lab-enclave/env (readable by the enclave uids)
#   2 /srv/rad-lab-enclave/{app,keys}: function code, one mode-600 key file per identity owned by that uid
#   3 NFS mount of the cohort view (not in fstab - nothing persistent)
#   4 Kafka topics on the events view (needed before de_setup.py)
# Idempotent. Needs sudo, docker, internet.
set -euo pipefail
cd "$(dirname "$0")"
R=$(pwd)
PKGS="python=3.12 bcftools=1.21 samtools=1.21 htslib=1.21 cyvcf2 pyarrow pandas scikit-learn boto3 awscli"
PIP="vastdb==2.1.0 trino confluent-kafka pod5 ibis-framework vastpy fastapi uvicorn"

mkdir -p bin logs results snippets
[ -x bin/micromamba ] || curl -sL https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xj -C . bin/micromamba
# install -d gives only the LAST path component the owner; intermediate dirs end up root-owned - list each one
for D in /srv/rad-lab-enclave /srv/rad-lab-enclave/app /srv/rad-lab-enclave/app/functions /srv/rad-lab-enclave/app/functions/transform; do
  sudo install -d -o "$(id -u)" -g "$(id -g)" -m 755 "$D"
done
sudo install -d -o root -g root -m 755 /srv/rad-lab-enclave/keys
for P in "$R/env" /srv/rad-lab-enclave/env; do
  if [ ! -x "$P/bin/python" ]; then
    MAMBA_ROOT_PREFIX="$(dirname "$P")/mamba" ./bin/micromamba create -y -q -p "$P" -c conda-forge -c bioconda $PKGS
  fi
  "$P/bin/pip" install -q $PIP
done
echo "envs ok: $(./env/bin/bcftools --version | head -1)"

# function code + per-identity keys
cp rle.py /srv/rad-lab-enclave/app/ && cp functions/transform/*.py /srv/rad-lab-enclave/app/functions/transform/
chmod -R a+rX /srv/rad-lab-enclave/app
chmod 600 keys.json
./env/bin/python - <<'PY'
import json, os, subprocess
k = json.load(open("keys.json"))
for user, uid in (("rle-svc", 7101), ("rle-ingest", 7102), ("rle-analyst", 7103), ("rle-auditor", 7104)):
    p = f"/tmp/rle-{user}.json"
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600); os.write(fd, json.dumps({user: k[user]}).encode()); os.close(fd)
    subprocess.run(["sudo", "install", "-o", str(uid), "-g", str(uid), "-m", "600", p, f"/srv/rad-lab-enclave/keys/{user}.json"], check=True)
    os.remove(p)
print("keys installed")
PY
sudo install -d -m 1777 /tmp/rle-run

# NFS mount of the cohort view (VIP pool rad-lab-enclave)
sudo mkdir -p /mnt/rad-lab-enclave
mountpoint -q /mnt/rad-lab-enclave || sudo mount -t nfs -o vers=3,nconnect=8,rsize=1048576 \
  198.18.0.43:/rad-lab-enclave/cohort /mnt/rad-lab-enclave
echo "mount ok"

# Kafka topics on the events view
./env/bin/python - <<'PY'
from confluent_kafka.admin import AdminClient, NewTopic
a = AdminClient({"bootstrap.servers": "198.18.0.46:9092"})
want = {"rle-landing-events": 4, "rle-dlq": 1}
have = a.list_topics(timeout=15).topics
new = [NewTopic(t, num_partitions=n, replication_factor=1) for t, n in want.items() if t not in have]
for t, f in (a.create_topics(new).items() if new else []):
    f.result(); print("created topic", t)
print("topics:", sorted(a.list_topics(timeout=15).topics))
PY
echo BOOTSTRAP_DONE
