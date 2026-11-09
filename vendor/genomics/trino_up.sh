#!/bin/bash
# The image's own catalog/vast.properties is a template with unfilled ${..} placeholders and stops startup,
# so the whole catalog dir is replaced. Trino 478 (the 5.5 pairing) on client-n5, loopback-only. Two catalogs:
#   enclave = rle-analyst key on the rad-lab-enclave VIPs (read-only by identity policy)
#   audit   = rle-auditor key (audit table only; denied the cohort)
#   lake    = Hive connector, Parquet files on the same view over S3 (rle-analyst key)
#   catalog = rad-lab-rc-catalog key on the lab cluster's main pool (VAST Catalog, read-only)
set -e
cd ~/genomics-kit
D=/srv/rad-lab-enclave/trino/catalog; mkdir -p $D /srv/rad-lab-enclave/trino/metastore
./env/bin/python - <<PY
import json, os
k = json.load(open("keys.json"))
def write(name, user, ep, eps):
    body = "\n".join([f"connector.name=vast", f"endpoint={ep}", "region=us-east-1",
        f"access_key_id={k[user]['access_key']}", f"secret_access_key={k[user]['secret_key']}",
        "data_endpoints=" + ",".join(eps), "num_of_splits=64", "num_of_subsplits=10",
        "vast.http-client.request-timeout=100h", "vast.http-client.idle-timeout=290s",
        "vast.http-client.max-connections-per-server=50"]) + "\n"
    p = f"$D/{name}.properties"; fd = os.open(p, os.O_WRONLY|os.O_CREAT|os.O_TRUNC, 0o600); os.write(fd, body.encode()); os.close(fd)
write("enclave", "rle-analyst", "http://198.18.0.44", [f"http://198.18.0.{i}" for i in (43,44,45)])
write("audit", "rle-auditor", "http://198.18.0.44", [f"http://198.18.0.{i}" for i in (43,44,45)])
# lake = Hive connector over the SAME view's S3 endpoint, reading Parquet files (export_parquet.py), analyst key.
# File metastore on local disk (table definitions only; data stays on VAST). http endpoint: no self-signed TLS in the JVM.
hive = "\n".join(["connector.name=hive", "hive.metastore=file", "hive.metastore.catalog.dir=local:///",
    "fs.native-local.enabled=true", "local.location=/data/metastore", "fs.native-s3.enabled=true",
    "s3.endpoint=http://198.18.0.43", "s3.path-style-access=true", "s3.region=us-east-1",
    f"s3.aws-access-key={k['rle-analyst']['access_key']}", f"s3.aws-secret-key={k['rle-analyst']['secret_key']}",
    "hive.non-managed-table-writes-enabled=false"]) + "\n"
p = "$D/lake.properties"; fd = os.open(p, os.O_WRONLY|os.O_CREAT|os.O_TRUNC, 0o600); os.write(fd, hive.encode()); os.close(fd)
write("catalog", "rad-lab-rc-catalog", "http://198.18.0.1", [f"http://198.18.0.{i}" for i in range(1,7)])
PY
docker rm -f rad-lab-enclave-trino >/dev/null 2>&1 || true
docker run -d --name rad-lab-enclave-trino -p 127.0.0.1:8085:8080 -m 96g \
  -v $D:/etc/trino/catalog:ro \
  -v /srv/rad-lab-enclave/trino/metastore:/data/metastore \
  vastdataorg/trino-vast:478
for i in $(seq 60); do docker exec rad-lab-enclave-trino trino --execute "select 1" >/dev/null 2>&1 && { echo TRINO_UP; exit 0; }; sleep 3; done
echo TRINO_TIMEOUT; docker logs --tail 30 rad-lab-enclave-trino; exit 1
