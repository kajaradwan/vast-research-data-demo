"""Deploy step on the client: beat 1's tables, built the way the demo claims, through the trigger.

If genomics.genotypes already holds rows, nothing happens (idempotent). Otherwise: land the baseline over S3 as the
ingest identity, wait for the landing function (running as rlu-svc) to write samples, variants and genotypes, then
export the Parquet copy for the third arm. ~6 min on the lab (152 s transform, 200 s export, 4 Nov 2026).
"""
import os, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "lib"))
import rlu  # noqa: E402
from env import E, P, BUCKET, SCHEMA_G  # noqa: E402

PY = [os.path.join(ROOT, "env", "bin", "python")]
LOG = f"{E['RLU_SRV']}/run/genomics.log"


def rows(t):
    try:
        with rlu.db(f"{P}-svc").transaction() as tx:
            sc = tx.bucket(BUCKET).schema(SCHEMA_G, fail_if_missing=False)
            tb = sc.table(t, fail_if_missing=False) if sc else None
            if tb is None: return 0
            return sum(b.num_rows for b in tb.select(columns=["pos"] if t == "genotypes" else None, limit_rows=1))
    except Exception:
        return 0


if rows("genotypes") and rows("samples"):
    print("baseline tables present: skip (reset keeps them; teardown removes them)")
else:
    seen = open(LOG).read().count('"vcf->genotypes+site"') if os.path.exists(LOG) else 0
    subprocess.run(PY + [os.path.join(ROOT, "beats", "land.py"), "baseline"], check=True)
    t = time.time()
    while time.time() - t < 1800:
        txt = open(LOG).read() if os.path.exists(LOG) else ""
        if txt.count('"vcf->genotypes+site"') > seen: break
        time.sleep(5)
    else:
        sys.exit("BASELINE_TIMEOUT: no genotypes event from the landing function in 30 min (./client/fn.sh log genomics)")
    print([l for l in open(LOG).read().splitlines() if '"vcf->genotypes+site"' in l][-1][-160:])
    subprocess.run(PY + [os.path.join(ROOT, "tools", "export_parquet.py")], check=True)
print("BASELINE_DONE")
