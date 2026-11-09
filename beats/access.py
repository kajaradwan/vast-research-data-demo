"""One set of access rules for files and tables: the same identity policy decides NFS, S3 and VAST DB access.

The analyst reads the VCF over NFS (as its uid), the same object over S3, and the genotypes table over VAST DB: all
allowed. The ingest identity, which landed those files, is refused on all three: it can land data but not read it back.
Each refusal has an allowed request beside it, so a deny proves something.
"""
import logging, os, subprocess, sys
logging.getLogger("vastdb").setLevel(logging.CRITICAL)   # the SDK logs its own traceback on a 403
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
import rlu  # noqa: E402
from env import P, USERS, BUCKET, SCHEMA_G  # noqa: E402

KEY = "incoming/genomics/baseline/vcf/chr22.recalibrated_variants.vcf.gz"
PATH = os.path.join(rlu.MNT, KEY)
print(f"object: s3://{BUCKET}/{KEY}  (27.9 GB, landed over S3 by {P}-ingest)\n")
for who in (f"{P}-analyst", f"{P}-ingest"):
    uid = USERS[who]
    r = subprocess.run(["sudo", "-n", "setpriv", f"--reuid={uid}", f"--regid={uid}", "--clear-groups",
                        "dd", f"if={PATH}", "of=/dev/null", "bs=1M", "count=4", "iflag=direct", "status=none"],
                       capture_output=True, text=True)
    nfs = "read 4 MiB OK" if r.returncode == 0 else "REFUSED: " + r.stderr.strip().split(": ")[-1]
    try:
        n = len(rlu.s3(who).get_object(Bucket=BUCKET, Key=KEY, Range="bytes=0-4194303")["Body"].read())
        s3 = f"GET 4 MiB OK ({n} bytes)"
    except Exception as e:
        s3 = "REFUSED: " + rlu.err(e)
    try:
        with rlu.db(who).transaction() as tx:
            t = tx.bucket(BUCKET).schema(SCHEMA_G).table("genotypes")
            rows = sum(b.num_rows for b in t.select(columns=["pos", "sample_id"], limit_rows=5))
        db = f"select OK ({rows} rows)"
    except Exception as e:
        db = "REFUSED: " + ("403 AccessDenied" if "AccessDenied" in str(e) else (type(e).__name__ + " " + str(e))[:70])
    print(f"{who:12} uid {uid}")
    print(f"   NFS      {nfs}")
    print(f"   S3       {s3}")
    print(f"   VAST DB  {db}\n")
print("one identity policy per role, enforced on every protocol (the data view is S3_NATIVE: mode bits don't decide)")
