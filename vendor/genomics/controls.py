"""Controls: every enclave control exercised with its allow-path control beside it. Data path, runs on client-n5.

A deny only counts if the matching allow-path returns OK in the same run - otherwise the row is INCONCLUSIVE.
Writes results/controls-<ts>.json.
"""
import json, os, subprocess, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import rle, logging
logging.disable(logging.CRITICAL)        # vastdb logs a rollback traceback on every expected 403

CRAM = "incoming/cram/NA12878.chr22.cram"
rows = []

def s3case(label, user, fn, expect):
    try: fn(rle.s3(user)); got = "OK"
    except Exception as e: got = rle.err(e)
    rows.append({"control": label, "identity": user, "expect": expect, "got": got,
                 "pass": (got == "OK") == (expect == "OK")})

def nfs(label, uid, cmd, expect, host=None):
    if host:
        p = subprocess.run(["ssh", "-o", "BatchMode=yes", host, cmd], capture_output=True, text=True, timeout=90)
    elif uid == 0:
        p = subprocess.run(["sudo", "bash", "-c", cmd], capture_output=True, text=True)
    else:
        p = subprocess.run(["sudo", "setpriv", "--reuid", str(uid), "--regid", str(uid), "--clear-groups", "bash", "-c", cmd],
                           capture_output=True, text=True)
    got = "OK" if p.returncode == 0 else (p.stderr.strip().splitlines() or ["rc=%d" % p.returncode])[-1].rsplit(": ", 1)[-1]
    rows.append({"control": label, "identity": f"uid {uid}" if uid is not None else host, "expect": expect, "got": got,
                 "pass": (got == "OK") == (expect == "OK")})

def dbcase(label, user, expect):
    try:
        with rle.db(user).transaction() as tx:
            [x.name for x in tx.bucket(rle.BUCKET).schema(rle.SCHEMA).tables()]
        got = "OK"
    except Exception as e:
        got = f"{type(e).__name__} {getattr(e, 'status', '') or ''}".strip()
    rows.append({"control": label, "identity": user, "expect": expect, "got": got,
                 "pass": (got == "OK") == (expect == "OK")})

B = rle.BUCKET
# S3 identity policies: ingest lands only; analyst reads only
s3case("ingest: PUT into incoming/ (allow-path)", "rle-ingest",
       lambda s: s.put_object(Bucket=B, Key="incoming/controls/probe.txt", Body=b"probe"), "OK")
s3case("ingest: PUT outside incoming/", "rle-ingest", lambda s: s.put_object(Bucket=B, Key="derived/probe.txt", Body=b"x"), "AccessDenied")
s3case("ingest: GET a CRAM it landed", "rle-ingest", lambda s: s.get_object(Bucket=B, Key=CRAM, Range="bytes=0-99")["Body"].read(), "AccessDenied")
s3case("analyst: GET the CRAM (allow-path)", "rle-analyst", lambda s: s.get_object(Bucket=B, Key=CRAM, Range="bytes=0-99")["Body"].read(), "OK")
s3case("analyst: PUT", "rle-analyst", lambda s: s.put_object(Bucket=B, Key="incoming/controls/x.txt", Body=b"x"), "AccessDenied")
s3case("analyst: DELETE the CRAM", "rle-analyst", lambda s: s.delete_object(Bucket=B, Key=CRAM), "AccessDenied")
s3case("auditor: GET the CRAM", "rle-auditor", lambda s: s.get_object(Bucket=B, Key=CRAM, Range="bytes=0-99")["Body"].read(), "AccessDenied")
# VAST DB (Tabular) access follows the same identity policies
dbcase("analyst: list cohort tables (allow-path)", "rle-analyst", "OK")
dbcase("ingest: list cohort tables", "rle-ingest", "Forbidden")
dbcase("auditor: list cohort tables", "rle-auditor", "Forbidden")
# NFS: root squash, identity policy on the NFS uid (S3-native flavour), host allow-list
f = f"{rle.MNT}/{CRAM}"
nfs("NFS uid 7103 (analyst) reads the CRAM (allow-path)", 7103, f"head -c 100 {f} >/dev/null", "OK")
nfs("NFS root reads the CRAM (root squash)", 0, f"head -c 100 {f} >/dev/null", "Permission denied")
nfs("NFS uid 7102 (ingest, the file's owner) reads it", 7102, f"head -c 100 {f} >/dev/null", "Operation not permitted")
nfs("NFS uid 7103 (analyst) writes", 7103, f"echo x > {rle.MNT}/incoming/controls/nfs.txt", "Permission denied")
nfs("NFS uid 1000 (unmapped local user) reads it", 1000, f"head -c 100 {f} >/dev/null", "Permission denied")
# the host allow-list control (mount from `lab`, which is not on the list) is run by demo.py from the Mac
# tidy the probe object as the service identity
try: rle.s3("rle-svc").delete_object(Bucket=B, Key="incoming/controls/probe.txt")
except Exception: pass
# a deny is only evidence when its allow-path control passed in the same run
allow_ok = all(r["pass"] for r in rows if r["expect"] == "OK")
for r in rows:
    if r["expect"] != "OK" and not allow_ok: r["pass"] = "INCONCLUSIVE"
w = max(len(r["control"]) for r in rows)
for r in rows:
    print(f"{r['control']:<{w}}  {r['identity']:<14} expect {r['expect']:<26} got {r['got']:<28} {r['pass']}")
out = f"results/controls-{time.strftime('%Y%m%d-%H%M%S')}.json"
os.makedirs("results", exist_ok=True); json.dump(rows, open(out, "w"), indent=1); print("wrote", out)
