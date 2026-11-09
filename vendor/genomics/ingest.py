"""Ingest: push staged files into rad-lab-enclave-cohort/incoming/ over S3 as rle-ingest (the instrument / ingest
service). Data first, index last - the index object is the completion marker the transform keys on.

Usage: ingest.py [--only small|vcf-raw|all]   Prints one JSON line per object with bytes, seconds and MB/s.
"""
import argparse, glob, json, os, sys, time
from boto3.s3.transfer import TransferConfig
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import rle

ST = os.path.expanduser("~/genomics-kit/stage")
ap = argparse.ArgumentParser(); ap.add_argument("--only", default="all"); a = ap.parse_args()
s3 = rle.s3("rle-ingest")
cfg = TransferConfig(multipart_threshold=64 << 20, multipart_chunksize=64 << 20, max_concurrency=48)

def put(local, key):
    t0 = time.time(); n = os.path.getsize(local)
    s3.upload_file(local, rle.BUCKET, key, Config=cfg)
    dt = time.time() - t0
    print(json.dumps({"put": key, "bytes": n, "seconds": round(dt, 2), "MBps": round(n / dt / 1e6, 1)}), flush=True)
    return n, dt

plan = []
if a.only in ("small", "all"):
    plan += [(f, "incoming/reference/" + os.path.basename(f)) for f in sorted(glob.glob(f"{ST}/ref/*"))]
    plan += [(f, "incoming/meta/" + os.path.basename(f)) for f in sorted(glob.glob(f"{ST}/meta/*"))]
    plan += [(f, "incoming/nanopore/" + os.path.basename(f)) for f in sorted(glob.glob(f"{ST}/pod5/*.pod5"))]
    for c in sorted(glob.glob(f"{ST}/cram/*.cram")):
        plan += [(c, "incoming/cram/" + os.path.basename(c)), (c + ".crai", "incoming/cram/" + os.path.basename(c) + ".crai")]
    for v in sorted(glob.glob(f"{ST}/vcf/*.annotated.vcf.gz")):
        plan += [(v, "incoming/vcf/" + os.path.basename(v)), (v + ".tbi", "incoming/vcf/" + os.path.basename(v) + ".tbi")]
if a.only in ("vcf-raw", "all"):
    for v in sorted(glob.glob(f"{ST}/vcf/*.recalibrated_variants.vcf.gz")):
        plan += [(v, "incoming/vcf/" + os.path.basename(v)), (v + ".tbi", "incoming/vcf/" + os.path.basename(v) + ".tbi")]
tot_b = tot_t = 0
for local, key in plan:
    b, t = put(local, key); tot_b += b; tot_t += t
print(json.dumps({"ingest_total_bytes": tot_b, "objects": len(plan), "seconds": round(tot_t, 1),
                  "MBps": round(tot_b / tot_t / 1e6, 1) if tot_t else None}), flush=True)
