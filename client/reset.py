"""Reset to demo-start, between audiences. Runs on the client (the deck's Reset button, or deploy.sh --reset).

Demo-start = beat 1's tables in place (samples, variants, genotypes + their Parquet copy; built by the trigger at deploy)
and beat 5's homes and curated copy seeded. Everything a live beat adds is removed:
  beat 2  objects under incoming/genomics/live-*/ ; tables genomics.cram_qc, genomics.nanopore_runs, genomics.ingest_log
  slide 13 the landed VCF slice (also under live-*/) ; table genomics.genotypes_live
  beat 3  objects under incoming/hep/ ; tables hep.event_scores, hep.function_log
  beat 5  stray files a demo click may have left (none expected: the /datasets export refuses writes)
then restarts the function stand-ins so their consumers start at the end of the topics, and records the topics' offsets
so the deck's event counters start from zero. Prints each step and the total time.
"""
import json, os, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "lib"))
import rlu  # noqa: E402
from env import P, BUCKET, SCHEMA_G, SCHEMA_H, KAFKA, TOPICS, STATE  # noqa: E402

t0 = time.time()
step = lambda m: print(f"[{time.time() - t0:5.1f}s] {m}", flush=True)
subprocess.run([os.path.join(HERE, "fn.sh"), "stop", "all"], check=False)
step("function stand-ins stopped")

s3 = rlu.s3(f"{P}-svc")
for prefix in ("incoming/genomics/live-", "incoming/hep/"):
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs: s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objs}); n += len(objs)
    step(f"deleted {n} objects under {prefix}")

with rlu.db(f"{P}-svc").transaction() as tx:
    b = tx.bucket(BUCKET)
    for schema, tables in ((SCHEMA_G, ("cram_qc", "nanopore_runs", "ingest_log", "genotypes_live")), (SCHEMA_H, ("event_scores", "function_log"))):
        sc = b.schema(schema, fail_if_missing=False)
        for t in tables:
            tb = sc.table(t, fail_if_missing=False) if sc else None
            if tb is not None: tb.drop(); step(f"dropped {schema}.{t} (the function recreates it empty)")

import glob, shutil  # noqa: E402
arch = os.path.join(ROOT, "results", "archive"); os.makedirs(arch, exist_ok=True)
old = glob.glob(os.path.join(ROOT, "results", "sfs-*.json"))
for f in old: shutil.move(f, arch)
step(f"archived {len(old)} SFS result files (slide 12 starts empty)")

from confluent_kafka import Consumer, TopicPartition  # noqa: E402
c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"{P}-deck-offsets"})
marks = {}
for k in ("genomics", "hep"):
    md = c.list_topics(TOPICS[k], timeout=15).topics[TOPICS[k]].partitions
    marks[k] = sum(c.get_watermark_offsets(TopicPartition(TOPICS[k], p), timeout=10)[1] for p in md)
c.close()
json.dump({"at": time.time(), "offsets": marks}, open(os.path.join(STATE, "reset-marks.json"), "w"))
step(f"event counters zeroed at topic offsets {marks}")

r = subprocess.run([os.path.join(HERE, "fn.sh"), "start", "all"], capture_output=True, text=True)
print(r.stdout.strip())
step("function stand-ins up: tables are recreated empty on start")
print(f"RESET_DONE in {time.time() - t0:.1f}s")
