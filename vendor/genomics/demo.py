#!/usr/bin/env python3
"""Genomics enclave demo on the lab cluster - one script, end to end:

    preflight -> [reset] -> ingest (S3) -> transform (DE trigger -> function -> VAST DB) -> Parquet export -> controls
              -> query (VAST DB vs bcftools on NFS) -> audit -> snapshot -> catalog -> ML -> report

Runs on the Mac. Management calls (VMS) go from here; everything on the data path runs on client-n5
(vastdata@192.0.2.111, 128 threads) over ssh, because 198.18.0.0/16 is not routed to the Mac.

    .venv/bin/python demo.py            # full run from an empty cohort (drops tables, deletes incoming/)
    .venv/bin/python demo.py --keep     # re-run controls/query/audit/snapshot/catalog/ML on what is loaded
"""
import argparse, json, os, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
N5 = "vastdata@192.0.2.111"
R = "~/genomics-kit"
APP = "/srv/rad-lab-enclave/app"
PY = "./env/bin/python"
ap = argparse.ArgumentParser(); ap.add_argument("--keep", action="store_true"); a = ap.parse_args()
LOCAL = bool(os.environ.get("RLE_LOCAL"))   # running ON the data client (e.g. from the dashboard there)
RUN = time.strftime("%Y%m%d-%H%M%S"); LOG = os.path.join(HERE, "results", f"run-{RUN}.log")
os.makedirs(os.path.dirname(LOG), exist_ok=True)
summary = {"run": RUN, "steps": {}}

def say(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"; print(line, flush=True); open(LOG, "a").write(line + "\n")

def n5(cmd, timeout=3600, check=True):
    argv = ["bash", "-c", f"cd {R} && {cmd}"] if LOCAL else ["ssh", "-o", "BatchMode=yes", N5, f"cd {R} && {cmd}"]
    p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    out = "\n".join(l for l in (p.stdout + p.stderr).splitlines() if not l.startswith("[W::") and "jline" not in l
                    and "dumb terminal" not in l)
    open(LOG, "a").write(out + "\n")
    if check and p.returncode: raise SystemExit(f"step failed on n5: {cmd}\n{out[-2000:]}")
    return out

def step(name, fn):
    say(f"== {name}"); t = time.time(); out = fn(); dt = round(time.time() - t, 1)
    summary["steps"][name] = {"seconds": dt}; say(f"   {name} done in {dt}s"); return out

# ---------------------------------------------------------------- preflight
def preflight():
    sys.path.insert(0, HERE); from vms import client, L
    c = L(client().clusters.get())[0]
    say(f"   lab {c['build']} state {c['state']}")
    if c["state"] != "ONLINE": raise SystemExit("lab not ONLINE")
    if not LOCAL:   # on the client the code is already here
        subprocess.run(["scp", "-q", *[os.path.join(HERE, f) for f in ("rle.py", "ingest.py", "query.py", "controls.py", "audit.py",
                        "ml.py", "catalog.py", "export_parquet.py", "trino_up.sh")], f"{N5}:{R}/"], check=True)
        subprocess.run(["scp", "-q", *[os.path.join(HERE, "snippets", f) for f in ("catalog_bytes_by_ext_by_uid.sql", "lake_ddl.sql")],
                        f"{N5}:{R}/snippets/"], check=True)
        subprocess.run(["scp", "-q", *[os.path.join(HERE, "functions", "transform", f) for f in ("main.py", "runner.py")],
                        f"{N5}:{R}/functions/transform/"], check=True)
    n5(f"cp rle.py {APP}/ && cp functions/transform/*.py {APP}/functions/transform/ && chmod -R a+rX {APP}"
       " && (mountpoint -q /mnt/rad-lab-enclave || sudo mount -t nfs -o vers=3,nconnect=8,rsize=1048576"
       " 198.18.0.43:/rad-lab-enclave/cohort /mnt/rad-lab-enclave)"
       " && (docker ps --format '{{.Names}}' | grep -qx rad-lab-enclave-trino || ./trino_up.sh)"
       " && sudo install -d -m 1777 /tmp/rle-run && du -sh stage")
step("preflight", preflight)

# ---------------------------------------------------------------- reset to an empty cohort
RESET = r'''
import rle
s3 = rle.s3("rle-svc"); n = 0
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=rle.BUCKET, Prefix="incoming/"):
    objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
    if objs: s3.delete_objects(Bucket=rle.BUCKET, Delete={"Objects": objs}); n += len(objs)
with rle.db("rle-svc").transaction() as tx:
    sc = tx.bucket(rle.BUCKET).schema(rle.SCHEMA, fail_if_missing=False)
    dropped = [t.name for t in (sc.tables() if sc else [])]
    for t in (sc.tables() if sc else []): t.drop()
print("deleted objects", n, "dropped tables", dropped)
'''
if not a.keep:
    step("reset", lambda: say("   " + n5(f"{PY} - <<'EOF'\n{RESET}\nEOF").strip()))

# ---------------------------------------------------------------- ingest + transform
def ingest_transform():
    group = f"rle-run-{RUN}"
    n5(f"cd {APP}/functions/transform && (sudo setpriv --reuid 7101 --regid 7101 --clear-groups env HOME=/tmp/rle-run"
       f" LD_LIBRARY_PATH=/srv/rad-lab-enclave/env/lib PATH=/srv/rad-lab-enclave/env/bin:/usr/bin RLE_WORKERS=120"
       f" /srv/rad-lab-enclave/env/bin/python runner.py --group {group} --idle-exit 90 > /tmp/rle-run/{group}.log 2>&1 &)"
       f" && for i in $(seq 60); do grep -q runner /tmp/rle-run/{group}.log && break; sleep 1; done; head -1 /tmp/rle-run/{group}.log")
    t = time.time()
    ing = n5(f"{PY} ingest.py --only all")
    summary["ingest"] = json.loads([l for l in ing.splitlines() if "ingest_total_bytes" in l][-1])
    say(f"   ingest: {summary['ingest']}")
    n5(f"while pgrep -f '[r]unner.py --group {group}' >/dev/null; do sleep 5; done; true", timeout=7200)   # [r]: don't match this shell
    log = n5(f"cat /tmp/rle-run/{group}.log")
    handled = [json.loads(l) for l in log.splitlines() if l.startswith("{") and '"handler"' in l]
    summary["transform"] = {"events_handled": len(handled), "wall_seconds_from_first_put": round(time.time() - t, 1),
                            "by_handler": {}}
    for h in handled:
        d = summary["transform"]["by_handler"].setdefault(h["handler"], {"objects": 0, "rows": 0, "seconds": 0, "errors": 0})
        d["objects"] += 1; d["rows"] += h["rows"]; d["seconds"] = round(d["seconds"] + h["seconds"], 2)
        d["errors"] += h["status"] != "ok"
    for k, d in summary["transform"]["by_handler"].items(): say(f"   transform {k}: {d}")
    if len(handled) != 13 or any(d["errors"] for d in summary["transform"]["by_handler"].values()):
        raise SystemExit(f"transform incomplete: {len(handled)} of 13 handled")
step("ingest+transform", ingest_transform)

# the same tables as Parquet files on the same view, for the Q4 "data lake" arm (Trino Hive connector over S3)
step("parquet", lambda: say("   " + n5(f"{PY} export_parquet.py", timeout=1800).strip().replace(chr(10), chr(10) + "   ")))

# ---------------------------------------------------------------- controls
def controls():
    out = n5(f"{PY} controls.py"); say("\n" + out)
    if LOCAL:   # the client has no route/key to an off-list host; this control runs when demo.py runs on the Mac
        summary["host_allow_list"] = {"from": "skipped on the client - run demo.py from the Mac for this control",
                                      "expect": "access denied by server", "got": "not run", "pass": None}
        say("   host allow-list: skipped (run from the Mac)"); return
    p = subprocess.run(["ssh", "-o", "BatchMode=yes", "lab",
                        "sudo mkdir -p /mnt/rle-probe && timeout 40 sudo mount -t nfs -o vers=3 198.18.0.43:/rad-lab-enclave/cohort"
                        " /mnt/rle-probe; rc=$?; mountpoint -q /mnt/rle-probe && sudo umount /mnt/rle-probe; sudo rmdir /mnt/rle-probe; exit $rc"],
                       capture_output=True, text=True, timeout=120)
    got = (p.stderr.strip().splitlines() or ["mounted"])[-1]
    summary["host_allow_list"] = {"from": "lab 198.18.13.128 (not on the list)", "expect": "access denied by server",
                                  "got": got, "pass": p.returncode != 0 and "denied" in got}
    say(f"   host allow-list: {summary['host_allow_list']}")
step("controls", controls)

step("query", lambda: say("\n" + n5(f"RLE_Q4_RUNS=1 {PY} query.py", timeout=7200)))
step("audit", lambda: say("\n" + n5(f"{PY} audit.py NA12878", timeout=900)))

def snapshot():
    out = subprocess.run([sys.executable if LOCAL else os.path.join(HERE, ".venv", "bin", "python"), os.path.join(HERE, "snapshot.py")],
                         capture_output=True, text=True, check=True).stdout
    say("\n" + out)
    name = json.loads(out.splitlines()[0])["snapshot"]
    rd = n5(f"""{PY} - <<'EOF'
import rle
with rle.db("rle-analyst").transaction() as tx:
    snap = tx.bucket(rle.BUCKET).snapshot("{name}").schema(rle.SCHEMA)
    live = tx.bucket(rle.BUCKET).schema(rle.SCHEMA)
    for t in ("genotypes", "variants", "cram_qc", "samples"):
        print(t, "live", live.table(t).get_stats().num_rows, "as-of-snapshot", snap.table(t).get_stats().num_rows)
EOF""")
    say("\n" + rd); summary["snapshot"] = {"name": name, "read": rd.strip()}
step("snapshot", snapshot)
step("catalog", lambda: say("\n" + n5(f"{PY} catalog.py", timeout=1800)))
step("ml", lambda: say("\n" + n5(f"{PY} ml.py", timeout=1800)))

# ---------------------------------------------------------------- collect
if not LOCAL:
    subprocess.run(["rsync", "-a", f"{N5}:{R}/results/", os.path.join(HERE, "results") + "/"], check=True)
json.dump(summary, open(os.path.join(HERE, "results", f"run-{RUN}.json"), "w"), indent=1)
if not LOCAL:   # the dashboard runs on the client and reads results/ there
    subprocess.run(["scp", "-q", os.path.join(HERE, "results", f"run-{RUN}.json"), f"{N5}:{R}/results/"], check=False)
say(f"== done. results/run-{RUN}.json and .log; step results in results/*.json")
