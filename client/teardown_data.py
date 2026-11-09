"""Teardown, data-plane half (runs on the client): everything that lives behind the data network or on the client.

  1 stop the function stand-ins; remove the Trino container
  2 drop every VAST DB table and schema in the data bucket (as rlu-svc)
  3 delete every object in the data, events and datasets buckets (as rlu-svc)
  4 remove the PI homes (each as its own uid: root is squashed)
  5 delete the Kafka topics on the events view
  6 unmount the views; remove function code, per-uid keys, Trino catalogs and metastore (the tools env goes with --final,
    after the VMS half has run on it)
The VMS half is mgmt/teardown.py. Idempotent: a second run finds nothing and says so.
"""
import os, shutil, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "lib"))
from env import E, P, BUCKET, EVENTS, TENANT, HOMES, KAFKA, TOPICS, MNT, SRV  # noqa: E402

t0 = time.time()
say = lambda m: print(f"[{time.time() - t0:5.1f}s] {m}", flush=True)
sh = lambda c: subprocess.run(c, shell=True, capture_output=True, text=True)

sh(f"{HERE}/fn.sh stop all"); say("function stand-ins stopped")
r = sh(f"docker rm -f {TENANT}-trino"); say(f"trino container: {'removed' if r.returncode == 0 else 'not present'}")

have_keys = os.path.exists(f"{SRV}/keys") or os.path.exists(os.path.join(ROOT, "state", "keys.json"))
if have_keys and os.path.ismount(f"{MNT}/data"):
    import rlu
    try:
        with rlu.db(f"{P}-svc").transaction() as tx:
            b = tx.bucket(BUCKET)
            for sc in b.schemas():
                for t in sc.tables(): t.drop()
                say(f"dropped schema {sc.name} and its tables"); sc.drop()
    except Exception as e:
        say(f"VAST DB: {type(e).__name__} {str(e)[:120]}")
    s3 = rlu.s3(f"{P}-svc")
    for bucket in (BUCKET, EVENTS, f"{TENANT}-datasets"):
        n = 0
        try:
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
                objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if objs: s3.delete_objects(Bucket=bucket, Delete={"Objects": objs}); n += len(objs)
            say(f"deleted {n} objects in {bucket}")
        except Exception as e:
            say(f"{bucket}: {rlu.err(e)}")
    for pi, uid in HOMES.items():
        r = sh(f"sudo -n setpriv --reuid={uid} --regid={uid} --clear-groups rm -rf {MNT}/home/{pi}")
        say(f"removed home {pi} as uid {uid}" + (f" ({r.stderr.strip()[:80]})" if r.returncode else ""))
    try:
        from confluent_kafka.admin import AdminClient
        a = AdminClient({"bootstrap.servers": KAFKA})
        have = a.list_topics(timeout=15).topics
        gone = [t for t in TOPICS.values() if t in have]
        for t, f in (a.delete_topics(gone).items() if gone else []):
            f.result()
        say(f"deleted Kafka topics {gone or '(none left)'}")
    except Exception as e:
        say(f"Kafka: {type(e).__name__} {str(e)[:120]}")
else:
    say("no keys or no mount: data plane already gone")

for v in ("data", "home", "datasets"):
    p = f"{MNT}/{v}"
    if os.path.ismount(p):
        r = sh(f"sudo timeout 20 umount {p} || sudo umount -l {p}"); say(f"unmounted {p}")
sh(f"sudo rmdir {MNT}/data {MNT}/home {MNT}/datasets {MNT} 2>/dev/null")
# keep $SRV/env (and the mamba package cache it links to): the VMS half of teardown still runs on it.
# `client/teardown_data.py --final` removes the rest once that is done.
if "--final" in sys.argv:
    if os.path.exists(SRV): sh(f"sudo rm -rf {SRV}"); say(f"removed {SRV} (tools env)")
    say("TEARDOWN_FINAL_DONE"); sys.exit(0)
for d in ("app", "keys", "trino", "run"):
    if os.path.exists(f"{SRV}/{d}"):
        sh(f"sudo rm -rf {SRV}/{d}"); say(f"removed {SRV}/{d}")
for f in ("state/keys.json", "state/reset-marks.json"):
    p = os.path.join(ROOT, f)
    if os.path.exists(p): os.remove(p); say(f"removed {f}")
say("TEARDOWN_DATA_DONE")
