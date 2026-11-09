#!/usr/bin/env python3
"""Tear down the Genomics enclave demo, in dependency order. DRY RUN unless --yes.

Order (each step depends on the ones before it):
  n5   1 stop any transform runner; remove Trino container + image; unmount NFS
  VAST 2 unlock + delete the locked snapshots          (a locked snapshot blocks delete and the tenant delete)
       3 drop VAST DB tables, then schema `genomics`   (as rle-svc, over the data path from n5)
       4 delete every object in both buckets           (as rle-svc)
       5 Data Engine: element trigger -> DE topic -> DELETE /api/dataengine/data-engine (tenant disable),
         then drop the events bucket's leftover `kafka_topics` schema via VMS (else view 112 cannot be deleted)
       6 Kafka topics rle-landing-events, rle-dlq      (from n5)
       7 views 111 /rad-lab-enclave/cohort, 112 /rad-lab-enclave/events
       8 view policy 59
       9 detach + delete S3 policies 32, 33, 34, 37; delete users 53, 54, 55, 58 (their keys go with them)
      10 VIP pools 33, 34
      11 DELETE tenants/22/?force=true (force: view deletes leave dirs, plain delete 503s - lab-inventory trap)
      12 the tenant's local provider (delete, or rename if Data Engine's hidden user pins it); the dashboard's two
         VMS managers and their roles; default tenant: user rad-lab-rc-catalog + its S3 policy
  n5  13 rm -rf /srv/rad-lab-enclave ~/genomics-kit /tmp/rle-run; rmdir /mnt/rad-lab-enclave
  Mac 14 delete keys.json (code stays)
"""
import json, os, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from vms import client, session, BASE, L
YES = "--yes" in sys.argv
N5 = "vastdata@192.0.2.111"
ids = json.load(open(os.path.join(HERE, "ids.json")))
v = client()

def do(label, fn):
    print(("RUN   " if YES else "PLAN  ") + label, flush=True)
    if YES:
        try: r = fn(); print("      ->", str(r)[:200] if r is not None else "ok")
        except Exception as e: print("      !!", type(e).__name__, str(e)[:300])

def n5(cmd):
    p = subprocess.run(["ssh", "-o", "BatchMode=yes", N5, cmd], capture_output=True, text=True)
    if p.returncode: raise RuntimeError(f"rc={p.returncode}: {(p.stderr or p.stdout)[-400:]}")
    return p.stdout[-400:]

do("n5: stop runners, remove Trino, unmount",
   # [r]unner: a bare `pkill -f 'runner.py ...'` matches this ssh shell's own command line and kills the step
   # (measured 20 Oct 2026 - the whole step silently died). umount -l if the mount is hung.
   lambda: n5("pgrep -f '[r]unner.py --group rle-' | xargs -r sudo kill; pkill -f '[a]pp/server.py'; docker rm -f rad-lab-enclave-trino;"
              " docker rmi vastdataorg/trino-vast:478; sudo timeout 20 umount /mnt/rad-lab-enclave"
              " || sudo umount -l /mnt/rad-lab-enclave; echo done"))
# find snapshots by name, not from ids.json: the dashboard on the client records its snapshots in ITS copy of ids.json
for s in [x for x in L(v.snapshots.get(tenant_id=ids["tenant"])) if x["name"].startswith("rad-lab-enclave-")]:
    do(f"unlock + delete snapshot {s['id']} {s['name']}",
       lambda s=s: (v.snapshots[s["id"]].patch(locked=False), v.snapshots[s["id"]].delete()))
DATA = r'''
import rle
with rle.db("rle-svc").transaction() as tx:
    b = tx.bucket(rle.BUCKET); sc = b.schema(rle.SCHEMA, fail_if_missing=False)
    if sc:
        for t in sc.tables(): t.drop()
        sc.drop()
s3 = rle.s3("rle-svc"); n = 0
for bucket in (rle.BUCKET, "rad-lab-enclave-events"):
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs: s3.delete_objects(Bucket=bucket, Delete={"Objects": objs}); n += len(objs)
from confluent_kafka.admin import AdminClient
fs = AdminClient({"bootstrap.servers": rle.KAFKA}).delete_topics(["rle-landing-events", "rle-dlq"])
[f.result() for f in fs.values()]
print("tables+schema dropped, objects deleted", n, "kafka topics deleted")
'''
do("drop VAST DB tables + schema, delete bucket objects, delete Kafka topics (n5, as rle-svc)",
   lambda: n5(f"cd ~/genomics-kit && ./env/bin/python - <<'EOF'\n{DATA}\nEOF"))
def de_off():
    s = session("rad-lab-enclave"); out = []
    for t in s.get(f"{BASE}/api/dataengine/triggers/").json().get("data", []):
        out.append(("trigger", t["name"], s.delete(f"{BASE}/api/dataengine/triggers/{t['guid']}").status_code))
    for t in s.get(f"{BASE}/api/dataengine/topics/").json().get("data", []):
        out.append(("topic", t["name"], s.delete(f"{BASE}/api/dataengine/topics/{t['guid']}").status_code))
    out.append(("data-engine", s.delete(f"{BASE}/api/dataengine/data-engine").status_code))
    return out
do("Data Engine: trigger, topic, tenant disable", de_off)
# A KAFKA view keeps an internal `kafka_topics` schema even after every topic is deleted, and VMS refuses to delete
# a view that still has schemas ("Deleting a view with created schemas or tables is not allowed"); the tenant
# force-delete then fails on that view too. Drop the (empty) schema through VMS first. Measured 20 Oct 2026.
def drop_kafka_schema():
    s = session(); out = []
    q = {"database_name": "rad-lab-enclave-events", "tenant_id": ids["tenant"], "schema_name": "kafka_topics"}
    # topics the n5 step could not delete (Kafka error, or n5 already gone) are tables here; VMS deletes them too
    # (21 Oct 2026: a topic left in the schema makes the schema drop 409 with "drop_schema blocked (source=schema_rows)")
    for t in s.get(f"{BASE}/api/latest/tables/", params=q).json().get("results", []):
        out.append(("topic " + t["name"], s.delete(f"{BASE}/api/latest/tables/delete/", json={**q, "name": t["name"]}).status_code))
    for sc in s.get(f"{BASE}/api/latest/schemas/", params={"database_name": "rad-lab-enclave-events",
                                                            "tenant_id": ids["tenant"]}).json():
        r = s.delete(f"{BASE}/api/latest/schemas/delete/", json={"database_name": "rad-lab-enclave-events",
                                                                 "name": sc["name"], "tenant_id": ids["tenant"]})
        out.append((sc["name"], r.status_code))
    return out
do("drop leftover schemas in the events bucket (kafka_topics) via VMS", drop_kafka_schema)
for k in ("view_cohort", "view_events"):
    do(f"delete view {ids[k]}", lambda k=k: v.views[ids[k]].delete())
do(f"delete view policy {ids['viewpolicy']}", lambda: v.viewpolicies[ids["viewpolicy"]].delete())
for name, uid in ids["users"].items():
    do(f"detach policies from user {uid} {name}", lambda uid=uid: v.users[uid].patch(s3_policies_ids=[]))
for name, pid in ids["s3policies"].items():
    do(f"delete S3 policy {pid} {name}", lambda pid=pid: v.s3policies[pid].delete())
for name, uid in ids["users"].items():
    do(f"delete user {uid} {name}", lambda uid=uid: v.users[uid].delete())
for k in ("pool", "kafka_pool"):
    do(f"delete VIP pool {ids[k]}", lambda k=k: v.vippools[ids[k]].delete())
def tenant_force_delete():
    # async VMS task: a 200 here only means "scheduled" - poll the task, it can FAIL on a blocked view
    import time
    s = session(); r = s.delete(f"{BASE}/api/latest/tenants/{ids['tenant']}/?force=true")
    tid = (r.json().get("async_task") or {}).get("id")
    for _ in range(30):
        t = s.get(f"{BASE}/api/latest/vtasks/{tid}/").json()
        if t.get("state") not in ("RUNNING", "PENDING"): break
        time.sleep(10)
    return t.get("state"), (t.get("messages") or [])[-2:]
do(f"delete tenant {ids['tenant']} (force, async task polled)", tenant_force_delete)
def provider_cleanup():
    # The tenant force-delete leaves the tenant's local provider behind, and Data Engine leaves a hidden `dataengine`
    # user in it that no users/ endpoint returns, so the delete can 409 ("related object: User dataengine").
    # Rename it then, so a rebuild with the same names is not blocked (measured 21 Oct 2026).
    try:
        v.localproviders[ids["local_provider"]].delete(); return "deleted"
    except Exception as e:
        new = f"provider-rad-lab-enclave-orphan-{time.strftime('%Y%m%d%H%M')}"
        v.localproviders[ids["local_provider"]].patch(name=new); return f"delete refused ({str(e)[-80:]}); renamed to {new}"
do(f"local provider {ids['local_provider']} (left behind by the tenant force-delete)", provider_cleanup)
for key in ("tenant_admin", "manager"):
    if ids.get("dash", {}).get(key):
        do(f"delete dashboard VMS {key} {ids['dash'][key]}", lambda k=key: v.managers[ids["dash"][k]].delete())
for rid in ids.get("dash", {}).get("roles", []):
    do(f"delete dashboard VMS role {rid}", lambda rid=rid: v.roles[rid].delete())
cr = ids.get("catalog_reader", {})
if cr:
    do(f"default tenant: detach + delete S3 policy {cr['s3policy']}, delete user {cr['user']} rad-lab-rc-catalog",
       lambda: (v.users[cr["user"]].patch(s3_policies_ids=[]), v.s3policies[cr["s3policy"]].delete(), v.users[cr["user"]].delete()))
do("n5: remove /srv/rad-lab-enclave, ~/genomics-kit, /tmp/rle-run, mount dir",
   lambda: n5("sudo rm -rf /srv/rad-lab-enclave /tmp/rle-run ~/genomics-kit; sudo rmdir /mnt/rad-lab-enclave; echo done"))
do("Mac: delete keys.json and .vms-dash.json", lambda: [os.remove(os.path.join(HERE, f)) for f in ("keys.json", ".vms-dash.json")
                                                     if os.path.exists(os.path.join(HERE, f))])
if not YES: print("\nDry run. Re-run with --yes to execute.")
