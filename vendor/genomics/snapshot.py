"""Reproducibility: a LOCKED snapshot of /rad-lab-enclave/cohort (files + VAST DB tables), expiring in 14 days.

Runs on the Mac (VMS). Proves the lock: a DELETE while locked must be refused. Prints the snapshot name so the
data-path side can read the genotypes table AS OF the snapshot.

Why locked and not indestructible: indestructibility is armed on the lab cluster (`is_locked: true`), so an indestructible
snapshot could only be removed with the cluster's indestructibility password after a 24 h unlock delay, and it would
block deleting the tenant. The API and code path exist on 5.4.3 and 5.5.0; it is not exercised on a shared lab cluster.
"""
import datetime, json, os, sys
from vms import client, tenant_client, BASE, L

HERE = os.path.dirname(os.path.abspath(__file__))
ids = json.load(open(os.path.join(HERE, "ids.json")))
v = tenant_client()          # tenant admin on the client, cluster admin on the Mac
stamp = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
name = f"rad-lab-enclave-cohort-{stamp}"
exp = (datetime.datetime.utcnow() + datetime.timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
snap = v.snapshots.post(name=name, path="/rad-lab-enclave/cohort/", tenant_id=ids["tenant"], expiration_time=exp, locked=True)
S = v.snapshots[snap["id"]].get()
print(json.dumps({"snapshot": S["name"], "id": S["id"], "path": S["path"], "locked": S.get("locked"),
                  "indestructible": S.get("indestructible"), "expiration_time": S.get("expiration_time"), "state": S.get("state")}))
try:
    v.snapshots[snap["id"]].delete(); res = {"status": 200, "body": "DELETED - lock not enforced"}
except Exception as e:
    msg = str(e); res = {"status": int(msg.split("error ")[1][:3]) if "error " in msg else None, "body": msg[-160:]}
print(json.dumps({"control": "DELETE locked snapshot", "expect": "refused", **res}))
ids.setdefault("snapshots", []).append({"id": S["id"], "name": name, "expires": exp, "locked": True})
json.dump(ids, open(os.path.join(HERE, "ids.json"), "w"), indent=1)
