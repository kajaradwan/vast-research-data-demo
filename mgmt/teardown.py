"""Teardown, VMS half, in dependency order, then an orphan sweep. Run after client/teardown_data.py.

  teardown.py [build|ops] [--dry-run]
    build  on the Mac with the cluster admin login (deploy.sh --teardown)
    ops    on the client with the deck's delete-only login (the deck's Teardown button)

  1 DataEngine: triggers -> DE topics -> disable on the tenant; drop the events bucket's leftover kafka_topics schema
    (else VMS refuses the view: "Deleting a view with created schemas or tables is not allowed"; genomics kit, 21 Oct)
  2 views -> view policies -> detach + delete identity policies -> users (keys go with them) -> VIP pools
  3 DELETE tenants/<id>/?force=true, polling the async task (the first one can FAIL while the tenant stays)
  4 the tenant's local provider: delete, or rename *-orphan-* if DataEngine's hidden user pins it (VMS 409)
  5 the deck's VMS logins and roles (ops deletes the others, then itself; its role is left for build to remove)
  6 sweep: report anything still named after the tenant or prefix, with its id
"""
import json, os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from env import TENANT, P, EVENTS, ids as load_ids, save_ids, STATE  # noqa: E402
from vms import client, session, DE, VMS, L, LOGINS  # noqa: E402

prof = next((a for a in sys.argv[1:] if not a.startswith("-")), "build")
DRY = "--dry-run" in sys.argv
ids = load_ids()
v = client(prof)
t0 = time.time()
left = []


def do(label, fn):
    print(f"[{time.time() - t0:5.1f}s] {'PLAN' if DRY else 'RUN '} {label}", flush=True)
    if DRY: return
    try:
        r = fn()
        if r is not None: print("        ->", str(r)[:220])
    except Exception as e:
        msg = str(e)
        if "error 404" in msg:
            print("        -> already gone (removed with the tenant)"); return
        print("        !!", type(e).__name__, msg[msg.find("error"):][:220] if "error" in msg else msg[:220])


tid = ids.get("tenant") or next((t["id"] for t in L(v.tenants.get()) if t["name"] == TENANT), None)
if tid and any(t["id"] == tid for t in L(v.tenants.get())):
    s = session(prof)

    def de_off():
        out = []
        for t in s.get(f"{DE}/triggers/").json().get("data", []):
            out.append(("trigger", t["name"], s.delete(f"{DE}/triggers/{t['guid']}").status_code))
        for t in s.get(f"{DE}/topics/").json().get("data", []):
            out.append(("topic", t["name"], s.delete(f"{DE}/topics/{t['guid']}").status_code))
        out.append(("data-engine off", s.delete(f"{DE}/data-engine").status_code))
        return out
    do("DataEngine: triggers, topics, disable on the tenant", de_off)

    def drop_kafka_schema():
        sv = session(prof, tenant=None); out = []; base = f"https://{VMS}/api/latest"
        q = {"database_name": EVENTS, "tenant_id": tid, "schema_name": "kafka_topics"}
        for t in sv.get(f"{base}/tables/", params=q).json().get("results", []):
            out.append((t["name"], sv.delete(f"{base}/tables/delete/", json={**{k: q[k] for k in q}, "name": t["name"]}).status_code))
        for sc in sv.get(f"{base}/schemas/", params={"database_name": EVENTS, "tenant_id": tid}).json():
            out.append((sc["name"], sv.delete(f"{base}/schemas/delete/", json={"database_name": EVENTS, "name": sc["name"],
                                                                              "tenant_id": tid}).status_code))
        return out or "none"
    do("drop leftover schemas in the events bucket", drop_kafka_schema)

    for x in sorted([x for x in L(v.views.get()) if x.get("tenant_id") == tid], key=lambda x: -len(x["path"])):
        do(f"delete view {x['id']} {x['path']}", lambda x=x: v.views[x["id"]].delete())
    for x in [x for x in L(v.viewpolicies.get()) if x.get("tenant_id") == tid and x["name"].startswith(TENANT)]:
        do(f"delete view policy {x['id']} {x['name']}", lambda x=x: v.viewpolicies[x["id"]].delete())
    users = ids.get("users", {})
    for name, uid in users.items():
        do(f"detach identity policies from user {uid} {name}", lambda uid=uid: v.users[uid].patch(s3_policies_ids=[]))
    for name, pid in ids.get("s3policies", {}).items():
        do(f"delete identity policy {pid} {name}", lambda pid=pid: v.s3policies[pid].delete())
    for name, uid in users.items():
        do(f"delete user {uid} {name}", lambda uid=uid: v.users[uid].delete())
    for x in [x for x in L(v.vippools.get()) if x["name"] in (TENANT, f"{TENANT}-kafka")]:
        do(f"delete VIP pool {x['id']} {x['name']}", lambda x=x: v.vippools[x["id"]].delete())

    def tenant_force_delete():
        sv = session(prof, tenant=None); base = f"https://{VMS}/api/latest"
        for attempt in (1, 2):
            r = sv.delete(f"{base}/tenants/{tid}/?force=true")
            task = (r.json().get("async_task") or {}).get("id") if r.content else None
            st = None
            for _ in range(36):
                if not task: break
                st = sv.get(f"{base}/vtasks/{task}/").json()
                if st.get("state") not in ("RUNNING", "PENDING"): break
                time.sleep(5)
            if sv.get(f"{base}/tenants/{tid}/").status_code == 404:
                return f"tenant {tid} gone (attempt {attempt})"
        return f"tenant {tid} still present: {(st or {}).get('state')} {(st or {}).get('messages', [])[-1:]}"
    do(f"delete tenant {tid} (force, async task polled)", tenant_force_delete)
else:
    print(f"tenant {TENANT}: not present")

lp = ids.get("local_provider")
if lp and any(p["id"] == lp for p in L(v.localproviders.get())):
    def provider():
        try:
            v.localproviders[lp].delete(); return "deleted"
        except Exception as e:
            new = f"provider-{TENANT}-orphan-{time.strftime('%Y%m%d%H%M')}"
            v.localproviders[lp].patch(name=new)
            return f"delete refused ({str(e)[-70:]}); renamed {new}: a hidden dataengine user pins it"
    do(f"local provider {lp}", provider)

deck = ids.get("deck", {})
order = [p for p in ("ro", "tadmin", "ops") if p in deck]
me = json.load(open(LOGINS)).get(prof, {}).get("username") if os.path.exists(LOGINS) else None
for p in order:
    m = deck[p]["manager"]
    do(f"delete deck login {p} (manager {m})", lambda m=m: v.managers[m].delete())
    if p != prof:
        do(f"delete deck role {p} ({deck[p]['role']})", lambda r=deck[p]["role"]: v.roles[r].delete())

# sweep with a fresh login where possible (ops just deleted itself)
try:
    v2 = client("build") if prof == "build" else None
except SystemExit:
    v2 = None
if v2 and not DRY:
    for ep, key in (("tenants", "name"), ("vippools", "name"), ("viewpolicies", "name"), ("s3policies", "name"),
                    ("managers", "username"), ("roles", "name"), ("localproviders", "name")):
        for x in L(getattr(v2, ep).get()):
            n = x.get(key) or ""
            if n.startswith(TENANT) or n.startswith(f"{P}-") or (ep == "localproviders" and TENANT in n):
                left.append(f"{ep}/{x['id']} {n}")
    for x in L(v2.views.get()):
        if x["path"].startswith(f"/{TENANT}"): left.append(f"views/{x['id']} {x['path']}")
    orphans = [x for x in left if "-orphan-" in x]
    left = [x for x in left if "-orphan-" not in x]
    print("\nsweep:", "clean" if not left else "LEFT BEHIND: " + "; ".join(left))
    if orphans: print("known orphan (a hidden dataengine user pins it; renamed, harmless):", "; ".join(orphans))
elif prof == "ops" and not DRY:
    print(f"\nsweep: the ops login removed itself; role {deck.get('ops', {}).get('role')} and any orphan provider "
          f"remain for `./deploy.sh --teardown` on the Mac to sweep")
if not DRY and prof == "build":
    for f in ("keys.json", "reset-marks.json"):      # the users these keys belonged to are gone
        p = os.path.join(STATE, f)
        if os.path.exists(p): os.remove(p); print("removed state/" + f)
if not DRY:
    for f in ("ids.json", "vms-logins.json"):
        p = os.path.join(STATE, f)
        if os.path.exists(p) and (prof == "build" or f == "vms-logins.json"):
            if f == "ids.json" and prof == "build" and not left:
                os.remove(p)
            elif f == "vms-logins.json":
                os.remove(p)
print(f"TEARDOWN_VMS_DONE in {time.time() - t0:.1f}s")
