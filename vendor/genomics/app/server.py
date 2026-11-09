"""Genomics enclave - live demo dashboard.

On the data client (client-n5), beside Trino:   ./dash_up.sh   (RLE_LOCAL=1, least-privilege VMS logins)
then from the Mac:  ssh -N -L 8095:127.0.0.1:8095 vastdata@192.0.2.111   ->  http://localhost:8095
It can still run on the Mac (no RLE_LOCAL): then it drives the client over ssh as before.

Management state comes from VMS (the Mac reaches 192.0.2.10). Data-plane state and every pipeline step run on
client-n5 over ssh (198.18.0.0/16 is not routed to the Mac). No secret is ever sent to the browser.
"""
import asyncio, glob, json, os, subprocess, sys, threading, time, uuid
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
import uvicorn
from vms import client, de_session, BASE, L

N5 = "vastdata@192.0.2.111"
LOCAL = bool(os.environ.get("RLE_LOCAL"))   # set when the dashboard runs ON the data client (dash_up.sh)
SSH = ["bash", "-c"] if LOCAL else ["ssh", "-o", "BatchMode=yes", "-o", "ControlMaster=auto", "-o", "ControlPersist=10m",
       "-o", f"ControlPath={os.path.expanduser('~/.ssh')}/cm-rle-%r@%h:%p", N5]
PYBIN = sys.executable if LOCAL else os.path.join(HERE, ".venv", "bin", "python")
R = "~/genomics-kit"
GUI = "https://192.0.2.10"
app = FastAPI()
_cache = {}

def cached(key, ttl, fn):
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < ttl: return hit[1]
    v = fn(); _cache[key] = (time.time(), v); return v

def n5(cmd, timeout=120):
    p = subprocess.run(SSH + [f"cd {R} && {cmd}"], capture_output=True, text=True, timeout=timeout)
    if p.returncode: raise HTTPException(502, (p.stderr or p.stdout)[-600:])
    return p.stdout

def n5json(args, timeout=120): return json.loads(n5(f"./env/bin/python state.py {args}", timeout).strip().splitlines()[-1])

# ---------------------------------------------------------------- management plane (VMS)
def vms_state():
    ids = json.load(open(os.path.join(HERE, "ids.json")))
    v = client(); tid = ids["tenant"]; out = {"ids": ids}
    c = L(v.clusters.get())[0]
    out["cluster"] = {k: c.get(k) for k in ("name", "build", "state", "enable_encryption", "allow_encryption", "encryption_type",
                                            "enable_vast_db_audit", "enable_json_audit", "free_usable_capacity_tb")}
    t = v.tenants[tid].get()
    out["tenant"] = {k: t.get(k) for k in ("id", "name", "data_engine_enabled", "encryption_group", "vippool_names", "local_provider_id")}
    out["pools"] = [{k: p.get(k) for k in ("id", "name", "ip_ranges", "role")} for p in L(v.vippools.get()) if p.get("tenant_id") == tid]
    p = v.viewpolicies[ids["viewpolicy"]].get()
    out["viewpolicy"] = {k: p.get(k) for k in ("id", "name", "flavor", "auth_source", "nfs_root_squash", "nfs_no_squash",
                                               "nfs_read_write", "s3_read_write", "protocols", "protocols_audit")}
    out["views"] = [{k: x.get(k) for k in ("id", "path", "protocols", "bucket", "bucket_owner")} for x in
                    (v.views[ids["view_cohort"]].get(), v.views[ids["view_events"]].get())]
    pols = {x["id"]: x for x in L(v.s3policies.get()) if x.get("tenant_id") == tid}
    out["users"] = []
    for name, uid in ids["users"].items():
        u = v.users[uid].get()
        out["users"].append({"name": u["name"], "uid": u["uid"], "id": uid, "s3_superuser": u.get("s3_superuser"),
                             "policies": [pols[i]["name"] for i in (u.get("s3_policies_ids") or []) if i in pols]})
    out["policies"] = {x["name"]: json.loads(x["policy"]) for x in pols.values()}
    s = de_session("rad-lab-enclave")
    out["de"] = {"setup": s.get(f"{BASE}/api/dataengine/data-engine").json().get("setup", {}).get("status"),
                 "triggers": [{k: x.get(k) for k in ("name", "guid", "type", "status", "source_types", "topic_name", "config")}
                              for x in s.get(f"{BASE}/api/dataengine/triggers/").json().get("data", [])]}
    out["snapshots"] = [{k: x.get(k) for k in ("id", "name", "path", "locked", "indestructible", "expiration_time", "state", "created")}
                        for x in L(v.snapshots.get(tenant_id=tid)) if str(x.get("path", "")).startswith("/rad-lab-enclave")]
    ind = L(v.indestructibility.get())[0]
    out["indestructibility"] = {"is_locked": ind.get("is_locked"), "passwd_delay": ind.get("passwd_delay")}
    bc = L(v.bigcatalogconfig.get())[0]
    out["catalog"] = {"enabled": bc.get("enable"), "state": bc.get("state"), "frames": bc.get("frames")}
    return out

LINKS = {
    "tenant": [("Tenants", f"{GUI}/#/crud/logical/tenants")],
    "network": [("Virtual IP pools", f"{GUI}/#/crud/network/vippools")],
    "views": [("Views", f"{GUI}/#/crud/logical/views"), ("View policies", f"{GUI}/#/crud/logical/policies")],
    "identity": [("Local users", f"{GUI}/#/crud/security/users"), ("Identity policies", f"{GUI}/#/crud/identitymanagement/s3userpolicies")],
    "dataengine": [("DataEngine triggers", f"{GUI}/dataengine/triggers"), ("DataEngine home", f"{GUI}/dataengine/")],
    "database": [("VAST DB browser", f"{GUI}/#/database")],
    "audit": [("Audit log query", f"{GUI}/#/query/audit"), ("Auditing settings", f"{GUI}/#/crud/settings/auditing")],
    "snapshots": [("Snapshots", f"{GUI}/#/crud/dataprotection/snapshots"), ("Indestructibility", f"{GUI}/#/crud/settings/indestructibility")],
    "catalog": [("VAST Catalog query", f"{GUI}/#/query/catalog"), ("Catalog settings", f"{GUI}/#/crud/settings/big_catalog_config")],
    "encryption": [("Encrypted paths", f"{GUI}/#/crud/logical/encryptedpaths")],
    "dataflow": [("Data flow", f"{GUI}/#/flow")],
}

@app.get("/api/vms")
def api_vms(): return {**cached("vms", 15, vms_state), "links": LINKS, "gui": GUI}

@app.get("/api/data")
def api_data(): return cached("data", 8, lambda: n5json("state"))

@app.get("/api/events")
def api_events(n: int = 15): return cached(f"ev{n}", 5, lambda: n5json(f"events {int(n)}"))

@app.get("/api/audit")
def api_audit(n: int = 40): return n5json(f"audit {int(n)}")

@app.get("/api/carriers")
def api_carriers(gene: str): return n5json(f"carriers {gene}")

@app.get("/api/try")
def api_try(identity: str, action: str):
    if identity not in ("rle-ingest", "rle-analyst", "rle-auditor", "rle-svc") or action not in ("get", "put", "delete", "tables"):
        raise HTTPException(400, "bad identity/action")
    if identity == "rle-svc" and action == "delete": raise HTTPException(400, "not deleting real data from the dashboard")
    return n5json(f"try {identity} {action}")

@app.get("/api/read")
def api_read(sample: str): return n5json(f"read {sample}")

@app.get("/api/snippet")
def api_snippet(): return {"sql": open(os.path.join(HERE, "snippets", "catalog_bytes_by_ext_by_uid.sql")).read()}

@app.get("/api/results")
def api_results():
    out = {}
    for kind in ("query", "controls", "audit", "catalog", "ml", "run", "parquet"):
        fs = sorted(glob.glob(os.path.join(HERE, "results", f"{kind}-*.json")))
        if fs: out[kind] = {"file": os.path.basename(fs[-1]), "data": json.load(open(fs[-1]))}
    return out

# ---------------------------------------------------------------- jobs (one at a time), streamed as server-sent events
STEPS = {
    "reset":    "reset",            # handled specially (python heredoc)
    "land":     "land",
    "controls": "./env/bin/python controls.py",
    "parquet":  "./env/bin/python -u export_parquet.py",
    "query":    "RLE_Q4_RUNS=1 ./env/bin/python -u query.py",
    "audit":    "./env/bin/python -u audit.py {arg}",
    "catalog":  "./env/bin/python -u catalog.py",
    "ml":       "./env/bin/python -u ml.py",
    "snapshot": "local",
    "all":      "local-all",
}
JOBS, LOCK = {}, threading.Lock()

def _job(jid, step, arg):
    j = JOBS[jid]
    def emit(line):
        if line.startswith("[W::") or "jline" in line or "dumb terminal" in line: return
        j["lines"].append(line.rstrip("\n"))
    def stream(cmd):
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in p.stdout: emit(line)
        return p.wait()
    try:
        if step == "all":
            rc = stream([PYBIN, "-u", os.path.join(HERE, "demo.py")])
        elif step == "snapshot":
            rc = stream([PYBIN, "-u", os.path.join(HERE, "snapshot.py")])
        elif step == "reset":
            rc = stream(SSH + [f"cd {R} && ./env/bin/python -u - <<'EOF'\n" + open(os.path.join(HERE, "demo.py")).read().split("RESET = r'''")[1].split("'''")[0] + "\nEOF"])
        elif step == "land":
            group = f"rle-dash-{time.strftime('%H%M%S')}"
            emit(f"starting the DE function stand-in as uid 7101 (consumer group {group}) ...")
            stream(SSH + [f"cd /srv/rad-lab-enclave/app/functions/transform && (sudo setpriv --reuid 7101 --regid 7101 --clear-groups env"
                          f" HOME=/tmp/rle-run LD_LIBRARY_PATH=/srv/rad-lab-enclave/env/lib PATH=/srv/rad-lab-enclave/env/bin:/usr/bin"
                          f" RLE_WORKERS=120 /srv/rad-lab-enclave/env/bin/python runner.py --group {group} --idle-exit 90"
                          f" > /tmp/rle-run/{group}.log 2>&1 &) && for i in $(seq 60); do grep -q runner /tmp/rle-run/{group}.log && break; sleep 1; done;"
                          f" head -1 /tmp/rle-run/{group}.log"])
            emit("landing files over S3 as rle-ingest (data first, index last) ...")
            stream(SSH + [f"cd {R} && ./env/bin/python -u ingest.py --only {arg or 'all'}"])
            emit("transform: following the function's log until it goes idle ...")
            rc = stream(SSH + [f"tail -n +1 --pid=$(pgrep -f '[r]unner.py --group {group}' | head -1) -f /tmp/rle-run/{group}.log 2>/dev/null | grep --line-buffered -v '^\\[W::'"])
        else:
            rc = stream(SSH + [f"cd {R} && " + STEPS[step].format(arg=arg or "NA12878")])
        if not LOCAL:
            subprocess.run(["rsync", "-a", f"{N5}:{R}/results/", os.path.join(HERE, "results") + "/"], capture_output=True)
        j["rc"] = rc
    except Exception as e:
        emit(f"!! {e}"); j["rc"] = -1
    finally:
        j["done"] = True; _cache.clear(); LOCK.release()

@app.post("/api/run/{step}")
def api_run(step: str, arg: str = ""):
    if step not in STEPS: raise HTTPException(404, "no such step")
    if not LOCK.acquire(blocking=False): raise HTTPException(409, "a step is already running")
    jid = uuid.uuid4().hex[:8]
    JOBS[jid] = {"step": step, "lines": [], "done": False, "rc": None, "started": time.time()}
    threading.Thread(target=_job, args=(jid, step, arg), daemon=True).start()
    return {"job": jid}

@app.get("/api/job/{jid}")
async def api_job(jid: str):
    if jid not in JOBS: raise HTTPException(404)
    async def gen():
        i = 0
        while True:
            j = JOBS[jid]
            while i < len(j["lines"]):
                yield f"data: {json.dumps({'line': j['lines'][i]})}\n\n"; i += 1
            if j["done"]:
                yield f"data: {json.dumps({'done': True, 'rc': j['rc'], 'seconds': round(time.time() - j['started'], 1)})}\n\n"; return
            await asyncio.sleep(0.4)
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/api/jobs")
def api_jobs(): return {k: {kk: vv for kk, vv in v.items() if kk != "lines"} for k, v in JOBS.items()}

@app.get("/")
def index():   # no-cache: a browser kept serving an old copy of the page after a deploy
    return FileResponse(os.path.join(HERE, "app", "index.html"), headers={"Cache-Control": "no-store"})

if __name__ == "__main__":
    # loopback only, like Trino beside it: reach it with  ssh -N -L 8095:127.0.0.1:8095 vastdata@192.0.2.111
    uvicorn.run(app, host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8095")), log_level="warning")
