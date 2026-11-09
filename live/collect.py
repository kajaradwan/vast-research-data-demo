"""The deck's live panels, read from the real system every few seconds and written to live/state.json.

Runs beside live/server.py (which is stdlib only and just serves this file). Sources, all least-privilege:
  VMS            the read-only deck login: cluster release, the tenant's views, quotas (used / hard)
  DataEngine     the tenant-admin deck login: the triggers and their status
  Kafka          the events view's broker: CloudEvents on each trigger's topic since the last reset
  VAST DB        Trino as the analyst: row counts (the 331M-row table is counted once a minute, the live ones each pass)
  stand-ins      whether each function runner is up (pgrep)
Never writes a secret or an address to the file.
"""
import json, os, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "lib"))
from env import E, TENANT, BUCKET, SCHEMA_G, SCHEMA_H, KAFKA, TOPICS, TRIGGERS, STATE, USERS, P  # noqa: E402

OUT = os.path.join(HERE, "state.json")
EVERY = float(os.environ.get("RLU_LIVE_POLL", "3"))
SLOW = {"genotypes", "variants", "samples"}
last_slow, slow_cache = 0, {}


_vms = {}


def vms_part():
    from vms import client, session, DE, L
    if "v" not in _vms: _vms["v"], _vms["s"] = client("ro"), session("tadmin")
    v = _vms["v"]
    cl = L(v.clusters.get())[0]
    t = [x for x in L(v.tenants.get()) if x["name"] == TENANT]
    out = {"release": ".".join(cl["sw_version"].split(".")[:4]), "build": cl.get("build", ""), "state": cl.get("state"),
           "tenant": TENANT, "exists": bool(t), "views": [], "quotas": [], "triggers": []}
    if not t: return out
    tid = t[0]["id"]
    for x in sorted(L(v.views.get()), key=lambda x: x["path"]):
        if x.get("tenant_id") == tid:
            out["views"].append({"path": x["path"], "protocols": x.get("protocols"), "bucket": x.get("bucket") or ""})
    for q in L(v.quotas.get()):
        if q.get("tenant_id") == tid or q["path"].startswith(f"/{TENANT}/"):
            out["quotas"].append({"path": q["path"], "used_gb": round((q.get("used_effective_capacity") or q.get("used_capacity") or 0) / 1e9, 1),
                                  "logical_gb": round((q.get("used_capacity") or 0) / 1e9, 1),
                                  "hard_gb": round((q.get("hard_limit") or 0) / 1e9, 1)})
    try:
        s = _vms["s"]
        for tr in s.get(f"{DE}/triggers/", timeout=10).json().get("data", []):
            cfg = tr.get("config") or {}
            out["triggers"].append({"name": tr["name"], "status": tr.get("status"), "type": tr.get("type"),
                                    "prefixes": (cfg.get("name_filters") or {}).get("prefixes"),
                                    "events": cfg.get("events"), "topic": tr.get("topic_name") or (tr.get("topic") or {}).get("name")})
    except Exception as e:
        out["triggers_error"] = str(e)[:120]
    return out


def kafka_part():
    from confluent_kafka import Consumer, TopicPartition
    marks = {}
    p = os.path.join(STATE, "reset-marks.json")
    if os.path.exists(p): marks = json.load(open(p)).get("offsets", {})
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"{P}-deck-panel"})
    out = {}
    try:
        for k in ("genomics", "hep"):
            parts = c.list_topics(TOPICS[k], timeout=8).topics[TOPICS[k]].partitions
            hi = sum(c.get_watermark_offsets(TopicPartition(TOPICS[k], q), timeout=5)[1] for q in parts)
            out[k] = {"topic": TOPICS[k], "total": hi, "since_reset": hi - marks.get(k, 0)}
    finally:
        c.close()
    return out


def trino(sql):
    """Trino's HTTP API on loopback (a CLI call starts a JVM: ~2 s each)."""
    import urllib.request
    req = urllib.request.Request(f"http://127.0.0.1:{E['RLU_TRINO_PORT']}/v1/statement", data=sql.encode(),
                                 headers={"X-Trino-User": f"{P}-deck"})
    r = json.load(urllib.request.urlopen(req, timeout=30))
    rows = []
    while True:
        if r.get("error"): raise RuntimeError(r["error"].get("message", "")[:160])
        rows += r.get("data") or []
        if not r.get("nextUri"): return rows
        r = json.load(urllib.request.urlopen(r["nextUri"], timeout=60))


def tables_part():
    global last_slow, slow_cache
    have = {}
    for schema in (SCHEMA_G, SCHEMA_H):
        try:
            for (t,) in trino(f'SHOW TABLES FROM vast."{BUCKET}/{schema}"'):
                have.setdefault(schema, []).append(t)
        except Exception:
            have.setdefault(schema, [])
    want = {SCHEMA_G: ["samples", "variants", "genotypes", "genotypes_live", "cram_qc", "nanopore_runs", "ingest_log"],
            SCHEMA_H: ["event_scores", "function_log"]}
    slow_due = time.time() - last_slow > 60
    parts, out = [], {}
    for schema, tabs in want.items():
        for t in tabs:
            key = f"{schema}.{t}"
            if t not in have.get(schema, []):
                out[key] = None; continue
            if t in SLOW and not slow_due and key in slow_cache:
                out[key] = slow_cache[key]; continue
            # count a column, not count(*): the connector can answer count(*) from table stats, which lag new inserts
            col = {"genotypes": "pos", "genotypes_live": "pos", "variants": "pos", "samples": "sample_id", "event_scores": "score"}.get(t, "object_key")
            parts.append(f"SELECT '{key}', count({col}) FROM vast.\"{BUCKET}/{schema}\".{t}")
    if parts:
        for k, n in trino(" UNION ALL ".join(parts)):
            out[k] = int(n)
            if k.split(".")[1] in SLOW: slow_cache[k] = int(n)
        if slow_due: last_slow = time.time()
    latest = {}
    try:
        if "ingest_log" in have.get(SCHEMA_G, []):
            latest["genomics"] = [dict(zip(("object", "handler", "status", "rows", "seconds", "principal"), r)) for r in trino(
                f'SELECT regexp_extract(object_key, \'[^/]+$\'), handler, status, rows, round(seconds, 1), principal '
                f'FROM vast."{BUCKET}/{SCHEMA_G}".ingest_log ORDER BY event_time DESC LIMIT 6')]
        if "function_log" in have.get(SCHEMA_H, []):
            latest["hep"] = [dict(zip(("object", "status", "events", "seconds", "principal"), r)) for r in trino(
                f'SELECT regexp_extract(object_key, \'[^/]+$\'), status, events_scored, round(rmm_s + score_s + write_s, 1), principal '
                f'FROM vast."{BUCKET}/{SCHEMA_H}".function_log ORDER BY processed_at DESC LIMIT 6')]
    except Exception as e:
        latest["error"] = str(e)[:120]
    return out, latest


def runners():
    uid = USERS[f"{P}-svc"]
    out = {}
    for f in ("genomics", "hep"):
        out[f] = subprocess.run(["pgrep", "-u", str(uid), "-f", f"[r]unner.py {f}"], capture_output=True).returncode == 0
    return out


while True:
    t = time.time()
    s = {"at": time.strftime("%H:%M:%S"), "ts": time.time(), "runners": runners()}   # ts: the browser shows its own local time
    for name, fn in (("vms", vms_part), ("kafka", kafka_part)):
        try: s[name] = fn()
        except Exception as e:
            s[name] = {"error": f"{type(e).__name__}: {str(e)[:140]}"}
            _vms.clear()                                   # a VMS restart or token expiry: log in again next pass
    try: s["tables"], s["latest"] = tables_part()
    except Exception as e: s["tables"], s["latest"] = {"error": str(e)[:140]}, {}
    s["poll_s"] = round(time.time() - t, 2)
    tmp = OUT + ".tmp"
    json.dump(s, open(tmp, "w")); os.replace(tmp, OUT)
    if "--once" in sys.argv: print(json.dumps(s, indent=1)[:3000]); break
    time.sleep(max(0.5, EVERY - (time.time() - t)))
