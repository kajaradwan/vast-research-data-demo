"""Data-plane state for the dashboard, one JSON object on stdout. Runs on client-n5 (vastdata), read-only.

  python state.py              -> tables, objects, kafka, runner, trino, mount
  python state.py events N     -> last N CloudEvents from the element trigger's topic
  python state.py audit N      -> last N audit rows for the enclave tenant (as rle-auditor via Trino)
  python state.py carriers GENE -> live DB answer: rare PASS carriers in GENE's promoter, by superpopulation
  python state.py try IDENTITY ACTION -> one live control attempt (S3 / VAST DB) as that identity
"""
import json, logging, os, subprocess, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
logging.disable(logging.CRITICAL)
import rle

VT = 'enclave."rad-lab-enclave-cohort/genomics"'
AT = 'audit."vast-audit-log-bucket/vast_audit_log_schema".vast_audit_log_table'

def trino(sql, user="rle-analyst", catalog="enclave"):
    import trino as T
    cur = T.dbapi.connect(host="127.0.0.1", port=8085, user=user, catalog=catalog, request_timeout=60).cursor()
    t = time.time(); cur.execute(sql); rows = cur.fetchall()
    return {"columns": [d[0] for d in cur.description], "rows": rows, "seconds": round(time.time() - t, 3)}

def state():
    out = {"ts": time.time()}
    # exact counts through Trino: vastdb get_stats() lags a bulk load and can even over-count right after a
    # drop-and-reload (6,404 vs 3,202 measured 21 Oct 2026); stats are only the fallback when Trino is down
    names = ("samples", "variants", "genotypes", "cram_qc", "nanopore_runs", "ingest_log")
    try:
        r = trino("SELECT " + ", ".join(f"(SELECT count(*) FROM {VT}.{n})" for n in names))
        out["tables"] = dict(zip(names, r["rows"][0])); out["tables_source"] = "count(*)"
    except Exception:
        try:
            with rle.db("rle-analyst").transaction() as tx:
                sc = tx.bucket(rle.BUCKET).schema(rle.SCHEMA, fail_if_missing=False)
                out["tables"] = {t.name: t.get_stats().num_rows for t in (sc.tables() if sc else [])}
                out["tables_source"] = "stats"
        except Exception as e: out["tables_error"] = str(e)[:200]
    try:
        n = b = 0; kinds = {}
        for page in rle.s3("rle-analyst").get_paginator("list_objects_v2").paginate(Bucket=rle.BUCKET, Prefix="incoming/"):
            for o in page.get("Contents", []):
                n += 1; b += o["Size"]; k = o["Key"].split("/")[1] if o["Key"].count("/") > 1 else "other"
                kinds[k] = kinds.get(k, 0) + 1
        out["objects"] = {"count": n, "bytes": b, "by_prefix": kinds}
    except Exception as e: out["objects_error"] = str(e)[:200]
    try:
        from confluent_kafka import Consumer, TopicPartition
        c = Consumer({"bootstrap.servers": rle.KAFKA, "group.id": "rle-dash-peek"})
        md = c.list_topics("rle-landing-events", timeout=5).topics["rle-landing-events"].partitions
        out["kafka_events_total"] = sum(c.get_watermark_offsets(TopicPartition("rle-landing-events", p), timeout=5)[1] for p in md)
        c.close()
    except Exception as e: out["kafka_error"] = str(e)[:200]
    ps = subprocess.run(["pgrep", "-af", "runner.py --group"], capture_output=True, text=True).stdout.strip()
    out["runner"] = [l.split("--group ")[1].split()[0] for l in ps.splitlines() if "--group" in l]
    out["trino"] = subprocess.run(["docker", "inspect", "-f", "{{.State.Status}}", "rad-lab-enclave-trino"],
                                  capture_output=True, text=True).stdout.strip() or "absent"
    out["mount"] = subprocess.run(["mountpoint", "-q", rle.MNT]).returncode == 0
    return out

def events(n):
    from confluent_kafka import Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": rle.KAFKA, "group.id": f"rle-dash-{time.time()}", "enable.auto.commit": False})
    parts = c.list_topics("rle-landing-events", timeout=5).topics["rle-landing-events"].partitions
    tps = []
    for p in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition("rle-landing-events", p), timeout=5)
        tps.append(TopicPartition("rle-landing-events", p, max(lo, hi - n)))
    c.assign(tps); got = []; end = time.time() + 4
    while time.time() < end:
        m = c.poll(0.5)
        if m is None or m.error(): continue
        h = {k: (v.decode() if isinstance(v, bytes) else v) for k, v in (m.headers() or [])}
        rec = json.loads(m.value())["Records"][0]
        got.append({"time": h.get("ce_time"), "type": h.get("ce_type"), "path": h.get("ce_elementpath"),
                    "source": h.get("ce_elementsourcetype"), "principal": rec["userIdentity"]["principalId"],
                    "ip": rec["requestParameters"]["sourceIPAddress"], "event": rec["eventName"], "id": h.get("ce_id")})
    c.close()
    return sorted(got, key=lambda x: x["time"] or "", reverse=True)[:n]

def audit(n):
    return trino(f"""SELECT time, protocol, rpc_type, status, coalesce(login_name,'') AS who, uid, client_ip,
                            path.path AS path, num_bytes
                     FROM {AT} WHERE tenant = 'rad-lab-enclave' AND time > now() - interval '24' hour
                     ORDER BY time DESC LIMIT {int(n)}""", user="rle-auditor", catalog="audit")

def carriers(gene):
    gene = "".join(ch for ch in gene if ch.isalnum() or ch in "-_.")
    win = trino(f"""SELECT min(pos), max(pos) FROM {VT}.variants
                    WHERE gene = '{gene}' AND effect IN ('upstream_gene_variant', '5_prime_UTR_variant')""")
    a, b = win["rows"][0]
    if a is None: return {"gene": gene, "error": "no promoter/upstream annotation for that gene on chr22"}
    r = trino(f"""SELECT s.superpopulation, count(DISTINCT g.sample_id) AS carriers, count(*) AS carrier_genotypes
                  FROM {VT}.genotypes g JOIN {VT}.samples s ON g.sample_id = s.sample_id
                  WHERE g.chrom = 'chr22' AND g.pos BETWEEN {a} AND {b} AND g.filter = 'PASS' AND g.af < 0.01
                  GROUP BY 1 ORDER BY 1""")
    return {"gene": gene, "window": f"chr22:{a}-{b}", "result": r}

def try_(identity, action):
    B = rle.BUCKET; key = "incoming/cram/NA12878.chr22.cram"
    acts = {
        "get": lambda: rle.s3(identity).get_object(Bucket=B, Key=key, Range="bytes=0-99")["Body"].read(),
        "put": lambda: rle.s3(identity).put_object(Bucket=B, Key="incoming/controls/dash-probe.txt", Body=b"probe"),
        "delete": lambda: rle.s3(identity).delete_object(Bucket=B, Key=key),
        "tables": lambda: [t.name for t in rle.db(identity).transaction().__enter__().bucket(B).schema(rle.SCHEMA).tables()],
    }
    t = time.time()
    try: acts[action](); res = "200 OK"
    except Exception as e: res = rle.err(e) if "botocore" in type(e).__module__ else f"{type(e).__name__} {getattr(e, 'status', '')}"
    if action == "put" and res == "200 OK":
        rle.s3("rle-svc").delete_object(Bucket=B, Key="incoming/controls/dash-probe.txt")
    return {"identity": identity, "action": action, "result": res, "ms": round((time.time() - t) * 1000)}

def read_(sample):
    """The analyst reads one sample's CRAM: 16 MiB over NFS (O_DIRECT, uid 7103) and a 1 MiB ranged GET over S3."""
    sample = "".join(ch for ch in sample if ch.isalnum())
    key = f"incoming/cram/{sample}.chr22.cram"; t = time.time()
    # O_DIRECT: a page-cache hit on n5 would send no READ RPC to VAST, so nothing would reach the audit trail
    p = subprocess.run(["sudo", "setpriv", "--reuid", "7103", "--regid", "7103", "--clear-groups", "dd",
                        f"if={rle.MNT}/{key}", "of=/dev/null", "bs=1M", "count=16", "iflag=direct"], capture_output=True, text=True)
    rle.s3("rle-analyst").get_object(Bucket=rle.BUCKET, Key=key, Range="bytes=0-1048575")["Body"].read()
    return {"sample": sample, "nfs_read": (p.stderr.strip().splitlines() or ["?"])[-1],
            "s3_ranged_get": "1 MiB", "at_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(t)), "identity": "rle-analyst / uid 7103"}

if __name__ == "__main__":
    a = sys.argv[1:] or ["state"]
    fn = {"state": lambda: state(), "events": lambda: events(int(a[1])), "audit": lambda: audit(a[1]),
          "carriers": lambda: carriers(a[1]), "try": lambda: try_(a[1], a[2]), "read": lambda: read_(a[1])}[a[0]]
    print(json.dumps(fn(), default=str))
