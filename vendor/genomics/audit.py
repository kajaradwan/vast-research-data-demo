"""Audit: "who read sample NA12878's CRAM?" answered from the protocol audit trail in VAST DB, as rle-auditor.

1. generate reads: the analyst reads the CRAM over NFS (O_DIRECT, uid 7103) and over S3 (ranged GET)
2. poll the audit table (Trino catalog `audit`, auditor key) until those reads appear
3. answer the question; also report which tenants' rows the auditor can see
Writes results/audit-<ts>.json.
"""
import json, os, subprocess, sys, time, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import rle, trino

SAMPLE = sys.argv[1] if len(sys.argv) > 1 else "NA12878"
CRAM = f"incoming/cram/{SAMPLE}.chr22.cram"
AT = 'audit."vast-audit-log-bucket/vast_audit_log_schema".vast_audit_log_table'

def tq(sql):
    cur = trino.dbapi.connect(host="127.0.0.1", port=8085, user="rle-auditor", catalog="audit").cursor()
    t = time.time(); cur.execute(sql); r = cur.fetchall(); return r, [d[0] for d in cur.description], time.time() - t

t_mark = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=5)
since = t_mark.strftime("%Y-%m-%d %H:%M:%S")
# 1. reads to be found. O_DIRECT on purpose: NFS audit records the RPCs that reach VAST, and a client page-cache
#    hit sends no READ (measured 20 Oct 2026 - the transform had already pulled this CRAM into n5's cache).
subprocess.run(["sudo", "setpriv", "--reuid", "7103", "--regid", "7103", "--clear-groups",
                "dd", f"if={rle.MNT}/{CRAM}", "of=/dev/null", "bs=1M", "count=16", "iflag=direct"],
               check=True, capture_output=True)
rle.s3("rle-analyst").get_object(Bucket=rle.BUCKET, Key=CRAM, Range="bytes=0-1048575")["Body"].read()
print("reads generated at", since, "UTC", flush=True)

# 2. wait for the audit trail to catch up (it is written to VAST DB asynchronously)
q_who = f"""
SELECT coalesce(login_name, '') AS login_name, uid, protocol, rpc_type, status, client_ip,
       count(*) AS ops, sum(num_bytes) AS bytes, min(time) AS first_seen, max(time) AS last_seen
FROM {AT}
WHERE tenant = 'rad-lab-enclave'
  AND (path.path LIKE '%/{SAMPLE}.chr22.cram' OR name.name = '{SAMPLE}.chr22.cram')
  AND rpc_type IN ('READ', 'GET_OBJECT', 'HEAD_OBJECT')
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY first_seen"""
t0 = time.time(); lag = None
while time.time() - t0 < 600:
    r, cols, _ = tq(q_who + "")
    recent = [x for x in r if str(x[9]) >= since]
    if any(x[2].startswith("NFS") and x[1] == 7103 for x in recent) and any(x[2] == "S3" and x[0] == "rle-analyst" for x in recent):
        lag = round(time.time() - t0, 1); break
    time.sleep(10)
r, cols, qsec = tq(q_who)
print(f"audit visible after {lag} s; question answered in {qsec:.2f} s")
print("\t".join(cols))
for x in r: print("\t".join(map(str, x)))
tenants, _, _ = tq(f"SELECT tenant, count(*) FROM {AT} WHERE time > now() - interval '6' hour GROUP BY 1")
rpc, _, _ = tq(f"""SELECT protocol, rpc_type, coalesce(login_name,''), count(*) FROM {AT}
                   WHERE tenant = 'rad-lab-enclave' AND time > now() - interval '6' hour GROUP BY 1,2,3 ORDER BY 4 DESC LIMIT 25""")
print("tenants visible to rle-auditor (6 h):", tenants)
res = {"sample": SAMPLE, "reads_generated_utc": since, "audit_lag_seconds": lag, "query_seconds": round(qsec, 2),
       "columns": cols, "who_read": [list(map(str, x)) for x in r], "tenants_visible": tenants,
       "enclave_rpc_mix": [list(map(str, x)) for x in rpc], "sql": q_who.strip()}
out = f"results/audit-{time.strftime('%Y%m%d-%H%M%S')}.json"
os.makedirs("results", exist_ok=True); json.dump(res, open(out, "w"), indent=1, default=str); print("wrote", out)
