"""rle-auditor (tenant rad-lab-enclave, uid 7104): read-only on the VAST DB audit table, nothing else. Idempotent."""
import json, os
from vms import client, tenant_users, L
HERE = os.path.dirname(os.path.abspath(__file__))
IDS, KEYS = os.path.join(HERE, "ids.json"), os.path.join(HERE, "keys.json")
NAME, UID, PNAME = "rle-auditor", 7104, "rad-lab-enclave-auditor"
v = client(); ids = json.load(open(IDS)); keys = json.load(open(KEYS)); tid = ids["tenant"]
u = tenant_users(v, ids["local_provider"], [NAME]).get(NAME) or v.users.post(name=NAME, uid=UID, local_provider_id=ids["local_provider"])["id"]
arn = ["arn:aws:s3:::vast-audit-log-bucket", "arn:aws:s3:::vast-audit-log-bucket/*"]
doc = json.dumps({"Version": "2012-10-17", "Statement": [
  {"Sid": "AuditRead", "Effect": "Allow", "Resource": arn,
   "Action": ["s3:TabularListSchemas", "s3:TabularListTables", "s3:TabularListColumns", "s3:TabularGetTableStats",
              "s3:TabularBeginTransaction", "s3:TabularCommitTransaction", "s3:TabularRollbackTransaction",
              "s3:TabularQueryData", "s3:TabularGetTransaction", "s3:TabularListProjections",
              "s3:TabularListProjectionColumns", "s3:TabularGetProjectionStats", "s3:TabularListViews",
              "s3:TabularGetRowColumnSecurity"]},
  {"Sid": "NoCohortData", "Effect": "Deny", "Resource": ["arn:aws:s3:::rad-lab-enclave-cohort", "arn:aws:s3:::rad-lab-enclave-cohort/*"],
   "Action": ["s3:GetObject", "s3:TabularQueryData"]}]})
p = next((x for x in L(v.s3policies.get(name=PNAME)) if x["name"] == PNAME), None)
if p: v.s3policies[p["id"]].patch(policy=doc)
else: p = v.s3policies.post(name=PNAME, policy=doc, tenant_id=tid, enabled=True)
cur = v.users[u].get().get("s3_policies_ids") or []
if p["id"] not in cur: v.users[u].patch(s3_policies_ids=cur + [p["id"]])
if NAME not in keys:
    r = v.users[u].access_keys.post(tenant_id=tid)
    keys[NAME] = {"access_key": r["access_key"], "secret_key": r["secret_key"], "uid": UID}
    fd = os.open(KEYS, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600); os.write(fd, json.dumps(keys, indent=1).encode()); os.close(fd)
    print("minted key for", NAME)
ids["users"][NAME] = u; ids["s3policies"][PNAME] = p["id"]; json.dump(ids, open(IDS, "w"), indent=1)
print("auditor user", u, "policy", p["id"])
