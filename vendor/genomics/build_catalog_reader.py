"""A dedicated read-only VAST Catalog identity in the default tenant (stands in for research computing's catalog reader). Idempotent.

"""
import json, os
from vms import client, tenant_users, L

HERE = os.path.dirname(os.path.abspath(__file__))
IDS, KEYS = os.path.join(HERE, "ids.json"), os.path.join(HERE, "keys.json")
NAME, UID, PNAME = "rad-lab-rc-catalog", 7110, "rad-lab-rc-catalog-read"
v = client(); ids = json.load(open(IDS)); keys = json.load(open(KEYS))
lp = v.tenants[1].get()["local_provider_id"]
u = tenant_users(v, lp, [NAME]).get(NAME) or v.users.post(name=NAME, uid=UID, local_provider_id=lp)["id"]
arn = ["arn:aws:s3:::vast-big-catalog-bucket", "arn:aws:s3:::vast-big-catalog-bucket/*"]
doc = json.dumps({"Version": "2012-10-17", "Statement": [{"Sid": "CatalogRead", "Effect": "Allow", "Resource": arn,
       "Action": ["s3:TabularListSchemas", "s3:TabularListTables", "s3:TabularListColumns", "s3:TabularGetTableStats",
                  "s3:TabularBeginTransaction", "s3:TabularCommitTransaction", "s3:TabularRollbackTransaction",
                  "s3:TabularQueryData", "s3:TabularGetTransaction", "s3:TabularListProjections",
                  "s3:TabularListProjectionColumns", "s3:TabularGetProjectionStats", "s3:TabularListViews",
                  "s3:TabularGetRowColumnSecurity", "s3:ListAllMyBuckets", "s3:ListBucket", "s3:GetBucketLocation"]}]})
p = next((x for x in L(v.s3policies.get(name=PNAME)) if x["name"] == PNAME), None)
p = v.s3policies[p["id"]].patch(policy=doc) or p if p else v.s3policies.post(name=PNAME, policy=doc, tenant_id=1, enabled=True)
cur = v.users[u].get().get("s3_policies_ids") or []
if p["id"] not in cur: v.users[u].patch(s3_policies_ids=cur + [p["id"]])
if NAME not in keys:
    r = v.users[u].access_keys.post(tenant_id=1)
    keys[NAME] = {"access_key": r["access_key"], "secret_key": r["secret_key"], "uid": UID}
    fd = os.open(KEYS, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600); os.write(fd, json.dumps(keys, indent=1).encode()); os.close(fd)
    print("minted key for", NAME)
ids["catalog_reader"] = {"user": u, "s3policy": p["id"], "tenant": 1}
json.dump(ids, open(IDS, "w"), indent=1)
U = v.users[u].get()
print("user", u, U["name"], "uid", U["uid"], "policies", U.get("s3_policies_ids"), "s3_superuser", U.get("s3_superuser"))
