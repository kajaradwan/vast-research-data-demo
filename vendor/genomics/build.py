"""Genomics demo on the lab cluster - VMS build. Idempotent; records every id in ids.json.

Runs on the Mac (management plane). Never prints a secret. Steps:
  tenant -> VIP pools -> local users -> S3 keys -> view policy (root squash, host allow-list, audit)
  -> views (cohort NFS+S3+DATABASE, events S3+DATABASE+KAFKA for the DE broker) -> identity policies
"""
import json, os, sys
from vms import client, tenant_users, L

HERE = os.path.dirname(os.path.abspath(__file__))
IDS = os.path.join(HERE, "ids.json")
KEYS = os.path.join(HERE, "keys.json")
TENANT = "rad-lab-enclave"
N5 = ["198.18.5.111", "198.18.6.111"]        # client-n5 data NICs: the only hosts allowed to mount
USERS = {"rle-svc": 7101, "rle-ingest": 7102, "rle-analyst": 7103}

v = client()
ids = json.load(open(IDS)) if os.path.exists(IDS) else {}
def save(): json.dump(ids, open(IDS, "w"), indent=1)

def find(ep, **q):
    name = q.get("name") or q.get("path")
    for x in L(getattr(v, ep).get(**q)):
        if x.get("name") == name or x.get("path") == name:
            if "tenant_id" in q and x.get("tenant_id") not in (None, q["tenant_id"]): continue
            return x
def ensure(ep, find_q, body, key):
    x = find(ep, **find_q)
    if x: print(f"exists  {ep:14} {x['id']:>6} {key}")
    else:
        x = getattr(v, ep).post(**body); print(f"created {ep:14} {x['id']:>6} {key}")
    ids[key] = x["id"]; save(); return x

# 1. tenant (local users only; no AD join - the enclave brings its own identity scope)
T = ensure("tenants", {"name": TENANT}, {"name": TENANT}, "tenant")
tid = T["id"]
T = v.tenants[tid].get(); ids["local_provider"] = T["local_provider_id"]; save()

# 2. VIP pools scoped to the tenant: where a user enters decides which provider maps them
ensure("vippools", {"name": TENANT}, {"name": TENANT, "role": "PROTOCOLS", "subnet_cidr": 16,
       "ip_ranges": [["198.18.0.43", "198.18.0.45"]], "tenant_id": tid}, "pool")
ensure("vippools", {"name": f"{TENANT}-kafka"}, {"name": f"{TENANT}-kafka", "role": "PROTOCOLS", "subnet_cidr": 16,
       "ip_ranges": [["198.18.0.46", "198.18.0.47"]], "tenant_id": tid}, "kafka_pool")

# 3. local users in the tenant's own provider
lp = ids["local_provider"]
have = tenant_users(v, lp, USERS)
for name, uid in USERS.items():
    if name in have: print(f"exists  users          {have[name]:>6} {name}")
    else:
        u = v.users.post(name=name, uid=uid, local_provider_id=lp); have[name] = u["id"]
        print(f"created users          {u['id']:>6} {name}")
ids.setdefault("users", {}).update(have); save()

# 4. S3 keys -> keys.json (600). Never printed.
keys = json.load(open(KEYS)) if os.path.exists(KEYS) else {}
for name, uid_ in have.items():
    if name in keys: continue
    r = v.users[uid_].access_keys.post(tenant_id=tid)
    keys[name] = {"access_key": r["access_key"], "secret_key": r["secret_key"], "uid": USERS[name]}
    print("minted key for", name, r["access_key"][:4] + "...")
fd = os.open(KEYS, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600); os.write(fd, json.dumps(keys, indent=1).encode()); os.close(fd)

# 5. view policy: S3-native security flavour presented over NFS too (identity policy governs the NFS uid),
#    root squash for everyone, NFS limited to n5's data NICs, protocol audit on.
audit = {"create_delete_files_dirs_objects": True, "modify_data": True, "modify_data_md": True,
         "read_data": True, "read_data_md": False, "log_username": True, "log_full_path": True}
vp_body = {"tenant_id": tid, "flavor": "S3_NATIVE", "auth_source": "RPC_AND_PROVIDERS",
           "vip_pools": [ids["pool"], ids["kafka_pool"]],
           "nfs_root_squash": ["*"], "nfs_no_squash": [], "nfs_read_write": N5, "nfs_read_only": [],
           "s3_read_write": ["*"], "protocols": ["NFSv3", "NFSv4", "S3", "DATABASE"], "protocols_audit": audit}
vp = ensure("viewpolicies", {"name": TENANT, "tenant_id": tid}, {"name": TENANT, **vp_body}, "viewpolicy")
v.viewpolicies[vp["id"]].patch(**{k: vp_body[k] for k in ("nfs_root_squash", "nfs_no_squash", "nfs_read_write",
                                  "protocols_audit")})
# gotcha (measured 20 Oct): protocols sent together with the rest came back as ['S3']; a PATCH on its own sticks
v.viewpolicies[vp["id"]].patch(protocols=vp_body["protocols"])

# 6. views
ensure("views", {"path": "/rad-lab-enclave/cohort", "tenant_id": tid},
       {"path": "/rad-lab-enclave/cohort", "name": "rad-lab-enclave-cohort", "tenant_id": tid, "policy_id": vp["id"],
        "create_dir": True, "protocols": ["NFS", "S3", "DATABASE"], "bucket": "rad-lab-enclave-cohort",
        "bucket_owner": "rle-svc"}, "view_cohort")
ensure("views", {"path": "/rad-lab-enclave/events", "tenant_id": tid},
       {"path": "/rad-lab-enclave/events", "name": "rad-lab-enclave-events", "tenant_id": tid, "policy_id": vp["id"],
        "create_dir": True, "protocols": ["S3", "DATABASE", "KAFKA"], "bucket": "rad-lab-enclave-events",
        "kafka_vip_pools": [ids["kafka_pool"]], "bucket_owner": "rle-svc"}, "view_events")

# 7. identity policies: ingest can only land files; analyst can only read; svc runs the transform
arn = lambda b, p="*": [f"arn:aws:s3:::{b}", f"arn:aws:s3:::{b}/{p}"]
TAB_RO = ["s3:TabularListSchemas", "s3:TabularListTables", "s3:TabularListColumns", "s3:TabularGetTableStats",
          "s3:TabularBeginTransaction", "s3:TabularCommitTransaction", "s3:TabularRollbackTransaction",
          "s3:TabularQueryData", "s3:TabularGetTransaction", "s3:TabularListProjections",
          "s3:TabularListProjectionColumns", "s3:TabularGetProjectionStats", "s3:TabularListViews",
          "s3:TabularGetRowColumnSecurity"]
QE = ["s3:QueryEngineStartQuery", "s3:QueryEngineQueryStatus", "s3:QueryEngineFinishQuery",
      "s3:QueryEngineListQueries", "s3:QueryEngineGetData", "s3:QueryEngineFinishData", "s3:QueryEngineGetCapabilities"]
POL = {
  "rad-lab-enclave-ingest": ("rle-ingest", [
     {"Sid": "LandOnly", "Effect": "Allow", "Resource": [f"arn:aws:s3:::rad-lab-enclave-cohort/incoming/*"],
      "Action": ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]},
     {"Sid": "SeeBucket", "Effect": "Allow", "Resource": ["arn:aws:s3:::rad-lab-enclave-cohort"],
      "Action": ["s3:ListBucket", "s3:ListBucketMultipartUploads"]},
     {"Sid": "NoReadBack", "Effect": "Deny", "Resource": arn("rad-lab-enclave-cohort"),
      "Action": ["s3:GetObject", "s3:DeleteObject"] + TAB_RO}]),
  "rad-lab-enclave-analyst": ("rle-analyst", [
     {"Sid": "ReadCohort", "Effect": "Allow", "Resource": arn("rad-lab-enclave-cohort"),
      "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation", "s3:ListAllMyBuckets"] + TAB_RO},
     {"Sid": "QueryEngine", "Effect": "Allow", "Resource": "*", "Action": QE},
     {"Sid": "CatalogRead", "Effect": "Allow", "Action": TAB_RO,
      "Resource": ["arn:aws:s3:::vast-big-catalog-bucket", "arn:aws:s3:::vast-big-catalog-bucket/*"]},
     {"Sid": "NoWrite", "Effect": "Deny", "Resource": arn("rad-lab-enclave-cohort"),
      "Action": ["s3:PutObject", "s3:DeleteObject", "s3:TabularInsertRows", "s3:TabularUpdateRows",
                 "s3:TabularDeleteRows", "s3:TabularCreateTable", "s3:TabularDropTable"]}]),
  "rad-lab-enclave-svc": ("rle-svc", [
     {"Sid": "Transform", "Effect": "Allow", "Resource": arn("rad-lab-enclave-cohort") + arn("rad-lab-enclave-events"),
      "Action": ["s3:*"]},
     {"Sid": "QueryEngine", "Effect": "Allow", "Resource": "*", "Action": QE}]),
}
ids["s3policies"] = ids.get("s3policies", {})
for pname, (uname, stmts) in POL.items():
    doc = json.dumps({"Version": "2012-10-17", "Statement": stmts})
    p = find("s3policies", name=pname)
    if p: v.s3policies[p["id"]].patch(policy=doc); print(f"updated s3policies     {p['id']:>6} {pname}")
    else: p = v.s3policies.post(name=pname, policy=doc, tenant_id=tid, enabled=True); print(f"created s3policies     {p['id']:>6} {pname}")
    uid_ = have[uname]
    cur = v.users[uid_].get().get("s3_policies_ids") or []
    if p["id"] not in cur: v.users[uid_].patch(s3_policies_ids=cur + [p["id"]])
    ids["s3policies"][pname] = p["id"]; save()
    print(f"        attached -> {uname}: {v.users[uid_].get().get('s3_policies_ids')}")

# install check (accepted != enforced; the effect is tested in demo.py)
P = v.viewpolicies[vp["id"]].get()
print("viewpolicy check:", {k: P[k] for k in ("flavor", "nfs_root_squash", "nfs_no_squash", "nfs_read_write", "protocols")})
print("audit:", P["protocols_audit"])
for k in ("view_cohort", "view_events"):
    V = v.views[ids[k]].get(); print(k, V["path"], V["protocols"], V.get("bucket"), V.get("bucket_owner"))
print(json.dumps(ids))
