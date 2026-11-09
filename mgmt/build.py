"""VMS build for vast-research-data-demo. Idempotent: re-running converges. Records ids in state/ids.json.

Runs on the Mac (management plane) as the build login. Prints no secret. Order:
  tenant -> VIP pools -> local users -> S3 keys -> view policies -> views -> identity policies
Patterns (S3_NATIVE over NFS, root squash, host allow-list, ingest-can't-read-back) are the genomics kit's
(vendor/genomics/build.py), measured on the lab cluster 20-21 Oct 2026.
"""
import json, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from env import (E, TENANT, P, USERS, HOMES, BUCKET, EVENTS, VIEW_DATA, VIEW_EVENTS, VIEW_HOME, VIEW_DATASETS, PREFIX,
                 STATE, ids as load_ids, save_ids, write_secret, rng)
from vms import client, L

v = client("build")
ids = load_ids()
KEYS = os.path.join(STATE, "keys.json")
NFS_HOSTS = E["RLU_CLIENT_NFS_IPS"].split(",")
DATASETS = f"{TENANT}-datasets"


def find(ep, name, key="name", tid=None):
    for x in L(getattr(v, ep).get(**({key: name}))):
        if x.get(key) == name and (tid is None or x.get("tenant_id") in (None, tid)):
            return x


def ensure(ep, name, body, idkey, key="name", tid=None):
    x = find(ep, name, key, tid)
    if x: print(f"exists  {ep:13} {x['id']:>5} {name}")
    else:
        x = getattr(v, ep).post(**body); print(f"created {ep:13} {x['id']:>5} {name}")
    ids[idkey] = x["id"]; save_ids(ids); return x


# 1. tenant, with its own local identity provider
T = ensure("tenants", TENANT, {"name": TENANT}, "tenant")
tid = T["id"]
ids["local_provider"] = v.tenants[tid].get()["local_provider_id"]; save_ids(ids)

# 2. VIP pools on the tenant
for nm, r, k in ((TENANT, E["RLU_POOL_RANGE"], "pool"), (f"{TENANT}-kafka", E["RLU_KAFKA_RANGE"], "kafka_pool")):
    ensure("vippools", nm, {"name": nm, "role": "PROTOCOLS", "subnet_cidr": int(E["RLU_SUBNET_CIDR"]),
                            "ip_ranges": [list(rng(r))], "tenant_id": tid}, k)

# 3. local users: the three pipeline identities, and four PI home owners (beat 5)
lp = ids["local_provider"]
have = {}
for name, uid in {**USERS, **HOMES}.items():
    hit = [u for u in L(v.users.get(name=name)) if u["name"] == name and (u.get("local_provider") or {}).get("id") == lp]
    if hit: have[name] = hit[0]["id"]; print(f"exists  users         {hit[0]['id']:>5} {name}")
    else:
        u = v.users.post(name=name, uid=uid, local_provider_id=lp); have[name] = u["id"]
        print(f"created users         {u['id']:>5} {name}")
ids["users"] = have; save_ids(ids)

# 4. S3 keys for the pipeline identities -> state/keys.json (600), never printed
keys = json.load(open(KEYS)) if os.path.exists(KEYS) else {}
for name in USERS:
    if keys.get(name, {}).get("user_id") == have[name]: continue     # a key minted for THIS user (not a torn-down one)
    r = v.users[have[name]].access_keys.post(tenant_id=tid)
    keys[name] = {"access_key": r["access_key"], "secret_key": r["secret_key"], "uid": USERS[name], "user_id": have[name]}
    print("minted key for", name, r["access_key"][:4] + "...")
write_secret(KEYS, keys)

# 5. view policies
pools = [ids["pool"], ids["kafka_pool"]]
VP = {
    # landing zone + tables + Parquet: identity policies decide access on every protocol, NFS included
    f"{TENANT}-data": {"flavor": "S3_NATIVE", "nfs_read_write": NFS_HOSTS, "nfs_read_only": [],
                       "protocols": ["NFSv3", "NFSv4", "S3", "DATABASE"]},
    # beat 5, today's world: POSIX homes, each PI's own copy
    f"{TENANT}-home": {"flavor": "NFS", "nfs_read_write": NFS_HOSTS, "nfs_read_only": [],
                       "protocols": ["NFSv3", "NFSv4"]},
    # beat 5, the proposal: one curated copy, read-only over NFS everywhere; the curator writes over S3
    f"{TENANT}-datasets": {"flavor": "NFS", "nfs_read_write": [], "nfs_read_only": NFS_HOSTS,
                           "protocols": ["NFSv3", "NFSv4", "S3"]},
}
for nm, b in VP.items():
    body = {"tenant_id": tid, "auth_source": "RPC_AND_PROVIDERS", "vip_pools": pools,
            "nfs_root_squash": ["*"], "nfs_no_squash": [], "s3_read_write": ["*"], **b}
    x = ensure("viewpolicies", nm, {"name": nm, **{k: w for k, w in body.items()}}, f"vp:{nm}", tid=tid)
    v.viewpolicies[x["id"]].patch(**{k: body[k] for k in ("nfs_root_squash", "nfs_no_squash", "nfs_read_write",
                                                             "nfs_read_only", "flavor")})
    # measured 20 Oct: protocols sent with the rest came back as ['S3']; a PATCH on its own sticks
    v.viewpolicies[x["id"]].patch(protocols=body["protocols"])

# 6. views
VIEWS = [
    (VIEW_DATA, {"protocols": ["NFS", "S3", "DATABASE"], "bucket": BUCKET, "bucket_owner": f"{P}-svc"}, f"{TENANT}-data", "view_data"),
    (VIEW_EVENTS, {"protocols": ["S3", "DATABASE", "KAFKA"], "bucket": EVENTS, "bucket_owner": f"{P}-svc",
                   "kafka_vip_pools": [ids["kafka_pool"]]}, f"{TENANT}-data", "view_events"),
    (VIEW_HOME, {"protocols": ["NFS"]}, f"{TENANT}-home", "view_home"),
    (VIEW_DATASETS, {"protocols": ["NFS", "S3"], "bucket": DATASETS, "bucket_owner": f"{P}-svc"}, f"{TENANT}-datasets", "view_datasets"),
]
for path, b, pol, k in VIEWS:
    ensure("views", path, {"path": path, "name": path.strip("/").replace("/", "-"), "tenant_id": tid,
                           "policy_id": ids[f"vp:{pol}"], "create_dir": True, **b}, k, key="path", tid=tid)

# 7. identity policies: ingest can only land files under incoming/; analyst only reads; svc runs the functions
arn = lambda b, p="*": [f"arn:aws:s3:::{b}", f"arn:aws:s3:::{b}/{p}"]
TAB_RO = ["s3:TabularListSchemas", "s3:TabularListTables", "s3:TabularListColumns", "s3:TabularGetTableStats",
          "s3:TabularBeginTransaction", "s3:TabularCommitTransaction", "s3:TabularRollbackTransaction",
          "s3:TabularQueryData", "s3:TabularGetTransaction", "s3:TabularListProjections",
          "s3:TabularListProjectionColumns", "s3:TabularGetProjectionStats", "s3:TabularListViews",
          "s3:TabularGetRowColumnSecurity"]
QE = ["s3:QueryEngineStartQuery", "s3:QueryEngineQueryStatus", "s3:QueryEngineFinishQuery",
      "s3:QueryEngineListQueries", "s3:QueryEngineGetData", "s3:QueryEngineFinishData", "s3:QueryEngineGetCapabilities"]
POL = {
    f"{TENANT}-ingest": (f"{P}-ingest", [
        {"Sid": "LandOnly", "Effect": "Allow", "Resource": [f"arn:aws:s3:::{BUCKET}/incoming/*"],
         "Action": ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]},
        {"Sid": "SeeBucket", "Effect": "Allow", "Resource": [f"arn:aws:s3:::{BUCKET}"],
         "Action": ["s3:ListBucket", "s3:ListBucketMultipartUploads"]},
        {"Sid": "NoReadBack", "Effect": "Deny", "Resource": arn(BUCKET), "Action": ["s3:GetObject", "s3:DeleteObject"] + TAB_RO}]),
    f"{TENANT}-analyst": (f"{P}-analyst", [
        {"Sid": "Read", "Effect": "Allow", "Resource": arn(BUCKET) + arn(DATASETS),
         "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation", "s3:ListAllMyBuckets"] + TAB_RO},
        {"Sid": "QueryEngine", "Effect": "Allow", "Resource": "*", "Action": QE},
        {"Sid": "CatalogRead", "Effect": "Allow", "Action": TAB_RO, "Resource": arn("vast-big-catalog-bucket")},
        {"Sid": "NoWrite", "Effect": "Deny", "Resource": arn(BUCKET) + arn(DATASETS),
         "Action": ["s3:PutObject", "s3:DeleteObject", "s3:TabularInsertRows", "s3:TabularUpdateRows",
                    "s3:TabularDeleteRows", "s3:TabularCreateTable", "s3:TabularDropTable"]}]),
    f"{TENANT}-svc": (f"{P}-svc", [
        {"Sid": "Functions", "Effect": "Allow", "Resource": arn(BUCKET) + arn(EVENTS) + arn(DATASETS), "Action": ["s3:*"]},
        {"Sid": "QueryEngine", "Effect": "Allow", "Resource": "*", "Action": QE}]),
}
ids["s3policies"] = ids.get("s3policies", {})
for pname, (uname, stmts) in POL.items():
    doc = json.dumps({"Version": "2012-10-17", "Statement": stmts})
    p = find("s3policies", pname)
    if p: v.s3policies[p["id"]].patch(policy=doc); print(f"updated s3policies    {p['id']:>5} {pname}")
    else: p = v.s3policies.post(name=pname, policy=doc, tenant_id=tid, enabled=True); print(f"created s3policies    {p['id']:>5} {pname}")
    cur = v.users[have[uname]].get().get("s3_policies_ids") or []
    if p["id"] not in cur: v.users[have[uname]].patch(s3_policies_ids=cur + [p["id"]])
    ids["s3policies"][pname] = p["id"]; save_ids(ids)

# quotas the deck's panels read back from VMS: the PI homes (beat 5's duplicates fill it) and the curated copy
for path, gib, k in ((VIEW_HOME, 200, "quota_home"), (VIEW_DATASETS, 50, "quota_datasets")):
    ensure("quotas", f"{path.strip('/').replace('/', '-')}", {"name": path.strip("/").replace("/", "-"), "path": path,
           "tenant_id": tid, "hard_limit": gib << 30, "soft_limit": (gib * 9 // 10) << 30, "grace_period": "7 00:00:00"}, k)

# beat 5's PI homes are made by each PI uid over NFS (beats/seed_homes.py): the home view's root is 0777.

# accepted != enforced: the effects are exercised by the beats; this only shows what VMS stored
for pol in VP:
    x = v.viewpolicies[ids[f"vp:{pol}"]].get()
    print("viewpolicy", pol, {k: x[k] for k in ("flavor", "nfs_root_squash", "nfs_read_write", "nfs_read_only", "protocols")})
print("BUILD_DONE", json.dumps({k: w for k, w in ids.items() if not isinstance(w, dict)}))
