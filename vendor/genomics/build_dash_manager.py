"""Least-privilege VMS logins for the dashboard on the data client, so the cluster admin login never leaves the Mac.

  manager rad-lab-enclave-dash    role rad-lab-enclave-dash-ro: view_* on every realm, cluster-wide (state panels)
  manager rad-lab-enclave-tadmin  TENANT_ADMIN of rad-lab-enclave, role rad-lab-enclave-tadmin-role (tenant_id = the
                                  enclave): create/edit/view logical + view monitoring -> snapshots and the Data Engine API
Credentials go to .vms-dash.json (mode 600, gitignored): {"username","password","tenant_admin":{...}}.

Why two logins (measured 21 Oct 2026): a role created with `tenants=[t]` is NOT tenant-scoped (the field is ignored on
create and patch; tenant roles carry `tenant_id`), so `create_logical` on the read-only manager let it create a view in
the DEFAULT tenant. The tenant admin is confined: it cannot even see tenant 1. Idempotent. Prints no secret.
"""
import json, os, secrets, string
from vms import client, L

HERE = os.path.dirname(os.path.abspath(__file__))
IDS, OUT = os.path.join(HERE, "ids.json"), os.path.join(HERE, ".vms-dash.json")
v = client(); ids = json.load(open(IDS)); tid = ids["tenant"]
VIEW = ["view_" + r for r in ("database", "security", "applications", "monitoring", "logical", "settings", "events",
                              "hardware", "support")]
creds = json.load(open(OUT)) if os.path.exists(OUT) else {}
pw = lambda: "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(20)) + "!Aa1"

def find(ep, key, name): return next((x for x in L(getattr(v, ep).get()) if x[key] == name), None)

ro = find("roles", "name", "rad-lab-enclave-dash-ro") or v.roles.post(name="rad-lab-enclave-dash-ro", permissions_list=VIEW)
tr = find("roles", "name", "rad-lab-enclave-tadmin-role") or v.roles.post(
    name="rad-lab-enclave-tadmin-role", tenant_id=tid,
    permissions_list=["create_logical", "edit_logical", "view_logical", "view_monitoring"])
assert v.roles[tr["id"]].get().get("tenant_id") == tid, "tenant admin role is not tenant-scoped - refusing"

def manager(name, roles, **extra):
    m = find("managers", "username", name)
    if m and name == creds.get("username", "") or (m and creds.get("tenant_admin", {}).get("username") == name):
        v.managers[m["id"]].patch(roles=roles); return m, None
    p = pw()
    if m: v.managers[m["id"]].patch(password=p, roles=roles)
    else: m = v.managers.post(username=name, password=p, first_name="rad-lab", last_name=name.split("-")[-1],
                              roles=roles, password_expiration_disabled=True, **extra)
    return m, p

m1, p1 = manager("rad-lab-enclave-dash", [ro["id"]])
m2, p2 = manager("rad-lab-enclave-tadmin", [tr["id"]], user_type="TENANT_ADMIN", tenant_id=tid)
if p1: creds.update(username="rad-lab-enclave-dash", password=p1)
if p2: creds["tenant_admin"] = {"username": "rad-lab-enclave-tadmin", "password": p2}
fd = os.open(OUT, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600); os.write(fd, json.dumps(creds).encode()); os.close(fd)
ids["dash"] = {"manager": m1["id"], "tenant_admin": m2["id"], "roles": [ro["id"], tr["id"]]}
json.dump(ids, open(IDS, "w"), indent=1)
for m in (m1, m2):
    M = v.managers[m["id"]].get(); print("manager", M["id"], M["username"], M.get("user_type"), "tenant", M.get("tenant_id"),
                                         [r["name"] for r in M["roles"]])
print("credentials in .vms-dash.json (600)")
