"""The deck's VMS logins, so the cluster admin login never leaves the Mac. Idempotent. Prints no secret.

  ro      <tenant>-deck-ro      view_* on every realm: the live panels (views, quotas, tenant, Catalog config)
  tadmin  <tenant>-deck-tadmin  TENANT_ADMIN of the demo tenant only: DataEngine API (triggers), reset
  ops     <tenant>-deck-ops     view + edit + delete on logical/security/database, and NO create permission: the deck's
                                Teardown button. It cannot make anything, but it can delete outside the tenant, so it
                                exists only while the demo stands; teardown deletes it last.

Writes state/vms-logins.json (mode 600). Why a tenant admin and not a tenant-scoped role on a cluster manager: a role's
`tenants` field is ignored, so create rights on it reached the default tenant (genomics kit, measured 21 Oct 2026).
"""
import json, os, secrets, string, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from env import TENANT, ids as load_ids, save_ids, write_secret  # noqa: E402
from vms import client, L, LOGINS  # noqa: E402

v = client("build")
ids = load_ids(); tid = ids["tenant"]
creds = json.load(open(LOGINS)) if os.path.exists(LOGINS) else {}
pw = lambda: "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(22)) + "!Aa1"
REALMS = ("database", "security", "applications", "monitoring", "logical", "settings", "events", "hardware", "support")
ROLES = {
    "ro": dict(name=f"{TENANT}-deck-ro", permissions_list=[f"view_{r}" for r in REALMS]),
    "tadmin": dict(name=f"{TENANT}-deck-tadmin", tenant_id=tid,
                   permissions_list=["create_logical", "edit_logical", "delete_logical", "view_logical",
                                     "create_database", "edit_database", "delete_database", "view_database",
                                     "view_monitoring", "view_security"]),
    "ops": dict(name=f"{TENANT}-deck-ops",
                permissions_list=[f"view_{r}" for r in REALMS] +
                [f"{a}_{r}" for a in ("edit", "delete") for r in ("logical", "security", "database")]),
}
ids["deck"] = ids.get("deck", {})
for prof, body in ROLES.items():
    r = next((x for x in L(v.roles.get()) if x["name"] == body["name"]), None) or v.roles.post(**body)
    if prof == "tadmin":
        assert v.roles[r["id"]].get().get("tenant_id") == tid, "tenant admin role is not tenant-scoped: refusing"
    uname = body["name"]
    m = next((x for x in L(v.managers.get()) if x["username"] == uname), None)
    extra = {"user_type": "TENANT_ADMIN", "tenant_id": tid} if prof == "tadmin" else {}
    if m and creds.get(prof, {}).get("username") == uname:
        v.managers[m["id"]].patch(roles=[r["id"]])
    else:
        p = pw()
        if m: v.managers[m["id"]].patch(password=p, roles=[r["id"]])
        else: m = v.managers.post(username=uname, password=p, first_name="rad-lab", last_name=prof, roles=[r["id"]],
                                  password_expiration_disabled=True, **extra)
        creds[prof] = {"username": uname, "password": p}
    ids["deck"][prof] = {"manager": m["id"], "role": r["id"]}
    M = v.managers[m["id"]].get()
    print(f"login {prof:6} manager {M['id']:>4} {uname:28} {M.get('user_type') or 'MANAGER':13} "
          f"tenant {M.get('tenant_id') or '-'}  role {r['id']}")
write_secret(LOGINS, creds); save_ids(ids)
print("LOGINS_DONE: state/vms-logins.json (600)")
