"""VMS access by login profile. Never prints a secret.

  build  the cluster admin from ~/.vast/vms-creds.json[RLU_CLUSTER] — Mac only, used by deploy/teardown run there
  ro     read-only manager for the deck's state panels            } state/vms-logins.json (mode 600) on the client,
  tadmin TENANT_ADMIN confined to the demo tenant (reset, DE API)  } made by mgmt/logins.py. The deck never holds
  ops    teardown login (logical + security + database, no admin)  } the cluster admin login.
"""
import json, os, urllib3, requests
from vastpy import VASTClient
from env import E, TENANT, STATE

urllib3.disable_warnings()
VMS = E["RLU_VMS"]
LOGINS = os.path.join(STATE, "vms-logins.json")


def creds(profile):
    if os.path.exists(LOGINS):
        d = json.load(open(LOGINS))
        if profile in d:
            return d[profile]["username"], d[profile]["password"]
    if profile in ("build", "ops") and os.path.exists(os.path.expanduser("~/.vast/vms-creds.json")):
        d = json.load(open(os.path.expanduser("~/.vast/vms-creds.json")))
        c = d.get(E["RLU_CLUSTER"], {}).get("admin") or d["defaults"]["admin"]
        return c["username"], c["password"]
    raise SystemExit(f"no VMS login for profile '{profile}' (expected {LOGINS} or ~/.vast/vms-creds.json)")


def client(profile="build", tenant=None):
    u, p = creds(profile)
    kw = {"tenant": tenant} if tenant else {}
    return VASTClient(user=u, password=p, address=VMS, **kw)


def session(profile="build", tenant=TENANT):
    """requests session for the DataEngine API (basic auth + X-Tenant-Name)."""
    s = requests.Session()
    s.verify = False
    u, p = creds(profile)
    # the cluster admin needs a bearer token here (basic auth -> "Invalid username/password", 4 Nov); the tenant admin
    # works with basic auth + X-Tenant-Name (genomics kit, 21 Oct)
    r = s.post(f"https://{VMS}/api/token/", json={"username": u, "password": p}, timeout=20)
    if r.ok and "access" in r.json():
        s.headers["Authorization"] = f"Bearer {r.json()['access']}"
    else:
        s.auth = (u, p)
    if tenant:
        s.headers["X-Tenant-Name"] = tenant
    return s


DE = f"https://{VMS}/api/dataengine"


def L(r):
    return r.get("results", r) if isinstance(r, dict) else r


def find(v, ep, key, value, **flt):
    for x in L(getattr(v, ep).get(**({key: value} if key in ("name", "path", "username") else {}))):
        if x.get(key) == value and all(x.get(k) in (None, w) for k, w in flt.items()):
            return x
