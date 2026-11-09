"""VMS client for the lab cluster - loads admin creds from ~/.vast/vms-creds.json, never prints them."""
import json, os, urllib3, requests
urllib3.disable_warnings()
from vastpy import VASTClient
CLUSTER = "lab"   # key into ~/.vast/vms-creds.json
BASE = "https://192.0.2.10"

def creds(cluster=CLUSTER, role="admin"):
    # On the data client the dashboard runs as a least-privilege manager (build_dash_manager.py), never as admin:
    # RLE_VMS_CREDS points at its mode-600 {"username","password"} file.
    if os.environ.get("RLE_VMS_CREDS"):
        c = json.load(open(os.environ["RLE_VMS_CREDS"]))
        return c["username"], c["password"]
    d = json.load(open(os.path.expanduser("~/.vast/vms-creds.json")))
    c = d.get(cluster, {}).get(role) or d["defaults"][role]
    return c["username"], c["password"]

def client(cluster=CLUSTER, role="admin"):
    u, p = creds(cluster, role)
    return VASTClient(user=u, password=p, address="192.0.2.10")

def session(tenant=None):
    """requests session with a bearer token; X-Tenant-Name when given (Data Engine API)."""
    u, p = creds()
    s = requests.Session(); s.verify = False
    tok = s.post(f"{BASE}/api/token/", json={"username": u, "password": p}).json()["access"]
    s.headers["Authorization"] = f"Bearer {tok}"
    if tenant: s.headers["X-Tenant-Name"] = tenant
    return s

def L(r):
    return r.get("results", r) if isinstance(r, dict) else r

def tenant_users(v, local_provider_id, names):
    """{name: id}. users.get(local_provider_id=) ignores the filter; look up by name and check provider."""
    out = {}
    for n in names:
        hit = [u for u in L(v.users.get(name=n)) if u["name"] == n and (u.get("local_provider") or {}).get("id") == local_provider_id]
        if hit: out[n] = hit[0]["id"]
    return out


def _dash():
    p = os.environ.get("RLE_VMS_CREDS")
    return json.load(open(p)) if p else None

def tenant_client(tenant="rad-lab-enclave"):
    """VMS client for in-tenant writes (snapshots). On the client: the tenant admin (confined to the tenant).
    On the Mac: the cluster admin, as before."""
    d = _dash()
    if d and d.get("tenant_admin"):
        return VASTClient(user=d["tenant_admin"]["username"], password=d["tenant_admin"]["password"],
                          address="192.0.2.10", tenant=tenant)
    return client()

def de_session(tenant="rad-lab-enclave"):
    """Session for the Data Engine API. Tenant admin (basic auth + X-Tenant-Name) on the client, admin token on the Mac."""
    d = _dash()
    if d and d.get("tenant_admin"):
        s = requests.Session(); s.verify = False
        s.auth = (d["tenant_admin"]["username"], d["tenant_admin"]["password"]); s.headers["X-Tenant-Name"] = tenant
        return s
    return session(tenant)
