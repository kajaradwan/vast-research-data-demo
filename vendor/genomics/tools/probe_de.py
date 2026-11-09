"""Which lab tenants have Data Engine, and does any have a k8s compute cluster + registry linked? Read-only."""
from vms import client, session, L
v = client()
for t in L(v.tenants.get()):
    if not t.get("data_engine_enabled"): continue
    s = session(t["name"])
    out = {}
    for ep in ("compute-clusters", "container-registries", "functions", "pipelines", "triggers"):
        r = s.get(f"https://192.0.2.10/api/dataengine/{ep}/")
        try: d = r.json(); items = d.get("data", d) if isinstance(d, dict) else d
        except Exception: items = r.text[:80]
        out[ep] = (r.status_code, [ (x.get("name"), x.get("status") or x.get("state")) for x in items][:5] if isinstance(items, list) else items)
    print(t["id"], t["name"], out)
