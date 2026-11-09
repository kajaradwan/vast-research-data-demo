"""Step 1: what can lab host? Read-only. Run from the repo root: .venv/bin/python -m tools.probe_lab"""
import json
from vms import client, session, L
v = client()
c = L(v.clusters.get())[0]
keys = ["name","sw_version","build","state","usable_capacity_tb","free_usable_capacity_tb","physical_space_in_use_percent",
        "logical_space_in_use","encryption_type","enable_encryption","ekm_servers","fips_enabled","enable_similarity","enable_dr",
        "enable_vast_db_audit","enable_json_audit","protocols","is_audit_enabled","audit_dir_name","audit_view_name","vast_db_audit_enabled"]
print({k: c.get(k) for k in keys if k in c})
print("encryption-ish keys:", {k: c[k] for k in c if any(s in k.lower() for s in ("encr","ekm","kms","fips"))})
print("audit-ish keys:", {k: c[k] for k in c if "audit" in k.lower()})
print("capacity-ish:", {k: c[k] for k in c if any(s in k.lower() for s in ("capacity","usable","free"))})
print("TENANTS:")
for t in L(v.tenants.get()):
    print(" ", t["id"], t["name"], {k: t.get(k) for k in ("encryption_group","use_ekm","ekm_id","is_dataengine_enabled","data_engine","vippool_names") if k in t})
print("tenant keys sample:", sorted(L(v.tenants.get())[0].keys()))
print("VIP POOLS:")
for p in L(v.vippools.get()):
    print(" ", p["id"], p["name"], p.get("role"), p.get("ip_ranges"), "tenant", p.get("tenant_id"))
s = session()
for ep in ("encryptiongroups","ekmservers","kms","indestructibility","bigcatalogconfig","vastauditlog","snapshots"):
    r = s.get(f"https://192.0.2.10/api/latest/{ep}/")
    print(ep, r.status_code, r.text[:300].replace("\n"," "))
