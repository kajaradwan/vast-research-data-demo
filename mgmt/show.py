"""Print configuration as the cluster stores it (not as our code meant it), with the deck's read-only logins.

  show.py triggers   the DataEngine element triggers on the tenant (tenant-admin login)
  show.py policies   the identity policies attached to the pipeline identities (read-only login)
"""
import json, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from env import TENANT, USERS  # noqa: E402
from vms import client, session, DE, L  # noqa: E402

what = sys.argv[1] if len(sys.argv) > 1 else "triggers"
if what == "triggers":
    s = session("tadmin")
    for t in s.get(f"{DE}/triggers/").json().get("data", []):
        keep = {k: t.get(k) for k in ("name", "type", "status", "description", "source_types", "topic_name", "custom_extensions")}
        keep["config"] = t.get("config")
        keep["broker"] = (t.get("broker") or {}).get("name")
        print(json.dumps(keep, indent=2))
    st = s.get(f"{DE}/data-engine").json().get("setup", {})
    print(f"\nDataEngine on tenant {TENANT}: {st.get('status')}  ·  functions: none registered "
          f"(no Kubernetes compute cluster linked; the handlers run as stand-in consumers)")
elif what == "policies":
    v = client("ro")
    pols = {p["id"]: p for p in L(v.s3policies.get()) if p["name"].startswith(TENANT)}
    for name in USERS:
        u = next((x for x in L(v.users.get(name=name)) if x["name"] == name), None)
        if not u: continue
        print(f"## {name} (uid {u['uid']})")
        for pid in u.get("s3_policies_ids") or []:
            if pid in pols:
                doc = json.loads(pols[pid]["policy"])
                for st in doc["Statement"]:
                    acts = st["Action"] if isinstance(st["Action"], list) else [st["Action"]]
                    short = sorted({a.split(":")[1].replace("Tabular", "Tabular*") if a.startswith("s3:Tabular") else a for a in acts})
                    short = [a for i, a in enumerate(short) if a != "Tabular*" or i == short.index("Tabular*")]
                    res = st["Resource"] if isinstance(st["Resource"], list) else [st["Resource"]]
                    print(f"  {st['Effect']:5} {st.get('Sid', ''):12} {', '.join(short)[:110]}")
                    print(f"        on {', '.join(r.replace('arn:aws:s3:::', '') for r in res)[:110]}")
        print()
