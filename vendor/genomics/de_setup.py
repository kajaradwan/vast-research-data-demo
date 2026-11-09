"""Data Engine on rad-lab-enclave: enable on the tenant, then an element trigger on the cohort bucket. Idempotent.

Runs on the Mac. Kafka topics rle-landing-events / rle-dlq must already exist on the events view (de_topics in demo.py).
PATCH tenants/<id>/data_engine/ is 403 for admin, so this goes through POST /api/dataengine/data-engine .
"""
import json, os, time
from vms import session, BASE

HERE = os.path.dirname(os.path.abspath(__file__))
ids = json.load(open(os.path.join(HERE, "ids.json")))
s = session("rad-lab-enclave")
DE = f"{BASE}/api/dataengine"
broker = {"type": "Internal", "name": "rad-lab-enclave-events"}

st = s.get(f"{DE}/data-engine").json()
if st.get("setup", {}).get("status") != "completed":
    r = s.post(f"{DE}/data-engine", json={"kafka_broker": broker, "default_topic_name": "rle-landing-events",
                                          "dead_letter_topic_name": "rle-dlq", "vip_pools": [ids["pool"]]})
    print("DE setup", r.status_code, r.text[:300])
    for _ in range(40):
        st = s.get(f"{DE}/data-engine").json()
        if st.get("setup", {}).get("status") in ("completed", "error"): break
        time.sleep(5)
print("DE setup status:", st.get("setup", {}).get("status"), st.get("setup", {}).get("message"))

def names(ep): return {x["name"]: x for x in s.get(f"{DE}/{ep}/").json().get("data", [])}
topics = names("topics")
if "rle-landing-events" not in topics:
    r = s.post(f"{DE}/topics/", json={"name": "rle-landing-events", "broker": broker,
                                      "name_in_broker": "rle-landing-events", "default": True})
    print("DE topic", r.status_code, r.text[:300])
trig = names("triggers")
if "rle-cohort-landing" not in trig:
    body = {"name": "rle-cohort-landing", "description": "VCF/CRAM lands in incoming/ over S3 -> transform",
            "broker": broker, "topic_name": "rle-landing-events", "source_types": ["S3"],
            "custom_extensions": {"demo": "genomics-kit", "zone": "enclave-incoming"},
            "config": {"events": ["ObjectCreated:*"], "source_bucket_name": "rad-lab-enclave-cohort",
                       "name_filters": {"prefixes": ["incoming/"]}}}
    r = s.post(f"{DE}/triggers/element/", json=body)
    print("element trigger", r.status_code, r.text[:500])
    if r.status_code >= 400:   # retry without the prefix filter if this build rejects it
        body["config"].pop("name_filters"); r = s.post(f"{DE}/triggers/element/", json=body)
        print("element trigger (no filter)", r.status_code, r.text[:500])
for n, t in names("triggers").items():
    print("trigger", n, t.get("type"), t.get("status"), t.get("source_types"), t.get("guid"))
    ids.setdefault("de", {})["trigger"] = n
json.dump(ids, open(os.path.join(HERE, "ids.json"), "w"), indent=1)
