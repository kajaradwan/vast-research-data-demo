"""DataEngine on the demo tenant: enable it, register the topics, and one element trigger per landing prefix.

Idempotent. Runs on the Mac (build login) at deploy, or on the client with the tenant admin. The Kafka topics must
exist on the events view first (client/bootstrap.sh). The API shape is the genomics kit's (vendor/genomics/de_setup.py),
measured on 5.5.0-sp6. The triggers are real; the functions they feed run as stand-in consumers (functions/runner.py)
because no Kubernetes compute cluster is linked to the tenant.
"""
import os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from env import TENANT, BUCKET, EVENTS, TOPICS, TRIGGERS, PREFIX, ids as load_ids, save_ids
from vms import session, DE

profile = sys.argv[1] if len(sys.argv) > 1 else "build"
ids = load_ids()
s = session(profile)
broker = {"type": "Internal", "name": EVENTS}

st = s.get(f"{DE}/data-engine").json()
if st.get("setup", {}).get("status") != "completed":
    r = s.post(f"{DE}/data-engine", json={"kafka_broker": broker, "default_topic_name": TOPICS["genomics"],
                                          "dead_letter_topic_name": TOPICS["dlq"], "vip_pools": [ids["pool"]]})
    print("DataEngine enable", r.status_code, r.text[:200])
    for _ in range(60):
        st = s.get(f"{DE}/data-engine").json()
        if st.get("setup", {}).get("status") in ("completed", "error"): break
        time.sleep(5)
print("DataEngine on", TENANT, ":", st.get("setup", {}).get("status"), st.get("setup", {}).get("message") or "")


def names(ep):
    return {x["name"]: x for x in s.get(f"{DE}/{ep}/").json().get("data", [])}


topics = names("topics")
for k in ("genomics", "hep"):
    if TOPICS[k] not in topics:
        r = s.post(f"{DE}/topics/", json={"name": TOPICS[k], "broker": broker, "name_in_broker": TOPICS[k],
                                          "default": k == "genomics"})
        print("topic", TOPICS[k], r.status_code, r.text[:160] if r.status_code >= 300 else "")
trig = names("triggers")
DESC = {"genomics": "VCF/CRAM/POD5 lands under incoming/genomics/ over S3 -> landing function",
        "hep": "event files land under incoming/hep/ over S3 -> ADFilter scoring function"}
for k in ("genomics", "hep"):
    if TRIGGERS[k] in trig: continue
    body = {"name": TRIGGERS[k], "description": DESC[k], "broker": broker, "topic_name": TOPICS[k],
            "source_types": ["S3"], "custom_extensions": {"demo": "research-data", "beat": k},
            "config": {"events": ["ObjectCreated:*"], "source_bucket_name": BUCKET,
                       "name_filters": {"prefixes": [PREFIX[k]]}}}
    r = s.post(f"{DE}/triggers/element/", json=body)
    print("trigger", TRIGGERS[k], r.status_code, r.text[:300] if r.status_code >= 300 else "")
for n, t in names("triggers").items():
    print("trigger", n, t.get("type"), t.get("status"), (t.get("config") or {}).get("name_filters"))
ids["de"] = {"triggers": list(TRIGGERS.values()), "topics": list(TOPICS.values())}
save_ids(ids)
print("DE_DONE")
