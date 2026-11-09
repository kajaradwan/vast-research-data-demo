"""Settings for every script, from .env (site.env.example is the template) plus the names derived from it.

No script hard-codes a cluster, an address or an id. Ids the build creates go to state/ids.json.
"""
import json, os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATE = os.path.join(ROOT, "state")


def _load():
    env = {}
    path = os.path.join(ROOT, ".env")
    if not os.path.exists(path):
        raise SystemExit("no .env: copy site.env.example to .env and edit it")
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    env.update({k: v for k, v in os.environ.items() if k.startswith("RLU_")})
    return env


E = _load()
TENANT, P = E["RLU_TENANT"], E["RLU_PREFIX"]
UID = int(E["RLU_UID_BASE"])


def rng(s):
    a, b = s.split("-")
    return a, b


def ips(s):
    a, b = rng(s)
    base, lo, hi = a.rsplit(".", 1)[0], int(a.rsplit(".", 1)[1]), int(b.rsplit(".", 1)[1])
    return [f"{base}.{i}" for i in range(lo, hi + 1)]


# identities: name -> uid
USERS = {f"{P}-svc": UID + 1, f"{P}-ingest": UID + 2, f"{P}-analyst": UID + 3}
HOMES = {f"pi-{x}": UID + 11 + i for i, x in enumerate(("alpha", "bravo", "charlie", "delta"))}

# objects
BUCKET = f"{TENANT}-data"          # one view: NFS + S3 + DATABASE (landing zone, tables, Parquet)
EVENTS = f"{TENANT}-events"        # Event Broker view (KAFKA) for the DataEngine triggers
VIEW_DATA = f"/{TENANT}/data"
VIEW_EVENTS = f"/{TENANT}/events"
VIEW_HOME = f"/{TENANT}/home"      # beat 5: four PI homes with duplicated corpora (POSIX view)
VIEW_DATASETS = f"/{TENANT}/datasets"  # beat 5: the proposal, one read-only copy
SCHEMA_G, SCHEMA_H = "genomics", "hep"
TOPICS = {"genomics": f"{P}-genomics-events", "hep": f"{P}-hep-events", "dlq": f"{P}-dlq"}
TRIGGERS = {"genomics": f"{P}-genomics-landing", "hep": f"{P}-hep-landing"}
PREFIX = {"genomics": "incoming/genomics/", "hep": "incoming/hep/"}
# slide 13: a 5 Mb slice of the same VCF, landed live into its own table so slide 12's tables are untouched
LIVE_VCF = {"name": "chr22.30-35Mb", "region": "chr22:30000000-34999999", "lo": 30_000_000, "hi": 34_999_999}

POOL, KPOOL = ips(E["RLU_POOL_RANGE"]), ips(E["RLU_KAFKA_RANGE"])
S3_EP = f"http://{POOL[0]}"
DB_EP = f"http://{POOL[1 % len(POOL)]}"
KAFKA = f"{KPOOL[0]}:9092"
SRV, MNT = E["RLU_SRV"], E["RLU_MNT"]
KEYDIR = f"{SRV}/keys"


def ids():
    p = os.path.join(STATE, "ids.json")
    return json.load(open(p)) if os.path.exists(p) else {}


def save_ids(d):
    os.makedirs(STATE, exist_ok=True)
    json.dump(d, open(os.path.join(STATE, "ids.json"), "w"), indent=1)


def write_secret(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.write(fd, json.dumps(obj, indent=1).encode())
    os.close(fd)
