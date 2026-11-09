"""Read-only preflight: checks what deploy relies on but does not create. Changes nothing. Exit 1 on any FAIL.

Run on the Mac (it needs the build login to read VMS); deploy.sh runs it first. Checks the client over ssh.
"""
import ipaddress, os, subprocess, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from env import E, TENANT, P, POOL, KPOOL, USERS, HOMES, ids
from vms import client, L, VMS

bad = 0
def check(ok, what, detail=""):
    global bad
    bad += not ok
    print(f"{'ok  ' if ok else 'FAIL'}  {what}" + (f"  ({detail})" if detail else ""))

v = client("build")
cl = L(v.clusters.get())[0]
ver = cl["sw_version"]
check(cl.get("state") == "ONLINE", f"cluster {cl['name']} reachable at {VMS}", f"{ver.split('.')[0:4]} {cl.get('state')}")
major = ".".join(ver.split(".")[:2])
want_trino = "475" if major == "5.4" else "478"
check(E["RLU_TRINO_IMAGE"].endswith(want_trino), f"Trino image pairs with {major}", E["RLU_TRINO_IMAGE"])

# our VIPs must be free: another pool on any tenant owning one of them breaks the data path (happened twice on the lab cluster)
mine = {f"{TENANT}", f"{TENANT}-kafka"}
taken = {}
for p in L(v.vippools.get()):
    if p["name"] in mine: continue
    for a, b in p.get("ip_ranges") or []:
        try:
            lo, hi = int(ipaddress.ip_address(a)), int(ipaddress.ip_address(b))
        except ValueError: continue
        for ip in POOL + KPOOL:
            if lo <= int(ipaddress.ip_address(ip)) <= hi: taken[ip] = p["name"]
check(not taken, f"VIPs {POOL[0]}..{KPOOL[-1]} free", ", ".join(f"{k} held by {n}" for k, n in taken.items()))

# names: either unused, or ours from a previous deploy (ids.json) - deploy converges either way
t = [x for x in L(v.tenants.get()) if x["name"] == TENANT]
known = ids().get("tenant")
check(not t or t[0]["id"] == known, f"tenant name '{TENANT}' unused or ours", f"id {t[0]['id']}" if t else "unused")
mgrs = [m["username"] for m in L(v.managers.get()) if m["username"].startswith(f"{TENANT}-")]
check(not mgrs or known, "deck VMS logins unused or ours", ",".join(mgrs))

# features this build uses
try:
    from vms import session, DE
    r = session("build", None).get(f"{DE}/data-engine", timeout=15)
    check(r.status_code < 500, "DataEngine API answers (400 = needs a tenant header: expected)", f"HTTP {r.status_code}")
except Exception as e:
    check(False, "DataEngine API answers", str(e)[:120])
bc = (L(v.bigcatalogconfig.get()) or [{}])[0]
every = ",".join(f.get("every", "?") for f in bc.get("frames", []))
check(bool(bc.get("enable")), "VAST Catalog enabled", f"snapshot every {every}: that is beat 5's lag")

# the client
c = E["RLU_CLIENT"]
def ssh(cmd):
    return subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", c, cmd], capture_output=True, text=True, timeout=60)
r = ssh(f"curl -sk -o /dev/null -w '%{{http_code}}' --max-time 5 https://{VMS}/api/; echo; "
        f"for ip in {' '.join(POOL[:1] + KPOOL[:1])}; do ping -c1 -W1 $ip >/dev/null && echo up $ip || echo down $ip; done; "
        "df -BG --output=avail ~ | tail -1; docker info --format '{{.ServerVersion}}' 2>&1 | head -1; sudo -n true && echo sudo; "
        f"ss -ltn | grep -cE ':({E['RLU_TRINO_PORT']}|{E['RLU_LIVE_PORT']})\\b' || true; "
        f"mountpoint -q {E['RLU_MNT']} && echo mounted || echo notmounted")
out = r.stdout.split("\n")
check(r.returncode == 0 and out[0] == "200", f"client {c} reaches VMS", out[0] if out else r.stderr[:80])
check(all("up" not in l for l in out[1:3]) or bool(known), "our VIPs not answering before deploy (nobody else on them)",
      " ".join(out[1:3]))
avail = int(out[3].strip().rstrip("G")) if len(out) > 3 and out[3].strip().rstrip("G").isdigit() else 0
check(avail >= 80, "client has 80 GB free for the public data", f"{avail} GB")
check(len(out) > 4 and out[4][:1].isdigit(), "docker on the client", out[4] if len(out) > 4 else "")
check("sudo" in out, "sudo on the client (mount, per-uid key files)")
check(len(out) > 6 and (out[6] == "0" or bool(known)), f"ports {E['RLU_TRINO_PORT']}/{E['RLU_LIVE_PORT']} free on the client", out[6] if len(out) > 6 else "")
print(f"\npreflight: {'PASS' if not bad else f'{bad} FAIL'} (read-only; nothing was changed)")
sys.exit(1 if bad else 0)
