#!/usr/bin/env python3
"""Live deck for vast-research-data-demo: the slides, with Run buttons that execute the demo for real.

Runs on the compute client (stdlib only) and listens on the management network, so the presenting laptop opens it
directly: no tunnel. Because its buttons change the cluster, every request needs the token from .live.token: open the
link ./live.sh prints (http://<host>:<port>/?k=<token>) once and the server swaps it for a cookie.

The browser can only name a step from STEPS below; it never sends a command, a path or an argument. One step runs at a
time. Real stdout streams back; for display only, htslib header warnings and Trino's terminal notice are dropped and
data-network addresses are shown as <addr>. The live panels come from live/collect.py (least-privilege logins), which
writes live/state.json; configuration slides read the real files from disk. The deck holds no cluster admin login.
"""
import glob, hmac, json, os, re, socketserver, subprocess, sys, threading
from http.server import BaseHTTPRequestHandler, HTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LIVE = os.path.join(ROOT, "live")


def load_env():
    env = {}
    for line in open(os.path.join(ROOT, ".env")):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1); env[k] = v
    return env


E = load_env()
PORT = int(os.environ.get("RLU_LIVE_PORT", E.get("RLU_LIVE_PORT", "8097")))
BIND = os.environ.get("RLU_LIVE_BIND", "0.0.0.0")
TOKEN = os.environ.get("RLU_LIVE_TOKEN", "")
PY = os.path.join(ROOT, "env", "bin", "python")
RUNENV = {"PYTHONPATH": os.path.join(ROOT, "lib"), "LD_LIBRARY_PATH": os.path.join(E["RLU_SRV"], "env", "lib"),
          "PYTHONUNBUFFERED": "1"}

# step id -> (label, shown command, argv). argv runs in ROOT. Nothing else can be run.
STEPS = {
    "status":      ("What is standing",                   "./client/fn.sh status",              ["bash", "-c", "./client/fn.sh status; ./live.sh status | cut -d: -f1"]),
    "reset":       ("Reset to demo-start",                "client/reset.py",                    [PY, "client/reset.py"]),
    "teardown":    ("Tear down: delete everything",       "teardown_data.py && teardown.py ops", ["bash", "-c", f"{PY} client/teardown_data.py && {PY} mgmt/teardown.py ops && {PY} client/teardown_data.py --final"]),
    "triggers":    ("The triggers, as DataEngine has them", "mgmt/show.py triggers",            [PY, "mgmt/show.py", "triggers"]),
    "policies":    ("The identity policies, as VMS has them", "mgmt/show.py policies",          [PY, "mgmt/show.py", "policies"]),
    "access":      ("One set of rules: files and tables", "beats/access.py",                    [PY, "beats/access.py"]),
    "sfs_vastdb":  ("SFS on VAST DB (Trino)",             "beats/sfs.py vastdb",                [PY, "beats/sfs.py", "vastdb"]),
    "sfs_parquet": ("SFS on Parquet, same view (Trino)",  "beats/sfs.py parquet",               [PY, "beats/sfs.py", "parquet"]),
    "sfs_files":   ("SFS from the VCF on NFS (bcftools)", "beats/sfs.py files",                 [PY, "beats/sfs.py", "files"]),
    "sfs_check":   ("Check: the consortium's own counts", "beats/sfs.py consortium",            [PY, "beats/sfs.py", "consortium"]),
    "land_vcf":    ("Drop a 5 Mb VCF slice over S3",      "beats/land.py vcf",                  [PY, "beats/land.py", "vcf"]),
    "sfs_landed":  ("SFS on what just landed",            "beats/sfs.py landed",                [PY, "beats/sfs.py", "landed"]),
    "land_live":   ("Drop CRAM + POD5 over S3",           "beats/land.py live",                 [PY, "beats/land.py", "live"]),
    "land_hep":    ("Drop event files over S3",           "beats/land.py hep",                  [PY, "beats/land.py", "hep"]),
    "hep_pipeline": ("What the function did, per file",  "sql/hep_pipeline.sql",              [PY, "beats/hep_query.py", "pipeline"]),
    "top":         ("Top 100 anomalous per m_jb bin",     "sql/top_anomalies.sql",              [PY, "beats/hep_query.py", "top"]),
    "top_events":  ("The 10 most anomalous events",       "beats/hep_query.py events",          [PY, "beats/hep_query.py", "events"]),
    "dupes":       ("Who has copies of the same data?",   "sql/catalog_dupes.sql",              [PY, "beats/catalog.py", "dupes"]),
    "byext":       ("Bytes by extension, by owner",       "catalog_bytes_by_ext_by_uid.sql",    [PY, "beats/catalog.py", "byext"]),
    "verify":      ("Same bytes? Check with sha256",      "beats/catalog.py verify",            [PY, "beats/catalog.py", "verify"]),
    "datasets":    ("The proposal: one read-only copy",   "beats/catalog.py datasets",          [PY, "beats/catalog.py", "datasets"]),
}

# Files the configuration slides may show, live from disk.
FILES = {
    "sfs_sql": "sql/sfs.sql", "landing": "functions/landing/main.py", "csq": "functions/landing/csq.py",
    "runner": "functions/runner.py", "de_setup": "mgmt/de_setup.py", "adfilter": "functions/adfilter/handler.py",
    "rmm": "functions/adfilter/rmm.py", "top_sql": "sql/top_anomalies.sql", "pipe_sql": "sql/hep_pipeline.sql", "dupes_sql": "sql/catalog_dupes.sql",
    "byext_sql": "vendor/genomics/snippets/catalog_bytes_by_ext_by_uid.sql",
}

NOISE = ("bcf_hrec_check", "org.jline", "Unable to create a system terminal")
ADDR = re.compile(r"\b(?:10|172)\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")
run_lock = threading.Lock()


def sfs_result():
    """Latest result per arm across results/sfs-*.json, plus the spectrum itself (all arms share it when digests match)."""
    arms, sfs, what = {}, None, ""
    for p in sorted(glob.glob(os.path.join(ROOT, "results", "sfs-*.json"))):
        try: d = json.load(open(p))
        except ValueError: continue
        for k, a in d.get("arms", {}).items(): arms[k] = dict(a, file=os.path.basename(p))
        sfs, what = d.get("sfs") or sfs, d.get("what", what)
    return {"arms": arms, "sfs": sfs, "what": what}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"  # close-delimited bodies stream cleanly to fetch()

    def log_message(self, fmt, *args):
        sys.stderr.write("live: " + (fmt % args) + "\n")

    def send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def authorized(self):
        for part in (self.headers.get("Cookie") or "").split(";"):
            k, _, v = part.strip().partition("=")
            if k == "rlu" and hmac.compare_digest(v.encode(), TOKEN.encode()):
                return True
        return False

    def deny(self):
        return self.send(403, "Open the link that ./live.sh start printed (it carries the token).\n", "text/plain; charset=utf-8")

    def do_GET(self):
        p, _, q = self.path.partition("?")
        if q.startswith("k="):  # first visit: trade the token in the URL for a cookie, then drop it
            if not hmac.compare_digest(q[2:].split("&")[0].encode(), TOKEN.encode()):
                return self.deny()
            self.send_response(303)
            self.send_header("Set-Cookie", "rlu=%s; Path=/; HttpOnly; SameSite=Strict" % TOKEN)
            self.send_header("Location", p or "/")
            self.end_headers()
            return
        if not self.authorized():
            return self.deny()
        if p in ("/", "/index.html"):
            return self.send(200, open(os.path.join(LIVE, "index.html"), "rb").read(), "text/html; charset=utf-8")
        if p == "/vast-strip.png":
            return self.send(200, open(os.path.join(LIVE, "vast-strip.png"), "rb").read(), "image/png")
        if p == "/api/steps":
            return self.send(200, json.dumps({k: [v[0], v[1]] for k, v in STEPS.items()}))
        if p.startswith("/api/file/"):
            key = p[len("/api/file/"):]
            if key not in FILES:
                return self.send(404, '{"error":"unknown file"}')
            path = os.path.join(ROOT, FILES[key])
            body = open(path).read() if os.path.exists(path) else "(not on disk)\n"
            return self.send(200, body, "text/plain; charset=utf-8")
        if p == "/api/state":
            path = os.path.join(LIVE, "state.json")
            try:
                age = os.path.getmtime(path)
                d = json.load(open(path)); d["age_s"] = round(__import__("time").time() - age, 1)
                return self.send(200, json.dumps(d))
            except (OSError, ValueError) as e:
                return self.send(502, json.dumps({"error": "collector not running (%s)" % type(e).__name__}))
        if p == "/api/result/sfs":
            return self.send(200, json.dumps(sfs_result()))
        if p == "/api/result/validation":
            path = os.path.join(ROOT, "models", "adfilter", "validation.json")
            return self.send(200, open(path).read() if os.path.exists(path) else "{}")
        return self.send(404, '{"error":"not found"}')

    def do_POST(self):
        if not self.authorized():
            return self.deny()
        p = self.path.split("?", 1)[0]
        if not p.startswith("/api/run/"):
            return self.send(404, '{"error":"not found"}')
        step = p[len("/api/run/"):]
        if step not in STEPS:
            return self.send(404, '{"error":"unknown step"}')
        if not run_lock.acquire(blocking=False):
            return self.send(409, "Another step is still running.\n", "text/plain; charset=utf-8")
        try:
            label, shown, argv = STEPS[step]
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
            self.wfile.write(("$ %s\n" % shown).encode()); self.wfile.flush()
            proc = subprocess.Popen(argv, cwd=ROOT, env=dict(os.environ, **RUNENV), stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, universal_newlines=True, bufsize=1)
            gone = False
            for line in proc.stdout:
                if gone or any(n in line for n in NOISE):
                    continue
                try:
                    self.wfile.write(ADDR.sub("<addr>", line).encode()); self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    gone = True  # browser went away: keep draining so the step finishes cleanly
            rc = proc.wait()
            if not gone:
                self.wfile.write(("\n[exit %d]\n" % rc).encode()); self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            run_lock.release()


class Server(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


if __name__ == "__main__":
    if len(TOKEN) < 16:
        sys.exit("live: RLU_LIVE_TOKEN missing or short; start it with ./live.sh start")
    srv = Server((BIND, PORT), Handler)
    print("live deck listening on %s:%d (token required)" % (BIND, PORT), flush=True)
    srv.serve_forever()
