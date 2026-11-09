"""Beat 5: "who has N copies of the same dataset?" from the VAST Catalog, then the curated /datasets proposal.

  catalog.py dupes      sql/catalog_dupes.sql over the PI homes (duplicate candidates by size + extension)
  catalog.py verify     confirm the candidates are the same bytes (sha256 over NFS, as the analyst)
  catalog.py byext      the genomics kit's research computing query (vendor/genomics/snippets/catalog_bytes_by_ext_by_uid.sql)
  catalog.py datasets   the proposal, proven on the mount: one copy, readable by every PI, writable by none over NFS

Runs as the analyst (Catalog read granted by identity policy). The Catalog is a snapshot of the namespace (every 15 min
on the lab cluster), so files written in the last few minutes may not show yet: that lag is the design.
"""
import os, re, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "lib"))
import rlu  # noqa: E402
from env import E, HOMES, VIEW_HOME  # noqa: E402

TRINO = f"{E['RLU_TENANT']}-trino"


def trino(sql):
    sql = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
    t = time.time()
    r = subprocess.run(["docker", "exec", TRINO, "trino", "--output-format", "ALIGNED", "--execute", sql],
                       capture_output=True, text=True)
    dt = time.time() - t
    if r.returncode: raise SystemExit(r.stderr.strip().splitlines()[-1])
    return r.stdout, dt


mode = sys.argv[1] if len(sys.argv) > 1 else "dupes"
def tsv(sql):
    sql = "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))
    t = time.time()
    r = subprocess.run(["docker", "exec", TRINO, "trino", "--output-format", "TSV", "--execute", sql], capture_output=True, text=True)
    if r.returncode: raise SystemExit(r.stderr.strip().splitlines()[-1])
    return [l.split("\t") for l in r.stdout.strip().splitlines() if l], time.time() - t


DUPES = open(os.path.join(ROOT, "sql", "catalog_dupes.sql")).read().replace("${HOME}", VIEW_HOME)
if mode == "dupes":
    rows, dt = tsv(DUPES)
    if not rows:
        print(f"No PI-home files in the Catalog yet ({dt:.2f}s). The Catalog is a snapshot of the namespace, refreshed every "
              "15 min on this cluster: files seeded at deploy appear after the next one. Run this step again then.")
        sys.exit(2)
    print(f"{'ext':7} {'size':>9} {'copies':>6} {'owners':>6}  {'redundant':>9}  names seen")
    for ext, size, copies, owners, names, gb in rows:
        print(f"{ext:7} {int(size) / 1e9:8.2f}G {copies:>6} {owners:>6}  {float(gb):8.2f}G  {names.replace(' | ', ', ')}")
    print(f"\n{sum(float(r[5]) for r in rows):.1f} GB held more than once across {len(HOMES)} homes · "
          f"Catalog query {dt:.2f}s as the analyst · same size + extension = candidates; step 3 checks content")
elif mode == "verify":
    # the Catalog has no content hash: confirm the candidates are the same bytes (sha256, read as the analyst)
    import hashlib
    from concurrent.futures import ThreadPoolExecutor
    q = DUPES.replace("SELECT ext, size,", "SELECT ext, size, array_join(array_agg(parent_path || '/' || name), '|') AS paths,")
    rows, _ = tsv(q)
    if not rows:
        print("No duplicate candidates in the Catalog yet (see step 1)."); sys.exit(2)
    from env import USERS, P
    an = USERS[f"{P}-analyst"]
    def sha(path):
        local = rlu.MNT_HOME + path[len(VIEW_HOME):]
        r = subprocess.run(["sudo", "-n", "setpriv", f"--reuid={an}", f"--regid={an}", "--clear-groups", "sha256sum", local],
                           capture_output=True, text=True)
        return path, (r.stdout.split() or ["?"])[0][:16]
    t = time.time()
    for ext, size, paths, *_ in rows:
        ps = paths.split("|")
        with ThreadPoolExecutor(len(ps)) as ex: hs = dict(ex.map(sha, ps))
        same = len(set(hs.values())) == 1
        print(f"{ext:7} {int(size) / 1e9:5.2f}G  {len(ps)} copies  {'SAME BYTES' if same else 'DIFFERENT'}  sha256 {list(hs.values())[0]}…")
        for p_, h in hs.items(): print(f"          {p_[len(VIEW_HOME) + 1:]:60} {h}")
    print(f"\nchecked in {time.time() - t:.0f}s, read over NFS as the analyst")
elif mode == "byext":
    q = open(os.path.join(ROOT, "vendor", "genomics", "snippets", "catalog_bytes_by_ext_by_uid.sql")).read()
    q = q.replace("    -- AND (parent_path LIKE '/projects/%' OR parent_path LIKE '/home/%')",
                  f"    AND parent_path LIKE '{VIEW_HOME}/%'").rstrip().rstrip(";")
    q = re.sub(r"LIMIT \d+", "LIMIT 20", q)
    out, dt = trino(q)
    print(out.rstrip()); print(f"\nCatalog query {dt:.2f}s as the analyst")
elif mode == "datasets":
    d = rlu.MNT_DATASETS
    sh = lambda uid, c: subprocess.run(["sudo", "-n", "setpriv", f"--reuid={uid}", f"--regid={uid}", "--clear-groups",
                                        "sh", "-c", c], capture_output=True, text=True)
    print(f"$ findmnt {d}")
    print(subprocess.run(["findmnt", "-n", "-o", "SOURCE,OPTIONS", d], capture_output=True, text=True).stdout.strip())
    print(f"\n$ ls -R {d}   (one curated copy, written once by the curator over S3)")
    print(subprocess.run(f"cd {d} && find . -type f -printf '%10s  %p\\n' | sort -k2", shell=True, capture_output=True, text=True).stdout.rstrip())
    for pi, uid in HOMES.items():
        r = sh(uid, f"head -c 1048576 {d}/reference/GRCh38/GRCh38_full_analysis_set_plus_decoy_hla.fa | wc -c")
        w = sh(uid, f"touch {d}/reference/GRCh38/mine.txt")
        print(f"{pi:10} uid {uid}: read {'OK' if r.stdout.strip() == '1048576' else 'FAILED ' + r.stderr.strip()[:60]}"
              f" · write {'REFUSED: ' + (w.stderr.strip().split(': ')[-1] or 'error') if w.returncode else 'ALLOWED (unexpected)'}")
    # the export itself is read-only, not just the mode bits: even the files' owner (the curator uid) cannot write here
    from env import USERS, P
    w = sh(USERS[f"{P}-svc"], f"touch {d}/reference/GRCh38/curator.txt")
    print(f"{'curator':10} uid {USERS[f'{P}-svc']} (owns the files): write over NFS "
          f"{'REFUSED: ' + w.stderr.strip().split(': ')[-1] if w.returncode else 'ALLOWED (export is not read-only!)'}"
          f"  -> curation happens over S3 by the curator identity, nowhere else")
else:
    sys.exit(__doc__)
