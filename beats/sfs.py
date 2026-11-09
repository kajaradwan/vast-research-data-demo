"""Beat 1: the folded SFS by superpopulation, synonymous vs missense, three ways over the same data.

  sfs.py [vastdb|parquet|files|consortium|all] [--repeat N]

  vastdb      Trino (vast connector) on the tables the trigger's function wrote, sql/sfs.sql
  parquet     Trino (Hive connector) on the same rows exported as Parquet on the same view, the same SQL
  files       bcftools 1.21 over the raw VCF and annotated VCF on the NFS mount, parallel by region (RLU_WORKERS)
  landed      slide 13: the same SQL on genotypes_live (a 5 Mb slice landed live, no export job), next to the baseline
              table over the same span. Same digest = the landing gives the same answer. Prints only; writes no result.
  consortium  not an arm: the SFS from the 1000G consortium's own AC_<pop>_unrel / AN_<pop>_unrel tags in the
              annotated VCF, as an independent check that the genotype route counts correctly

Every arm runs as the analyst (uid base+3, read-only by identity policy). The answer is compared by digest: a sha256 of
the sorted (superpopulation, class, mac, sites) rows. Timings are from a shared lab cluster and one client: not a
performance claim. Writes results/sfs-<ts>.json (the deck plots from it).
"""
import argparse, hashlib, json, os, subprocess, sys, tempfile, time
from concurrent.futures import ThreadPoolExecutor
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "lib"))
sys.path.insert(0, os.path.join(ROOT, "functions", "landing"))
from env import E, BUCKET, SCHEMA_G, P, MNT, LIVE_VCF  # noqa: E402
from csq import classify  # noqa: E402

TRINO = f"{E['RLU_TENANT']}-trino"
SQL = open(os.path.join(ROOT, "sql", "sfs.sql")).read()
TABLES = {"vastdb": (f'vast."{BUCKET}/{SCHEMA_G}".genotypes', f'vast."{BUCKET}/{SCHEMA_G}".samples'),
          "parquet": ("lake.genomics.genotypes", "lake.genomics.samples")}
VCF = f"{MNT}/data/incoming/genomics/baseline/vcf/chr22.recalibrated_variants"
BIN = f"{E['RLU_SRV']}/env/bin"
POPS = ("AFR", "AMR", "EAS", "EUR", "SAS")
# the file arm reads the VCF over NFS as the analyst: the view is S3_NATIVE, so the identity policy decides the read
AS = f"sudo -n setpriv --reuid={int(E['RLU_UID_BASE']) + 3} --regid={int(E['RLU_UID_BASE']) + 3} --clear-groups "


def digest(rows):
    canon = "\n".join(f"{sp},{c},{m},{n}" for sp, c, m, n in sorted(rows))
    return hashlib.sha256(canon.encode()).hexdigest()[:12]


def trino(sql):
    r = subprocess.run(["docker", "exec", TRINO, "trino", "--output-format", "TSV", "--execute", sql],
                       capture_output=True, text=True)
    if r.returncode: raise RuntimeError(r.stderr.strip().splitlines()[-1] if r.stderr.strip() else "trino failed")
    return [l.split("\t") for l in r.stdout.strip().splitlines() if l]


def arm_sql(name):
    g, s = TABLES[name]
    q = SQL.replace("${G}", g).replace("${S}", s)
    q = "\n".join(l for l in q.splitlines() if not l.strip().startswith("--"))
    t = time.time()
    rows = [(sp, c, int(m), int(n)) for sp, c, m, n in trino(q)]
    return rows, time.time() - t


def samples_unrelated():
    """(sample -> superpopulation) for the 2,504 unrelated, read from the samples table as the analyst."""
    g, s = TABLES["vastdb"]
    return {a: b for a, b in trino(f"SELECT sample_id, superpopulation FROM {s} WHERE unrelated")}


def _region_files(args):
    region, pops_file, unrel_file = args
    view = (f"{AS}{BIN}/bcftools view -r {region} -f PASS -m2 -M2 -v snps -i 'INFO/AN=6404' -Ou {VCF}.vcf.gz"
            f" | {BIN}/bcftools view -S {unrel_file} -Ou"
            f" | {BIN}/bcftools +fill-tags -Ou -- -S {pops_file} -t AC,AN"
            f" | {BIN}/bcftools query -f '%POS\\t%REF\\t%ALT" + "".join(f"\\t%AC_{p}\\t%AN_{p}" for p in POPS) + "\\n'")
    ann = subprocess.run(f"{AS}{BIN}/bcftools query -r {region} -f '%POS\\t%REF\\t%ALT\\t%ANN\\n' {VCF}.annotated.vcf.gz",
                         shell=True, capture_output=True, text=True).stdout
    cls = {}
    for l in ann.splitlines():
        pos, ref, alt, a = l.split("\t", 3)
        for al in alt.split(","):
            cls[(pos, ref, al)] = classify(a, al)
    start = int(region.split(":")[1].split("-")[0])
    out = {}
    for l in subprocess.run(view, shell=True, capture_output=True, text=True).stdout.splitlines():
        f = l.split("\t")
        if int(f[0]) < start: continue
        c = cls.get((f[0], f[1], f[2]), "other")
        if c not in ("synonymous", "missense"): continue
        for i, p in enumerate(POPS):
            ac, an = int(f[3 + 2 * i]), int(f[4 + 2 * i])
            m = min(ac, an - ac)
            if m > 0: out[(p, c, m)] = out.get((p, c, m), 0) + 1
    return out


def arm_files():
    unrel = samples_unrelated()
    d = tempfile.mkdtemp(prefix="rlu-sfs-")
    pf, uf = os.path.join(d, "pops.txt"), os.path.join(d, "unrel.txt")
    open(pf, "w").write("".join(f"{s}\t{p}\n" for s, p in unrel.items()))
    open(uf, "w").write("".join(f"{s}\n" for s in unrel))
    os.chmod(d, 0o755); os.chmod(pf, 0o644); os.chmod(uf, 0o644)
    L, W = 50_818_468, int(E.get("RLU_WORKERS", "96"))
    step = -(-L // (W * 2))
    regions = [f"chr22:{s}-{min(s + step - 1, L)}" for s in range(1, L + 1, step)]
    t = time.time()
    tot = {}
    with ThreadPoolExecutor(W) as ex:
        for part in ex.map(_region_files, [(r, pf, uf) for r in regions]):
            for k, n in part.items(): tot[k] = tot.get(k, 0) + n
    dt = time.time() - t
    return [(p, c, m, n) for (p, c, m), n in tot.items()], dt, {"regions": len(regions), "workers": W}


def consortium():
    """The 1000G consortium's own unrelated-sample counts (AC_<pop>_unrel) from the annotated sites VCF, on the SAME
    sites (raw PASS biallelic SNVs, AN=6404). The annotated VCF splits raw multiallelic records into biallelic lines
    (raw C>T,A becomes two records; measured 4 Nov: 84,360 such SNV lines), so without the site list it counts sites
    this SFS deliberately leaves out. With it, a match means the genotype rows and the 2,504-sample set agree with the
    consortium's own tags, site by site."""
    g, _ = TABLES["vastdb"]
    t = time.time()
    keep = {(int(p), r, a) for p, r, a in trino(f"SELECT DISTINCT pos, ref, alt FROM {g} WHERE filter = 'PASS' "
                                                f"AND n_alt = 1 AND snv AND an = 6404")}
    f = "%POS\\t%REF\\t%ALT\\t%ANN" + "".join(f"\\t%AC_{p}_unrel\\t%AN_{p}_unrel" for p in POPS) + "\\n"
    r = subprocess.run(f"{AS}{BIN}/bcftools query -f '{f}' -i 'TYPE=\"snp\"' {VCF}.annotated.vcf.gz",
                       shell=True, capture_output=True, text=True)
    tot = {}
    for l in r.stdout.splitlines():
        x = l.split("\t")
        if (int(x[0]), x[1], x[2]) not in keep: continue
        c = classify(x[3], x[2])
        if c not in ("synonymous", "missense"): continue
        for i, p in enumerate(POPS):
            try: ac, an = int(x[4 + 2 * i]), int(x[5 + 2 * i])
            except ValueError: continue
            m = min(ac, an - ac)
            if m > 0: tot[(p, c, m)] = tot.get((p, c, m), 0) + 1
    return [(p, c, m, n) for (p, c, m), n in tot.items()], time.time() - t


def landed():
    g, s = TABLES["vastdb"]
    live = f'vast."{BUCKET}/{SCHEMA_G}".genotypes_live'
    n = trino(f"SELECT count(pos), count(DISTINCT pos) FROM {live}")[0]
    if int(n[0]) == 0: sys.exit("genotypes_live is empty: run the landing step first")
    print(f"genotypes_live: {int(n[0]):,} genotype rows over {int(n[1]):,} sites, written by the function as the slice landed")
    span = f"(SELECT * FROM {g} WHERE pos BETWEEN {LIVE_VCF['lo']} AND {LIVE_VCF['hi']})"
    out = {}
    for name, tbl in (("landed just now", live), (f"baseline, {LIVE_VCF['region']}", span)):
        q = SQL.replace("${G}", tbl).replace("${S}", s)
        q = "\n".join(l for l in q.splitlines() if not l.strip().startswith("--"))
        t = time.time()
        rows = [(sp, c, int(m), int(k)) for sp, c, m, k in trino(q)]
        out[name] = digest(rows)
        print(f"{name:28} {time.time() - t:5.2f}s  digest {out[name]}  {len(rows)} rows, {sum(r[3] for r in rows):,} site-pop pairs", flush=True)
    same = len(set(out.values())) == 1
    print("same answer: the slice was queryable as it landed, with no export job" if same else "ANSWERS DIFFER")
    sys.exit(0 if same else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("arm", nargs="?", default="all")
    ap.add_argument("--repeat", type=int, default=1)
    a = ap.parse_args()
    if a.arm == "landed": landed()
    arms = ["vastdb", "parquet", "files", "consortium"] if a.arm == "all" else [a.arm]
    res = {"what": "folded SFS, chr22, 2,504 unrelated, PASS biallelic SNVs with AN=6404, synonymous vs missense",
           "uid": os.getuid(), "arms": {}}
    rows_out, got = None, {arm: {"times": [], "extra": {}} for arm in arms}
    for i in range(a.repeat):                       # interleaved: arm order rotates each round
        for arm in arms[i % len(arms):] + arms[:i % len(arms)]:
            if arm in TABLES: rows, dt = arm_sql(arm)
            elif arm == "files": rows, dt, got[arm]["extra"] = arm_files()
            else: rows, dt = consortium()
            got[arm]["times"].append(round(dt, 2)); got[arm]["rows"] = rows
    for arm in arms:
        rows, times = got[arm]["rows"], got[arm]["times"]
        dg = digest(rows)
        res["arms"][arm] = {"seconds": times, "median_s": sorted(times)[len(times) // 2], "digest": dg, "rows": len(rows),
                            "sites": sum(r[3] for r in rows), **got[arm]["extra"]}
        print(f"{arm:10} {' / '.join(f'{x:.2f}s' for x in times):>24}  digest {dg}  {len(rows)} rows, "
              f"{sum(r[3] for r in rows):,} site-pop pairs", flush=True)
        rows_out = rows_out or rows
    ds = {v["digest"] for v in res["arms"].values()}
    res["same_answer"] = len(ds) == 1
    print("same answer on every arm:" if len(ds) == 1 else "ANSWERS DIFFER:", ", ".join(sorted(ds)))
    res["sfs"] = sorted(rows_out)
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    p = os.path.join(ROOT, "results", f"sfs-{time.strftime('%Y%m%d-%H%M%S')}.json")
    try:
        json.dump(res, open(p, "w"))
    except PermissionError:
        pass
    print(json.dumps({"result": p, "same_answer": res["same_answer"]}))
