"""Query: the same cohort questions answered two ways, as the analyst (rle-analyst / uid 7103).

  DB   - Trino 478 over VAST DB (catalog `enclave`, analyst key), plus one VAST DB Python SDK query
  file - bcftools 1.21 over the NFS mount of the same view, run as uid 7103 (root is squashed)

Each question is checked for the SAME answer both ways (MATCH / MISMATCH) and both wall times are recorded.
File runs: run 1 is cold (the VCF was written over S3, so n5 never had it in page cache); run 2 is warm.
Writes results/query-<ts>.json.
"""
import csv, hashlib, json, os, subprocess, sys, time
from collections import Counter, defaultdict
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import rle
import trino

BCF = "/srv/rad-lab-enclave/env/bin/bcftools"
RAW = f"{rle.MNT}/incoming/vcf/20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr22.recalibrated_variants.vcf.gz"
ANN = f"{rle.MNT}/incoming/vcf/20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr22.recalibrated_variants.annotated.vcf.gz"
PED = f"{rle.MNT}/incoming/meta/20130606_g1k_3202_samples_ped_population.txt"
VT = 'enclave."rad-lab-enclave-cohort/genomics"'
WORK = "/tmp/rle-run/q"                    # analyst scratch (uid 7103)
PROMOTER = ("chr22", 28741882, 28746831)    # CHEK2 upstream/promoter, from the variants table's own annotation
WINDOW = ("chr22", 29000000, 30000000)      # 1 Mb for allele counts by superpopulation
SAMPLE = "NA12878"
JOIN = "g.pos = v.pos AND g.ref = v.ref AND g.alt = v.alt"
os.makedirs("results", exist_ok=True)


def tq(sql, catalog="enclave"):
    con = trino.dbapi.connect(host="127.0.0.1", port=8085, user="rle-analyst", catalog=catalog)
    cur = con.cursor(); t = time.time(); cur.execute(sql); rows = cur.fetchall()
    return rows, time.time() - t

def as_analyst(cmd):
    """Run a shell pipeline as uid 7103 over NFS. Returns (stdout, seconds)."""
    t = time.time()
    p = subprocess.run(["sudo", "setpriv", "--reuid", "7103", "--regid", "7103", "--clear-groups",
                        "env", "LD_LIBRARY_PATH=/srv/rad-lab-enclave/env/lib", "HOME=/tmp/rle-run",
                        "bash", "-o", "pipefail", "-c", cmd], capture_output=True, text=True)
    if p.returncode: raise RuntimeError(p.stderr[-800:])
    return p.stdout, time.time() - t

def digest(items): return hashlib.sha1("\n".join(sorted(map(str, items))).encode()).hexdigest()[:12]

def run_file(cmd, parse, runs=2):
    out, times = None, []
    for _ in range(runs):
        o, dt = as_analyst(cmd); times.append(round(dt, 2)); out = parse(o)
    return out, times

def run_db(sql, parse, runs=2, catalog="enclave"):
    out, times = None, []
    for _ in range(runs):
        r, dt = tq(sql, catalog); times.append(round(dt, 2)); out = parse(r)
    return out, times

LAKE = "lake.genomics"   # the same tables as Parquet files on the same view, read by Trino's Hive connector over S3
def lake_ready():
    """Register the Parquet tables (export_parquet.py output) in the lake catalog; False if the arm cannot run."""
    try:
        for stmt in open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "snippets", "lake_ddl.sql")).read().split(";"):
            if stmt.strip(): tq(stmt, "lake")
        n, _ = tq(f"SELECT count(*) FROM {LAKE}.genotypes", "lake")
        return n[0][0] > 0
    except Exception as e:
        print("lake arm unavailable:", str(e)[:200], flush=True); return False


results = {"when": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "identity": "rle-analyst (uid 7103)", "questions": []}
subprocess.run(["sudo", "install", "-d", "-o", "7103", "-g", "7103", "-m", "700", WORK], check=True)

def record(qid, question, db, file_, note="", lake=None):
    (dans, dt), (fans, ft) = db, file_
    q = {"id": qid, "question": question, "db_seconds": dt, "file_seconds": ft,
         "db_answer": dans, "file_answer": fans, "match": dans == fans, "note": note}
    if lake:   # third arm: Parquet on VAST via Trino Hive; the row matches only if all three answers agree
        q.update(lake_seconds=lake[1], lake_answer=lake[0], match=dans == fans == lake[0])
    results["questions"].append(q)
    print(json.dumps({k: q[k] for k in ("id", "db_seconds", "lake_seconds", "file_seconds", "match") if k in q}),
          "| db:", str(dans)[:120], "| lake:", str(lake[0] if lake else "-")[:120], "| file:", str(fans)[:120], flush=True)

LAKE_OK = lake_ready()
def lake_arm(sql, parse):   # the same SQL over the Parquet copies; None when the lake catalog is unavailable
    return run_db(sql.replace(VT, LAKE), parse, catalog="lake") if LAKE_OK else None

# ---- Q1: who carries a RARE (AF<1%) PASS allele in the CHEK2 promoter? (non-coding; indexed on both sides)
c, a, b = PROMOTER
sql = f"""SELECT DISTINCT g.sample_id FROM {VT}.genotypes g
          WHERE g.chrom = '{c}' AND g.pos BETWEEN {a} AND {b} AND g.filter = 'PASS' AND g.af < 0.01"""
parse1 = lambda r: {"samples": len(r), "digest": digest(x[0] for x in r)}
db = run_db(sql, parse1); lake1 = lake_arm(sql, parse1)
cmd = (f"{BCF} view --threads 4 -r {c}:{a}-{b} -f PASS {RAW} | {BCF} norm -m- 2>/dev/null | {BCF} view -i 'INFO/AF<0.01' "
       f"| {BCF} query -i 'GT=\"alt\"' -f '[%SAMPLE\\n]' | sort -u")
fl = run_file(cmd, lambda o: {"samples": len(o.split()), "digest": digest(o.split())})
record("Q1", f"Samples carrying a rare (AF<1%) PASS allele in the CHEK2 promoter/upstream window {c}:{a}-{b}", db, fl,
       "Region query: tabix index on the file side, sorted (chrom,pos) table on the DB side.", lake=lake1)

# vastdb SDK version of Q1 (no SQL engine): predicate pushdown on the sorted genotypes table
import pyarrow.compute as pc
from ibis import _
t0 = time.time()
with rle.db("rle-analyst").transaction() as tx:
    sc = tx.bucket(rle.BUCKET).schema(rle.SCHEMA)
    j = sc.table("genotypes").select(columns=["sample_id"],
            predicate=(_.pos >= a) & (_.pos <= b) & (_["filter"] == "PASS") & (_.af < 0.01)).read_all()
ids_ = set(j.column("sample_id").to_pylist())
sdk = {"samples": len(ids_), "digest": digest(ids_)}
results["q1_sdk"] = {"seconds": round(time.time() - t0, 2), "answer": sdk, "match_trino": sdk == db[0]}
print("Q1 via vastdb SDK:", results["q1_sdk"], flush=True)

# ---- Q2: alternate-allele count by superpopulation across a 1 Mb window (PASS)
c, a, b = WINDOW
sql = f"""SELECT s.superpopulation, sum(g.gt) FROM {VT}.genotypes g JOIN {VT}.samples s ON g.sample_id = s.sample_id
          WHERE g.chrom = '{c}' AND g.pos BETWEEN {a} AND {b} AND g.filter = 'PASS' GROUP BY 1"""
parse2 = lambda r: {k: int(v) for k, v in sorted(r)}
db = run_db(sql, parse2); lake2 = lake_arm(sql, parse2)
pops = f"{WORK}/superpop.txt"
as_analyst(f"awk 'NR>1{{print $2\"\\t\"$7}}' {PED} > {pops}")
cmd = (f"{BCF} view --threads 4 -r {c}:{a}-{b} -f PASS {RAW} | "
       f"{BCF} +fill-tags -- -S {pops} -t AC | {BCF} query -f '%INFO/AC_AFR\\t%INFO/AC_AMR\\t%INFO/AC_EAS\\t%INFO/AC_EUR\\t%INFO/AC_SAS\\n'")
def parse_q2(o):
    tot = Counter()
    for line in o.splitlines():
        for pop, f in zip(("AFR", "AMR", "EAS", "EUR", "SAS"), line.split("\t")):
            tot[pop] += sum(int(x) for x in f.split(",") if x not in (".", ""))
    return dict(sorted(tot.items()))
fl = run_file(cmd, parse_q2)
record("Q2", f"Alternate-allele count by superpopulation, PASS sites, {c}:{a}-{b} (1 Mb)", db, fl,
       "bcftools needs +fill-tags with a sample->population file; the DB joins the samples table.", lake=lake2)

# ---- Q3: cohort-wide. Rare (AF<1%), non-coding, CADD>=15, PASS variants: carriers by superpopulation
sql = f"""WITH sel AS (SELECT pos, ref, alt FROM {VT}.variants
                       WHERE filter = 'PASS' AND NOT coding AND af < 0.01 AND cadd_phred >= 15)
          SELECT s.superpopulation, count(*) AS carrier_genotypes, count(DISTINCT g.sample_id) AS samples
          FROM {VT}.genotypes g JOIN sel v ON {JOIN} JOIN {VT}.samples s ON g.sample_id = s.sample_id
          GROUP BY 1"""
parse3 = lambda r: {k: [int(x), int(y)] for k, x, y in sorted(r)}
db = run_db(sql, parse3); lake3 = lake_arm(sql, parse3)
# file side: (a) select sites from the annotated VCF with the same rule, (b) pull genotypes at those sites
SELECT_PY = r'''
import sys
coding = set(sys.argv[1].split(","))
for line in sys.stdin:
    c, p, r, alts, flt, af, cadd, ann = line.rstrip("\n").split("\t")
    if flt != "PASS": continue
    anns = ann.split(",")
    for i, alt in enumerate(alts.split(",")):
        a_af = af.split(",")[i] if af != "." else "."
        a_cadd = cadd.split(",")[0]
        try:
            if float(a_af) >= 0.01 or float(a_cadd) < 15: continue
        except ValueError: continue
        eff = next((x.split("|") for x in anns if x.split("|")[0] == alt), anns[0].split("|"))
        e = eff[1] if len(eff) > 1 else ""
        if e and any(t in coding for t in e.split("&")): continue
        print(c, p, r, alt, sep="\t")
'''
open("/tmp/rle-run/select_sites.py", "w").write(SELECT_PY); os.chmod("/tmp/rle-run/select_sites.py", 0o644)
from functions.transform.main import CODING as CODING_SET
COUNT_PY = r'''
import sys
from collections import Counter
pop = dict(l.split() for l in open(sys.argv[1]))
want = set(tuple(l.split()) for l in open(sys.argv[2]))
seen, samples = set(), {}
for line in sys.stdin:
    s, c, p, r, alts, gt = line.rstrip("\n").split("\t")
    al = gt.replace("|", "/").split("/")
    for i, alt in enumerate(alts.split(","), start=1):
        if (c, p, r, alt) in want and str(i) in al:
            seen.add((s, p, r, alt)); samples.setdefault(pop[s], set()).add(s)
rows = Counter(pop[s] for s, *_ in seen)
for k in sorted(rows): print(k, rows[k], len(samples[k]))
'''
open("/tmp/rle-run/count_carriers.py", "w").write(COUNT_PY); os.chmod("/tmp/rle-run/count_carriers.py", 0o644)
PY = "/srv/rad-lab-enclave/env/bin/python"
cmd = (f"{BCF} query -f '%CHROM\\t%POS\\t%REF\\t%ALT\\t%FILTER\\t%INFO/AF\\t%INFO/CADD_phred\\t%INFO/ANN\\n' {ANN} 2>/dev/null"
       f" | {PY} /tmp/rle-run/select_sites.py {','.join(sorted(CODING_SET))} > {WORK}/q3_sites.tsv"
       f" && cut -f1,2 {WORK}/q3_sites.tsv | sort -u -k2,2n > {WORK}/q3_regions.tsv"
       f" && {BCF} view --threads 8 -R {WORK}/q3_regions.tsv --regions-overlap pos {RAW} 2>/dev/null"
       f" | {BCF} query -i 'GT=\"alt\"' -f '[%SAMPLE\\t%CHROM\\t%POS\\t%REF\\t%ALT\\t%GT\\n]'"
       f" | {PY} /tmp/rle-run/count_carriers.py {pops} {WORK}/q3_sites.tsv")
fl = run_file(cmd, lambda o: {l.split()[0]: [int(l.split()[1]), int(l.split()[2])] for l in o.splitlines()}, runs=1)
record("Q3", "Cohort-wide: carriers of rare (AF<1%) non-coding PASS variants with CADD>=15, by superpopulation "
       "(genotype rows, distinct samples)", db, fl,
       "File side is a 2-step pipeline (site selection from the annotated VCF, then an indexed region read (-R) of the 28 GB "
       "genotype VCF via its tabix index) - the realistic HPC way. Single run on the file side.", lake=lake3)

# ---- Q4: one sample, everything it carries with HIGH/MODERATE impact (PASS)
sql = f"""SELECT g.pos, g.ref, g.alt FROM {VT}.genotypes g JOIN {VT}.variants v ON {JOIN}
          WHERE g.sample_id = '{SAMPLE}' AND v.impact IN ('HIGH', 'MODERATE') AND v.filter = 'PASS'"""
parse4 = lambda r: {"variants": len(r), "digest": digest(f"{p}:{rf}:{al}" for p, rf, al in r)}
db = run_db(sql, parse4)
lake4 = lake_arm(sql, parse4)
cmd = (f"{BCF} view -h {ANN} >/dev/null 2>&1; {BCF} query -i 'FILTER=\"PASS\" && (INFO/ANN~\"HIGH\" || INFO/ANN~\"MODERATE\")' "
       f"-f '%CHROM\\t%POS\\t%REF\\t%ALT\\t%INFO/ANN\\n' {ANN} 2>/dev/null > {WORK}/q4_ann.tsv"
       f" && cut -f1,2 {WORK}/q4_ann.tsv | sort -u -k2,2n > {WORK}/q4_regions.tsv"
       f" && {BCF} view --threads 8 -s {SAMPLE} -R {WORK}/q4_regions.tsv --regions-overlap pos {RAW} 2>/dev/null"
       f" | {BCF} query -i 'GT=\"alt\"' -f '%CHROM\\t%POS\\t%REF\\t%ALT\\t[%GT]\\n'")
def q4_file():
    out, dt = as_analyst(cmd)
    annt, _ = as_analyst(f"cat {WORK}/q4_ann.tsv")
    imp = {}
    for l in annt.splitlines():
        c, p, r, alts, ann = l.split("\t")
        anns = ann.split(",")
        for alt in alts.split(","):
            eff = next((x.split("|") for x in anns if x.split("|")[0] == alt), anns[0].split("|"))
            imp[(p, r, alt)] = eff[2] if len(eff) > 2 else ""
    hits = set()
    for l in out.splitlines():
        c, p, r, alts, gt = l.split("\t")
        al = gt.replace("|", "/").split("/")
        for i, alt in enumerate(alts.split(","), start=1):
            if str(i) in al and imp.get((p, r, alt)) in ("HIGH", "MODERATE"):
                hits.add(f"{p}:{r}:{alt}")
    return {"variants": len(hits), "digest": digest(hits)}, round(dt, 2)
runs4 = [q4_file() for _ in range(int(os.environ.get("RLE_Q4_RUNS", "2")))]
record("Q4", f"Everything {SAMPLE} carries with HIGH/MODERATE predicted impact (PASS)", db,
       (runs4[-1][0], [t for _, t in runs4]),
       "File side: sites from the annotated VCF, then a single-sample indexed region read (-R) of the genotype VCF. "
       "Lake arm: the same SQL over Parquet copies of the tables (export_parquet.py: snappy, sorted by pos, 1M-row "
       "row groups) on the same view, read by Trino's Hive connector over S3 as the analyst.", lake=lake4)

out = f"results/query-{time.strftime('%Y%m%d-%H%M%S')}.json"
json.dump(results, open(out, "w"), indent=1); print("wrote", out)
