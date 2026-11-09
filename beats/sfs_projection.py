"""Does keeping only complete-call sites change the SFS? (peer-review finding, 4 Nov 2026)

The genotypes table stores non-reference calls only, so a missing call cannot be told from a reference call and the SFS
query keeps sites called in all 3,202 samples (AN = 6404). The standard alternative is projection: for every site,
draw n haplotypes hypergeometrically from the AN called ones. This uses the consortium's per-population unrelated
counts (AC/AN_<pop>_unrel in the annotated VCF) on the raw PASS biallelic SNV sites, and compares, at n = 100 haplotypes
per superpopulation, folded:
  A  complete-call sites only (what the demo's query uses)
  B  every site with AN_<pop>_unrel >= 100, projected (keeps the incomplete sites)
Reports the share of sites in the first minor-allele classes, synonymous vs missense. Runs as the analyst.
"""
import os, subprocess, sys
import numpy as np
from scipy.stats import hypergeom
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "lib")); sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "functions", "landing"))
from sfs import trino, TABLES, AS, BIN, VCF, POPS  # noqa: E402
from csq import classify  # noqa: E402

N = 100
g, _ = TABLES["vastdb"]
sites = {(int(p), r, a): int(an) for p, r, a, an in trino(
    f"SELECT DISTINCT pos, ref, alt, an FROM {g} WHERE filter = 'PASS' AND n_alt = 1 AND snv AND csq IN ('synonymous','missense')")}
f = "%POS\\t%REF\\t%ALT\\t%ANN" + "".join(f"\\t%AC_{p}_unrel\\t%AN_{p}_unrel" for p in POPS) + "\\n"
out = subprocess.run(f"{AS}{BIN}/bcftools query -f '{f}' -i 'TYPE=\"snp\"' {VCF}.annotated.vcf.gz", shell=True,
                     capture_output=True, text=True).stdout
acc = {(s, p, c): np.zeros(N // 2 + 1) for s in "AB" for p in POPS for c in ("synonymous", "missense")}
cache = {}
def fold(ac, an):
    if (ac, an) not in cache:
        pmf = hypergeom(an, ac, N).pmf(np.arange(N + 1))
        f_ = pmf[:N // 2 + 1].copy(); f_[:N // 2] += pmf[N:N // 2:-1][:N // 2]
        cache[(ac, an)] = f_
    return cache[(ac, an)]
for l in out.splitlines():
    x = l.split("\t")
    k = (int(x[0]), x[1], x[2])
    if k not in sites: continue
    c = classify(x[3], x[2])
    for i, p in enumerate(POPS):
        try: ac, an = int(x[4 + 2 * i]), int(x[5 + 2 * i])
        except ValueError: continue
        if an < N: continue
        v = fold(ac, an)
        acc[("B", p, c)] += v
        if sites[k] == 6404: acc[("A", p, c)] += v
print(f"folded SFS projected to n = {N} haplotypes per superpopulation; share of segregating sites in MAC 1 and MAC 1-5")
print(f"{'pop':4} {'class':10} {'A complete-call':>22} {'B all sites, projected':>26}   sites A / B")
for p in POPS:
    for c in ("synonymous", "missense"):
        a, b = acc[("A", p, c)], acc[("B", p, c)]
        sa, sb = a[1:].sum(), b[1:].sum()
        print(f"{p:4} {c:10} {a[1] / sa:10.3f} {a[1:6].sum() / sa:10.3f}   {b[1] / sb:12.3f} {b[1:6].sum() / sb:12.3f}   {sa:8.0f} / {sb:8.0f}")
