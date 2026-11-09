"""rlu-landing: the DataEngine function behind trigger <prefix>-genomics-landing (incoming/genomics/).

handler(ctx, event) is the genomics kit's handler (vendor/genomics/functions/transform/main.py), reused unchanged for
CRAM -> cram_qc, POD5 -> nanopore_runs and the annotated sites VCF -> variants. Two things are this demo's own:

* raw joint-called VCF -> genotypes, **with the site attributes on every genotype row**: AN, number of ALT alleles,
  SNV flag, and the consequence class (synonymous / missense / other) looked up in the annotated VCF *inside the
  function*. The kit measured (20 Oct) that ~9% of raw alleles are written differently in the annotated VCF and do not
  join on pos/ref/alt afterwards; carrying the attributes here means the SFS query needs no such join.
* pedigree -> samples with an `unrelated` flag (the 2,504 unrelated samples of the 1000G 30x release), because an SFS
  over the 3,202 including trio children counts the same haplotypes twice.

A VCF landed under live-*/ (slide 13: a slice of the same VCF) goes to genotypes_live instead, so the
baseline tables slide 12 queries never change; its annotated companion is read by the raw VCF's landing, not loaded.

Index-last convention (the kit's): a VCF is processed when its .tbi lands; the annotated VCF must land before the raw
VCF's .tbi. Runs as uid <base>+1 (rlu-svc) through functions/runner.py, the stand-in for the DE runtime.
"""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "lib"))
import rlu  # noqa: E402  (registers itself as `rle` for the kit)
from env import LIVE_VCF  # noqa: E402
import importlib.util  # noqa: E402
_spec = importlib.util.spec_from_file_location(
    "kit_transform", os.path.join(ROOT, "vendor", "genomics", "functions", "transform", "main.py"))
kit = importlib.util.module_from_spec(_spec)
sys.modules["kit_transform"] = kit                      # forked workers unpickle the kit's chunk function by this name
_spec.loader.exec_module(kit)
sys.path.insert(0, HERE)
import pyarrow as pa  # noqa: E402
from multiprocessing import get_context  # noqa: E402
from csq import classify  # noqa: E402

kit.BIN = os.path.join(rlu.E["RLU_SRV"], "env", "bin")
kit.REF_LOCAL = os.path.join(rlu.MNT, rlu.REF)
kit.WORKERS = int(rlu.E.get("RLU_WORKERS", "96"))

kit.SCHEMAS["genotypes"] = pa.schema([
    ("chrom", pa.string()), ("pos", pa.int64()), ("ref", pa.string()), ("alt", pa.string()),
    ("filter", pa.string()), ("af", pa.float32()),
    ("an", pa.int32()), ("n_alt", pa.int8()), ("snv", pa.bool_()), ("csq", pa.string()),   # site attributes
    ("sample_id", pa.string()), ("gt", pa.int8()), ("gq", pa.int16()), ("dp", pa.int16())])
kit.SCHEMAS["samples"] = kit.SCHEMAS["samples"].append(pa.field("unrelated", pa.bool_()))
G = kit.SCHEMAS["genotypes"]
kit.SCHEMAS["genotypes_live"], kit.SORT["genotypes_live"] = G, kit.SORT["genotypes"]


def _ann_index(path, region):
    """(pos, ref, alt) -> consequence class, from the annotated VCF over one region."""
    from cyvcf2 import VCF
    out = {}
    for v in VCF(path, threads=1, lazy=True)(region):
        ann = v.INFO.get("ANN") or ""
        for alt in v.ALT:
            out[(v.POS, v.REF, alt)] = classify(ann, alt)
    return out


def _chunk(args):
    raw, annotated, region, table = args
    from cyvcf2 import VCF
    import numpy as np
    start = int(region.split(":")[1].split("-")[0])
    ann = _ann_index(annotated, region)
    s = rlu.db(f"{rlu.E['RLU_PREFIX']}-svc")
    vcf = VCF(raw, threads=1, lazy=True)
    names = np.array(vcf.samples)
    cols, n = {k: [] for k in G.names}, 0
    for v in vcf(region):
        if v.POS < start: continue                     # record starts in the previous chunk
        alts = v.ALT
        an, nalt = v.INFO.get("AN"), len(alts)
        snv = len(v.REF) == 1 and all(len(a) == 1 and a != "*" for a in alts)
        a = v.genotype.array()[:, :2]                  # allele indices per sample, -1 = missing
        gq, dp = v.gt_quals, v.gt_depths
        afv = v.INFO.get("AF")
        for ai, alt in enumerate(alts, start=1):
            cnt = (a == ai).sum(axis=1)
            idx = np.nonzero(cnt)[0]
            if idx.size == 0: continue
            k = idx.size
            site = {"chrom": v.CHROM, "pos": v.POS, "ref": v.REF, "alt": alt, "filter": v.FILTER or "PASS",
                    "af": kit._f(afv[ai - 1] if isinstance(afv, tuple) else afv), "an": an, "n_alt": nalt,
                    "snv": snv, "csq": ann.get((v.POS, v.REF, alt), "unannotated")}
            for c, val in site.items(): cols[c] += [val] * k
            cols["sample_id"] += names[idx].tolist(); cols["gt"] += cnt[idx].astype("int8").tolist()
            cols["gq"] += np.clip(gq[idx], -1, 32767).astype("int16").tolist()
            cols["dp"] += np.clip(dp[idx], -1, 32767).astype("int16").tolist()
        if len(cols["pos"]) >= 400_000:
            n += kit.insert(s, table, pa.RecordBatch.from_pydict(cols, schema=G)); cols = {k: [] for k in G.names}
    if cols["pos"]: n += kit.insert(s, table, pa.RecordBatch.from_pydict(cols, schema=G))
    return n


_kit_vcf = kit.do_vcf


def do_vcf(session, vcf_key):
    live = "/live-" in vcf_key
    if ".annotated." in vcf_key:
        if live: return "annotated: read by the raw VCF's landing", 0
        return _kit_vcf(session, vcf_key)              # sites VCF -> variants, the kit's path
    raw = os.path.join(rlu.MNT, vcf_key)
    annotated = raw.replace(".vcf.gz", ".annotated.vcf.gz")
    if not os.path.exists(annotated):
        raise FileNotFoundError(f"annotated VCF must land before the raw VCF's index: {annotated}")
    if live:                                           # each live landing replaces the last: a second click can't double it
        with session.transaction() as tx:
            t = tx.bucket(rlu.BUCKET).schema(rlu.SCHEMA).table("genotypes_live", fail_if_missing=False)
            if t is not None: t.drop()
        kit.ensure_tables(session)
    lo, hi = (LIVE_VCF["lo"], LIVE_VCF["hi"]) if live else (1, kit.CHR22_LEN)   # a slice: only its own span
    step = -(-(hi - lo + 1) // kit.WORKERS) if live else kit.CHUNK              # a slice: one chunk per worker
    regions = [f"chr22:{s}-{min(s + step - 1, hi)}" for s in range(lo, hi + 1, step)]
    with get_context("fork").Pool(min(kit.WORKERS, len(regions))) as pool:
        table = "genotypes_live" if live else "genotypes"
        n = sum(pool.imap_unordered(_chunk, [(raw, annotated, r, table) for r in regions]))
    return f"vcf->{table}", n


def do_ped(session, key):
    idx = os.path.join(os.path.dirname(os.path.join(rlu.MNT, key)), "1000G_2504_high_coverage.sequence.index")
    unrel = set()
    for line in open(idx):
        if line.startswith("#") or not line.strip(): continue
        f = line.rstrip("\n").split("\t")
        unrel.add(f[9].strip())                        # SAMPLE_NAME; 3 of them carry a trailing space (HG02635 ...)
    rows = []
    for line in open(os.path.join(rlu.MNT, key)):
        f = line.split()
        if not f or f[0] == "FamilyID": continue
        rows.append({"sample_id": f[1], "family_id": f[0], "father_id": f[2], "mother_id": f[3], "sex": int(f[4]),
                     "population": f[5], "superpopulation": f[6], "unrelated": f[1] in unrel})
    kit.ensure_tables(session)                         # recreate the table if a reset dropped it
    return "ped->samples", kit.insert(session, "samples", rows)


kit.do_vcf, kit.do_ped = do_vcf, do_ped                # kit.route() looks these up by name at call time
handler, ensure_tables = kit.handler, kit.ensure_tables
