"""rle-transform: the Data Engine function for the enclave's incoming/ zone.

Shape is a DataEngine Python handler - handler(ctx, event) receiving the CloudEvent from the element trigger
`rle-cohort-landing`. On lab no Kubernetes compute cluster is linked to the tenant, so runner.py stands in for the DE
runtime (consumes the same CloudEvents from Kafka and calls handler()). It runs as uid 7101 = rle-svc.

Index-last convention (what a sequencer's completion marker does): a VCF is processed when its .tbi lands, a CRAM when
its .crai lands; POD5 and the population file are processed on arrival. Data is read over the NFS mount of the same
view the ingest wrote over S3 (multiprotocol), and rows go to VAST DB in rad-lab-enclave-cohort/genomics.
"""
import json, os, re, subprocess, sys, time, traceback
from multiprocessing import get_context

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
import pyarrow as pa
import rle

BIN = "/srv/rad-lab-enclave/env/bin"
REF_LOCAL = os.path.join(rle.MNT, rle.REF)
CHR22_LEN = 50_818_468
WORKERS = int(os.environ.get("RLE_WORKERS", "96"))
CHUNK = int(os.environ.get("RLE_CHUNK_BP", "250000"))
CODING = {"missense_variant", "synonymous_variant", "stop_gained", "stop_lost", "start_lost", "frameshift_variant",
          "inframe_insertion", "inframe_deletion", "disruptive_inframe_insertion", "disruptive_inframe_deletion",
          "stop_retained_variant", "initiator_codon_variant", "protein_altering_variant", "coding_sequence_variant"}

SCHEMAS = {
  "samples": pa.schema([("sample_id", pa.string()), ("family_id", pa.string()), ("father_id", pa.string()),
                        ("mother_id", pa.string()), ("sex", pa.int8()), ("population", pa.string()),
                        ("superpopulation", pa.string())]),
  "variants": pa.schema([("chrom", pa.string()), ("pos", pa.int64()), ("ref", pa.string()), ("alt", pa.string()),
                         ("qual", pa.float32()), ("filter", pa.string()), ("ac", pa.int32()), ("an", pa.int32()),
                         ("af", pa.float32()), ("af_afr", pa.float32()), ("af_amr", pa.float32()), ("af_eas", pa.float32()),
                         ("af_eur", pa.float32()), ("af_sas", pa.float32()), ("effect", pa.string()), ("impact", pa.string()),
                         ("gene", pa.string()), ("coding", pa.bool_()), ("cadd_phred", pa.float32()), ("phylop", pa.float32())]),
  # filter/af are the RAW VCF's site values, so site-level questions need no join to the annotated table
  # (measured 20 Oct: ~9% of raw alleles - spanning deletions '*', multiallelic indels - are written differently
  #  in the annotated VCF and do not join on pos/ref/alt)
  "genotypes": pa.schema([("chrom", pa.string()), ("pos", pa.int64()), ("ref", pa.string()), ("alt", pa.string()),
                          ("filter", pa.string()), ("af", pa.float32()),
                          ("sample_id", pa.string()), ("gt", pa.int8()), ("gq", pa.int16()), ("dp", pa.int16())]),
  "cram_qc": pa.schema([("sample_id", pa.string()), ("object_key", pa.string()), ("bytes", pa.int64()),
                        ("raw_reads", pa.int64()), ("mapped_reads", pa.int64()), ("mapped_pct", pa.float32()),
                        ("properly_paired_pct", pa.float32()), ("duplicate_pct", pa.float32()), ("error_rate", pa.float32()),
                        ("avg_quality", pa.float32()), ("insert_size_avg", pa.float32()), ("bases_mapped_cigar", pa.int64()),
                        ("chr22_mean_depth", pa.float32()), ("processed_at", pa.timestamp("s"))]),
  "nanopore_runs": pa.schema([("object_key", pa.string()), ("bytes", pa.int64()), ("reads", pa.int64()),
                              ("signal_samples", pa.int64()), ("mean_read_seconds", pa.float32()), ("sample_rate", pa.int32()),
                              ("flow_cell_id", pa.string()), ("sequencing_kit", pa.string()), ("experiment", pa.string()),
                              ("processed_at", pa.timestamp("s"))]),
  "ingest_log": pa.schema([("event_id", pa.string()), ("event_time", pa.string()), ("object_key", pa.string()),
                           ("principal", pa.string()), ("source_ip", pa.string()), ("handler", pa.string()),
                           ("status", pa.string()), ("rows", pa.int64()), ("seconds", pa.float32()),
                           ("detail", pa.string())]),
}
SORT = {"variants": ["chrom", "pos"], "genotypes": ["chrom", "pos"], "samples": ["sample_id"]}


def ensure_tables(session):
    with session.transaction() as tx:
        b = tx.bucket(rle.BUCKET)
        sc = b.schema(rle.SCHEMA, fail_if_missing=False) or b.create_schema(rle.SCHEMA)
        for name, schema in SCHEMAS.items():
            if sc.table(name, fail_if_missing=False) is None:
                sc.create_table(name, schema, sorting_key=SORT.get(name, []))


def insert(session, table, rows_or_batch):
    if not isinstance(rows_or_batch, (pa.RecordBatch, pa.Table)):
        if not rows_or_batch: return 0
        rows_or_batch = pa.RecordBatch.from_pylist(rows_or_batch, schema=SCHEMAS[table])
    if rows_or_batch.num_rows == 0: return 0
    with session.transaction() as tx:
        tx.bucket(rle.BUCKET).schema(rle.SCHEMA).table(table).insert(rows_or_batch)
    return rows_or_batch.num_rows


# ---------------------------------------------------------------- VCF: one worker per region chunk
def _f(x):
    try: return None if x is None else float(x[0] if isinstance(x, tuple) else x)
    except (TypeError, ValueError): return None

def _vcf_chunk(args):
    path, region, mode = args
    from cyvcf2 import VCF
    import numpy as np
    s = rle.db("rle-svc")
    vcf = VCF(path, threads=1, lazy=True)
    n, rows, cols = 0, [], None
    if mode == "genotypes":
        names = np.array(vcf.samples)
        cols = {k: [] for k in SCHEMAS["genotypes"].names}
    for v in vcf(region):
        if v.POS < int(region.split(":")[1].split("-")[0]): continue    # record starts in the previous chunk
        alts = v.ALT
        if mode == "variants":
            ann = (v.INFO.get("ANN") or "").split(",")
            for i, alt in enumerate(alts):
                eff = next((a.split("|") for a in ann if a.split("|")[0] == alt), ann[0].split("|") if ann[0] else [])
                effect = eff[1] if len(eff) > 1 else None
                ac, af = v.INFO.get("AC"), v.INFO.get("AF")
                pick = lambda x: (x[i] if isinstance(x, tuple) else x)
                rows.append({"chrom": v.CHROM, "pos": v.POS, "ref": v.REF, "alt": alt, "qual": v.QUAL,
                             "filter": v.FILTER or "PASS", "ac": pick(ac), "an": v.INFO.get("AN"), "af": _f(pick(af)),
                             **{f"af_{p}": _f(pick(v.INFO.get(f"AF_{p.upper()}"))) for p in ("afr", "amr", "eas", "eur", "sas")},
                             "effect": effect, "impact": eff[2] if len(eff) > 2 else None,
                             "gene": eff[3] if len(eff) > 3 else None,
                             "coding": bool(effect and any(e in CODING for e in effect.split("&"))),
                             "cadd_phred": _f(str(v.INFO.get("CADD_phred") or "").split(",")[0] or None),
                             "phylop": _f(v.INFO.get("phyloP20way_mammalian"))})
        else:
            g = v.genotype.array()            # (n_samples, ploidy+1): allele indices, -1 missing, last col phased
            a = g[:, :2]
            gq, dp = v.gt_quals, v.gt_depths
            for ai, alt in enumerate(alts, start=1):
                cnt = (a == ai).sum(axis=1)
                idx = np.nonzero(cnt)[0]
                if idx.size == 0: continue
                k = idx.size
                cols["chrom"] += [v.CHROM] * k; cols["pos"] += [v.POS] * k
                cols["ref"] += [v.REF] * k; cols["alt"] += [alt] * k
                cols["filter"] += [v.FILTER or "PASS"] * k
                afv = v.INFO.get("AF"); cols["af"] += [_f(afv[ai - 1] if isinstance(afv, tuple) else afv)] * k
                cols["sample_id"] += names[idx].tolist(); cols["gt"] += cnt[idx].astype("int8").tolist()
                cols["gq"] += np.clip(gq[idx], -1, 32767).astype("int16").tolist()
                cols["dp"] += np.clip(dp[idx], -1, 32767).astype("int16").tolist()
            if len(cols["pos"]) >= 400_000:
                n += insert(s, "genotypes", pa.RecordBatch.from_pydict(cols, schema=SCHEMAS["genotypes"]))
                cols = {k: [] for k in SCHEMAS["genotypes"].names}
        if len(rows) >= 100_000:
            n += insert(s, "variants", rows); rows = []
    if mode == "variants": n += insert(s, "variants", rows)
    elif cols["pos"]: n += insert(s, "genotypes", pa.RecordBatch.from_pydict(cols, schema=SCHEMAS["genotypes"]))
    return n

def do_vcf(session, vcf_key):
    path = os.path.join(rle.MNT, vcf_key)
    mode = "variants" if ".annotated." in vcf_key else "genotypes"
    regions = [f"chr22:{s}-{min(s + CHUNK - 1, CHR22_LEN)}" for s in range(1, CHR22_LEN + 1, CHUNK)]
    with get_context("fork").Pool(min(WORKERS, len(regions))) as pool:
        n = sum(pool.imap_unordered(_vcf_chunk, [(path, r, mode) for r in regions]))
    return f"vcf->{mode}", n


# ---------------------------------------------------------------- CRAM: samtools stats + idxstats
def do_cram(session, cram_key):
    path = os.path.join(rle.MNT, cram_key)
    st = subprocess.run([f"{BIN}/samtools", "stats", "-@", "16", "--reference", REF_LOCAL, path],
                        capture_output=True, text=True, check=True).stdout
    sn = {m.group(1).strip(): m.group(2).strip() for m in re.finditer(r"^SN\t([^:]+):\t([^\t\n]+)", st, re.M)}
    num = lambda k: float(sn.get(k, "0"))
    raw = num("raw total sequences")
    idx = subprocess.run([f"{BIN}/samtools", "idxstats", path], capture_output=True, text=True, check=True).stdout
    chr22 = next((l.split("\t") for l in idx.splitlines() if l.startswith("chr22\t")), None)
    row = {"sample_id": os.path.basename(cram_key).split(".")[0], "object_key": cram_key,
           "bytes": os.path.getsize(path), "raw_reads": int(raw), "mapped_reads": int(num("reads mapped")),
           "mapped_pct": 100 * num("reads mapped") / raw if raw else None,
           "properly_paired_pct": 100 * num("reads properly paired") / raw if raw else None,
           "duplicate_pct": 100 * num("reads duplicated") / raw if raw else None,
           "error_rate": num("error rate"), "avg_quality": num("average quality"),
           "insert_size_avg": num("insert size average"), "bases_mapped_cigar": int(num("bases mapped (cigar)")),
           "chr22_mean_depth": num("bases mapped (cigar)") / 39_159_782,      # chr22 non-N length (GRCh38)
           "processed_at": int(time.time())}
    return "cram->cram_qc", insert(session, "cram_qc", [row])


# ---------------------------------------------------------------- POD5 (ONT nanopore, Mason-lab pattern)
def do_pod5(session, key):
    import pod5
    path = os.path.join(rle.MNT, key)
    reads = samples = 0; rate = None; info = None
    with pod5.Reader(path) as r:
        for rec in r.reads():
            reads += 1; samples += rec.num_samples
            if info is None:
                info = rec.run_info; rate = int(info.sample_rate)
    row = {"object_key": key, "bytes": os.path.getsize(path), "reads": reads, "signal_samples": samples,
           "mean_read_seconds": (samples / reads / rate) if reads and rate else None, "sample_rate": rate,
           "flow_cell_id": getattr(info, "flow_cell_id", None), "sequencing_kit": getattr(info, "sequencing_kit", None),
           "experiment": os.path.basename(key).replace(".pod5", ""), "processed_at": int(time.time())}
    return "pod5->nanopore_runs", insert(session, "nanopore_runs", [row])


# ---------------------------------------------------------------- population file
def do_ped(session, key):
    rows = []
    for line in open(os.path.join(rle.MNT, key)):
        f = line.split()
        if not f or f[0] == "FamilyID": continue
        rows.append({"sample_id": f[1], "family_id": f[0], "father_id": f[2], "mother_id": f[3],
                     "sex": int(f[4]), "population": f[5], "superpopulation": f[6]})
    return "ped->samples", insert(session, "samples", rows)


def route(key):
    if key.endswith(".vcf.gz.tbi"):  return do_vcf, key[:-4]
    if key.endswith(".cram.crai"):   return do_cram, key[:-5]
    if key.endswith(".pod5"):        return do_pod5, key
    if key.endswith("_population.txt"): return do_ped, key
    return None, key


def handler(ctx, event):
    """DataEngine entry point. `event` is the CloudEvent: headers (ce_*) + the S3-style Records body."""
    body = event["data"] if isinstance(event.get("data"), dict) else json.loads(event["data"])
    rec = body["Records"][0]
    key = rec["s3"]["object"]["key"]
    fn, target = route(key)
    if fn is None:
        return {"skipped": key}
    session = ctx["session"]
    t0 = time.time(); status, rows, detail, name = "ok", 0, "", fn.__name__
    try:
        name, rows = fn(session, target)
    except Exception as e:
        status, detail = "error", f"{type(e).__name__}: {e}"[:500]
        traceback.print_exc()
    secs = time.time() - t0
    insert(session, "ingest_log", [{"event_id": event.get("id"), "event_time": rec.get("eventTime"), "object_key": target,
                                    "principal": rec.get("userIdentity", {}).get("principalId"),
                                    "source_ip": rec.get("requestParameters", {}).get("sourceIPAddress"),
                                    "handler": name, "status": status, "rows": rows, "seconds": secs, "detail": detail}])
    return {"object": target, "handler": name, "status": status, "rows": rows, "seconds": round(secs, 2)}
