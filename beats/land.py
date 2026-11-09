"""Land files over S3 as the ingest identity, the way a sequencer or a detector's transfer service would.

  land.py baseline   the SFS inputs: pedigree + unrelated index, reference, annotated VCF, raw VCF, raw .tbi last
  land.py live       beat 2: 8 chr22 CRAMs (+ .crai last) and 2 ONT POD5, under incoming/genomics/live-<ts>/
  land.py vcf        slide 13: a 5 Mb slice of the same chr22 VCF (+ its annotated slice), raw .tbi last
  land.py hep        beat 3: the event files under incoming/hep/live-<ts>/ (stage/hep/landing/*)

Index objects land after their data: that is the completion marker the function keys on (CloudEvents can arrive out of
order across Kafka partitions; measured 20 Oct). Prints one line per object. Never prints a key.
"""
import glob, json, os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
import rlu  # noqa: E402
from env import E, P, BUCKET, ROOT, LIVE_VCF  # noqa: E402
from boto3.s3.transfer import TransferConfig  # noqa: E402

ST = os.path.join(ROOT, "stage")
VCF = "chr22.recalibrated_variants"
cfg = TransferConfig(multipart_threshold=64 << 20, multipart_chunksize=64 << 20, max_concurrency=32)
s3 = rlu.s3(f"{P}-ingest")
mode = sys.argv[1] if len(sys.argv) > 1 else "live"
ts = time.strftime("%H%M%S")

if mode == "baseline":
    base = "incoming/genomics/baseline/"
    plan = [(f"{ST}/meta/1000G_2504_high_coverage.sequence.index", base + "meta/1000G_2504_high_coverage.sequence.index"),
            (f"{ST}/ref/GRCh38_full_analysis_set_plus_decoy_hla.fa", base + "reference/GRCh38_full_analysis_set_plus_decoy_hla.fa"),
            (f"{ST}/ref/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai", base + "reference/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai"),
            (f"{ST}/meta/20130606_g1k_3202_samples_ped_population.txt", base + "meta/20130606_g1k_3202_samples_ped_population.txt"),
            (f"{ST}/vcf/{VCF}.annotated.vcf.gz", base + f"vcf/{VCF}.annotated.vcf.gz"),
            (f"{ST}/vcf/{VCF}.annotated.vcf.gz.tbi", base + f"vcf/{VCF}.annotated.vcf.gz.tbi"),  # fires variants
            (f"{ST}/vcf/{VCF}.vcf.gz", base + f"vcf/{VCF}.vcf.gz"),
            (f"{ST}/vcf/{VCF}.vcf.gz.tbi", base + f"vcf/{VCF}.vcf.gz.tbi")]          # last: fires genotypes
elif mode == "live":
    base = f"incoming/genomics/live-{ts}/"
    crams = sorted(glob.glob(f"{ST}/cram/*.chr22.cram"))
    plan = [(c, base + "cram/" + os.path.basename(c)) for c in crams]
    plan += [(p, base + "pod5/" + os.path.basename(p)) for p in sorted(glob.glob(f"{ST}/pod5/*.pod5"))]
    plan += [(c + ".crai", base + "cram/" + os.path.basename(c) + ".crai") for c in crams]
elif mode == "vcf":
    base = f"incoming/genomics/live-{ts}/vcf/"
    v = f"{ST}/vcf-slice/{LIVE_VCF['name']}"
    plan = [(f"{v}.annotated.vcf.gz", base + f"{LIVE_VCF['name']}.annotated.vcf.gz"),
            (f"{v}.annotated.vcf.gz.tbi", base + f"{LIVE_VCF['name']}.annotated.vcf.gz.tbi"),
            (f"{v}.vcf.gz", base + f"{LIVE_VCF['name']}.vcf.gz"),
            (f"{v}.vcf.gz.tbi", base + f"{LIVE_VCF['name']}.vcf.gz.tbi")]   # last: fires genotypes_live
elif mode == "hep":
    base = f"incoming/hep/live-{ts}/"
    plan = [(f, base + os.path.basename(f)) for f in sorted(glob.glob(f"{ST}/hep/landing/*"))]
else:
    sys.exit(__doc__)

t0, total = time.time(), 0
for src, key in plan:
    t = time.time()
    s3.upload_file(src, BUCKET, key, Config=cfg)
    n = os.path.getsize(src); total += n
    print(f"PUT s3://{BUCKET}/{key}  {n / 1e6:,.1f} MB  {time.time() - t:.1f}s  as {P}-ingest", flush=True)
dt = time.time() - t0
print(json.dumps({"landed": len(plan), "bytes": total, "seconds": round(dt, 1), "prefix": base}), flush=True)

# Follow the function: one line per CloudEvent it handles, until every object landed here has been seen.
if "--no-wait" not in sys.argv and mode != "baseline":
    log = os.path.join(E["RLU_SRV"], "run", "hep.log" if mode == "hep" else "genomics.log")
    print(f"\nwaiting for the DataEngine events to reach the function (log: {os.path.basename(log)}) ...", flush=True)
    seen, t1 = set(), time.time()
    while len(seen) < len(plan) and time.time() - t1 < 600:
        for line in open(log).read().splitlines():
            if not line.startswith("{") or base not in line: continue
            d = json.loads(line)
            k = d.get("path", "").split(base, 1)[-1]
            if k in seen: continue
            seen.add(k)
            if "skipped" in d:
                print(f"  +{time.time() - t0:4.0f}s event {len(seen):2d}/{len(plan)}  {k:34} (data object: waits for its index)", flush=True)
            else:
                what = d.get("handler") or "scored"
                n = d.get("rows", d.get("events"))
                took = d.get("seconds") or round(sum(d.get(x, 0) for x in ("rmm_s", "score_s", "write_s")), 1)
                print(f"  +{time.time() - t0:4.0f}s event {len(seen):2d}/{len(plan)}  {k:34} {what} -> {n:,} rows in {took}s  [{d.get('status')}]", flush=True)
        time.sleep(1)
    print(f"function handled {len(seen)}/{len(plan)} events, {time.time() - t0:.0f}s after the first PUT")
    sys.exit(0 if len(seen) == len(plan) else 1)
