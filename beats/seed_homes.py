"""Beat 5 setup: four PI home directories holding their own copies of the same public corpora, the way groups that
each download 1000 Genomes or ATLAS Open Data into their own home end up; plus the proposal, one curated copy in the
read-only /datasets view.

Each PI's files are written over NFS by that PI's uid, so the VAST Catalog records the real owner. Some copies are
renamed (hg38.fa, 1000G_chr22_annotated.vcf.gz), as people do, so the duplicate query matches on size and extension
rather than on name. The curated copy is written over S3 by the curator identity (rlu-svc): NFS clients cannot write
that view. Idempotent: skips files already complete. Names are generic (pi-alpha..delta); no real user appears.
"""
import os, subprocess, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
import rlu  # noqa: E402
from env import P, HOMES, ROOT, TENANT  # noqa: E402
from boto3.s3.transfer import TransferConfig  # noqa: E402

ST = os.path.join(ROOT, "stage")
REF = f"{ST}/ref/GRCh38_full_analysis_set_plus_decoy_hla.fa"
VCF = f"{ST}/vcf/chr22.recalibrated_variants.annotated.vcf.gz"
POD5 = f"{ST}/pod5/m6A_DRACH.pod5"
TTBAR = f"{ST}/hep/mc_410000.ttbar_lep.1lep.root"
PLAN = {
    "pi-alpha":   [(REF, "refs/GRCh38_full_analysis_set_plus_decoy_hla.fa"), (VCF, "1kg/chr22.annotated.vcf.gz"),
                   (POD5, "nanopore/m6A_DRACH.pod5")],
    "pi-bravo":   [(REF, "genome/hg38.fa"), (VCF, "data/1000G_chr22_annotated.vcf.gz")],
    "pi-charlie": [(REF, "ref/GRCh38_full_analysis_set_plus_decoy_hla.fa"), (TTBAR, "atlas-opendata/mc_410000.ttbar_lep.1lep.root"),
                   (POD5, "ont/m6A_DRACH.pod5")],
    "pi-delta":   [(REF, "GRCh38.fa"), (TTBAR, "atlas/ttbar_lep.1lep.root"), (VCF, "1kg/chr22.annotated.vcf.gz")],
}
CURATED = [(REF, "reference/GRCh38/GRCh38_full_analysis_set_plus_decoy_hla.fa"),
           (VCF, "1000genomes/30x/chr22.recalibrated_variants.annotated.vcf.gz"),
           (POD5, "ont-open-data/rna-modbase-validation/m6A_DRACH.pod5"),
           (TTBAR, "atlas-opendata/13TeV-2020/1lep/mc_410000.ttbar_lep.1lep.root")]


def as_uid(uid, cmd, stdin=None):
    return subprocess.run(["sudo", "-n", "setpriv", f"--reuid={uid}", f"--regid={uid}", "--clear-groups", "sh", "-c", cmd],
                          stdin=stdin, capture_output=True, text=True)


total = 0
for pi, files in PLAN.items():
    uid, home = HOMES[pi], f"{rlu.MNT_HOME}/{pi}"
    as_uid(uid, f"mkdir -p {home} && chmod 755 {home}")
    for src, rel in files:
        dst = f"{home}/{rel}"
        have = as_uid(uid, f"stat -c %s {dst} 2>/dev/null").stdout.strip()
        if have == str(os.path.getsize(src)):
            print(f"have   {pi:10} {rel}"); continue
        with open(src, "rb") as f:     # read as the deploy user, write as the PI (the PI cannot read our stage dir)
            r = as_uid(uid, f"mkdir -p $(dirname {dst}) && cat > {dst}", stdin=f)
        if r.returncode: raise SystemExit(f"{pi}: {r.stderr.strip()}")
        total += os.path.getsize(src)
        print(f"wrote  {pi:10} uid {uid}  {rel}  {os.path.getsize(src) / 1e9:.2f} GB", flush=True)

s3 = rlu.s3(f"{P}-svc")
cfg = TransferConfig(multipart_threshold=64 << 20, multipart_chunksize=64 << 20, max_concurrency=32)
bucket = f"{TENANT}-datasets"
have = {o["Key"]: o["Size"] for o in s3.list_objects_v2(Bucket=bucket).get("Contents", [])}
for src, key in CURATED:
    if have.get(key) == os.path.getsize(src):
        print(f"have   curated    {key}"); continue
    s3.upload_file(src, bucket, key, Config=cfg)
    print(f"put    curated    s3://{bucket}/{key}  {os.path.getsize(src) / 1e9:.2f} GB as {P}-svc", flush=True)
print(f"SEED_DONE wrote {total / 1e9:.1f} GB to homes")
