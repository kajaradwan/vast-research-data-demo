"""Drop and recreate genomics.samples, then land the pedigree again so the trigger's function rewrites it.
Used once on 4 Nov 2026 after fixing the trailing-space bug in the unrelated index; kept for the same repair."""
import os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
import rlu
from env import P, BUCKET, SCHEMA_G, ROOT
with rlu.db(f"{P}-svc").transaction() as tx:
    t = tx.bucket(BUCKET).schema(SCHEMA_G).table("samples", fail_if_missing=False)
    if t is not None: t.drop(); print("dropped samples")
key = "incoming/genomics/baseline/meta/20130606_g1k_3202_samples_ped_population.txt"
rlu.s3(f"{P}-ingest").upload_file(os.path.join(ROOT, "stage/meta/20130606_g1k_3202_samples_ped_population.txt"), BUCKET, key)
print("landed", key)
