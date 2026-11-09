"""The third arm's data: the genotypes and samples tables written again as Parquet on the same view, then registered
in Trino's Hive catalog `lake`. This is the "convert the VCF to a columnar lake" answer a genomics team would reach for,
and the strongest realistic alternative (genomics kit, 21 Oct: Parquet matched or beat VAST DB on simple filters).

  s3://<bucket>/parquet/{genotypes,samples}/part-NNN.parquet   snappy, 1M-row row groups, ~8M rows/file, sorted by pos

`parquet/` is outside the trigger's incoming/ prefix, so writing it does not fire the pipeline. Runs as rlu-svc's key.
The export logic is the kit's (vendor/genomics/export_parquet.py); this copy is .env-driven and covers the site
attributes the SFS needs. Idempotent: replaces the prefix. Prints timings to results/parquet-<ts>.json.
"""
import json, os, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "lib"))
import rlu  # noqa: E402
from env import E, P, BUCKET, SCHEMA_G, S3_EP  # noqa: E402
import pyarrow as pa, pyarrow.compute as pc, pyarrow.parquet as pq  # noqa: E402
from pyarrow import fs  # noqa: E402

ROWS_PER_FILE, ROW_GROUP = 8_000_000, 1_000_000
k = rlu.key(f"{P}-svc")
s3fs = fs.S3FileSystem(access_key=k["access_key"], secret_key=k["secret_key"], scheme="http",
                       endpoint_override=S3_EP.split("://")[1], region="us-east-1")
s3 = rlu.s3(f"{P}-svc")
out = {}
ONLY = [t for t in sys.argv[1:] if t in ("samples", "genotypes")] if sys.argv[1:] else ["samples", "genotypes"]
# `export_parquet.py ddl` re-registers the existing files in the lake catalog without exporting again
for table in ONLY:
    t0 = time.time()
    prefix = f"parquet/{table}/"
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs: s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objs})
    with rlu.db(f"{P}-svc").transaction() as tx:
        tab = tx.bucket(BUCKET).schema(SCHEMA_G).table(table).select().read_all()
    if "pos" not in tab.column_names:
        pq.write_table(tab, f"{BUCKET}/{prefix}part-000.parquet", filesystem=s3fs, compression="snappy")
        out[table] = {"rows": tab.num_rows, "files": 1, "total_s": round(time.time() - t0, 1)}
        print(json.dumps({table: out[table]}), flush=True); continue
    # string -> large_string so take() across chunks cannot overflow 32-bit offsets (331M rows; kit, 21 Oct)
    tab = tab.cast(pa.schema([pa.field(f.name, pa.large_string() if f.type == pa.string() else f.type) for f in tab.schema]))
    order = pc.sort_indices(tab["pos"])
    n, files, nbytes = tab.num_rows, 0, 0
    for i in range(0, n, ROWS_PER_FILE):
        path = f"{BUCKET}/{prefix}part-{files:03d}.parquet"
        pq.write_table(tab.take(order.slice(i, ROWS_PER_FILE)), path, filesystem=s3fs, compression="snappy",
                       row_group_size=ROW_GROUP)
        nbytes += s3fs.get_file_info(path).size; files += 1
    out[table] = {"rows": n, "files": files, "bytes": nbytes, "total_s": round(time.time() - t0, 1)}
    print(json.dumps({table: out[table]}), flush=True)

DDL = [
    f"CREATE SCHEMA IF NOT EXISTS lake.genomics",
    "DROP TABLE IF EXISTS lake.genomics.genotypes", "DROP TABLE IF EXISTS lake.genomics.samples",
    f"""CREATE TABLE lake.genomics.genotypes (chrom varchar, pos bigint, ref varchar, alt varchar, filter varchar,
        af real, an integer, n_alt tinyint, snv boolean, csq varchar, sample_id varchar, gt tinyint, gq smallint,
        dp smallint) WITH (external_location = 's3://{BUCKET}/parquet/genotypes/', format = 'PARQUET')""",
    f"""CREATE TABLE lake.genomics.samples (sample_id varchar, family_id varchar, father_id varchar, mother_id varchar,
        sex tinyint, population varchar, superpopulation varchar, unrelated boolean)
        WITH (external_location = 's3://{BUCKET}/parquet/samples/', format = 'PARQUET')""",
    # table statistics, so the planner is not blind (the open question from the kit's Q3: a stats-less Hive planner)
    "ANALYZE lake.genomics.samples", "ANALYZE lake.genomics.genotypes",
]
t = time.time(); failed = 0
for q in DDL:
    r = subprocess.run(["docker", "exec", f"{E['RLU_TENANT']}-trino", "trino", "--execute", q], capture_output=True, text=True)
    print(("ok   " if r.returncode == 0 else "FAIL ") + q.split("(")[0].strip()[:80], r.stderr.strip()[-200:] if r.returncode else "")
    failed += r.returncode != 0
out["ddl_analyze_s"] = round(time.time() - t, 1)
os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
json.dump(out, open(os.path.join(ROOT, "results", f"parquet-{time.strftime('%Y%m%d-%H%M%S')}.json"), "w"), indent=1)
print("PARQUET_DONE" if not failed else f"PARQUET_DDL_FAILED ({failed})", json.dumps(out))
sys.exit(1 if failed else 0)
