"""Write the transform's tables a second time as Parquet files on the same VAST view, for the third query arm:
Trino's Hive connector reading Parquet over S3 (the "data lake on object storage" answer), between VAST DB and
bcftools on NFS.

  s3://rad-lab-enclave-cohort/parquet/genotypes/part-NNN.parquet   sorted by (chrom, pos), like the VAST DB table
  s3://rad-lab-enclave-cohort/parquet/variants/part-NNN.parquet, parquet/samples/part-000.parquet
Snappy, 1M-row row groups, ~8M rows per file. `parquet/` is outside the trigger's `incoming/` prefix, so this does
not fire the pipeline. Runs on the data client as rle-svc (the transform identity). Idempotent: replaces the prefix.
"""
import json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pyarrow as pa, pyarrow.compute as pc, pyarrow.parquet as pq
from pyarrow import fs
import rle

ROWS_PER_FILE, ROW_GROUP = 8_000_000, 1_000_000
k = rle.key("rle-svc")
s3fs = fs.S3FileSystem(access_key=k["access_key"], secret_key=k["secret_key"], scheme="http",
                       endpoint_override=rle.S3_EP.split("://")[1], region="us-east-1")
s3 = rle.s3("rle-svc")
out = {}
for table in ("samples", "variants", "genotypes"):
    t0 = time.time()
    prefix = f"parquet/{table}/"
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=rle.BUCKET, Prefix=prefix):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs: s3.delete_objects(Bucket=rle.BUCKET, Delete={"Objects": objs})
    with rle.db("rle-svc").transaction() as tx:
        tab = tx.bucket(rle.BUCKET).schema(rle.SCHEMA).table(table).select().read_all()
    t_read = time.time() - t0
    if "pos" not in tab.column_names:   # samples: small, no genomic order
        pq.write_table(tab, f"{rle.BUCKET}/{prefix}part-000.parquet", filesystem=s3fs, compression="snappy")
        out[table] = {"rows": tab.num_rows, "files": 1, "total_s": round(time.time() - t0, 1)}
        print(json.dumps({table: out[table]}), flush=True); continue
    # one chromosome, so (chrom, pos) order == pos order. Sort indices only and take per output file: sorting the
    # whole table concatenates string columns past Arrow's 2 GiB offset limit (331M rows)
    assert pc.count_distinct(tab["chrom"]).as_py() == 1
    # string -> large_string so take() across chunks cannot overflow 32-bit offsets; Parquet stores both the same way
    tab = tab.cast(pa.schema([pa.field(f.name, pa.large_string() if f.type == pa.string() else f.type) for f in tab.schema]))
    order = pc.sort_indices(tab["pos"])
    n, files, nbytes = tab.num_rows, 0, 0
    for i in range(0, n, ROWS_PER_FILE):
        path = f"{rle.BUCKET}/{prefix}part-{files:03d}.parquet"
        pq.write_table(tab.take(order.slice(i, ROWS_PER_FILE)), path, filesystem=s3fs, compression="snappy",
                       row_group_size=ROW_GROUP)
        nbytes += s3fs.get_file_info(path).size; files += 1
    out[table] = {"rows": n, "files": files, "bytes": nbytes, "read_s": round(t_read, 1), "total_s": round(time.time() - t0, 1)}
    print(json.dumps({table: out[table]}), flush=True)
os.makedirs("results", exist_ok=True)
json.dump(out, open(f"results/parquet-{time.strftime('%Y%m%d-%H%M%S')}.json", "w"), indent=1)
