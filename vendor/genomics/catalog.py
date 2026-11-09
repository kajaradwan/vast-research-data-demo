"""Run the research computing Catalog snippet (snippets/catalog_bytes_by_ext_by_uid.sql) as the read-only catalog identity.
Twice, to show the Catalog is scoped by the CALLER's tenant (measured 20 Oct 2026; source: s3_tabular.cpp
big_catalog_get_tenant_handle - with no tenant_id in the query the search starts at the caller's tenant root):
  - as rad-lab-rc-catalog (default tenant)   -> default-tenant files only (top 20)
  - as rle-analyst (rad-lab-enclave tenant)    -> only the enclave's own files
Writes results/catalog-<ts>.json.
"""
import json, os, re, sys, time
import trino
sql = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "snippets", "catalog_bytes_by_ext_by_uid.sql")).read()
sql = re.sub(r"--.*", "", sql).replace("vast.\"vast-big", "catalog.\"vast-big").strip().rstrip(";")
def run(q):
    cur = trino.dbapi.connect(host="127.0.0.1", port=8085, user="rad-lab-rc-catalog", catalog="catalog").cursor()
    t = time.time(); cur.execute(q); r = cur.fetchall(); return [d[0] for d in cur.description], r, time.time() - t
out = {}
cols, r, dt = run(sql.replace("LIMIT 500", "LIMIT 20")); out["cluster_top20"] = {"seconds": round(dt, 2), "rows": r}
print(f"cluster-wide, top 20 ({dt:.1f}s)"); print("\t".join(cols)); [print("\t".join(map(str, x))) for x in r]
def run_as_enclave(q):
    cur = trino.dbapi.connect(host="127.0.0.1", port=8085, user="rle-analyst", catalog="enclave").cursor()
    t = time.time(); cur.execute(q); r = cur.fetchall(); return [d[0] for d in cur.description], r, time.time() - t
cols, r, dt = run_as_enclave(sql.replace('catalog."vast-big', 'enclave."vast-big')); out["enclave"] = {"seconds": round(dt, 2), "rows": r}
print(f"\nsame SQL as rle-analyst, tenant rad-lab-enclave ({dt:.1f}s)"); print("\t".join(cols)); [print("\t".join(map(str, x))) for x in r]
cur = trino.dbapi.connect(host="127.0.0.1", port=8085, user="x", catalog="catalog").cursor()
cur.execute('SELECT max(mtime) FROM catalog."vast-big-catalog-bucket/vast_big_catalog_schema".vast_big_catalog_table'); out["newest_mtime_in_catalog"] = str(cur.fetchall()[0][0])
print("newest mtime in catalog:", out["newest_mtime_in_catalog"])
p = f"results/catalog-{time.strftime('%Y%m%d-%H%M%S')}.json"; os.makedirs("results", exist_ok=True)
json.dump(out, open(p, "w"), indent=1, default=str); print("wrote", p)
