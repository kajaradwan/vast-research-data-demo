-- VAST Catalog: file count and bytes by extension, per owner uid.
-- For a research-computing team to run on your cluster (Trino with the VAST connector, or VAST's query engine).
-- Read-only: the Catalog is a VAST DB table that VAST refreshes from filesystem snapshots (every 30 min).
-- Needs an identity with Tabular read on vast-big-catalog-bucket (s3:TabularQueryData etc.); superuser alone is not enough.
-- SCOPE: rows are limited to the CALLER's tenant (no tenant_id predicate -> the search starts at the caller's tenant
-- root). Run it as a default-tenant identity for /home and /projects; a tenant identity sees only its own tree.
-- Replace `vast` with your Trino catalog name. Tested 20 Oct 2026 on VAST 5.5.0-sp6 + trino-vast 478.
--
-- Compound extensions are kept whole (vcf.gz, vcf.gz.tbi, fastq.gz, cram.crai, nii.gz, tar.gz) because genomics and imaging
-- formats hide behind .gz. Uncomment the uid / path filters to scope it (e.g. the top-32 uids, /projects only).
WITH f AS (
  SELECT uid,
         owner_name,
         size,
         coalesce(nullif(regexp_extract(lower(name),
             '\.([a-z0-9]{1,10}(\.(gz|bgz|bz2|xz|zst|zip))?(\.(tbi|csi|crai|bai|idx|md5))?)$', 1), ''), '(none)') AS ext
  FROM vast."vast-big-catalog-bucket/vast_big_catalog_schema".vast_big_catalog_table
  WHERE element_type = 'FILE'
    -- AND uid IN (11203, 11350, 1114, 10001)
    -- AND (parent_path LIKE '/projects/%' OR parent_path LIKE '/home/%')
)
SELECT uid,
       max(owner_name)                      AS owner_name,
       ext,
       count(*)                             AS files,
       sum(size)                            AS bytes,
       round(sum(size) / 1e12, 3)           AS tb,
       round(avg(size) / 1048576.0, 2)      AS avg_mib
FROM f
GROUP BY uid, ext
ORDER BY bytes DESC
LIMIT 500;
