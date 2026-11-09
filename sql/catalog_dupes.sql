-- "Who has N copies of the same dataset?" from the VAST Catalog (a VAST DB table VAST refreshes from snapshots).
-- Same size and same extension under more than one home = the same corpus downloaded again; names often differ
-- (hg38.fa vs GRCh38_full_analysis_set_plus_decoy_hla.fa). Needs Tabular read on vast-big-catalog-bucket.
-- Rows are limited to the CALLER's tenant. On the target cluster: run as a default-tenant identity, and widen
-- the path filter to /home/ and /projects/.
WITH f AS (
  SELECT uid, owner_name, name, parent_path, size,
         coalesce(nullif(regexp_extract(lower(name),
             '\.([a-z0-9]{1,10}(\.(gz|bgz|bz2|xz|zst|zip))?)$', 1), ''), '(none)') AS ext
  FROM vast."vast-big-catalog-bucket/vast_big_catalog_schema".vast_big_catalog_table
  WHERE element_type = 'FILE' AND parent_path LIKE '${HOME}/%' AND size > 100000000
)
SELECT ext, size,
       count(*) AS copies,
       count(DISTINCT uid) AS owners,
       array_join(array_sort(array_distinct(array_agg(name))), ' | ') AS names_seen,
       round((count(*) - 1) * size / 1e9, 2) AS gb_redundant
FROM f
GROUP BY ext, size
HAVING count(*) > 1
ORDER BY gb_redundant DESC
