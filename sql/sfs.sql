-- Folded site frequency spectrum by superpopulation, synonymous vs missense, chr22, 1000 Genomes 30x (NYGC).
-- The input to DFE inference (fitdadi-style): one count of sites per minor-allele-count class.
--
-- Folded, because the VCF carries no ancestral allele (no AA tag; checked 4 Nov 2026).
-- Sites: PASS, biallelic SNVs, called in all 3,202 samples (AN = 6404: no missing genotype anywhere, 88% of PASS
--   biallelic SNVs), consequence from SnpEff (first ANN entry for the allele), carried on each genotype row by the
--   landing function, so there is no join on pos/ref/alt to the annotated table.
-- Samples: the 2,504 unrelated (the 698 related, including trio children, would count haplotypes twice).
-- ${G} / ${S} are the genotypes and samples tables: VAST DB, or the same rows as Parquet on the same view.
WITH n AS (
  SELECT superpopulation AS sp, 2 * count(*) AS n
  FROM ${S} WHERE unrelated GROUP BY superpopulation
), c AS (
  SELECT s.superpopulation AS sp, g.pos, g.alt, g.csq, sum(g.gt) AS ac
  FROM ${G} g JOIN ${S} s ON g.sample_id = s.sample_id
  WHERE s.unrelated AND g.filter = 'PASS' AND g.n_alt = 1 AND g.snv AND g.an = 6404
    AND g.csq IN ('synonymous', 'missense')
  GROUP BY s.superpopulation, g.pos, g.alt, g.csq
)
SELECT c.sp, c.csq, least(c.ac, n.n - c.ac) AS mac, count(*) AS sites
FROM c JOIN n ON c.sp = n.sp
WHERE least(c.ac, n.n - c.ac) > 0
GROUP BY c.sp, c.csq, least(c.ac, n.n - c.ac)
ORDER BY 1, 2, 3
