-- ADFilter in SQL: the 100 most anomalous events in each m_jb bin, from the scores the trigger's function wrote.
-- m_jb = invariant mass of the leading light jet and leading b-jet (GeV), one of the masses the RMM leaves out so the
-- anomaly cut does not sculpt it. Events the autoencoder trained on are excluded. Labels (sample) are MC truth,
-- shown only to check the toy model: a real search would not have them.
WITH s AS (
  SELECT sample, event_idx, m_jb, score,
         CASE WHEN m_jb < 200 THEN '0-200' WHEN m_jb < 400 THEN '200-400' WHEN m_jb < 700 THEN '400-700'
              WHEN m_jb < 1500 THEN '700-1500' ELSE '1500+' END AS m_jb_bin,
         row_number() OVER (PARTITION BY
           CASE WHEN m_jb < 200 THEN 0 WHEN m_jb < 400 THEN 1 WHEN m_jb < 700 THEN 2 WHEN m_jb < 1500 THEN 3 ELSE 4 END
           ORDER BY score DESC) AS rank_in_bin
  FROM ${T}
  WHERE m_jb IS NOT NULL AND NOT in_training AND sample <> 'data_A'
)
SELECT m_jb_bin,
       count(*) FILTER (WHERE rank_in_bin <= 100) AS top_n,
       round(100e0 * count_if(rank_in_bin <= 100 AND sample LIKE 'ZPrime%') / nullif(count_if(rank_in_bin <= 100), 0), 1)
         AS pct_zprime_in_top100,
       round(100e0 * count_if(sample LIKE 'ZPrime%') / count(*), 1) AS pct_zprime_in_bin,
       count(*) AS events_in_bin,
       round(max(score) FILTER (WHERE rank_in_bin <= 100), 2) AS top_score
FROM s
GROUP BY m_jb_bin
ORDER BY min(m_jb)
