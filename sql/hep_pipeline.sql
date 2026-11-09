-- The pipeline, not the physics: what the trigger's function did with each event file that landed, from its own log,
-- next to what it wrote to the scores table. No truth labels and no grading: the model is a stand-in.
SELECT regexp_extract(f.object_key, '[^/]+$') AS file,
       f.principal AS landed_by,
       f.events_scored,
       count(s.event_idx) AS rows_written,
       round(f.rmm_s + f.score_s + f.write_s, 1) AS function_s,
       f.status
FROM ${L} f LEFT JOIN ${T} s ON s.object_key = f.object_key
GROUP BY f.object_key, f.principal, f.events_scored, f.rmm_s, f.score_s, f.write_s, f.status
ORDER BY 1
