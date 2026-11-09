"""Beat 3's questions, in SQL over the scores the ADFilter function wrote (as the analyst, through Trino).

  hep_query.py pipeline sql/hep_pipeline.sql: per landed file, what the function scored and wrote (no labels)
  hep_query.py top      sql/top_anomalies.sql: the 100 most anomalous events per m_jb bin, and how many are Z'
  hep_query.py events   the 10 most anomalous events overall, with their kinematics
"""
import os, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "lib"))
from env import E, BUCKET, SCHEMA_H  # noqa: E402

T = f'vast."{BUCKET}/{SCHEMA_H}".event_scores'
L = f'vast."{BUCKET}/{SCHEMA_H}".function_log'
mode = sys.argv[1] if len(sys.argv) > 1 else "top"
if mode == "pipeline":
    q = open(os.path.join(ROOT, "sql", "hep_pipeline.sql")).read().replace("${T}", T).replace("${L}", L)
elif mode == "top":
    q = open(os.path.join(ROOT, "sql", "top_anomalies.sql")).read().replace("${T}", T)
else:
    q = (f"SELECT sample, event_idx, round(m_jb) AS m_jb_gev, n_jets, n_bjets, n_leptons, round(met_gev) AS met_gev, "
         f"round(score, 2) AS score FROM {T} WHERE NOT in_training ORDER BY score DESC LIMIT 10")
q = "\n".join(l for l in q.splitlines() if not l.strip().startswith("--"))
t = time.time()
r = subprocess.run(["docker", "exec", f"{E['RLU_TENANT']}-trino", "trino", "--output-format", "ALIGNED", "--execute", q],
                   capture_output=True, text=True)
print(r.stdout.rstrip() or r.stderr.strip().splitlines()[-1])
n = subprocess.run(["docker", "exec", f"{E['RLU_TENANT']}-trino", "trino", "--output-format", "TSV", "--execute",
                    f"SELECT count(*) FROM {T}"], capture_output=True, text=True).stdout.strip()
print(f"\n{int(n or 0):,} scored events · query {time.time() - t:.2f}s as the analyst" + (
      "" if mode == "pipeline" else " · labels are MC truth, shown only to check a toy model"))
sys.exit(r.returncode)
