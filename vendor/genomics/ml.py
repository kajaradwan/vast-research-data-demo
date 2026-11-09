"""ML hand-off (illustrative only): read the variant/genotype/sample tables into a dataframe and train a toy
classifier - predict a sample's 1000 Genomes superpopulation from chr22 genotype dosages. CPU, scikit-learn.

Reads as rle-analyst through Trino (catalog `enclave`). Writes results/ml-<ts>.json.
"""
import json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd, trino
from sklearn.decomposition import PCA
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.metrics import accuracy_score, confusion_matrix

VT = 'enclave."rad-lab-enclave-cohort/genomics"'
def df(sql):
    cur = trino.dbapi.connect(host="127.0.0.1", port=8085, user="rle-analyst", catalog="enclave").cursor()
    cur.execute(sql); return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])

t0 = time.time()
# common, PASS, biallelic-by-row SNVs thinned to every ~200th position -> a few thousand features
X_long = df(f"""SELECT g.sample_id, concat(cast(g.pos AS varchar), ':', g.ref, ':', g.alt) AS v, g.gt
               FROM {VT}.genotypes g JOIN {VT}.variants v ON g.pos = v.pos AND g.ref = v.ref AND g.alt = v.alt
               WHERE v.filter = 'PASS' AND v.af BETWEEN 0.05 AND 0.5 AND length(v.ref) = 1 AND length(v.alt) = 1
                 AND mod(v.pos, 200) = 0""")
samples = df(f"SELECT sample_id, population, superpopulation, sex FROM {VT}.samples")
qc = df(f"SELECT sample_id, chr22_mean_depth, mapped_pct, duplicate_pct, error_rate FROM {VT}.cram_qc ORDER BY sample_id")
t_read = time.time() - t0
X = X_long.pivot_table(index="sample_id", columns="v", values="gt", aggfunc="sum", fill_value=0)
X = X.reindex(samples.sample_id).fillna(0).astype(np.int8)          # samples with no carrier calls -> all-ref
y = samples.set_index("sample_id").loc[X.index, "superpopulation"].astype(str)
Xtr, Xte, ytr, yte = train_test_split(X.to_numpy(), y.to_numpy(dtype=object), test_size=0.25, random_state=7, stratify=y.to_numpy(dtype=object))
model = make_pipeline(PCA(n_components=20, random_state=7), LogisticRegression(max_iter=2000))
t1 = time.time(); model.fit(Xtr, ytr); t_fit = time.time() - t1
pred = model.predict(Xte); acc = accuracy_score(yte, pred)
labels = sorted(set(y))
cm = confusion_matrix(yte, pred, labels=labels)
res = {"rows_read": len(X_long), "samples": X.shape[0], "features": X.shape[1], "read_seconds": round(t_read, 1),
       "fit_seconds": round(t_fit, 2), "model": "PCA(20) + LogisticRegression", "test_accuracy": round(acc, 4),
       "labels": labels, "confusion": cm.tolist(), "cram_qc_frame": qc.round(3).to_dict(orient="records"),
       "caveat": "Illustrative: ancestry from common chr22 SNVs is an easy, well-known task. Not a clinical or research result."}
print(json.dumps({k: res[k] for k in ("rows_read", "samples", "features", "read_seconds", "fit_seconds", "test_accuracy")}))
print(pd.DataFrame(cm, index=labels, columns=labels))
print(qc.to_string(index=False))
out = f"results/ml-{time.strftime('%Y%m%d-%H%M%S')}.json"
os.makedirs("results", exist_ok=True); json.dump(res, open(out, "w"), indent=1); print("wrote", out)
