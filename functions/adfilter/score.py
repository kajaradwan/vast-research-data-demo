"""Anomaly score of RMM events with the exported autoencoder, numpy only.

score(X) = ln( mean_i (X_i - AE(X)_i)^2 )  per event: the log of the MSE reconstruction loss, as in ATLAS
PRL 132 (2024) 081801 (anomaly score = log of the AE loss; cuts at -9.1/-8.0/-6.5 for 10/1/0.1 pb) and ADFilter
(arXiv:2409.03065).  The log base is not printed in either paper; natural log is used here because the published
cut values only make sense on ln of the RMM MSE (ln: -9.1 -> 1.1e-4; log10 would put the cut at 8e-10, below every
event) and the reference code (github.com/chekanov/SHllbb) keeps the same -9.10 cut.  The cuts were set for the
ATLAS AE trained on data; they do NOT transfer to this toy model (see models/adfilter/validation.json).
"""
import json
import os

import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
_DEFAULT = os.environ.get("ADFILTER_MODEL_DIR",
                          os.path.join(_HERE, "..", "..", "models", "adfilter"))
_cache = {}


def load(model_dir=_DEFAULT):
    if model_dir not in _cache:
        w = np.load(os.path.join(model_dir, "ae_weights.npz"))
        meta = json.load(open(os.path.join(model_dir, "ae_meta.json")))
        layers = [(w[f"W{i}"].astype(np.float64), w[f"b{i}"].astype(np.float64))
                  for i in range(len(meta["layers"]))]
        _cache[model_dir] = (layers, float(meta["leaky_relu_alpha"]), meta)
    return _cache[model_dir]


def reconstruct(X, model_dir=_DEFAULT, batch=16384):
    """float64 forward pass (float32 weights promoted).  float64 is what makes the score reproducible to 1e-5:
    in float32 the MSE (~1e-7, a small difference of ~0.1-sized numbers) moves by ~3e-5 relative with BLAS
    summation order alone.  Cost: ~6.6 s per 1e5 events on one core, ~1.7 s multithreaded (client-n5)."""
    layers, alpha, _ = load(model_dir)
    X = np.ascontiguousarray(X, dtype=np.float64)
    out = np.empty_like(X)
    for s in range(0, len(X), batch):
        h = X[s:s + batch]
        for W, b in layers:                       # every layer, output included, is Dense + LeakyReLU
            h = h @ W + b
            h = np.where(h > 0, h, alpha * h)
        out[s:s + batch] = h
    return out


def score(X, model_dir=_DEFAULT):
    """X: (n, 1287) float RMM rows from rmm.rmm_from_file. Returns float32 (n,) ln(MSE)."""
    X = np.asarray(X, dtype=np.float64)
    if len(X) == 0:
        return np.zeros(0, np.float32)
    R = reconstruct(X, model_dir)
    mse = np.mean((X - R) ** 2, axis=1)
    return np.log(np.maximum(mse, 1e-30)).astype(np.float32)
