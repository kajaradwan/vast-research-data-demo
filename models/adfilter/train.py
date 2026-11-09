#!/usr/bin/env python3
"""Train the ADFilter-architecture autoencoder once, offline, on SM ttbar MC RMMs; export numpy weights.

Architecture (ATLAS PRL 132 (2024) 081801 / ADFilter arXiv:2409.03065 Sect. 3.6 / arXiv:2504.12418 Sect. 2.4):
  1287 -> 800 -> 400 -> 200 -> 400 -> 800 -> 1287, every layer Dense + LeakyReLU (output included), MSE loss.
Details taken from the authors' public training script (github.com/chekanov/SHllbb ana_truth/train/
arun_autoencoder.py, TensorFlow/Keras): tf.nn.leaky_relu (alpha = 0.2), Adam (Keras defaults, lr 1e-3),
L2 activity regulariser 0.5e-7 on the first hidden layer, batch 100, 7:3 train/validation split, early stopping on
validation loss with best weights restored, no input scaling (RMM cells are already in [0, 1]).
Trained here in PyTorch on CPU; exported to ae_weights.npz (W_i shaped (in, out): y = leaky(x @ W_i + b_i)).

Training sample: entries of mc_410000.ttbar_lep.1lep.root with entry % 10 == 0 (the reference code's "10 percent"
convention), after the RMM preselection.  Entries with entry % 10 == 5 are the held-out SM set for validate.py.

  hepenv/bin/python models/adfilter/train.py --bkg stage/hep/mc_410000.ttbar_lep.1lep.root
"""
import argparse
import datetime
import json
import os
import sys
import time
from multiprocessing import Pool

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
FUNC = os.path.join(HERE, "..", "..", "functions", "adfilter")
sys.path.insert(0, FUNC)
import rmm  # noqa: E402

ALPHA = 0.2          # tf.nn.leaky_relu default; PyTorch's default (0.01) would differ from the reference
SIZES = [rmm.D, 800, 400, 200, 400, 800, rmm.D]
ACT_L2 = 0.5e-7


def _load(args):
    path, a, b, mod, rem = args
    ids, X, m = rmm.rmm_from_file(path, entry_start=a, entry_stop=b)
    keep = (ids % mod) == rem
    return ids[keep], X[keep], {k: v[keep] for k, v in m.items() if isinstance(v, np.ndarray)}


def load_split(path, mod, rem, n_max, workers=32, block=200_000):
    """RMMs of entries with entry % mod == rem, read in parallel blocks until n_max events are selected."""
    import uproot
    n = uproot.open(path)["mini"].num_entries
    jobs = [(path, a, min(n, a + block), mod, rem) for a in range(0, n, block)]
    ids, Xs, metas, got = [], [], [], 0
    with Pool(workers) as pool:
        for i in range(0, len(jobs), workers):
            for r in pool.map(_load, jobs[i:i + workers]):
                ids.append(r[0]); Xs.append(r[1]); metas.append(r[2]); got += len(r[0])
            if got >= n_max:
                break
    ids = np.concatenate(ids)[:n_max]
    X = np.concatenate(Xs)[:n_max]
    meta = {k: np.concatenate([m[k] for m in metas])[:n_max] for k in metas[0]}
    return ids, X, meta, n


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bkg", required=True)
    p.add_argument("--n-train", type=int, default=300_000, help="events in the 7:3 train+validation pool")
    p.add_argument("--epochs", type=int, default=200)
    p.add_argument("--patience", type=int, default=10)
    p.add_argument("--minutes", type=float, default=12.0, help="wall-clock budget for the epoch loop")
    p.add_argument("--threads", type=int, default=48)
    p.add_argument("--batch", type=int, default=100)
    p.add_argument("--seed", type=int, default=101)
    a = p.parse_args()

    import torch
    torch.manual_seed(a.seed)
    torch.set_num_threads(a.threads)
    rng = np.random.default_rng(a.seed)

    t0 = time.time()
    ids, X, meta, n_entries = load_split(a.bkg, 10, 0, a.n_train)
    t_load = time.time() - t0
    print(f"loaded {len(X)} SM events (entry%10==0) from {n_entries} entries in {t_load:.0f}s", flush=True)
    perm = rng.permutation(len(X))
    n_val = int(0.3 * len(X))
    Xv, Xt = torch.from_numpy(X[perm[:n_val]]), torch.from_numpy(X[perm[n_val:]])

    layers = []
    for i in range(len(SIZES) - 1):
        layers += [torch.nn.Linear(SIZES[i], SIZES[i + 1]), torch.nn.LeakyReLU(ALPHA)]
    model = torch.nn.Sequential(*layers)
    # Keras Dense default init (glorot_uniform, zero bias), to stay close to the reference
    for m in model:
        if isinstance(m, torch.nn.Linear):
            torch.nn.init.xavier_uniform_(m.weight); torch.nn.init.zeros_(m.bias)
    opt = torch.optim.Adam(model.parameters(), lr=1e-3, betas=(0.9, 0.999), eps=1e-7)

    def val_loss():
        model.eval()
        with torch.no_grad():
            return float(sum(((model(Xv[s:s + 8192]) - Xv[s:s + 8192]) ** 2).mean(1).sum()
                             for s in range(0, len(Xv), 8192)) / len(Xv))

    best, best_state, bad, hist = np.inf, None, 0, []
    t1 = time.time()
    stop_reason = "max epochs"
    for ep in range(a.epochs):
        model.train()
        order = torch.randperm(len(Xt))
        tot = 0.0
        for s in range(0, len(Xt), a.batch):
            xb = Xt[order[s:s + a.batch]]
            h1 = model[1](model[0](xb))
            out = model[2:](h1)
            loss = ((out - xb) ** 2).mean() + ACT_L2 * (h1 ** 2).sum() / len(xb)
            opt.zero_grad(); loss.backward(); opt.step()
            tot += float(loss) * len(xb)
        vl = val_loss()
        hist.append({"epoch": ep + 1, "loss": tot / len(Xt), "val_loss": vl, "t_s": round(time.time() - t1, 1)})
        print(f"epoch {ep+1}  loss {tot/len(Xt):.4e}  val {vl:.4e}  {time.time()-t1:.0f}s", flush=True)
        if vl < best:
            best, bad = vl, 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            bad += 1
            if bad >= a.patience:
                stop_reason = f"early stopping (patience {a.patience})"
                break
        if (time.time() - t1) / 60 > a.minutes:
            stop_reason = f"wall-clock budget {a.minutes} min"
            break
    t_train = time.time() - t1
    model.load_state_dict(best_state)
    model.eval()

    lin = [m for m in model if isinstance(m, torch.nn.Linear)]
    np.savez(os.path.join(HERE, "ae_weights.npz"),
             **{f"W{i}": m.weight.detach().numpy().T.astype(np.float32) for i, m in enumerate(lin)},
             **{f"b{i}": m.bias.detach().numpy().astype(np.float32) for i, m in enumerate(lin)})

    sys.path.insert(0, FUNC)
    import score as S
    meta_json = {
        "created": datetime.datetime.now().isoformat(timespec="seconds"),
        "framework": f"torch {torch.__version__} (CPU, {a.threads} threads); inference numpy only",
        "layers": [{"in": SIZES[i], "out": SIZES[i + 1], "activation": "leaky_relu"} for i in range(len(SIZES) - 1)],
        "leaky_relu_alpha": ALPHA,
        "n_params": int(sum(m.weight.numel() + m.bias.numel() for m in lin)),
        "loss": "MSE over the 1287 RMM cells; + L2 activity 0.5e-7 on hidden layer 1 (training only)",
        "optimizer": "Adam lr=1e-3 beta=(0.9,0.999) eps=1e-7", "batch": a.batch, "seed": a.seed,
        "input_normalisation": "none: RMM cells are already scaled to [0,1] (masses, E_T, m_T by 1/sqrt(s), "
                               "rapidity terms by C=0.15)",
        "anomaly_score": "ln(MSE) per event (functions/adfilter/score.py)",
        "rmm": {"objects": dict(zip(rmm.TYPES, rmm.NMAX)), "met": 1, "matrix": f"{rmm.SIZE}x{rmm.SIZE}",
                "input_dim": rmm.D, "excluded_masses": list(rmm.EXCLUDED_MASSES),
                "sqrt_s_gev": rmm.SQRT_S, "C": rmm.C_ANG,
                "selection": ">=1 tight isolated e/mu pT>60 GeV, trigE|trigM; jets pT>30 |eta|<2.4 (JVT), "
                             "b-tag MV2c10>0.8244273 (77%), leptons pT>20, photons pT>20 |eta|<2.37 tight iso",
                "sources": ["ATLAS PRL 132 (2024) 081801, arXiv:2307.01612",
                            "ADFilter, arXiv:2409.03065 Sect. 3.5-3.6",
                            "Chekanov, Islam, Luongo, arXiv:2504.12418 Sect. 2.3-2.4",
                            "Chekanov, NIM A 931 (2019) 92, arXiv:1805.11650",
                            "github.com/chekanov/SHllbb (projectevent.cxx, arun_autoencoder.py)"]},
        "training_sample": {"file": os.path.basename(a.bkg),
                            "source": "ATLAS Open Data 13 TeV 2020, CERN record 15001, CC0",
                            "entries_in_file": int(n_entries), "rule": "entry % 10 == 0, after preselection",
                            "n_train": int(len(Xt)), "n_val": int(len(Xv)),
                            "load_s": round(t_load, 1)},
        "epochs_run": len(hist), "stop_reason": stop_reason, "train_s": round(t_train, 1),
        "best_val_loss": best, "final_train_loss": hist[-1]["loss"], "history": hist,
    }
    json.dump(meta_json, open(os.path.join(HERE, "ae_meta.json"), "w"), indent=1)
    check(model, Xv[:1000].numpy(), meta_json)
    print(f"train {t_train/60:.1f} min, {len(hist)} epochs, {stop_reason}")


def build_from_npz():
    import torch
    w = np.load(os.path.join(HERE, "ae_weights.npz"))
    layers = []
    for i in range(len(SIZES) - 1):
        lin = torch.nn.Linear(SIZES[i], SIZES[i + 1])
        lin.weight.data = torch.from_numpy(w[f"W{i}"].T.copy()); lin.bias.data = torch.from_numpy(w[f"b{i}"].copy())
        layers += [lin, torch.nn.LeakyReLU(ALPHA)]
    return torch.nn.Sequential(*layers).eval()


def check(model, xs, meta_json):
    """numpy score.py vs the training framework on the same events: torch float64 (same weights) is the
    like-for-like comparison; torch float32 (the training precision) is reported alongside."""
    import copy
    import torch
    import score as S
    S._cache.clear()
    sc_np = S.score(xs).astype(np.float64)
    res = {"n": int(len(xs))}
    for name, m, x in (("torch_float64", copy.deepcopy(model).double(), torch.from_numpy(xs).double()),
                       ("torch_float32", model.float(), torch.from_numpy(xs).float())):
        with torch.no_grad():
            mse = ((m(x) - x) ** 2).double().mean(1).numpy()
        d = np.abs(sc_np - np.log(mse))
        res[name] = {"max_abs_diff_score": float(d.max()),
                     "max_rel_diff_mse": float(np.max(np.abs(np.exp(sc_np) - mse) / mse)),
                     "pass_1e-5": bool(d.max() < 1e-5)}
    meta_json["numpy_vs_torch"] = res
    json.dump(meta_json, open(os.path.join(HERE, "ae_meta.json"), "w"), indent=1)
    print(json.dumps(res))


def check_only():
    """Re-run the numpy-vs-torch check on the saved weights (same 1,000 validation events as training)."""
    meta_json = json.load(open(os.path.join(HERE, "ae_meta.json")))
    tr = meta_json["training_sample"]
    _, X, _, _ = load_split(sys.argv[2], 10, 0, tr["n_train"] + tr["n_val"])
    perm = np.random.default_rng(meta_json["seed"]).permutation(len(X))
    check(build_from_npz(), X[perm[:tr["n_val"]]][:1000], meta_json)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--check-only"]:   # train.py --check-only <bkg.root>
        check_only()
    else:
        main()
