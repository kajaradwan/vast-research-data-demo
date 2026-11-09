#!/usr/bin/env python3
"""Sanity result for the toy AE: held-out SM ttbar vs Z'->ttbar, scored with the numpy-only score.py.

Writes models/adfilter/validation.json.  Held-out SM = ttbar entries with entry % 10 == 5 (never trained on).
Reported, all from these files and nothing else:
  * score quantiles per sample, AUC (SM vs each Z' mass), signal efficiency at the SM 99th / 99.9th percentile;
  * an unweighted "as-landed" mixture (held-out SM + every Z' event): signal fraction in the top 100 and top 1%,
    overall and per bin of the mass the demo bins in;
  * the same top-1% purity with cross-section weights at 10 fb^-1 (sigma * w / sum w from the ntuples), which is
    what the physics would actually give;
  * data_A (real 2015-16 data), if present: score quantiles only (data also holds W+jets, multijet, ... that the
    ttbar-only AE never saw, so a shift is expected and is not a signal).

  hepenv/bin/python models/adfilter/validate.py --dir stage/hep
"""
import argparse
import glob
import json
import os
import sys
import time

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "functions", "adfilter"))
sys.path.insert(0, HERE)
import rmm  # noqa: E402
import score as S  # noqa: E402
from train import load_split  # noqa: E402

LUMI_PB = 10064.0        # 2015-16 open-data luminosity, 10.064 fb^-1 (ATLAS open-data documentation)
BINS = [0.0, 1e-3, 200, 400, 700, 1000, 1500, 1e9]
BIN_NAMES = ["absent", "0-200", "200-400", "400-700", "700-1000", "1000-1500", ">1500"]


def auc(bkg, sig):
    allv = np.concatenate([bkg, sig])
    r = np.argsort(np.argsort(allv, kind="stable"), kind="stable") + 1.0
    rs = r[len(bkg):].sum()
    return float((rs - len(sig) * (len(sig) + 1) / 2) / (len(sig) * len(bkg)))


def q(x):
    return {k: round(float(v), 3) for k, v in zip(["p01", "p10", "p50", "p90", "p99", "p999"],
                                                   np.quantile(x, [0.01, 0.1, 0.5, 0.9, 0.99, 0.999]))} | \
           {"mean": round(float(x.mean()), 3), "n": int(len(x))}


def top_frac(scores, is_sig, k):
    if len(scores) == 0:
        return None
    k = min(k, len(scores))
    idx = np.argpartition(-scores, k - 1)[:k]
    return round(float(is_sig[idx].mean()), 4)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dir", default="stage/hep")
    p.add_argument("--n-sm", type=int, default=300_000)
    a = p.parse_args()
    ttbar = os.path.join(a.dir, "mc_410000.ttbar_lep.1lep.root")
    _, Xb, mb, _ = load_split(ttbar, 10, 5, a.n_sm)
    t0 = time.time(); sb = S.score(Xb[:100_000]); t_100k = time.time() - t0
    sb = S.score(Xb)
    out = {"model": json.load(open(os.path.join(HERE, "ae_meta.json")))["created"],
           "inference_100k_events_s": round(t_100k, 2),
           "mass_bin_var": rmm.MASS_BIN_VAR,
           "held_out_sm": {"file": os.path.basename(ttbar), "rule": "entry % 10 == 5", "score": q(sb)},
           "signals": {}}
    thr99, thr999 = np.quantile(sb, [0.99, 0.999])
    out["thresholds"] = {"sm_p99": round(float(thr99), 4), "sm_p999": round(float(thr999), 4)}
    cov = {"ttbar": {k: round(float((mb[k] > 0).mean()), 3) for k in ("m_jj", "m_jb", "m_bb")}}
    sig_s, sig_m, sig_w = [], [], []
    for f in sorted(glob.glob(os.path.join(a.dir, "mc_3013*.ZPrime*_tt.1lep.root")),
                    key=lambda s: int(s.split("ZPrime")[1].split("_")[0])):
        _, Xs, ms = rmm.rmm_from_file(f)
        ss = S.score(Xs)
        name = ms["sample"]
        out["signals"][name] = {"score": q(ss), "auc_vs_sm": round(auc(sb, ss), 4),
                                "eff_at_sm_p99": round(float((ss > thr99).mean()), 4),
                                "eff_at_sm_p999": round(float((ss > thr999).mean()), 4),
                                "xsec_pb_selected": float(ms["xsec_weight_pb"].sum())}
        cov[name] = {k: round(float((ms[k] > 0).mean()), 3) for k in ("m_jj", "m_jb", "m_bb")}
        sig_s.append(ss); sig_m.append(ms[rmm.MASS_BIN_VAR]); sig_w.append(ms["xsec_weight_pb"])
    out["mass_var_coverage"] = cov
    ss, ms_, ws = np.concatenate(sig_s), np.concatenate(sig_m), np.concatenate(sig_w)
    # unweighted as-landed mixture
    sc = np.concatenate([sb, ss]); lab = np.concatenate([np.zeros(len(sb), bool), np.ones(len(ss), bool)])
    mv = np.concatenate([mb[rmm.MASS_BIN_VAR], ms_])
    w = np.concatenate([mb["xsec_weight_pb"] * 10, ws]) * LUMI_PB    # SM held out is 1/10 of the file
    k1 = max(1, int(0.01 * len(sc)))
    mix = {"n_sm": int(len(sb)), "n_signal": int(len(ss)), "signal_share": round(float(lab.mean()), 4),
           "signal_frac_top100": top_frac(sc, lab, 100), "signal_frac_top1pct": top_frac(sc, lab, k1),
           "per_bin": {}}
    wmix = {"lumi_fb": LUMI_PB / 1000, "expected_sm": round(float(w[~lab].sum()), 1),
            "expected_signal_all_12_masses": round(float(w[lab].sum()), 1)}
    cut = np.quantile(sc, 0.99)
    sel = sc > cut
    wmix["weighted_signal_purity_top1pct"] = round(float(w[sel & lab].sum() / w[sel].sum()), 4)
    wmix["weighted_signal_purity_all"] = round(float(w[lab].sum() / w.sum()), 4)
    for lo, hi, nm in zip(BINS[:-1], BINS[1:], BIN_NAMES):
        inb = (mv >= lo) & (mv < hi) if nm != "absent" else (mv <= 0)
        if inb.sum() == 0:
            continue
        kb = max(1, int(0.01 * inb.sum()))
        mix["per_bin"][nm] = {"n_sm": int((inb & ~lab).sum()), "n_signal": int((inb & lab).sum()),
                              "signal_share": round(float(lab[inb].mean()), 4),
                              "signal_frac_top100": top_frac(sc[inb], lab[inb], 100),
                              "signal_frac_top1pct": top_frac(sc[inb], lab[inb], kb)}
    out["mixture_unweighted"] = mix
    out["mixture_xsec_weighted"] = wmix
    dfile = os.path.join(a.dir, "data_A.1lep.root")
    if os.path.exists(dfile):
        _, Xd, md = rmm.rmm_from_file(dfile)
        sd = S.score(Xd)
        out["data_A"] = {"score": q(sd), "frac_above_sm_p99": round(float((sd > thr99).mean()), 4)}
    json.dump(out, open(os.path.join(HERE, "validation.json"), "w"), indent=1)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
