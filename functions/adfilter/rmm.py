"""Rapidity-mass matrix (RMM) from ATLAS Open Data 13 TeV 2020 "1lep" ntuples (tree `mini`, MeV units).

Definition followed (sources in models/adfilter/ae_meta.json["rmm"]["sources"]):
  * ATLAS, PRL 132 (2024) 081801, arXiv:2307.01612 ("Kinematic features"): up to 10 light jets, 10 b-jets,
    5 muons, 5 electrons, 5 photons + MET -> 36 objects, 36x36 matrix; the nine two-body masses that the search
    bins in (m_jj, m_jb, m_bb, m_je, m_jmu, m_jgamma, m_be, m_bmu, m_bgamma) are removed from the input,
    "36^2 - 9 = 1287".  Same wording in ADFilter, arXiv:2409.03065 Sect. 3.5, and arXiv:2504.12418 Sect. 2.3.
  * Cell contents (ADFilter Sect. 3.5; S.V. Chekanov, NIM A 931 (2019) 92, arXiv:1805.11650; reference code
    github.com/chekanov/SHllbb ana_truth/lib/src/projectevent.cxx + ana.cc):
      [0,0]            MET / sqrt(s)
      [0,k]            m_T(MET, object k) / sqrt(s)                              (MET row)
      [k,0]            C * (cosh(y_k) - 1)                                       (MET column)
      [k,k] leading    E_T(object) / sqrt(s)        of the leading object of its type
      [k,k] others     (E_T,k-1 - E_T,k) / (E_T,k-1 + E_T,k)   E_T imbalance w.r.t. the previous object of that type
      [i,j] i<j        m(i,j) / sqrt(s)                                          (upper triangle: masses)
      [i,j] i>j        C * (cosh((y_i - y_j)/2) - 1)                              (lower triangle: rapidity diffs)
    with C = 0.15 and sqrt(s) = 13 TeV; empty slots are 0.  Block order is MET, j, b, mu, e, gamma (as in the
    reference code's index layout; the papers' prose lists e before mu but never fixes the order).
  * Event selection, ATLAS PRL preselection as far as the open-data ntuple allows: >=1 tight, isolated lepton with
    pT > 60 GeV, single-electron or single-muon trigger fired.

Pure numpy for the matrix; uproot + awkward only to read the ROOT file.
"""
import os
import re

import numpy as np

SQRT_S = 13000.0           # GeV, ATLAS Run 2
C_ANG = 0.15               # lower-triangle weight (ADFilter Sect. 3.5; `angNorm` in the reference code)
TYPES = ("j", "b", "mu", "e", "gamma")
NMAX = (10, 10, 5, 5, 5)
OFFSET = tuple(1 + int(np.sum(NMAX[:i])) for i in range(len(NMAX)))   # j=1, b=11, mu=21, e=26, gamma=31
SIZE = 1 + int(np.sum(NMAX))                                          # 36
_slot = {t: o for t, o in zip(TYPES, OFFSET)}
# The nine masses excluded from the AE input (ATLAS PRL 132 081801): leading-object pairs, upper-triangle cells.
EXCLUDED_MASSES = {
    "m_jj": (_slot["j"], _slot["j"] + 1), "m_jb": (_slot["j"], _slot["b"]), "m_bb": (_slot["b"], _slot["b"] + 1),
    "m_je": (_slot["j"], _slot["e"]), "m_jmu": (_slot["j"], _slot["mu"]), "m_jgamma": (_slot["j"], _slot["gamma"]),
    "m_be": (_slot["b"], _slot["e"]), "m_bmu": (_slot["b"], _slot["mu"]), "m_bgamma": (_slot["b"], _slot["gamma"]),
}
_flat_excl = sorted(r * SIZE + c for r, c in EXCLUDED_MASSES.values())
KEEP = np.setdiff1d(np.arange(SIZE * SIZE), _flat_excl)               # 1287 flattened (row-major) cell indices
D = KEEP.size
MASS_BIN_VAR = "m_jb"   # the mass the demo bins in; see models/adfilter/validation.json for why

# object selection (ATLAS PRL preselection, mapped onto open-data branches)
JET_PT, JET_ETA = 30.0, 2.4
MV2C10_77 = 0.8244273      # MV2c10 77% b-tag working point used throughout the ATLAS 13 TeV open data
LEP_PT, LEAD_LEP_PT = 20.0, 60.0
PHO_PT, PHO_ETA = 20.0, 2.37
ISO = 0.15                 # ptcone30/pT and etcone20/pT, as in the ATLAS open-data examples

BRANCHES = ["runNumber", "eventNumber", "channelNumber", "mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE",
            "scaleFactor_MUON", "scaleFactor_PHOTON", "scaleFactor_BTAG", "scaleFactor_LepTRIGGER",
            "XSection", "SumWeights", "trigE", "trigM",
            "lep_pt", "lep_eta", "lep_phi", "lep_E", "lep_type", "lep_isTightID", "lep_ptcone30", "lep_etcone20",
            "met_et", "met_phi",
            "jet_pt", "jet_eta", "jet_phi", "jet_E", "jet_jvt", "jet_MV2c10",
            "photon_pt", "photon_eta", "photon_phi", "photon_E", "photon_isTightID", "photon_ptcone30",
            "photon_etcone20"]


def sample_from_path(path):
    """'mc_301325.ZPrime1000_tt.1lep[.partNNN].root' -> 'ZPrime1000_tt'; 'data_A.1lep.root' -> 'data_A'."""
    b = os.path.basename(path)
    m = re.match(r"mc_\d+\.([^.]+)\.", b)
    if m:
        return m.group(1)
    m = re.match(r"(data_[A-Z])\.", b)
    return m.group(1) if m else b.split(".")[0]


def _pad(ak, arr, mask, n, fill=0.0):
    """Jagged -> dense (events, n), keeping the first n selected entries (inputs are pT-ordered)."""
    sel = arr[mask]
    return ak.to_numpy(ak.fill_none(ak.pad_none(sel, n, clip=True), fill)).astype(np.float64)


def _objects(ak, a):
    """Selected objects per type as dense (events, nmax, 4) [px, py, pz, E] GeV arrays + counts (uncapped)."""
    out, counts = {}, {}
    gev = 1e-3
    jsel = (a.jet_pt * gev > JET_PT) & (abs(a.jet_eta) < JET_ETA) & \
           ((a.jet_pt * gev > 60) | (abs(a.jet_eta) > 2.4) | (a.jet_jvt > 0.59))
    isb = a.jet_MV2c10 > MV2C10_77
    lsel = (a.lep_pt * gev > LEP_PT) & a.lep_isTightID & (a.lep_ptcone30 / a.lep_pt < ISO) & \
           (a.lep_etcone20 / a.lep_pt < ISO)
    lsel_e = lsel & (a.lep_type == 11) & (abs(a.lep_eta) < 2.47)
    lsel_m = lsel & (a.lep_type == 13) & (abs(a.lep_eta) < 2.5)
    gsel = (a.photon_pt * gev > PHO_PT) & (abs(a.photon_eta) < PHO_ETA) & a.photon_isTightID & \
           (a.photon_ptcone30 / a.photon_pt < ISO) & (a.photon_etcone20 / a.photon_pt < ISO)
    spec = {"j": ("jet", jsel & ~isb), "b": ("jet", jsel & isb), "mu": ("lep", lsel_m), "e": ("lep", lsel_e),
            "gamma": ("photon", gsel)}
    for t, n in zip(TYPES, NMAX):
        p, m = spec[t]
        pt = _pad(ak, a[p + "_pt"] * gev, m, n)
        eta = _pad(ak, a[p + "_eta"], m, n)
        phi = _pad(ak, a[p + "_phi"], m, n)
        e = _pad(ak, a[p + "_E"] * gev, m, n)
        counts[t] = ak.to_numpy(ak.sum(m, axis=1)).astype(np.int64)
        v = np.stack([pt * np.cos(phi), pt * np.sin(phi), pt * np.sinh(eta), e], axis=-1)
        v[pt <= 0] = 0.0
        # re-sort by E_T within the type (inputs are pT-ordered; E_T order differs only for massive jets)
        et = _et(v)
        order = np.argsort(-et, axis=1, kind="stable")
        out[t] = np.take_along_axis(v, order[..., None], axis=1)
    return out, counts


def _et(v):
    p = np.sqrt(v[..., 0] ** 2 + v[..., 1] ** 2 + v[..., 2] ** 2)
    pt = np.hypot(v[..., 0], v[..., 1])
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(p > 0, v[..., 3] * pt / p, 0.0)          # TLorentzVector::Et()


def _rapidity(v):
    e, pz = v[..., 3], v[..., 2]
    with np.errstate(invalid="ignore", divide="ignore"):
        y = 0.5 * np.log((e + pz) / (e - pz))
    return np.where(e > 0, np.nan_to_num(y, nan=0.0, posinf=0.0, neginf=0.0), 0.0)


def rmm_matrix(objs, met, met_phi):
    """objs: dict type -> (n, nmax, 4); met, met_phi: (n,). Returns (n, 36, 36) float32."""
    n = met.shape[0]
    P = np.concatenate([objs[t] for t in TYPES], axis=1)           # (n, 35, 4)
    present = P[..., 3] > 0
    et = _et(P)
    y = _rapidity(P)
    R = np.zeros((n, SIZE, SIZE), dtype=np.float64)
    # MET cell, MET row (transverse mass), MET column (cosh(y)-1)
    R[:, 0, 0] = met / SQRT_S
    mx, my = met * np.cos(met_phi), met * np.sin(met_phi)
    mt2 = (et + met[:, None]) ** 2 - (P[..., 0] + mx[:, None]) ** 2 - (P[..., 1] + my[:, None]) ** 2
    mt = np.sqrt(np.clip(mt2, 0, None))
    R[:, 0, 1:] = np.where(present & (met[:, None] > 0), mt / SQRT_S, 0.0)
    R[:, 1:, 0] = np.where(present, C_ANG * (np.cosh(y) - 1.0), 0.0)
    # pairwise: invariant mass above the diagonal, rapidity difference below
    S = P[:, :, None, :] + P[:, None, :, :]
    m2 = S[..., 3] ** 2 - S[..., 0] ** 2 - S[..., 1] ** 2 - S[..., 2] ** 2
    both = present[:, :, None] & present[:, None, :]
    mass = np.where(both, np.sqrt(np.clip(m2, 0, None)) / SQRT_S, 0.0)
    dy = np.where(both, C_ANG * (np.cosh(0.5 * (y[:, :, None] - y[:, None, :])) - 1.0), 0.0)
    iu = np.triu_indices(SIZE - 1, 1)
    il = np.tril_indices(SIZE - 1, -1)
    R[:, 1 + iu[0], 1 + iu[1]] = mass[:, iu[0], iu[1]]
    R[:, 1 + il[0], 1 + il[1]] = dy[:, il[0], il[1]]
    # diagonal: leading E_T / sqrt(s), then E_T imbalance to the previous object of the same type
    for t, o, nm in zip(TYPES, OFFSET, NMAX):
        e_t = et[:, o - 1:o - 1 + nm]
        pr = present[:, o - 1:o - 1 + nm]
        R[:, o, o] = e_t[:, 0] / SQRT_S
        with np.errstate(invalid="ignore", divide="ignore"):
            imb = (e_t[:, :-1] - e_t[:, 1:]) / (e_t[:, :-1] + e_t[:, 1:])
        idx = np.arange(1, nm)
        R[:, o + idx, o + idx] = np.where(pr[:, 1:], np.nan_to_num(imb), 0.0)
    return R.astype(np.float32)


def _process(ak, a, path, start):
    gev = 1e-3
    objs, counts = _objects(ak, a)
    lsel = (a.lep_pt * gev > LEP_PT) & a.lep_isTightID & (a.lep_ptcone30 / a.lep_pt < ISO) & \
           (a.lep_etcone20 / a.lep_pt < ISO) & (((a.lep_type == 11) & (abs(a.lep_eta) < 2.47)) |
                                                 ((a.lep_type == 13) & (abs(a.lep_eta) < 2.5)))
    lead_lep = ak.to_numpy(ak.fill_none(ak.max(a.lep_pt[lsel] * gev, axis=1), 0.0))
    trig = ak.to_numpy(a.trigE | a.trigM)
    sel = (lead_lep > LEAD_LEP_PT) & trig
    met = ak.to_numpy(a.met_et).astype(np.float64) * gev
    met_phi = ak.to_numpy(a.met_phi).astype(np.float64)
    objs = {t: v[sel] for t, v in objs.items()}
    ns = int(sel.sum())
    X = np.zeros((ns, D), np.float32)
    masses = {k: np.zeros(ns, np.float32) for k in EXCLUDED_MASSES}
    B = 20000                                                     # bounds the (B, 35, 35, 4) pair tensor to ~0.8 GB
    for s in range(0, ns, B):
        R = rmm_matrix({t: v[s:s + B] for t, v in objs.items()}, met[sel][s:s + B], met_phi[sel][s:s + B])
        X[s:s + B] = R.reshape(R.shape[0], -1)[:, KEEP]
        for k, (r, c) in EXCLUDED_MASSES.items():
            masses[k][s:s + B] = R[:, r, c] * np.float32(SQRT_S)   # GeV, 0 = absent
    w = ak.to_numpy(a.mcWeight * a.scaleFactor_PILEUP * a.scaleFactor_ELE * a.scaleFactor_MUON *
                    a.scaleFactor_PHOTON * a.scaleFactor_BTAG * a.scaleFactor_LepTRIGGER).astype(np.float64)
    xs = ak.to_numpy(a.XSection).astype(np.float64)
    sw = ak.to_numpy(a.SumWeights).astype(np.float64)
    is_data = os.path.basename(path).startswith("data_")
    # per-event weight in pb (sigma * w / sum w): multiply by luminosity in pb^-1 for expected events
    xsw = np.where(is_data, 1.0, np.where(sw != 0, w * xs / np.where(sw != 0, sw, 1.0), 0.0))
    lead = {}
    for t in TYPES:
        v = objs[t][:, 0]
        lead[f"{t}1_pt"] = np.hypot(v[:, 0], v[:, 1]).astype(np.float32)
    # entry number in the published source file: `src_entry` when reading a split_hep.py chunk, else tree entry
    idx = ak.to_numpy(a.src_entry)[sel] if "src_entry" in a.fields else np.nonzero(sel)[0] + start
    meta = {
        "event_idx": idx.astype(np.int64),
        "run": ak.to_numpy(a.runNumber)[sel].astype(np.int64),
        "event": ak.to_numpy(a.eventNumber)[sel].astype(np.int64),
        "channel": ak.to_numpy(a.channelNumber)[sel].astype(np.int64),
        "n_jets": counts["j"][sel].astype(np.int16),
        "n_bjets": counts["b"][sel].astype(np.int16),
        "n_leptons": (counts["e"] + counts["mu"])[sel].astype(np.int16),
        "n_photons": counts["gamma"][sel].astype(np.int16),
        "met_gev": met[sel].astype(np.float32),
        "lead_lep_pt": lead_lep[sel].astype(np.float32),
        "xsec_weight_pb": xsw[sel].astype(np.float64),
        **lead, **masses,
    }
    return X, meta, int(sel.size)


def rmm_from_file(path, entry_start=None, entry_stop=None, step_size="200 MB"):
    """Read an ATLAS open-data 1lep ROOT file (or a chunk written by client/split_hep.py).

    Returns (event_ids, X, meta):
      event_ids  int64 (n,)   entry number in the published source file (chunks carry it as `src_entry`)
      X          float32 (n, 1287)  flattened RMM with the nine binned masses removed
      meta       dict of per-event numpy arrays + scalars: run, event, n_jets, n_bjets, n_leptons, n_photons,
                 met_gev, the nine masses m_jj ... m_bgamma in GeV (0 when an object is absent), leading-object
                 pTs, xsec_weight_pb; scalars sample, mass_bin_var, m_gev (= meta[mass_bin_var]), n_read, n_selected.
    """
    import awkward as ak
    import uproot

    Xs, metas, n_read = [], [], 0
    with uproot.open(path) as f:
        tree = f["mini"]
        start = entry_start or 0
        br = BRANCHES + (["src_entry"] if "src_entry" in tree.keys() else [])
        for a in tree.iterate(br, step_size=step_size, entry_start=entry_start, entry_stop=entry_stop,
                              library="ak"):
            X, m, n = _process(ak, a, path, start)
            start += n
            n_read += n
            Xs.append(X)
            metas.append(m)
    X = np.concatenate(Xs) if Xs else np.zeros((0, D), np.float32)
    meta = {k: np.concatenate([m[k] for m in metas]) for k in metas[0]} if metas else {}
    meta.update(sample=sample_from_path(path), mass_bin_var=MASS_BIN_VAR, n_read=n_read, n_selected=len(X))
    if metas:
        meta["m_gev"] = meta[MASS_BIN_VAR]
    return meta.get("event_idx", np.zeros(0, np.int64)), X, meta
