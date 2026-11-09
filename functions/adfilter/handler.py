"""rlu-adfilter: the DataEngine function behind trigger <prefix>-hep-landing (incoming/hep/).

An event file lands over S3 -> the trigger's CloudEvent -> handler(ctx, event): read the file over NFS from the same
view, build each selected event's rapidity-mass matrix (rmm.py: 36x36 minus the nine binned masses = 1,287 inputs, as
in ATLAS PRL 132 (2024) 081801 and ADFilter), score it with an autoencoder in the ADFilter architecture
(score.py, numpy only, weights trained once offline: models/adfilter/), and write one row per event to
hep.event_scores. The question then is SQL: sql/top_anomalies.sql.

The model is a toy trained on ATLAS 13 TeV Open Data ttbar MC (CC0), not the ATLAS model: its scores and cuts are not
ATLAS's. Runs as rlu-svc through functions/runner.py, the stand-in for the DE runtime.
"""
import json, os, sys, time, traceback
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "..", "lib"))
import numpy as np  # noqa: E402
import pyarrow as pa  # noqa: E402
import rlu  # noqa: E402
from env import BUCKET, SCHEMA_H  # noqa: E402
import rmm, score  # noqa: E402

TRAIN_SAMPLE, TRAIN_MOD = "ttbar_lep", 10      # the AE trained on ttbar entries with entry % 10 == 0 (models/adfilter)
SCHEMAS = {
    "event_scores": pa.schema([
        ("object_key", pa.string()), ("sample", pa.string()), ("event_idx", pa.int64()), ("run", pa.int64()),
        ("event", pa.int64()), ("m_jb", pa.float32()), ("m_jj", pa.float32()), ("m_bb", pa.float32()),
        ("n_jets", pa.int16()), ("n_bjets", pa.int16()), ("n_leptons", pa.int16()), ("n_photons", pa.int16()),
        ("met_gev", pa.float32()), ("lead_lep_pt", pa.float32()), ("xsec_weight_pb", pa.float64()),
        ("in_training", pa.bool_()), ("score", pa.float32()), ("processed_at", pa.timestamp("s"))]),
    "function_log": pa.schema([
        ("event_id", pa.string()), ("object_key", pa.string()), ("principal", pa.string()), ("status", pa.string()),
        ("events_read", pa.int64()), ("events_scored", pa.int64()), ("rmm_s", pa.float32()), ("score_s", pa.float32()),
        ("write_s", pa.float32()), ("detail", pa.string()), ("processed_at", pa.timestamp("s"))]),
}


def ensure_tables(session):
    with session.transaction() as tx:
        b = tx.bucket(BUCKET)
        sc = b.schema(SCHEMA_H, fail_if_missing=False) or b.create_schema(SCHEMA_H)
        for name, schema in SCHEMAS.items():
            if sc.table(name, fail_if_missing=False) is None:
                sc.create_table(name, schema, sorting_key=["sample", "m_jb"] if name == "event_scores" else [])


def _insert(session, table, batch):
    with session.transaction() as tx:
        tx.bucket(BUCKET).schema(SCHEMA_H).table(table).insert(batch)


def handler(ctx, event):
    body = event["data"] if isinstance(event.get("data"), dict) else json.loads(event["data"])
    rec = body["Records"][0]
    key = rec["s3"]["object"]["key"]
    if not key.endswith(".root"):
        return {"skipped": key}
    session = ctx["session"]
    ensure_tables(session)                      # recreate after a reset dropped them
    t0 = time.time(); status, detail, n_read, n = "ok", "", 0, 0; t_rmm = t_sc = t_w = 0.0
    try:
        ids, X, meta = rmm.rmm_from_file(os.path.join(rlu.MNT, key))
        t_rmm = time.time() - t0; n_read = meta.get("n_read", 0); n = len(X)
        t = time.time(); s = score.score(X) if n else np.zeros(0); t_sc = time.time() - t
        t = time.time()
        sample = meta["sample"]
        cols = {"object_key": [key] * n, "sample": [sample] * n, "event_idx": ids, "run": meta.get("run"),
                "event": meta.get("event"), "m_jb": meta.get("m_jb"), "m_jj": meta.get("m_jj"), "m_bb": meta.get("m_bb"),
                "n_jets": meta.get("n_jets"), "n_bjets": meta.get("n_bjets"), "n_leptons": meta.get("n_leptons"),
                "n_photons": meta.get("n_photons"), "met_gev": meta.get("met_gev"), "lead_lep_pt": meta.get("lead_lep_pt"),
                "xsec_weight_pb": meta.get("xsec_weight_pb"),
                "in_training": (ids % TRAIN_MOD == 0) & (sample == TRAIN_SAMPLE) if n else np.zeros(0, bool),
                "score": s.astype(np.float32), "processed_at": np.full(n, int(time.time()), "datetime64[s]")}
        for m in ("m_jb", "m_jj", "m_bb"):          # 0 GeV = the object pair is absent: store NULL, not 0
            if cols[m] is not None:
                v = np.asarray(cols[m], np.float32); cols[m] = pa.array(v, mask=v <= 0)
        tbl = pa.table({k: (cols[k] if cols[k] is not None else pa.nulls(n)) for k in SCHEMAS["event_scores"].names},
                       schema=SCHEMAS["event_scores"])
        for b in tbl.to_batches(max_chunksize=200_000):
            _insert(session, "event_scores", b)
        t_w = time.time() - t
    except Exception as e:
        status, detail = "error", f"{type(e).__name__}: {e}"[:500]
        traceback.print_exc()
    _insert(session, "function_log", pa.RecordBatch.from_pylist([{
        "event_id": event.get("id"), "object_key": key, "principal": rec.get("userIdentity", {}).get("principalId"),
        "status": status, "events_read": int(n_read), "events_scored": int(n), "rmm_s": t_rmm, "score_s": t_sc,
        "write_s": t_w, "detail": detail, "processed_at": int(time.time())}], schema=SCHEMAS["function_log"]))
    return {"object": key, "status": status, "events": int(n), "rmm_s": round(t_rmm, 1), "score_s": round(t_sc, 1),
            "write_s": round(t_w, 1)}
