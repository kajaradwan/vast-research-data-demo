#!/usr/bin/env python3
"""Cut ATLAS open-data 1lep ROOT files into landing-sized ROOT chunks (tree `mini`), keeping only the branches
functions/adfilter/rmm.py reads, plus `src_entry` (int64: the entry number in the source file, so a chunk's
events stay traceable to the published file).  Output is ROOT, written with uproot (each jagged branch gets an
`n<branch>` counter, as uproot writes them); rmm_from_file reads the chunks and the originals the same way.

  hepenv/bin/python client/split_hep.py stage/hep/mc_410000.ttbar_lep.1lep.root [--out stage/hep/chunks] [--mb 100]

Chunk names: <source stem>.partNNN.root, e.g. mc_410000.ttbar_lep.1lep.part007.root (sample label survives).
Idempotent: a chunk that already exists is not rewritten (delete the directory to re-split).
"""
import argparse
import os
import sys

import awkward as ak
import numpy as np
import uproot

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "functions", "adfilter"))
from rmm import BRANCHES  # noqa: E402


def split(src, outdir, target_mb):
    os.makedirs(outdir, exist_ok=True)
    stem = os.path.basename(src)[:-len(".root")]
    with uproot.open(src) as f:
        t = f["mini"]
        n = t.num_entries
        # compressed bytes per entry of the kept branches -> entries per chunk
        kept = sum(t[b].compressed_bytes for b in BRANCHES)
        per = max(1000, int(target_mb * 1e6 / (kept / n)))
        nchunks = (n + per - 1) // per
        print(f"{src}: {n} entries, kept branches {kept/1e6:.0f} MB compressed -> {nchunks} chunks of {per}")
        for i in range(nchunks):
            dst = os.path.join(outdir, f"{stem}.part{i:03d}.root")
            if os.path.exists(dst):
                continue
            a, b = i * per, min(n, (i + 1) * per)
            arr = t.arrays(BRANCHES, entry_start=a, entry_stop=b, library="ak")
            data = {k: arr[k] for k in BRANCHES}
            data["src_entry"] = np.arange(a, b, dtype=np.int64)
            tmp = dst + ".tmp"
            with uproot.recreate(tmp, compression=uproot.ZLIB(4)) as g:
                g.mktree("mini", {k: str(ak.type(v).content) for k, v in data.items()}).extend(data)
            os.replace(tmp, dst)
            print(f"  {dst}  entries {a}-{b - 1}  {os.path.getsize(dst)/1e6:.0f} MB")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("src", nargs="+")
    p.add_argument("--out", default=None, help="default: <src dir>/chunks")
    p.add_argument("--mb", type=float, default=100.0, help="target chunk size in MB (default 100)")
    a = p.parse_args()
    for s in a.src:
        split(s, a.out or os.path.join(os.path.dirname(s), "chunks"), a.mb)
