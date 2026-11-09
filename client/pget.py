#!/usr/bin/env python3
"""Parallel ranged HTTPS download (stdlib only): pget.py URL DEST [STREAMS]. Resumable per part; verifies the size.

Public S3 over one HTTPS stream ran at ~2 MB/s from the lab client (4 Nov 2026), so the 27.9 GB VCF goes in parts.
"""
import os, subprocess, sys, urllib.request
from concurrent.futures import ThreadPoolExecutor

url, dest = sys.argv[1], sys.argv[2]
n = int(sys.argv[3]) if len(sys.argv) > 3 else 64
size = int(urllib.request.urlopen(urllib.request.Request(url, method="HEAD")).headers["Content-Length"])
if os.path.exists(dest) and os.path.getsize(dest) == size:
    print(f"have {dest} ({size} bytes)"); sys.exit(0)
parts = f"{dest}.parts"; os.makedirs(parts, exist_ok=True)
step = -(-size // n)

def part(i):
    lo, hi = i * step, min(size, (i + 1) * step) - 1
    p = os.path.join(parts, f"{i:04d}")
    for _ in range(8):
        have = os.path.getsize(p) if os.path.exists(p) else 0
        if have == hi - lo + 1: return
        if have > hi - lo + 1: os.remove(p); have = 0
        subprocess.run(["curl", "-sS", "-L", "--retry", "3", "-r", f"{lo + have}-{hi}", url], stdout=open(p, "ab"))
    raise SystemExit(f"part {i} incomplete")

with ThreadPoolExecutor(n) as ex:
    list(ex.map(part, range(n)))
with open(dest + ".tmp", "wb") as out:
    for i in range(n):
        with open(os.path.join(parts, f"{i:04d}"), "rb") as f:
            while True:
                b = f.read(1 << 24)
                if not b: break
                out.write(b)
assert os.path.getsize(dest + ".tmp") == size, "size mismatch"
os.replace(dest + ".tmp", dest)
subprocess.run(["rm", "-rf", parts])
print(f"ok {dest} {size} bytes")
