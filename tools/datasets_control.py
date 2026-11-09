"""Allow-path control for beat 5's read-only claim (run on the Mac, 4 Nov 2026 skeptic finding).

The curator uid owns /datasets/reference/GRCh38 (mode 755), so mode bits would let it write: if its NFS write is
refused, the export is the gate. To prove it, open the export read-write for a moment, show the same write succeeds,
remove the file, and close it again. Leaves the policy exactly as it found it.
"""
import os, subprocess, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "lib"))
from env import E, TENANT, USERS, P, ids
from vms import client
v = client("build"); pid = ids()[f"vp:{TENANT}-datasets"]
before = {k: v.viewpolicies[pid].get()[k] for k in ("nfs_read_write", "nfs_read_only")}
uid = USERS[f"{P}-svc"]; f = f"{E['RLU_MNT']}/datasets/reference/GRCh38/.control"
def touch():
    r = subprocess.run(["ssh", E["RLU_CLIENT"], f"sudo -n setpriv --reuid={uid} --regid={uid} --clear-groups sh -c 'touch {f} && rm -f {f}'"],
                       capture_output=True, text=True)
    return "OK" if r.returncode == 0 else r.stderr.strip().split(": ")[-1]
print("export read-only :", before, "-> curator write:", touch())
try:
    v.viewpolicies[pid].patch(nfs_read_write=before["nfs_read_only"], nfs_read_only=[]); time.sleep(20)
    print("export read-write (control) -> curator write:", touch())
finally:
    v.viewpolicies[pid].patch(**before); time.sleep(20)
    print("export restored", {k: v.viewpolicies[pid].get()[k] for k in before}, "-> curator write:", touch())
