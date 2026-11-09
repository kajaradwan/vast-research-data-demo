# vast-research-data-demo

**A live, PowerPoint-style demo of VAST data services doing research-support work. Each slide that matters runs the
system.** The configuration slides load the real files; the beat slides run the real steps against a VAST cluster.

> **The one sentence.** Features already in a VAST release (Catalog, VAST DB, DataEngine, S3, snapshots, audit) can do
> research-support work for you. Here are working examples, on public data, under one set of access rules.

## What it shows

| Beat | Shown, live | What it proves |
|---|---|---|
| 0 | Six tools in the release; how to read the demo | Framing, no claims |
| 1 | VAST Catalog: who holds copies of the same corpora across four PI homes; then a read-only `/datasets` view with one curated copy | Duplicates found by size + extension; reads allowed, NFS writes refused for everyone |
| 2 | Folded site frequency spectrum by superpopulation, synonymous vs missense, over 1000 Genomes 30x chr22, run three ways (bcftools on NFS, Trino on Parquet, Trino on VAST DB) plus the consortium's own counts as a check | Same answer by digest. The table was written in place by the trigger's function. One identity policy governs NFS, S3 and VAST DB. **Not speed: Parquet was faster.** |
| 3 | A managed-platform-style landing: 8 CRAM + 2 POD5 dropped over S3 → DataEngine trigger → tables fill live | The landing pattern of a cloud genomics platform, on data that never leaves the cluster |
| 4 | Event scoring on landing: ATLAS Open Data event files land → RMM (1,287 inputs) → autoencoder score → `hep.event_scores` → "top 100 per m_jb bin" in SQL | The pipeline. The toy model's limits are on the slide |

**Say these out loud** (a slide says them too):
- The DataEngine **trigger, CloudEvents and broker are real**. The **function runs as a stand-in consumer**
  (`functions/runner.py`) that calls the same `handler(ctx, event)`, because no Kubernetes compute cluster is linked.
- It was built on a shared lab cluster on 5.5.0-sp6, using only capabilities present in 5.4.3. Trino 478 there; **475** is the 5.4 pairing.
- Every timing comes from one client on a shared cluster. None is a performance claim.
- Public, open-licence data only: 1000 Genomes 30x (open consent), ONT open data, ATLAS 13 TeV Open Data (CC0).
- The anomaly model in beat 4 is a toy autoencoder trained on ATLAS Open Data tt̄ MC, following the RMM input
  convention of Chekanov et al. (arXiv:2504.12418). It is not a published model and not a physics result.

Keys in the deck: ← → to move, **N** speaker notes, **F** full screen. The last slide is an operator slide with
Status, Reset and Teardown buttons.

## Build it from nothing

Prerequisites: a VAST cluster (5.4.3 or later) with VMS reachable from where you run `deploy.sh`, a free VIP range,
a Linux client on the data network (it runs the function stand-in, Trino and the deck), and the cluster admin login
in `~/.vast/vms-creds.json` under the key `RLU_CLUSTER`.

```bash
git clone https://github.com/kajaradwan/vast-research-data-demo.git && cd vast-research-data-demo
cp site.env.example .env    # edit: VMS address, VIP ranges, client, client data IPs, uid base
./preflight.sh              # read-only; fails loudly before anything is created
./deploy.sh                 # idempotent; ~75 min cold (public-data fetch ~45 GB), ~12 min when the data is staged
```

All addresses in this repo are documentation placeholders (`192.0.2.0/24`, `198.18.0.0/15`). Set yours in `.env`.
The parts of `vendor/genomics/` that are still hard-coded (`vms.py`, `rle.py`, `trino_up.sh`) need the same edit.

`deploy.sh` runs on a management host and drives the client over ssh for the data path.
`deploy.sh --reset` (or the deck's Reset button) returns to demo-start in **~25 s** (~45 s through the deck).
`deploy.sh --teardown` deletes everything and sweeps for orphans. `deploy.sh --status` shows what is standing.

| Step | Where | Script |
|---|---|---|
| Preflight (read-only): release, Trino pairing, VIP range free, names unused, DataEngine API, Catalog, client | mgmt | `mgmt/preflight.py` |
| Tenant, VIP pools, local users, keys, view policies (S3_NATIVE data, POSIX homes, read-only datasets), views, quotas, identity policies | mgmt | `mgmt/build.py` |
| The deck's VMS logins: read-only, tenant admin, delete-only | mgmt | `mgmt/logins.py` |
| Tools env (micromamba, user space), public data, CRAM slices, HEP data | client | `client/env.sh`, `fetch.sh`, `slice.sh`, `fetch_hep.sh`, `split_hep.py` |
| Per-uid keys, NFS mounts, Kafka topics, Trino | client | `client/bootstrap.sh` |
| DataEngine: enable, topics, two element triggers | mgmt | `mgmt/de_setup.py` |
| Function stand-ins as `rlu-svc` | client | `client/fn.sh` → `functions/runner.py` |
| Beat 2 tables through the trigger, then the Parquet copy | client | `client/baseline.py` |
| Beat 1 homes (each PI's uid over NFS) and the curated copy (curator over S3) | client | `beats/seed_homes.py` |
| Deck + panel collector (with a watchdog) | client | `./live.sh start` |

### The genomics kit

`vendor/genomics/` is an earlier, standalone genomics enclave demo, vendored here.
- **Used unchanged:** the transform handler (CRAM → `cram_qc`, POD5 → `nanopore_runs`, annotated VCF → `variants`), the CRAM sample list, and the Catalog query.
- **What this repo adds:** `.env`-driven endpoints, site attributes on the genotype rows, and the `unrelated` flag. `lib/rlu.py` registers itself as the kit's `rle` module, and `functions/landing/main.py` loads the kit's handler by path and overrides two functions.

## The deck

`live/server.py` (stdlib only) and `live/index.html` are served from the client on `RLU_LIVE_PORT`. `./live.sh start|stop|status|url`.
- **Access.** Every request needs the token in `.live.token`. The first visit swaps it for an HttpOnly cookie. Without it, every route returns 403.
- **Buttons.** Beat slides run **allowlisted steps only** (`STEPS` in `server.py`). The browser never sends a command, path or argument. Steps run one at a time and real stdout streams onto the slide. Data-network addresses are shown as `<addr>`.
- **Live panels.** `live/collect.py` polls every ~3–4 s, with least-privilege logins: VMS (read-only manager) for views, quotas and release; DataEngine (tenant admin) for triggers; Kafka watermarks for CloudEvents since the last reset; Trino as the analyst for row counts and the latest function-log rows. `client/collect_watch.sh` restarts the collector if its output goes stale.
- **Configuration slides** load the real files from disk (`FILES` in `server.py`).
- **No cluster admin login on the client.** `state/vms-logins.json` (mode 600) holds three logins: `-deck-ro` (view only), `-deck-tadmin` (TENANT_ADMIN of this tenant only), and `-deck-ops` (edit and delete, **no create**, used only by the Teardown button). **`-deck-ops` can delete outside the tenant**, so it exists only while the demo stands, and teardown deletes it last.

**Browser test.** `.venv/bin/python tests/clickthrough.py "<deck url>"` runs Chromium at 1920×1080, screenshots every
slide, clicks every button and waits for its exit status. It also checks that a tokenless request gets 403 and that no
data-network address or lab host name is visible on any slide.

## Traps this build paid for (5.5.0-sp6, Nov 2026)

- **The ENA index has trailing spaces.** In `1000G_2504_high_coverage.sequence.index`, `SAMPLE_NAME` carries a
  trailing space for HG02635, HG03025 and HG03366. Unstripped, AFR has 658 "unrelated" samples instead of 661. The
  consortium cross-check (`beats/sfs.py consortium`) found it. Keep the check.
- **The annotated VCF splits multiallelic records** into separate biallelic lines, so counts from its `AC_*_unrel`
  tags include sites a raw-biallelic SFS excludes. Restrict the check to the same sites.
- **`bcftools query -f` is the format string, not a filter.** Use `-i 'FILTER="PASS"'`. In `bcftools view`, `-f PASS` *is* the filter.
- **The DataEngine API refuses the admin's basic auth.** Use a bearer token from `/api/token/`. A tenant admin works with basic auth plus `X-Tenant-Name` (`lib/vms.py`).
- **`folders/create_folder` cannot resolve tenant-local users** (503). A new view's root is 0777, so each PI's uid creates its own home over NFS instead.
- **`sudo -u '#uid'` fails** for uids with no passwd entry. Use `sudo setpriv --reuid --regid --clear-groups`.
- **A module named `main.py` cannot `import main`** from the kit: it imports itself. Load the kit's handler by path under another name.
- **Drop a table only when its function can recreate it.** The function calls `ensure_tables` before inserting, so reset can drop tables while the stand-in runs.
- **Single-stream HTTPS from public S3 ran at ~2 MB/s.** `client/pget.py` fetches large files in 64 ranges (~70 MB/s).
- **The Trino CLI starts a JVM per call** (~2 s). The panels use Trino's HTTP API on loopback.
- **A read-only NFS export answers `Permission denied`**, not `Read-only file system`, even for the files' owner.
- **Catalog steps on a fresh build return 0 rows** until the homes are snapshotted (every 15 min). They exit 2 with an explanation. Seed the homes at least 15 min before showing it; `deploy.sh` does, and reset leaves the homes alone.

## Not proven here

- DataEngine **functions** on a linked Kubernetes cluster (stand-in consumer only).
- Anything on 5.4.3 itself. vastdb 2.1.0 against 5.4.3 is untested.
- Portability to a second cluster: preflight has been run on one lab cluster only.

Built by Brian Radwanski with Claude Code. Not an official VAST Data product.
