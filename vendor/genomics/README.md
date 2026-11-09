# genomics-kit

**A genomics pipeline inside a VAST "secure enclave" tenant: ingest, transform, query, audit and ML, end to end, with every control shown working.**

Built Oct 2026 on a VAST lab cluster (`5.5.0-sp6`, 3 CNodes + 1 DBox). It reproduces a common research data pattern for genomics heavy users: large-file sequential scans with very little metadata. It also serves the HIPAA secure-enclave conversation by showing the controls, refusals and audit trail behind an enclave. **The controls are described, not certified. Nothing here is a HIPAA compliance claim.** Only open-consent public data is used.

---

## What it shows, in one run

```
instrument / ingest service ──S3 PUT (rle-ingest)──▶ <bucket>/incoming/      one view: NFS + S3 + DATABASE
                                                        │ index object lands last = completion marker
            Data Engine element trigger (prefix incoming/) ──▶ CloudEvent on VAST's internal Kafka
                                                        ▼
   transform  handler(ctx, event)  as uid 7101 (rle-svc), reads the same file over NFS
      VCF  → genomics.variants + genomics.genotypes (sorted chrom,pos)    CRAM → samtools → genomics.cram_qc
      POD5 → genomics.nanopore_runs     pedigree → genomics.samples     every event → genomics.ingest_log
                                                        ▼
   analyst: Trino + vastdb SDK on VAST DB  vs  bcftools on NFS  (same question, same files, answers compared)
   auditor: protocol audit table in VAST DB ("who read sample X?")    locked snapshot of files + tables
   research computing: VAST Catalog snippet (bytes by extension per uid)             pandas → scikit-learn (toy classifier)
```

Measured on the lab cluster, 20 Oct 2026. It ran clean twice from an empty cohort. The final run's outputs are in `evidence/2026-10-20/`.

| | Result |
| --- | --- |
| Data | 1000 Genomes 30x chr22: the joint-called VCF (3,202 samples, 27.9 GB), its annotated sites VCF, 8 CRAMs sliced to chr22, and 2 ONT RNA004 POD5 files. 26 objects, 33.9 GB |
| Ingest | 33.9 GB over S3 from one client in 42 s |
| Transform | 2,092,942 `variants` rows and **331,684,187** `genotypes` rows in about 50 s (120 workers), plus 8 CRAM QC rows and 2 nanopore runs |
| Q1 rare carriers in the CHEK2 promoter | VAST DB 0.06 s · bcftools 0.24 s (parity) · same 664 samples |
| Q2 allele counts by population, 1 Mb | 0.35 s · 20.7 s · identical counts |
| Q3 rare, non-coding, CADD ≥ 15, whole cohort | 0.3 s · 19.7 s · identical |
| Q4 everything NA12878 carries (HIGH/MODERATE) | 1.1 s · **680 s** · same 314 variants |
| Audit | "Who read NA12878's CRAM" answered in 0.1 s; a new read appears about 20 s after it happens. Denied attempts are recorded too |
| Controls | 16 of 16 pass, each beside an allow-path control (see below) |
| ML (illustrative) | 3,202 samples × 496 SNVs → PCA + logistic regression gives 95.8% superpopulation accuracy |

The timings come from a shared lab cluster and one client. **They are not a performance claim.**

### Controls exercised (each deny has an allow-path control in the same run)

- **Tenant isolation.** The tenant has its own VIP pool and its own local identity provider.
- **Server-side NFS host allow-list.** A mount from an unlisted host returns `access denied by server`.
- **Root squash.** Root on a client gets `Permission denied`.
- **S3-native view policy presented over NFS.** An identity policy decides NFS access, not mode bits. The file's own uid is refused (`Operation not permitted`) and the analyst uid can read it.
- **Least privilege per role.** Ingest can land files but cannot read them. The analyst can read but not write or delete. The auditor can read the audit table but not the cohort. The same policies govern the VAST DB (Tabular) API.
- **Protocol audit into VAST DB.** It is readable only by an identity with an explicit grant.
- **Locked snapshot of files and tables together.** A delete is refused while it's locked, and the tables can be read as of the snapshot.
- **Tenant-scoped VAST Catalog.** The same SQL returns only the caller's tenant.

---

## Layout

| Path | What it is | Runs on |
| --- | --- | --- |
| `vms.py` | VMS client; credentials are read from `~/.vast/vms-creds.json` and never printed | Mac |
| `build.py` | Creates the tenant, VIP pools, local users, S3 keys, view policy (S3_NATIVE, root squash, NFS allow-list, audit), views and identity policies. Idempotent; records ids in `ids.json` | Mac |
| `build_auditor.py` | `rle-auditor`: Tabular read on the audit table only, with an explicit Deny on the cohort | Mac |
| `build_catalog_reader.py` | A default-tenant identity with Catalog read only (stands in for research computing) | Mac |
| `de_setup.py` | Enables Data Engine on the tenant (`POST /api/dataengine/data-engine`) and creates the element trigger | Mac |
| `functions/transform/main.py` | **The Data Engine function**: `handler(ctx, event)`, which routes VCF, CRAM, POD5 and pedigree files to VAST DB | data client |
| `functions/transform/runner.py` | Stands in for the DE runtime: consumes the trigger's CloudEvents from Kafka and calls `handler()` | data client, as uid 7101 |
| `rle.py` | Data-path helpers: endpoints, per-identity S3/VAST DB clients, key loading | data client |
| `fetch.sh`, `slice.sh` | Stage the public data (anonymous `s3://1000genomes`, `s3://ont-open-data`) and slice CRAMs to chr22 over HTTPS | data client |
| `ingest.py` | Lands the staged files over S3 as `rle-ingest`, data first and index last | data client |
| `controls.py` | The control matrix, with allow-path controls | data client |
| `query.py` | Q1–Q4 on VAST DB (Trino and the SDK) against bcftools on NFS; answers compared by digest | data client |
| `audit.py` | Reads a sample's CRAM as the analyst, waits for the audit trail, and answers "who read it" | data client |
| `snapshot.py` | Takes a locked snapshot of the cohort view and shows that a delete is refused | Mac |
| `catalog.py`, `snippets/catalog_bytes_by_ext_by_uid.sql` | **The Catalog query for research computing**, run as two callers to show tenant scoping | data client |
| `ml.py` | VAST DB → pandas → scikit-learn (toy) | data client |
| `trino_up.sh` | Trino (`vastdataorg/trino-vast:478`) on the data client with three catalogs: `enclave` (analyst), `audit` (auditor), `catalog` (research computing reader) | data client |
| `demo.py` | **One script, end to end**: preflight → reset → ingest + transform → controls → query → audit → snapshot → catalog → ML. It writes `results/run-<ts>.{json,log}` | Mac (drives the client over ssh) |
| `app/server.py`, `app/index.html` | **The dashboard**, on 127.0.0.1:8095. `RLE_LOCAL=1` runs every step locally | data client (or Mac) |
| `dash_up.sh` | Starts the dashboard on the client with the least-privilege VMS logins. Not a service, so it stops on reboot | data client |
| `build_dash_manager.py` | The dashboard's VMS logins: a read-only manager plus a tenant-confined `TENANT_ADMIN` → `.vms-dash.json` (600, gitignored) | Mac |
| `bootstrap_client.sh` | One-shot client setup: micromamba envs, `/srv/rad-lab-enclave` (code + per-uid keys), NFS mount, Kafka topics | data client |
| `state.py` | The dashboard's data-plane probes (tables, objects, Kafka, audit, the live identity matrix, the live cohort query) | data client |
| `teardown.py` | Removes everything in dependency order. **Dry run by default**; `--yes` executes | Mac (+ ssh) |
| `tools/probe_lab.py`, `tools/probe_de.py` | Read-only checks of what the cluster can host | Mac |
| `ids.json` | Object ids from the 20 Oct 2026 build. **Delete it before a fresh build on another cluster** | — |
| `evidence/2026-10-20/` | Outputs of the final clean run | — |

**Why two machines.** The VAST data network (`198.18.0.0/16`) is not routed to the Mac, but the management network is. So VMS calls go from the Mac, and everything on the data path (S3, NFS, VAST DB, Kafka, Trino) runs on a client inside the data network: **client-n5** (`vastdata@192.0.2.111`, 128 threads), reached over ssh.

---

## Show it

The dashboard runs **on the data client (client-n5), beside Trino**, bound to loopback like Trino. From the Mac, open a
tunnel:

```bash
ssh -N -L 8095:127.0.0.1:8095 vastdata@192.0.2.111
```

Then open http://localhost:8095. To (re)start the dashboard on the client:

```bash
ssh vastdata@192.0.2.111 'cd ~/genomics-kit && ./dash_up.sh'
```

**On the client it never holds the cluster admin login.** `build_dash_manager.py` creates two VMS logins, and `dash_up.sh`
passes their mode-600 file (`.vms-dash.json`) in `RLE_VMS_CREDS`:

- **`rad-lab-enclave-dash`**: a read-only role on every realm, for the state panels.
- **`rad-lab-enclave-tadmin`**: a `TENANT_ADMIN` confined to the enclave tenant, for the Snapshot button and the Data Engine API.

Both were tested with negative controls. Before the tenant-scoped design, a cluster-wide role with `create_logical` could
create a view in the default tenant; that is why the write rights now sit on the tenant admin.

The dashboard can still run on the Mac (`.venv/bin/python app/server.py`, without `RLE_LOCAL`), driving the client over ssh.

The page has:
- live tenant state;
- a **Run** button per stage, with streaming output;
- a **"try it as ingest / analyst / auditor"** matrix, where each cell sends a real request;
- a live **"ask the cohort"** query by gene;
- **"read this sample's CRAM, then audit it"**;
- **Open in VAST** links into the VMS GUI and the Data Engine UI.

Say these out loud when you demo:
- The Data Engine **trigger, CloudEvents and broker are real**. The **function runs on the data client**, because no Kubernetes compute cluster is linked to the tenant.
- **Per-tenant encryption** isn't shown, because cluster encryption is off on the lab cluster.
- None of the timings is a performance claim.

## Run it end to end

```bash
cd ~/genomics-kit && .venv/bin/python demo.py
```

It takes about 27 minutes, about 12 of which are the bcftools side of Q4. `--keep` skips the reset.

---

## Build it from nothing

Prerequisites:
- **A cluster** where you are super admin. You need VMS writes, and DATABASE is a view property.
- **A data-network client.** It needs sudo, Docker and internet; Gen6 CNode-class hardware is plenty.
- **Credentials on the Mac.** VMS credentials in `~/.vast/vms-creds.json`, and passwordless ssh from the Mac to the client.

1. **Mac venv**

   ```bash
   python3 -m venv .venv && .venv/bin/pip install vastpy requests fastapi uvicorn
   ```

2. **VMS objects (Mac).** Run, in order:

   ```bash
   .venv/bin/python build.py
   ```

   ```bash
   .venv/bin/python build_auditor.py
   ```

   ```bash
   .venv/bin/python build_catalog_reader.py
   ```

   Before running them, ping the VIP ranges in `build.py` to make sure they're free.

3. **Dashboard logins (Mac):** `.venv/bin/python build_dash_manager.py`.

4. **Client runtime.** Copy the repo, `keys.json`, `.vms-dash.json` and `ids.json` to the client (`~/genomics-kit`), then:

   ```bash
   ssh vastdata@192.0.2.111 'cd ~/genomics-kit && ./bootstrap_client.sh'
   ```

   That builds two micromamba envs (`./env`, and `/srv/rad-lab-enclave/env` so the enclave uids can run it). It also
   installs one mode-600 key per identity, owned by that uid, mounts the cohort view, and creates the Kafka topics. Then,
   on the Mac:

   ```bash
   .venv/bin/python de_setup.py
   ```

5. **Data and Trino (client):** `./fetch.sh` (~32 GB, anonymous S3, POD5 included), `./slice.sh` (8 CRAMs to chr22 over
   HTTPS), `./trino_up.sh`.

6. **Run it (Mac):** `.venv/bin/python demo.py`. Then start the dashboard on the client with `./dash_up.sh`.

### Adapting it to another cluster, tenant or customer

All environment-specific values are constants at the top of these files:

| File | Change |
| --- | --- |
| `vms.py` | `CLUSTER` (the VMS address is `192.0.2.<CLUSTER>`) |
| `build.py` | `TENANT`, `N5` (the client data IPs allowed to mount), the VIP ranges, the uids |
| `rle.py` | `S3_EP`, `DB_EP`, `KAFKA`, `BUCKET`, `MNT` |
| `trino_up.sh` | the endpoints and data endpoints for each catalog |
| `demo.py`, `app/server.py`, `teardown.py` | `N5` (the client ssh target), `GUI` (the VMS URL), the lab host used for the allow-list control |
| `query.py` | `PROMOTER`, `WINDOW`, `SAMPLE`: the regions and the sample the questions use |

The transform routes by file suffix (`.vcf.gz.tbi`, `.cram.crai`, `.pod5`, `*_population.txt`). To add a format (FASTQ, BAM, h5ad, Zarr), add a `do_<fmt>()` handler and a line in `route()` in `functions/transform/main.py`.

### Turning the function into a real Data Engine function

`handler(ctx, event)` takes the CloudEvent the element trigger emits. To run it on Data Engine itself:
1. Link a Kubernetes cluster to the tenant: `vastde compute-clusters link` (needs the kube API URL, mTLS credentials and namespaces).
2. Link a container registry.
3. Build the function: `vastde functions build` / `create`.
4. Point the trigger at it with a pipeline.

The CLI is at github.com/vast-data/dataengine-cli; it has releases for 5.4.3, 5.4.6 and 5.5.0.

---

## Gotchas this build paid for (5.5.0-sp6)

- **NFS audit records the RPCs that reach VAST.** A client page-cache hit sends no READ, so the demo reads with O_DIRECT. Say this to an assessor.
- **The VAST Catalog is scoped by the caller's tenant.** Run research computing's query as a default-tenant identity.
- **Reading the audit table or the Catalog needs explicit Tabular grants.** Superuser alone is not enough.
- **View-policy create returned `protocols` (the audited protocols) as `['S3']`.** PATCH `protocols` on its own.
- **A locked snapshot ignores `expiration_time`** and never expires. Unlock it before teardown.
- **`get_stats().num_rows` lags a bulk insert by minutes.** Use `count(*)` for evidence.
- **In ibis, `_.filter` is a method.** Write `_["filter"]` for a column named `filter`.
- **The `trino-vast:478` image ships a templated `vast.properties`.** Mount your own catalog directory.
- **cyvcf2 in a micromamba env on Rocky 9 needs `LD_LIBRARY_PATH=<env>/lib`** inside multiprocessing workers.
- **Raw and annotated 1000G VCFs write about 9% of alleles differently** (`*` and multiallelic indels). Keep FILTER and AF on the genotype rows instead of joining for them.
- **`pgrep -f` inside an ssh command matches its own shell.** Use `[r]unner.py`.
- **The S3 copies of the 2022 1000G phased panels are 0-byte placeholders.** Use the raw joint-called VCF.

## Teardown

```bash
.venv/bin/python teardown.py
```

That prints the plan in dependency order:
1. client processes, Trino and the mount;
2. snapshots (unlock, then delete);
3. VAST DB tables and schema;
4. bucket objects;
5. Kafka topics;
6. the DE trigger and topic, then DE disable;
7. the views;
8. the view policy;
9. the S3 policies;
10. the users;
11. the VIP pools;
12. the tenant (`force=true`);
13. the default-tenant catalog reader;
14. client directories.

Add `--yes` to execute. The live ids on the lab cluster are also recorded in the vast-lab skill's `lab-inventory.md`.

The first real teardown (20 Oct 2026) hit two things that `teardown.py` now handles:

- **A KAFKA view keeps an empty `kafka_topics` schema after its topics are deleted.** VMS refuses to delete a view that still has schemas, and the tenant force-delete, an async task, then **fails** on that view even though the API call returned 200. Drop the schema through VMS (`DELETE /api/latest/schemas/delete/`) first, and poll the task.
- **`pkill -f '<pattern>'` run over ssh kills its own shell.** Use `pgrep -f '[r]unner…' | xargs kill`.

## Data sources and licensing

- **1000 Genomes Project, NYGC 30x high-coverage.** `s3://1000genomes/1000G_2504_high_coverage/` (AWS Open Data, open consent).
- **Oxford Nanopore open data.** `s3://ont-open-data/rna-modbase-validation_2025.03/subset/`.
- **GRCh38 reference.** `s3://1000genomes/technical/reference/GRCh38_reference_genome/`.

The repo holds no data. `fetch.sh` downloads it.

---

Built by Brian Radwanski with Claude Code.
