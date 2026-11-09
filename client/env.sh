#!/bin/bash
# Client tool environment, in user space plus one world-readable prefix so the function uids can run it.
#   $RLU_SRV/env  micromamba env: bcftools/samtools 1.21, cyvcf2, pyarrow, vastdb, trino, kafka, pod5, uproot, plotting
# Changes nothing on the host OS (micromamba is a single static binary in ./bin). Idempotent.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a
PKGS="python=3.12 bcftools=1.21 samtools=1.21 htslib=1.21 cyvcf2 pyarrow pandas numpy scikit-allel matplotlib boto3 uproot awkward"
PIP="vastdb==2.1.0 trino confluent-kafka pod5 vastpy requests"
mkdir -p bin logs
[ -x bin/micromamba ] || curl -sL https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xj -C . bin/micromamba
for D in "$RLU_SRV" "$RLU_SRV/app"; do sudo install -d -o "$(id -u)" -g "$(id -g)" -m 755 "$D"; done
P="$RLU_SRV/env"
if [ ! -x "$P/bin/python" ]; then
  MAMBA_ROOT_PREFIX="$RLU_SRV/mamba" ./bin/micromamba create -y -q -p "$P" -c conda-forge -c bioconda $PKGS
fi
"$P/bin/pip" install -q $PIP
chmod -R a+rX "$RLU_SRV/env"
ln -sfn "$P" env
echo "env ok: $(./env/bin/bcftools --version | head -1); vastdb $(./env/bin/python -c 'import vastdb;print(vastdb.__version__)' 2>/dev/null || echo ?)"
echo ENV_DONE
