#!/bin/bash
# Slide 9's live landing: a 5 Mb slice of the staged chr22 VCF and of its annotated companion (LIVE_VCF in lib/env.py).
# bcftools view -r keeps every sample and the INFO tags (AN included), so the slice's SFS equals the baseline's over
# the same span. ~2 min, once. Idempotent.
cd "$(dirname "$0")/.." && mkdir -p stage/vcf-slice
B=$(grep ^RLU_SRV= .env | cut -d= -f2)/env/bin
eval "$(./env/bin/python -c 'import sys; sys.path.insert(0, "lib"); from env import LIVE_VCF as L; print(f"N={L[\"name\"]} R={L[\"region\"]}")')"
V=stage/vcf/chr22.recalibrated_variants; O=stage/vcf-slice/$N
for s in "" .annotated; do
  [ -s $O$s.vcf.gz.tbi ] && continue
  $B/bcftools view --threads 32 -r $R -Oz -o $O$s.vcf.gz $V$s.vcf.gz 2>/dev/null && $B/tabix -f -p vcf $O$s.vcf.gz
done
ls -la stage/vcf-slice | tail -4; echo SLICE_VCF_DONE
