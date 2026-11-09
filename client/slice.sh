#!/bin/bash
# chr22 slices of 8 open-consent 1000G 30x CRAMs (8 populations), read by range over HTTPS with the .crai.
# The sample list is the genomics kit's (vendor/genomics/slice.sh). Idempotent.
cd "$(dirname "$0")/.." && mkdir -p stage/cram
REF=stage/ref/GRCh38_full_analysis_set_plus_decoy_hla.fa
U=https://1000genomes.s3.amazonaws.com/1000G_2504_high_coverage/data
grep -E '^ERR[0-9]+ [A-Z]{2}[0-9]+$' vendor/genomics/slice.sh | while read run s; do
  [ -s stage/cram/$s.chr22.cram.crai ] && continue
  ( ./env/bin/samtools view -@4 -C -T $REF -X -o stage/cram/$s.chr22.cram $U/$run/$s.final.cram $U/$run/$s.final.cram.crai chr22 \
    && ./env/bin/samtools index stage/cram/$s.chr22.cram && echo "$s $(stat -c %s stage/cram/$s.chr22.cram)" ) &
done
wait; ls stage/cram | grep -c crai; echo SLICE_DONE
