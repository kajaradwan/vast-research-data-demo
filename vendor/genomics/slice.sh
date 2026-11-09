#!/bin/bash
# chr22 slices of 8 open-consent 1000G 30x CRAMs (8 populations), read by range over HTTPS with the .crai.
cd ~/genomics-kit && mkdir -p stage/vcf stage/ref stage/meta stage/pod5 stage/cram logs
REF=stage/ref/GRCh38_full_analysis_set_plus_decoy_hla.fa
U=https://1000genomes.s3.amazonaws.com/1000G_2504_high_coverage/data
while read run s; do
  ( ./env/bin/samtools view -@4 -C -T $REF -X -o stage/cram/$s.chr22.cram $U/$run/$s.final.cram $U/$run/$s.final.cram.crai chr22 \
    && ./env/bin/samtools index stage/cram/$s.chr22.cram && echo "$s $(stat -c %s stage/cram/$s.chr22.cram)" ) &
done <<LIST
ERR3240114 HG00096
ERR3239334 NA12878
ERR3239646 NA18525
ERR3239683 NA19017
ERR3241828 HG01112
ERR3239785 NA20502
ERR3242467 HG03052
ERR3239894 NA19648
LIST
wait; echo SLICE_DONE
