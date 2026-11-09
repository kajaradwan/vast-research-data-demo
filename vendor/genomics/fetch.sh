#!/bin/bash
# Stage open-consent 1000 Genomes 30x data (anonymous s3://1000genomes) onto n5 local disk.
set -u
cd ~/genomics-kit && mkdir -p stage/vcf stage/ref stage/meta stage/pod5 stage/cram logs
A="./env/bin/aws s3 cp --no-sign-request --only-show-errors"
B=s3://1000genomes/1000G_2504_high_coverage
$A $B/additional_698_related/20130606_g1k_3202_samples_ped_population.txt stage/meta/
$A $B/1000G_2504_high_coverage.sequence.index stage/meta/
R=s3://1000genomes/technical/reference/GRCh38_reference_genome
$A $R/GRCh38_full_analysis_set_plus_decoy_hla.fa stage/ref/ && $A $R/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai stage/ref/
W=$B/working/20201028_3202_raw_GT_with_annot/20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr22
$A $W.recalibrated_variants.annotated.vcf.gz stage/vcf/ && $A $W.recalibrated_variants.annotated.vcf.gz.tbi stage/vcf/
$A $W.recalibrated_variants.vcf.gz.tbi stage/vcf/
$A $W.recalibrated_variants.vcf.gz stage/vcf/
mkdir -p stage/pod5
O=s3://ont-open-data/rna-modbase-validation_2025.03/subset
$A $O/m6A_DRACH.pod5 stage/pod5/ && $A $O/control_DRACH.pod5 stage/pod5/
echo FETCH_DONE
