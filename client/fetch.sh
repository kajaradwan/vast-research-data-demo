#!/bin/bash
# Stage the open-licence public data on the client's local disk (stage/). Idempotent: skips files already complete.
#   genomics  1000 Genomes 30x chr22 (NYGC, open consent): joint-called VCF 27.9 GB + annotated sites VCF, pedigree,
#             the 2,504-unrelated index, GRCh38; 8 CRAMs sliced to chr22; 2 ONT RNA004 POD5 (ont-open-data)
#             -> the list and slicing are the genomics kit's (vendor/genomics/fetch.sh, slice.sh)
#   hep       client/fetch_hep.sh (separate: different licence and source)
set -u
cd "$(dirname "$0")/.." && mkdir -p stage/vcf stage/ref stage/meta stage/pod5 stage/cram logs
get() {  # url dest: resumable, anonymous HTTPS; files over 1 GB go 64 ranges in parallel (one stream ~2 MB/s)
  local u=$1 d=$2
  local n=$(curl -sI "$u" | awk 'tolower($1)=="content-length:"{print $2+0}')
  if [ "${n:-0}" -gt 1000000000 ]; then python3 client/pget.py "$u" "$d" 64; return; fi
  if [ -s "$d" ] && [ "$(stat -c %s "$d")" = "$(curl -sI "$u" | awk 'tolower($1)=="content-length:"{print $2+0}')" ]; then return 0; fi
  curl -sS -L --retry 5 -C - -o "$d" "$u" || { echo "FAILED $u" >&2; return 1; }
}
S3=https://1000genomes.s3.amazonaws.com
B=$S3/1000G_2504_high_coverage
get $B/additional_698_related/20130606_g1k_3202_samples_ped_population.txt stage/meta/20130606_g1k_3202_samples_ped_population.txt &
get $B/1000G_2504_high_coverage.sequence.index stage/meta/1000G_2504_high_coverage.sequence.index &
R=$S3/technical/reference/GRCh38_reference_genome
get $R/GRCh38_full_analysis_set_plus_decoy_hla.fa stage/ref/GRCh38_full_analysis_set_plus_decoy_hla.fa &
get $R/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai stage/ref/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai &
W=$B/working/20201028_3202_raw_GT_with_annot/20201028_CCDG_14151_B01_GRM_WGS_2020-08-05_chr22
for s in annotated.vcf.gz annotated.vcf.gz.tbi vcf.gz.tbi vcf.gz; do
  get $W.recalibrated_variants.$s stage/vcf/chr22.recalibrated_variants.$s &
done
O=https://ont-open-data.s3.amazonaws.com/rna-modbase-validation_2025.03/subset
get $O/m6A_DRACH.pod5 stage/pod5/m6A_DRACH.pod5 &
get $O/control_DRACH.pod5 stage/pod5/control_DRACH.pod5 &
wait
ls -l stage/vcf stage/meta stage/pod5 stage/ref
echo FETCH_DONE
