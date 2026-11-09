#!/bin/bash
# Stage the HEP sample for the ADFilter-style anomaly-detection beat into stage/hep/. Idempotent and resumable.
#
# Source: ATLAS 13 TeV Open Data, 2020 release, "exactly one lepton" collection
#   CERN Open Data record 15001, DOI 10.7483/OPENDATA.ATLAS.NQ31.Y1OO, licence CC0-1.0 (public domain dedication)
#   https://opendata.cern.ch/record/15001   (anonymous HTTPS, byte ranges supported)
# Files (sizes are the server's Content-Length):
#   mc_410000.ttbar_lep.1lep.root        4,508,741,051 B  SM ttbar (>=1 lepton) MC   -> AE training background + held-out SM
#   mc_3013{22..33}.ZPrime{400..3000}_tt  12 files, 8-13 MB each (~138 MB)  BSM Z'->ttbar MC -> anomaly signal
#   data_A.1lep.root                     1,581,143,872 B  2015-16 collision data, period A (real data to score)
# Total ~6.2 GB.  `fetch_hep.sh core` skips data_A.
# CERN EOS serves ~0.4 MB/s per connection, so big files are fetched as 64 MiB byte ranges, $HEP_STREAMS at a time
# (default 32, ~10 MB/s measured from client-n5); each range is resumable on its own.
set -u
cd "$(dirname "$0")/.." && mkdir -p stage/hep logs
H=https://opendata.cern.ch/eos/opendata/atlas/OutreachDatasets/2020-08-19
SEG=$((64*1024*1024)); STREAMS=${HEP_STREAMS:-32}
clen() { curl -sIL "$1" | awk 'tolower($1)=="content-length:"{n=$2+0} END{print n+0}'; }
seg() {  # url partdir index total: fetch one byte range, resumable
  local u=$1 p=$2 i=$3 n=$4 a b want have
  a=$((i*SEG)); b=$((a+SEG-1)); [ $b -ge $n ] && b=$((n-1)); want=$((b-a+1))
  for t in 1 2 3 4 5 6; do
    have=$( [ -f $p/$i ] && stat -c %s $p/$i || echo 0 )
    [ "$have" = "$want" ] && return 0
    [ "$have" -gt "$want" ] && { rm -f $p/$i; have=0; }
    curl -sS -L -r $((a+have))-$b "$u" >> $p/$i || sleep 3
  done
  echo "FAILED range $i of $u" >&2; return 1
}
export -f seg; export SEG
get() {  # url dest: complete when local size == Content-Length
  local u=$1 d=$2 n k
  n=$(clen "$u"); [ "$n" -gt 0 ] || { echo "FAILED HEAD $u" >&2; return 1; }
  if [ -s "$d" ] && [ "$(stat -c %s "$d")" = "$n" ]; then echo "ok   $d"; return 0; fi
  k=$(( (n+SEG-1)/SEG )); mkdir -p "$d.parts"
  seq 0 $((k-1)) | xargs -P $STREAMS -I{} bash -c "seg '$u' '$d.parts' {} $n" || { echo "FAILED $u" >&2; return 1; }
  for i in $(seq 0 $((k-1))); do cat "$d.parts/$i"; done > "$d.tmp"
  [ "$(stat -c %s "$d.tmp")" = "$n" ] || { echo "FAILED size $d" >&2; rm -f "$d.tmp"; return 1; }
  mv "$d.tmp" "$d" && rm -rf "$d.parts" && echo "got  $d"
}
for z in 301322:400 301323:500 301324:750 301325:1000 301326:1250 301327:1500 301328:1750 301329:2000 \
         301330:2250 301331:2500 301332:2750 301333:3000; do
  f=mc_${z%%:*}.ZPrime${z##*:}_tt.1lep.root
  get $H/MC-1-1lep/tmp1/$f stage/hep/$f &
done
wait
get $H/MC-2-1lep/tmp2/mc_410000.ttbar_lep.1lep.root stage/hep/mc_410000.ttbar_lep.1lep.root
[ "${1:-all}" = core ] || get $H/Data-1lep/Data/data_A.1lep.root stage/hep/data_A.1lep.root
ls -l stage/hep
echo FETCH_HEP_DONE
