#!/usr/bin/env bash
set -euo pipefail

minutes=${1:-30}
out=${NIGHTLY_CHAOS_DIR:-$HOME/dmn-ledger-chaos}

mkdir -p "$out"
exec 9>"$out/lock"
if ! flock -n 9; then
	echo "nightly-chaos: another run holds $out/lock" >&2
	exit 1
fi

log=$out/chaos.log
if [[ -f $log && $(stat -c %s "$log") -gt $((10 << 20)) ]]; then
	mv "$log" "$log.1"
fi

repo=$(cd "$(dirname "$0")/.." && pwd)
from=$(( $(date -u +%Y%m%d) * 1000 ))
cd "$repo"

echo "$(date -u +%FT%TZ) commit $(git describe --always --dirty), seeds $from to $((from + 2)), ${minutes}m each, findings in $out" >>"$log"

status=0
for seed in "$from" $((from + 1)) $((from + 2)); do
	TMPDIR=$out go test -count=1 -v -tags chaos ./internal/chaos -run '^TestChaos$' \
		-chaos.seed "$seed" -chaos.duration "${minutes}m" -timeout $((minutes + 15))m >>"$log" 2>&1 || status=1
done

echo "search for planted bugs, B11 must show" >>"$log"
TMPDIR=$out go test -count=1 -v -tags 'chaos simbug' ./internal/chaos -run '^TestChaosFindsPlantedBugs$' \
	-chaos.seed "$from" -timeout 20m >>"$log" 2>&1 || status=1

exit "$status"
