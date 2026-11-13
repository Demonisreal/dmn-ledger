#!/usr/bin/env bash
set -euo pipefail

seeds=${1:-200000}
profile=${2:-all}
hunt=${3:-40000}
log=${NIGHTLY_LOG:-$HOME/dmn-ledger-nightly.log}

exec 9>"$log.lock"
if ! flock -n 9; then
	echo "nightly-sim: another run holds $log.lock" >&2
	exit 1
fi

if [[ -f $log && $(stat -c %s "$log") -gt $((10 << 20)) ]]; then
	mv "$log" "$log.1"
fi

repo=$(cd "$(dirname "$0")/.." && pwd)
from=$(( $(date -u +%Y%m%d) * 1000000 ))
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

cd "$repo"
go build -o "$work/sim" ./cmd/sim
go build -tags simbug -o "$work/simbug" ./cmd/sim

status=0
sweep() {
	"$work/sim" -seeds "$1" -from "$from" -profile "$2" >>"$log" 2>&1 || status=$?
}

echo "$(date -u +%FT%TZ) commit $(git describe --always --dirty), $seeds seeds from $from, profile $profile" >>"$log"
sweep "$seeds" "$profile"
sweep 10000 rot
sweep 10000 beyond

echo "search for planted bug B16 in $hunt seeds, profile rot, a finding is the wanted outcome" >>"$log"
found=0
"$work/simbug" -bug B16 -seeds "$hunt" -from "$from" -profile rot >>"$log" 2>&1 || found=$?
if [[ $found -gt 1 ]]; then
	status=$found
fi

exit "$status"
