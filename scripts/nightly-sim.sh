#!/usr/bin/env bash
set -euo pipefail

seeds=${1:-200000}
profile=${2:-all}
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

echo "$(date -u +%FT%TZ) commit $(git describe --always --dirty), $seeds seeds from $from, profile $profile" >>"$log"
"$work/sim" -seeds "$seeds" -from "$from" -profile "$profile" >>"$log" 2>&1
