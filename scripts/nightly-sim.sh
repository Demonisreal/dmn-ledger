#!/usr/bin/env bash
set -euo pipefail

seeds=${1:-200000}
profile=${2:-all}
log=${NIGHTLY_LOG:-$HOME/dmn-ledger-nightly.log}

repo=$(cd "$(dirname "$0")/.." && pwd)
from=$(( $(date -u +%Y%m%d) * 1000000 ))
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

cd "$repo"
go build -o "$work/sim" ./cmd/sim

echo "$(date -u +%FT%TZ) commit $(git rev-parse --short HEAD), $seeds seeds from $from, profile $profile" >>"$log"
status=0
"$work/sim" -seeds "$seeds" -from "$from" -profile "$profile" >>"$log" 2>&1 || status=$?
exit "$status"
