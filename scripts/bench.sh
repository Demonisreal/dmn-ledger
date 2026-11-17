#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
bench_dir=${BENCH_DIR:-/var/tmp/dmn-ledger-bench}
marker=.dmn-ledger-bench

usage() {
	echo "usage: scripts/bench.sh all|smoke [flags for bench plan]" >&2
	echo "       scripts/bench.sh clean" >&2
	exit 2
}

own_dir() {
	local dir
	dir=$(realpath -m -- "$bench_dir")
	if [[ $dir != /var/tmp/?* || $dir == "$HOME" || $HOME == "$dir"/* ]]; then
		echo "bench: BENCH_DIR=$bench_dir is $dir, which is not a folder below /var/tmp; refusing to touch it" >&2
		return 1
	fi
	if [[ -e $dir && ! -d $dir ]]; then
		echo "bench: $dir is not a folder; refusing to touch it" >&2
		return 1
	fi
	if [[ -d $dir && ! -e $dir/$marker && -n $(ls -A -- "$dir") ]]; then
		echo "bench: $dir holds files but no $marker, so it was not made by this script; refusing to touch it" >&2
		return 1
	fi
	printf '%s\n' "$dir"
}

power() {
	local supply
	for supply in /sys/class/power_supply/*; do
		if [[ $(cat "$supply/type" 2>/dev/null) == Mains && $(cat "$supply/online" 2>/dev/null) == 1 ]]; then
			echo mains
			return
		fi
	done
	echo "not on mains"
}

plan() {
	local mode=$1 dir out memory commit status=0
	shift
	dir=$(own_dir)
	mkdir -p "$dir/bin"
	touch "$dir/$marker"
	commit=$(git -C "$repo" rev-parse --short=7 HEAD)
	if [[ $mode == smoke ]]; then
		memory=${BENCH_MEMORY:-4G}
		out=$dir/smoke/$(date -u +%Y%m%dT%H%M%SZ)-$commit
		set -- -smoke "$@"
	else
		memory=${BENCH_MEMORY:-6G}
		shopt -s nullglob
		local earlier=("$repo"/docs/bench/*-"$commit")
		shopt -u nullglob
		if ((${#earlier[@]} > 1)); then
			echo "bench: ${#earlier[@]} folders for $commit under docs/bench, keep the one to go on with" >&2
			return 1
		fi
		out=${earlier[0]:-$repo/docs/bench/$(date -u +%F)-$commit}
	fi
	echo "bench: governor $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown), $(power), memory limit $memory"
	echo "bench: results go to $out"
	(cd "$repo/bench" && go build -o "$dir/bin/bench" ./cmd/bench)
	systemd-run --user --scope --quiet -p MemoryMax="$memory" -p MemorySwapMax=0 -- \
		taskset -c 8-15 "$dir/bin/bench" plan -dir "$dir" -repo "$repo" -out "$out" "$@" || status=$?
	if compgen -G "$out/raw/*.json" >/dev/null; then
		"$dir/bin/bench" report "$out" || status=1
	fi
	if [[ $mode == all && $status == 0 ]]; then
		"$dir/bin/bench" check "$out" || status=1
	fi
	return "$status"
}

clean() {
	local dir
	dir=$(own_dir)
	if command -v docker >/dev/null && docker container inspect dmn-ledger-bench-pg >/dev/null 2>&1; then
		docker stop dmn-ledger-bench-pg >/dev/null
		docker rm dmn-ledger-bench-pg >/dev/null
		echo "bench: removed the container dmn-ledger-bench-pg"
	fi
	rm -rf -- "$dir"
	echo "bench: removed $dir"
}

case ${1:-} in
all | smoke)
	mode=$1
	shift
	plan "$mode" "$@"
	;;
clean)
	(($# == 1)) || usage
	clean
	;;
*) usage ;;
esac
