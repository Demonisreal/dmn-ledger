#!/usr/bin/env bash
set -euo pipefail

repo=$(cd "$(dirname "$0")/.." && pwd)
bench_dir=${BENCH_DIR:-/var/tmp/dmn-ledger-bench}
marker=.dmn-ledger-bench

usage() {
	echo "usage: scripts/bench.sh all|smoke [flags for bench plan]" >&2
	echo "       scripts/bench.sh clean" >&2
	echo "all runs the plan for four to five hours; smoke runs it once with short windows in about 20 minutes" >&2
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

enough_memory() {
	local memory=$1 want have
	want=$(($(numfmt --from=iec "$memory") + (1 << 30)))
	have=$(($(awk '$1 == "MemAvailable:" {print $2}' /proc/meminfo) * 1024))
	if ((have < want)); then
		echo "bench: $((have >> 20)) MiB available, the plan wants $((want >> 20)) MiB (its limit of $memory and 1 GiB to spare), $(((want - have) >> 20)) MiB missing" >&2
		echo "bench: close something, or start it with a lower limit: BENCH_MEMORY=5G scripts/bench.sh all" >&2
		return 1
	fi
}

plan() {
	local mode=$1 dir out memory commit status=0 inhibit=()
	shift
	dir=$(own_dir)
	mkdir -p "$dir/bin"
	touch "$dir/$marker"
	commit=$(git -C "$repo" rev-parse --short=7 HEAD)
	if [[ $mode == smoke ]]; then
		memory=${BENCH_MEMORY:-4G}
		out=$dir/smoke/$(date -u +%Y%m%dT%H%M%SZ)-$commit
		set -- -smoke "$@"
	elif (($# > 0)); then
		memory=${BENCH_MEMORY:-6G}
		out=$dir/trial/$(date -u +%Y%m%dT%H%M%SZ)-$commit
		echo "bench: all with flags for bench plan ($*) is a trial, so its results go below $dir and not to docs/bench"
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
	enough_memory "$memory" || return 1
	if command -v systemd-inhibit >/dev/null; then
		inhibit=(systemd-inhibit --what=idle:sleep --who=dmn-ledger-bench --why="the benchmark plan runs")
	fi
	(cd "$repo/bench" && go build -o "$dir/bin/bench" ./cmd/bench)
	"${inhibit[@]}" systemd-run --user --scope --quiet -p MemoryMax="$memory" -p MemorySwapMax=0 -- \
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
