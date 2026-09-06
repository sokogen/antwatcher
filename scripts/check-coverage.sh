#!/bin/sh
# Fails when any package's statement coverage falls below a threshold.
#
# Usage: sh scripts/check-coverage.sh [threshold]   (default 80)
#
# Reads the profile written by `make test`. Three packages are excluded: cmd/...
# because the coverage target is scoped to packages outside it, internal/archtest
# because it declares no statements, and internal/bus/bustest because it is the
# conformance suite driver tests run and has no tests of its own by design.
set -eu

cd "$(dirname "$0")/.."

threshold=${1:-80}
profile=${COVERAGE_PROFILE:-coverage.out}
module=github.com/sokogen/antwatcher
excludes='cmd/ internal/archtest internal/bus/bustest'

if [ ! -f "$profile" ]; then
	echo "check-coverage: $profile not found, run 'make test' first" >&2
	exit 1
fi

rows=$(mktemp)
trap 'rm -f "$rows"' EXIT

awk -v prefix="$module/" -v excludes="$excludes" -v threshold="$threshold" '
	function excluded(pkg,   i, n, list) {
		n = split(excludes, list, " ")
		for (i = 1; i <= n; i++) {
			if (pkg == list[i]) return 1
			if (substr(list[i], length(list[i])) == "/" && index(pkg, list[i]) == 1) return 1
		}
		return 0
	}
	# Profile lines are "<file>:<start>,<end> <statements> <count>"; the leading
	# "mode:" line and anything else is not one.
	NF != 3 || $0 !~ /\.go:[0-9]/ { next }
	{
		pkg = $1
		sub(/:[0-9].*$/, "", pkg)
		sub(/\/[^\/]*$/, "", pkg)
		if (index(pkg, prefix) == 1) pkg = substr(pkg, length(prefix) + 1)
		if (excluded(pkg)) next
		total[pkg] += $2
		if ($3 > 0) covered[pkg] += $2
	}
	END {
		for (pkg in total) {
			# Round once so the printed percentage and the comparison agree.
			pct = int(covered[pkg] * 1000 / total[pkg] + 0.5) / 10
			printf "%s %.1f %d %d %s\n", pkg, pct, covered[pkg], total[pkg],
				(pct < threshold ? "FAIL" : "ok")
		}
	}
' "$profile" | sort > "$rows"

if [ ! -s "$rows" ]; then
	echo "check-coverage: $profile covers no eligible package" >&2
	exit 1
fi

printf '%-40s %8s %12s %s\n' PACKAGE COVERAGE STATEMENTS ''
failed=0
while read -r pkg pct covered total status; do
	note=''
	if [ "$status" = FAIL ]; then
		note="below $threshold%"
		failed=$((failed + 1))
	fi
	printf '%-40s %7s%% %12s %s\n' "$pkg" "$pct" "$covered/$total" "$note"
done < "$rows"

if [ "$failed" -ne 0 ]; then
	echo >&2
	echo "check-coverage: $failed package(s) below $threshold%" >&2
	exit 1
fi

echo
echo "check-coverage: every package at or above $threshold%"
