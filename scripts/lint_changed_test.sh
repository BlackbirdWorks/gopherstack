#!/usr/bin/env bash
# Repro for gopherstack-hou6t: a known finding must be printed and the exit non-zero;
# a fake tool failure (exit 201, "0 issues.") must be flagged, not look clean.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
d=internal/zzlintscratch
trap 'rm -rf "$d"' EXIT
mkdir -p "$d"
printf 'package zzlintscratch\n\nimport (\n\t"strings"\n\t"fmt"\n)\n\nfunc F() string { return strings.ToUpper(fmt.Sprint(1)) }\n' >"$d/a.go"
out=$(bash scripts/lint-changed.sh "$d/a.go" 2>&1); rc=$?
[[ $rc -ne 0 ]] || { echo "FAIL: exit 0 with finding"; exit 1; }
grep -q 'goimports' <<<"$out" || { echo "FAIL: finding not printed"; exit 1; }
grep -q 'SUMMARY: FAILED' <<<"$out" || { echo "FAIL: no summary"; exit 1; }
fake=$(mktemp -d); trap 'rm -rf "$d" "$fake"' EXIT
printf '#!/bin/sh\necho "0 issues."\nexit 201\n' >"$fake/golangci-lint"; chmod +x "$fake/golangci-lint"
out=$(PATH="$fake:$PATH" bash scripts/lint-changed.sh "$d/a.go" 2>&1); rc=$?
[[ $rc -ne 0 ]] || { echo "FAIL: fake 201 exit 0"; exit 1; }
grep -q 'tool failure' <<<"$out" || { echo "FAIL: tool failure not flagged"; exit 1; }
echo PASS
