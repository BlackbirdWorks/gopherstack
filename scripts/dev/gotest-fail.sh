#!/usr/bin/env bash
# gotest-fail.sh — run `go test -json -race` and print only failures.
#
# Shows failed tests (with output, subtests included), package-level
# failures (panics, races outside a test, TestMain, setup failed), build
# failures, and anything go printed to stderr. Exit code is go test's.
#
# Usage:
#   scripts/dev/gotest-fail.sh [go test flags/packages...]
#   scripts/dev/gotest-fail.sh ./services/sqs/... -run TestFoo -count=1
# With no package argument, ./... is used.

set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

for bin in go jq; do
  command -v "$bin" >/dev/null 2>&1 || {
    echo "gotest-fail: $bin not found on PATH" >&2
    exit 2
  }
done

# Add ./... only when no package argument is present. Flags that take a
# separate value (-run Foo) must not have the value mistaken for a package.
has_pkg=0
skip=0
for a in "$@"; do
  if [[ $skip -eq 1 ]]; then
    skip=0
    continue
  fi
  case "$a" in
    -run | -skip | -count | -timeout | -tags | -p | -parallel | -cpu | -bench | -benchtime | \
      -coverprofile | -coverpkg | -covermode | -ldflags | -gcflags | -asmflags | -vet | -shuffle | \
      -fuzz | -fuzztime | -mod | -o | -overlay | -pkgdir | -exec | -toolexec | -outputdir | \
      -blockprofile | -cpuprofile | -memprofile | -mutexprofile | -trace | -C)
      skip=1
      ;;
    -*) ;;
    *) has_pkg=1 ;;
  esac
done
args=("$@")
[[ $has_pkg -eq 0 ]] && args+=("./...")

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
json="$work/out.json"
err="$work/stderr.txt"

rc=0
go test -json -race "${args[@]}" >"$json" 2>"$err" || rc=$?

# fromjson? skips non-JSON lines that go test -json can interleave; they are
# collected separately so nothing is lost.
jq -R -r -s --rawfile stderr "$err" '
  def lines: split("\n") | map(select(length > 0));
  (lines | map(. as $l | try fromjson catch {Action: "raw", Output: ($l + "\n")})) as $ev
  | ($ev | map(select(.Action == "build-output" or .Action == "build-fail"))) as $b
  | ($ev | map(select(.Action == "raw"))) as $raw
  | ($ev | map(select(.Package != null))) as $pe
  | ($pe | map(.Package) | unique) as $pkgs
  | ($b | map(select(.Action == "build-fail") | .ImportPath) | unique) as $bfail
  | ($pe | map(select(.Action == "fail" and .Test == null) | .Package) | unique) as $fpkgs
  | ($pe | map(select(.Action == "pass" and .Test == null) | .Package) | unique) as $ppkgs
  | ($pe | map(select(.Action == "fail" and .Test != null)) ) as $ftests
  | ($ftests | map(.Package + " " + .Test)) as $ftn
  | (
      if ($b | length) > 0 or ($bfail | length) > 0 then
        "=== BUILD FAILED ===",
        ($b | map(select(.Action == "build-output")) | group_by(.ImportPath)[]
          | "--- " + (.[0].ImportPath // "?"),
            (map(.Output) | join("") | rtrimstr("\n")))
      else empty end
    ),
    (
      ($ev | map(select(.Package != null and .Action == "output"))) as $out
      | [ $ftests[] ] | sort_by(.Package, .Test)[]
      | . as $t
      | "=== FAIL " + $t.Package + " " + $t.Test + " ===",
        ($out | map(select(.Package == $t.Package and .Test == $t.Test) | .Output)
          | join("") | rtrimstr("\n"))
    ),
    (
      ($ev | map(select(.Package != null and .Action == "output"))) as $out
      | $fpkgs[]
      | . as $p
      | ($out | map(select(.Package == $p and .Test == null) | .Output) | join("") | rtrimstr("\n")) as $po
      | ($out | map(select(.Package == $p and .Test != null)) | map(.Output) | join("")) as $to
      | ($ftn | map(select(startswith($p + " "))) | length) as $nft
      | if $nft == 0 or ($po | test("panic:|DATA RACE|setup failed")) then
          "=== PACKAGE FAIL " + $p + " ===",
          $po,
          (if $nft == 0 and ($to | test("panic:|DATA RACE")) then
             "--- test output with panic/race ---", ($to | rtrimstr("\n"))
           else empty end)
        else empty end
    ),
    (if ($raw | length) > 0 then
       "=== NON-JSON OUTPUT ===", ($raw | map(.Output) | join("") | rtrimstr("\n"))
     else empty end),
    (if ($stderr | length) > 0 then
       "=== STDERR ===", ($stderr | rtrimstr("\n"))
     else empty end),
    "",
    "SUMMARY: \($ppkgs | length) passed pkgs, \($ftn | unique | length) failed tests, \($fpkgs | length) failed pkgs, \($bfail | length) build failures"
' "$json" | awk 'NF || prev {print} {prev=NF}'

exit "$rc"
