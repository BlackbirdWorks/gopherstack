#!/usr/bin/env bash
# svcops.sh — compare a service's SDK operations with what services/<svc> routes.
#
# Usage: scripts/dev/svcops.sh <service> [sdk-package]
#   service      directory under services/ (e.g. sqs, elbv2, cognitoidp)
#   sdk-package  aws-sdk-go-v2/service/<pkg> name, when it differs from the
#                service dir (a built-in alias table covers the known cases)
#
# SDK ops   = api_op_*.go files of the version pinned in go.mod (module cache).
# Routed    = union of:
#   listed   GetSupportedOperations as resolved by `go run ./cmd/opcensus`
#            (follows helpers, consts and dispatch tables; JSON cached under
#            the git dir, SVCOPS_NO_CACHE=1 to refresh) plus a text-based
#            resolution of the same method as a fallback
#   dispatch evidence of a handler outside that list: handle<Op> funcs,
#            case/map-key/operation:/setOperation(...) lines naming the op
# Output: implemented, missing, and routed-but-not-in-SDK (typos/extras).

set -euo pipefail
export LC_ALL=C

usage() {
  echo "usage: svcops.sh <service> [sdk-package]" >&2
  exit 2
}
[[ $# -ge 1 && $# -le 2 && "$1" != -* ]] || usage

for bin in go grep awk sort comm jq find; do
  command -v "$bin" >/dev/null 2>&1 || {
    echo "svcops: $bin not found on PATH" >&2
    exit 2
  }
done

cd "$(git rev-parse --show-toplevel)"
svc="$1"
dir="services/$svc"
[[ -d "$dir" ]] || {
  echo "svcops: no such service dir: $dir" >&2
  exit 1
}

pkg="${2:-}"
if [[ -z "$pkg" ]]; then
  case "$svc" in
    awsconfig) pkg=configservice ;;
    ce) pkg=costexplorer ;;
    cognitoidp) pkg=cognitoidentityprovider ;;
    dms) pkg=databasemigrationservice ;;
    elasticsearch) pkg=elasticsearchservice ;;
    elb) pkg=elasticloadbalancing ;;
    elbv2) pkg=elasticloadbalancingv2 ;;
    stepfunctions) pkg=sfn ;;
    *) pkg="$svc" ;;
  esac
fi

modcache="$(go env GOMODCACHE)"
base="$modcache/github.com/aws/aws-sdk-go-v2/service"
ver="$(awk -v m="github.com/aws/aws-sdk-go-v2/service/$pkg" '$1 == m {print $2; exit}' go.mod)"
sdkdir=""
if [[ -n "$ver" && -d "$base/$pkg@$ver" ]]; then
  sdkdir="$base/$pkg@$ver"
else
  latest="$(ls -d "$base/$pkg@"* 2>/dev/null | sort -V | tail -n1 || true)"
  if [[ -n "$latest" ]]; then
    sdkdir="$latest"
    echo "note: $pkg not pinned in go.mod (or not downloaded); using ${latest##*/}" >&2
  fi
fi
if [[ -z "$sdkdir" ]]; then
  echo "svcops: SDK module service/$pkg not found in $base" >&2
  cands="$(ls "$base" 2>/dev/null | sed 's/@.*//' | sort -u | grep -i -- "${svc:0:4}" | tr '\n' ' ' || true)"
  [[ -n "$cands" ]] && echo "  similar SDK packages: $cands" >&2
  echo "  pass the SDK package as 2nd arg: svcops.sh $svc <sdk-package>" >&2
  exit 1
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

find "$sdkdir" -maxdepth 1 -name 'api_op_*.go' ! -name '*_test.go' -printf '%f\n' |
  sed 's/^api_op_//; s/\.go$//' | sort -u >"$work/sdk"

# Non-test sources concatenated; funcs start at column 0 so awk can slice them.
find "$dir" -maxdepth 1 -name '*.go' ! -name '*_test.go' -print0 | sort -z | xargs -0 cat >"$work/all.go"

# const/var name -> string value, for op constants like opCreateQueue = "CreateQueue".
grep -oE '\b[A-Za-z_][A-Za-z0-9_]*[[:space:]]*=[[:space:]]*"[A-Z][A-Za-z0-9]+"' "$work/all.go" |
  sed -E 's/[[:space:]]*=[[:space:]]*"/ /; s/"$//' | sort -u >"$work/consts"

# func_body <name>: source of a top-level func declared with that name
# (any receiver) whose signature mentions []string; empty otherwise.
func_body() {
  awk -v n="$1" '
    $0 ~ "^func (\\([^)]*\\) )?" n "\\(" && $0 ~ /\[\]string/ { on = 1 }
    on { print }
    on && (/^}/ || (/\{.*\}[[:space:]]*$/ && !/\{[[:space:]]*$/)) { on = 0 }
  ' "$work/all.go"
}

# tokens_to_ops: stdin source text -> op-like names (literals + resolved consts).
tokens_to_ops() {
  local text
  text="$(cat)"
  grep -oE '"[A-Z][A-Za-z0-9]+"' <<<"$text" | tr -d '"' || true
  grep -oE '\b[A-Za-z_][A-Za-z0-9_]*\b' <<<"$text" | sort -u |
    awk 'NR==FNR {v[$1]=$2; next} ($1 in v) {print v[$1]}' "$work/consts" - || true
}

# listed: resolve GetSupportedOperations and every []string helper it calls.
: >"$work/listed"
: >"$work/visited"
queue=("GetSupportedOperations")
while [[ ${#queue[@]} -gt 0 ]]; do
  fn="${queue[0]}"
  queue=("${queue[@]:1}")
  grep -qx "$fn" "$work/visited" && continue
  echo "$fn" >>"$work/visited"
  body="$(func_body "$fn")"
  [[ -n "$body" ]] || continue
  tokens_to_ops <<<"$body" >>"$work/listed"
  while read -r callee; do
    [[ -n "$callee" ]] && queue+=("$callee")
  done < <(grep -oE '\b[A-Za-z_][A-Za-z0-9_]*\(' <<<"$body" | tr -d '(' | sort -u | grep -vxE 'append|make|len|string' || true)
done

# opcensus: the repo's own resolver for GetSupportedOperations (chases tables).
oc_json="$(git rev-parse --git-path svcops-opcensus.json)"
refresh=1
if [[ -z "${SVCOPS_NO_CACHE:-}" && -s "$oc_json" ]]; then
  newer="$(find services cmd/opcensus -name '*.go' -newer "$oc_json" -print -quit)"
  [[ -z "$newer" ]] && refresh=0
fi
if [[ $refresh -eq 1 ]]; then
  go run ./cmd/opcensus -json "$oc_json.tmp" >/dev/null 2>&1 || true
  [[ -s "$oc_json.tmp" ]] && mv -f "$oc_json.tmp" "$oc_json"
fi
oc_res="n/a"
if [[ -s "$oc_json" ]]; then
  jq -r --arg s "$svc" '.[] | select(.service == $s) | .allOps[]?' "$oc_json" >>"$work/listed" || true
  oc_res="$(jq -r --arg s "$svc" '.[] | select(.service == $s) | .resolution' "$oc_json" || true)"
  oc_res="${oc_res:-n/a}"
fi
sort -u -o "$work/listed" "$work/listed"
have_gso=1
[[ -s "$work/listed" ]] || have_gso=0

# dispatch evidence outside the listed set
{
  grep -oE '^func (\([^)]*\) )?handle[A-Z][A-Za-z0-9]*\(' "$work/all.go" |
    sed -E 's/^func (\([^)]*\) )?handle//; s/\($//'
  grep -vE 'R?W?[Ll]ock\(' "$work/all.go" |
    grep -E '(^|[^A-Za-z])case |setOperation\(|[Oo]peration[[:space:]]*[:=]|^[[:space:]]*("[A-Za-z0-9]+"|op[A-Za-z0-9]+|action[A-Za-z0-9]+)[[:space:]]*:|Action[[:space:]]*==' |
    tokens_to_ops
} | grep -E '^[A-Z][A-Za-z0-9]+$' | sort -u >"$work/evidence" || true

sort -u "$work/listed" "$work/evidence" >"$work/routed"

comm -12 "$work/sdk" "$work/routed" >"$work/impl"
comm -23 "$work/sdk" "$work/routed" >"$work/missing_all"
# Ops absent from static analysis but named in the service's own tests are
# probably registered dynamically (e.g. "Create"+kind); report them apart.
find "$dir" -maxdepth 1 -name '*_test.go' ! -name '*completeness*' -print0 | xargs -0 -r cat |
  grep -oE '"[A-Z][A-Za-z0-9]+"' | tr -d '"' | sort -u >"$work/testrefs" || true
comm -12 "$work/missing_all" "$work/testrefs" >"$work/dynamic"
comm -23 "$work/missing_all" "$work/dynamic" >"$work/missing"
# extras: listed names only (evidence tokens are too broad to call typos)
comm -13 "$work/sdk" "$work/listed" >"$work/extra"
comm -12 "$work/sdk" "$work/evidence" | comm -13 "$work/listed" - >"$work/unlisted"

n_sdk="$(wc -l <"$work/sdk")"
n_impl="$(wc -l <"$work/impl")"
n_miss="$(wc -l <"$work/missing")"
n_dyn="$(wc -l <"$work/dynamic")"
n_extra="$(wc -l <"$work/extra")"
n_unl="$(wc -l <"$work/unlisted")"

echo "== $svc (sdk: $pkg ${sdkdir##*@}) =="
if grep -qE '"(Create|Delete|Describe|List|Update|Start|Stop|Get|Put)"[[:space:]]*\+|\+[[:space:]]*"s"' "$work/all.go"; then
  echo "note: service builds some op names by string concatenation; check the unverified/missing lists by hand"
fi
[[ "$oc_res" == ambiguous ]] && echo "WARNING: op names are built dynamically (string concatenation); 'missing' may include implemented ops"
[[ $have_gso -eq 0 ]] && echo "note: no GetSupportedOperations list found; using dispatch evidence only (less reliable)"
echo "-- implemented ($n_impl)"
sed 's/^/  /' "$work/impl"
echo "-- missing ($n_miss)"
sed 's/^/  /' "$work/missing"
if [[ $n_dyn -gt 0 ]]; then
  echo "-- unverified: not found statically but named in tests, likely dynamic registration ($n_dyn)"
  sed 's/^/  /' "$work/dynamic"
fi
echo "-- routed but not in this SDK module ($n_extra; typos, or APIs co-hosted from another SDK module)"
sed 's/^/  /' "$work/extra"
if [[ $n_unl -gt 0 ]]; then
  echo "-- implemented via dispatch evidence but absent from GetSupportedOperations ($n_unl)"
  sed 's/^/  /' "$work/unlisted"
fi
echo "SUMMARY: sdk=$n_sdk implemented=$n_impl missing=$n_miss unverified=$n_dyn extra=$n_extra unlisted=$n_unl"
