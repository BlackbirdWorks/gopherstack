#!/usr/bin/env bash
# pgo.sh — regenerate the repo-root default.pgo Profile-Guided Optimization
# (PGO) profile that `go build` auto-consumes from the main package dir.
#
# Pipeline:
#   1. Build bin/gopherstack and bin/pgoload.
#   2. Start the server with GOPHERSTACK_PPROF_ADDR set so it also serves
#      net/http/pprof on a side address, isolated from the main API port.
#   3. Wait for the server's main port to accept connections.
#   4. Start a pprof CPU capture in the background for PGO_SECONDS.
#   5. During the capture window, hammer the server with bin/pgoload (heavy
#      DDB GSI/LSI + S3 traffic) and, best-effort, the integration test
#      suite pointed at the running server (GOPHERSTACK_ENDPOINT) for
#      broader coverage. The integration run is wrapped so it can never
#      fail the pipeline.
#   6. Wait for the capture to finish, move it to default.pgo at repo root.
#   7. Validate: `go tool pprof -top` produces output and `go build
#      -pgo=auto` succeeds against the new profile.
#
# All knobs are overridable via environment variables. Safe to re-run.
#
# Usage: bash scripts/pgo.sh
# Env:
#   PGO_SECONDS          pprof capture window, seconds (default 100)
#   PGO_LOAD_DURATION    pgoload run duration, seconds (default 90)
#   PGO_CONCURRENCY      pgoload concurrency (default 8)
#   PGO_INTEG_TIMEOUT    timeout(1) budget for the best-effort integration
#                         load, seconds (default 120)
#   PGO_SERVER_PORT      main API port the server listens on (default 8000)
#   PGO_PPROF_ADDR       pprof side address (default localhost:6060)
#   PGO_WAIT_TRIES       bounded retries waiting for the server (default 30)

set -euo pipefail

cd "$(git rev-parse --show-toplevel 2>/dev/null || dirname "${BASH_SOURCE[0]}"/..)"

PGO_SECONDS="${PGO_SECONDS:-100}"
PGO_LOAD_DURATION="${PGO_LOAD_DURATION:-90}"
PGO_CONCURRENCY="${PGO_CONCURRENCY:-8}"
PGO_INTEG_TIMEOUT="${PGO_INTEG_TIMEOUT:-120}"
PGO_SERVER_PORT="${PGO_SERVER_PORT:-8000}"
PGO_PPROF_ADDR="${PGO_PPROF_ADDR:-localhost:6060}"
PGO_WAIT_TRIES="${PGO_WAIT_TRIES:-30}"

BIN_SERVER="bin/gopherstack"
BIN_LOAD="bin/pgoload"
CPU_PROFILE="cpu.pprof"
PROFILE_OUT="default.pgo"

SERVER_PID=""

log() { echo "[pgo] $*"; }

cleanup() {
  local status=$?
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    log "stopping server (pid ${SERVER_PID})"
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -f "${CPU_PROFILE}" cpu1.pprof cpu2.pprof
  exit "${status}"
}
trap cleanup EXIT

log "building bin/gopherstack..."
go build -o "${BIN_SERVER}" .

log "building bin/pgoload..."
go build -o "${BIN_LOAD}" ./cmd/pgoload

# Pre-compile the integration test binary NOW (before the capture window) so its
# heavy compile does not contend for CPU with the pprof capture and starve it.
# Best-effort: if it fails to build, we simply skip the breadth load.
INTEG_BIN="bin/integration.test"
INTEG_OK=0
log "building integration test binary (breadth load)..."
if go test -c -o "${INTEG_BIN}" ./test/integration/ 2>/dev/null; then
  INTEG_OK=1
else
  log "integration test binary did not build; breadth load will be skipped"
fi

log "starting server (GOPHERSTACK_PPROF_ADDR=${PGO_PPROF_ADDR}, port ${PGO_SERVER_PORT})..."
GOPHERSTACK_PPROF_ADDR="${PGO_PPROF_ADDR}" "./${BIN_SERVER}" &
SERVER_PID=$!

log "waiting for http://localhost:${PGO_SERVER_PORT} to accept connections..."
ready=0
for ((i = 1; i <= PGO_WAIT_TRIES; i++)); do
  if curl -s -o /dev/null "http://localhost:${PGO_SERVER_PORT}/"; then
    ready=1
    break
  fi
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    log "server process exited unexpectedly while waiting for readiness"
    exit 1
  fi
  sleep 1
done
if [[ "${ready}" -ne 1 ]]; then
  log "server did not become ready after ${PGO_WAIT_TRIES}s"
  exit 1
fi
log "server is ready."

# Two SEQUENTIAL capture phases, then merge. pgoload and the integration suite
# must NOT overlap: the integration harness resets server state between tests,
# which would wipe pgoload's tables/buckets mid-run and make it error-spin
# (polluting the profile with futex/retry noise instead of the real hot paths).
CPU1="cpu1.pprof"
CPU2="cpu2.pprof"
PROFILES=()

# --- Phase 1: heavy DDB(GSI/LSI)+S3 load via pgoload, captured alone so the
# index-maintenance / item marshaling / map-access and S3 compression/hashing
# hot paths dominate this slice.
CAP1=$((PGO_LOAD_DURATION + 5))
log "phase 1: capturing ${CAP1}s while pgoload drives heavy DDB(GSI/LSI)+S3 load..."
rm -f "${CPU1}"
curl -s "http://${PGO_PPROF_ADDR}/debug/pprof/profile?seconds=${CAP1}" -o "${CPU1}" &
CAP1_PID=$!
"./${BIN_LOAD}" -endpoint "http://localhost:${PGO_SERVER_PORT}" \
  -duration "${PGO_LOAD_DURATION}s" -concurrency "${PGO_CONCURRENCY}" \
  || log "pgoload exited non-zero (continuing)"
wait "${CAP1_PID}"
if [[ -s "${CPU1}" ]]; then
  PROFILES+=("${CPU1}")
else
  log "phase 1 capture was empty"
fi

# --- Phase 2: breadth. Run the pre-built integration suite ALONE against the
# running server so every other service's wire/handler/map paths appear.
# Best-effort: failures/timeouts are ignored (many tests need Docker).
if [[ "${INTEG_OK}" == "1" ]]; then
  CAP2=$((PGO_INTEG_TIMEOUT + 5))
  log "phase 2: capturing ${CAP2}s while the integration suite drives breadth (timeout ${PGO_INTEG_TIMEOUT}s, failures ignored)..."
  rm -f "${CPU2}"
  curl -s "http://${PGO_PPROF_ADDR}/debug/pprof/profile?seconds=${CAP2}" -o "${CPU2}" &
  CAP2_PID=$!
  GOPHERSTACK_ENDPOINT="http://localhost:${PGO_SERVER_PORT}" \
    timeout "${PGO_INTEG_TIMEOUT}" "./${INTEG_BIN}" -test.count=1 >/dev/null 2>&1 || true
  wait "${CAP2_PID}"
  if [[ -s "${CPU2}" ]]; then
    PROFILES+=("${CPU2}")
  else
    log "phase 2 capture was empty (skipping breadth)"
  fi
fi

if [[ "${#PROFILES[@]}" -eq 0 ]]; then
  log "no non-empty captures were produced"
  exit 1
fi

log "stopping server..."
if kill -0 "${SERVER_PID}" 2>/dev/null; then
  kill "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
fi
SERVER_PID=""

if [[ "${#PROFILES[@]}" -gt 1 ]]; then
  log "merging ${#PROFILES[@]} phase captures -> ${PROFILE_OUT}"
  go tool pprof -proto "${PROFILES[@]}" >"${PROFILE_OUT}"
else
  cp "${PROFILES[0]}" "${PROFILE_OUT}"
fi
rm -f "${CPU1}" "${CPU2}"
log "wrote ${PROFILE_OUT}"

log "validating profile with go tool pprof..."
go tool pprof -top "${PROFILE_OUT}" | head -20

log "validating that go build -pgo=auto consumes ${PROFILE_OUT}..."
go build -pgo=auto -o /dev/null .

log "done. ${PROFILE_OUT} is ready at repo root."
