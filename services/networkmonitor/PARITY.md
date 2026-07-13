---
service: networkmonitor
sdk_module: aws-sdk-go-v2/service/networkmonitor@v1.14.6
last_audit_commit: 05aca6b4
last_audit_date: 2026-07-13
overall: A
ops:
  CreateMonitor: {wire: ok, errors: ok, state: ok, persist: ok, note: "probe packetSize now validated (56-8500); probe protocol now canonicalised to upper-case TCP/ICMP before storage"}
  GetMonitor: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMonitor: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMonitor: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMonitors: {wire: ok, errors: ok, state: ok, persist: ok, note: "pagination verified: token-past-end returns empty (not first page), state filter applied post-slice correctly"}
  CreateProbe: {wire: ok, errors: ok, state: ok, persist: ok, note: "same packetSize/protocol-normalisation fixes as CreateMonitor's nested probes"}
  GetProbe: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateProbe: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a validation no-op (see gaps fixed below); now validates protocol/destinationPort/packetSize/state and the cross-field TCP-requires-destinationPort invariant on the post-update result"}
  DeleteProbe: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matching: {status: ok, note: "RouteMatcher gates on signing-service + path prefix only (method-agnostic, correct); ExtractOperation correctly routes UpdateMonitor/UpdateProbe on PATCH (not PUT, which the real API does not expose). Added a matcher-routed test (TestHandler_RouteMatcher) and a full method+path->op matrix test (TestHandler_ExtractOperation_MethodPathMatrix) since none previously exercised RouteMatcher() or the PATCH-vs-PUT distinction directly -- prior tests only called Handler() end-to-end."}
gaps:
  - "No service-quota enforcement (e.g. probes-per-monitor, monitors-per-account) -- ServiceQuotaExceededException is defined in the real SDK's types/errors.go but gopherstack's handleError has no branch for it and no quota constants exist. Left unfixed: real AWS default quota values are undocumented in the SDK itself and would need external verification to avoid fabricating a wrong number. (file bd issue if this surface matters for a client under test)"
  - "updateProbeRequest.tags field (models.go) is accepted on PATCH /monitors/{name}/probes/{probeId} and applied to the probe's tags, but the real UpdateProbeInput has no Tags member at all -- the real SDK client can never send it. Harmless (dead code path from genuine SDK traffic, response still returns current tags via UpdateProbeOutput.Tags) but worth removing in a future cleanup pass for exact wire-contract fidelity."
deferred:
  - AccessDeniedException / ThrottlingException wiring (likely handled by shared auth/chaos middleware elsewhere in the stack, not service-specific; not audited here since out of scope for services/networkmonitor edits)
leaks: {status: clean, note: "no goroutines/janitors in this service; InMemoryBackend is a plain locked map+store.Table with no background work"}
---

## Notes

- Wire shapes (field names, epoch-seconds timestamps via `createdAt`/`modifiedAt`,
  flat `probeWireBody` for CreateProbe/GetProbe/UpdateProbe responses, nested
  `probes` array on GetMonitor) were checked directly against
  `aws-sdk-go-v2/service/networkmonitor@v1.14.6`'s `serializers.go`/`deserializers.go`
  and confirmed correct -- no changes needed there.
- MonitorState/ProbeState both go straight to `ACTIVE` on create rather than
  the real API's transient `PENDING`. This is deliberately NOT the disguised
  no-op bug class (stuck-forever PENDING) -- it's the opposite (immediately
  usable), which is safe for emulation and intentionally left as-is.
- Real bugs fixed this pass (all in `backend.go`):
  1. **Missing `packetSize` range validation (56-8500)** on `CreateMonitor`'s
     nested probes and `CreateProbe` -- any packetSize was silently accepted.
     Added `validatePacketSize` and wired it into `validateProbeInput`.
  2. **Probe `protocol` case not canonicalised on create** -- a caller
     sending `"tcp"`/`"icmp"` (validated case-insensitively) was stored and
     echoed back lower-case, diverging from the real API's uppercase enum
     wire contract (`"TCP"`/`"ICMP"`). `validateProbeInput` now returns the
     canonicalised value for callers to persist.
  3. **`UpdateProbe` had essentially no validation** -- `protocol`,
     `destinationPort`, `packetSize`, and `state` were all applied verbatim
     with no checks, so a PATCH could set `protocol: "GARBAGE"`,
     `destinationPort: -5`, `packetSize: 99999`, or `state: "BOGUS"` and get
     a 200 OK with the garbage value persisted and echoed back. Added
     `validateUpdateProbeRequest` (field-level validation) and
     `validateProbeUpdateResult` (the TCP-requires-destinationPort
     cross-field invariant against the *effective* post-update values, so
     switching protocol to TCP without a port, or clearing an existing TCP
     probe's port, is correctly rejected).
- Added `TestHandler_RouteMatcher` and
  `TestHandler_ExtractOperation_MethodPathMatrix` in `handler_test.go`:
  previously nothing called `RouteMatcher()` directly or exercised the
  method+path matrix as a table (existing tests only ever called `Handler()`
  end-to-end for the happy path), so a router regression (e.g. accidentally
  matching PUT for updates, matching the wrong signing service) would not
  have been caught.
