---
service: networkmonitor
sdk_module: aws-sdk-go-v2/service/networkmonitor@v1.16.4
last_audit_commit: 7e4e35369
last_audit_date: 2026-07-24
overall: A
ops:
  CreateMonitor: {wire: ok, errors: ok, state: ok, persist: ok, note: "probe packetSize now validated (56-8500); probe protocol now canonicalised to upper-case TCP/ICMP before storage; now enforces the real 'monitors per account per Region' (100) and nested-probe 'probes per monitor' (24) / 'probes per subnet per monitor' (4) service quotas -> ServiceQuotaExceededException (402)"}
  GetMonitor: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMonitor: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMonitor: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMonitors: {wire: ok, errors: ok, state: ok, persist: ok, note: "pagination verified: token-past-end returns empty (not first page), state filter applied post-slice correctly"}
  CreateProbe: {wire: ok, errors: ok, state: ok, persist: ok, note: "same packetSize/protocol-normalisation fixes as CreateMonitor's nested probes; now enforces the real 'probes per monitor' (24) and 'probes per subnet per monitor' (4) service quotas -> ServiceQuotaExceededException (402)"}
  GetProbe: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateProbe: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a validation no-op (see prior-pass notes below); now validates protocol/destinationPort/packetSize/state and the cross-field TCP-requires-destinationPort invariant on the post-update result. The invented updateProbeRequest.tags field (real UpdateProbeInput has no Tags member) has been deleted -- see 'Fixed this pass' below."}
  DeleteProbe: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matching: {status: ok, note: "RouteMatcher gates on signing-service + path prefix only (method-agnostic, correct); ExtractOperation correctly routes UpdateMonitor/UpdateProbe on PATCH (not PUT, which the real API does not expose). TestHandler_RouteMatcher and TestHandler_ExtractOperation_MethodPathMatrix (handler_test.go) exercise RouteMatcher() and the method+path matrix directly."}
gaps: []
deferred:
  - "AccessDeniedException (403) / ThrottlingException (429) are not wired: verified this pass that there is no shared auth/rate-limit middleware anywhere in gopherstack that would inject these for networkmonitor (checked pkgs/chaos, which is fault-injection only, not standard error mapping) -- corrects last pass's unverified guess that such middleware existed. This backend has no auth model and no request-rate accounting, so there is no real condition under which these codes would ever be produced; every other audited service in this repo (ce, pipes, ssoadmin, fis, apprunner, polly) follows the same pattern of only wiring exception branches that have a genuine backend trigger. Re-open if gopherstack ever grows a cross-service auth/throttle layer."
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

### Fixed this pass

1. **Service quotas implemented for real** (previously an open "gap": no
   quota enforcement, with the stated reason that AWS's default quota
   numbers were unverified and fabricating one would violate the no-stub
   rule). Fetched the authoritative numbers from
   <https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html#nw-monitor-quotas>
   (service code `networkmonitor`): "Number of monitors per account per AWS
   region" = 100, "Number of probes per monitor" = 24, "Number of probes per
   subnet for each monitor" = 4 (all adjustable in real AWS; gopherstack
   emulates the unmodified defaults -- see the `max*` constants in
   `store.go`). Enforced in `CreateMonitor` (monitor-count check, plus the
   nested-probes list via the new `buildNestedProbes`/`validateProbeQuotas`
   helpers) and `CreateProbe` (against the monitor's existing probe list).
   Violations return `ErrServiceQuotaExceeded`, mapped by `handleError` to
   HTTP 402 with `X-Amzn-Errortype: ServiceQuotaExceededException`, matching
   the documented status for every op's `ServiceQuotaExceededException` (e.g.
   <https://docs.aws.amazon.com/networkmonitor/latest/APIReference/API_CreateMonitor.html#API_CreateMonitor_Errors>).
   Quota checks run *after* per-field validation of nested probes (so a
   malformed probe still surfaces `ValidationException`, not a quota error,
   when both conditions hold). Locked by `quotas_test.go`.
2. **Deleted the invented `updateProbeRequest.tags` field** (`models.go`):
   the real `UpdateProbeInput` (`aws-sdk-go-v2/service/networkmonitor/api_op_UpdateProbe.go`)
   has no `Tags` member, so a genuine SDK client could never populate it --
   this was dead code that also technically let tags be set via an endpoint
   the real API doesn't support tagging through. Removed the field, its
   `export_test.go` mirror (`UpdateProbeRequestForTest`), and the
   `applyProbeUpdate` branch that copied it onto the probe. `UpdateProbeOutput`
   *does* have a `Tags` member (unaffected -- still served via `probeWireBody`
   in every probe response).
3. **`maxDestinationPort` corrected from 65535 to 65536.** Every real-SDK doc
   comment for a `destinationPort` field (`CreateMonitorProbeInput`,
   `ProbeInput`, `Probe`, `CreateProbeOutput`/`GetProbeOutput`,
   `UpdateProbeInput`/`UpdateProbeOutput` -- `types/types.go` and the
   `api_op_*.go` files) consistently documents the range as "a number between
   1 and 65536", one past TCP's usual 65535 ceiling. gopherstack previously
   used 65535 as the upper bound, which would incorrectly reject the
   documented-valid value 65536.
4. **Deleted the invented, unused `monitorStateDeleted = "DELETED"` constant**
   (`store.go`): the real `MonitorState` enum
   (`aws-sdk-go-v2/service/networkmonitor/types/enums.go`) has exactly five
   values -- `PENDING`, `ACTIVE`, `INACTIVE`, `ERROR`, `DELETING` -- with no
   `DELETED` member (unlike `ProbeState`, which does have one). The constant
   was never referenced anywhere in the backend (`DeleteMonitor` is a hard
   delete, not a state transition), so it was both wrong and dead.
5. **Corrected the `deferred` entry's reasoning for AccessDeniedException /
   ThrottlingException** -- see the `deferred` field above. The previous
   pass's guess ("likely handled by shared auth/chaos middleware") was
   unverified and, on inspection this pass, incorrect: no such middleware
   exists anywhere in gopherstack. The corrected reasoning is that this
   backend simply has no auth/throttling model to trigger these codes from,
   consistent with how every other audited service in the repo handles the
   same two exception types.

### Notes carried over from the prior pass

- **Missing `packetSize` range validation (56-8500)** on `CreateMonitor`'s
  nested probes and `CreateProbe` -- any packetSize was silently accepted.
  Fixed via `validatePacketSize`, wired into `validateProbeInput`.
- **Probe `protocol` case not canonicalised on create** -- a caller sending
  `"tcp"`/`"icmp"` (validated case-insensitively) was stored and echoed back
  lower-case, diverging from the real API's uppercase enum wire contract
  (`"TCP"`/`"ICMP"`). `validateProbeInput` returns the canonicalised value.
- **`UpdateProbe` had essentially no validation** -- `protocol`,
  `destinationPort`, `packetSize`, and `state` were all applied verbatim with
  no checks. Fixed via `validateUpdateProbeRequest` (field-level) and
  `validateProbeUpdateResult` (the TCP-requires-destinationPort cross-field
  invariant against the *effective* post-update values).
- `TestHandler_RouteMatcher` and `TestHandler_ExtractOperation_MethodPathMatrix`
  (`handler_test.go`) exercise `RouteMatcher()` and the method+path matrix
  directly, since prior tests only ever called `Handler()` end-to-end.

### 2026-08-21: gopherstack-r80d batch 21 -- required-output-member cut, 0 bugs

Verified as the largest remaining `gopherstack-r80d` candidate after
sagemaker (22 required output fields, 12 ops, 7 with >=1) via a fresh
`cmd/requiredoutputfields` run cross-checked against
`services/_REQUIRED_OUTPUT_CANDIDATES.md`. Read all 7 ops end to end
against `handler.go`, plus every domain struct in
`networkmonitor@v1.16.4`'s 158-line `types/types.go` (only 4 structs
declare required members at all: `CreateMonitorProbeInput`/`ProbeInput`
are input-only; `MonitorSummary` and `Probe` are the two
output-relevant ones, reachable through `ListMonitorsOutput.Monitors` and
`GetMonitorOutput.Probes` respectively -- neither field is itself required,
so this is the nested-domain-struct undercount class, checked anyway).
Instrument cross-checked three ways (character-level brace matcher,
`go/parser` AST walk, raw `grep -c`) -- all three agree at 52 total
required fields / 22 structs across `types/types.go` + every `api_op_*.go`
file, of which 22 fields across 7 ops are output-relevant (the rest are
`*Input` structs).

Came back clean: `GetMonitorOutput.CreatedAt`/`ModifiedAt` carry
`,omitempty` on `*float64` fields but are structurally unreachable --
`CreateMonitor` (the sole construction site for `Monitor`) unconditionally
stamps both with `time.Now()`, so no real client can observe them
omitted. `MonitorSummary.MonitorArn`/`MonitorName`/`State` and
`Probe.Destination`/`Protocol`/`SourceArn` are plain (non-pointer,
non-omitempty) strings in gopherstack's wire structs, and
`validateProbeInput` rejects an empty `destination`/`protocol`/`sourceArn`
for real (not just a nil check), so they can never be reachably empty
either. `DeleteMonitor`/`DeleteProbe`/`ListTagsForResource`/`TagResource`/
`UntagResource` all have zero required output members on the real SDK, so
their bare/omitted-body responses violate nothing. Verified against the
real deserializer's `awsRestjson1_deserializeOpDocument<Op>Output` bodies
directly (not just the Go field names) for `CreateMonitor`/`GetMonitor` to
confirm the flat, unwrapped response shape and exact epoch-seconds
timestamp encoding. No code changes; see
`services/_REQUIRED_OUTPUT_CANDIDATES.md`'s settled-services table.
