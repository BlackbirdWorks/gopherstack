---
service: fis
sdk_module: aws-sdk-go-v2/service/fis@v1.37.18   # version audited against
last_audit_commit: f8a54fdb                       # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: A            # genuine wire/error-code fixes found and applied
ops:
  CreateExperimentTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  GetExperimentTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateExperimentTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteExperimentTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: cascades target-account-configs + idempotency-token entries}
  ListExperimentTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  StartExperiment: {wire: ok, errors: ok, state: ok, persist: ok, note: real async goroutine lifecycle; see Notes}
  GetExperiment: {wire: ok, errors: ok, state: ok, persist: ok}
  StopExperiment: {wire: ok, errors: ok (fixed), state: ok, persist: ok, note: 'was wrongly 409 ConflictException on not-running; StopExperiment has no ConflictException case in the SDK — fixed to 400 ValidationException'}
  ListExperiments: {wire: ok, errors: ok, state: ok, persist: ok, note: experimentTemplateId/status query filters applied before pagination}
  ListExperimentResolvedTargets: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetAction: {wire: ok, errors: ok (fixed), state: ok, persist: n/a (built-in + provider-derived catalog)}
  ListActions: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetTargetResourceType: {wire: ok, errors: ok (fixed), state: ok, persist: n/a}
  ListTargetResourceTypes: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetSafetyLever: {wire: ok, errors: ok (fixed), state: ok, persist: ok}
  UpdateSafetyLeverState: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok (fixed), state: ok, persist: ok, note: 50-tag quota + aws:-prefix rejection enforced}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok (fixed), state: ok, persist: n/a}
  CreateTargetAccountConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTargetAccountConfiguration: {wire: ok, errors: ok (fixed), state: ok, persist: ok}
  GetTargetAccountConfiguration: {wire: ok, errors: ok (fixed), state: ok, persist: ok}
  UpdateTargetAccountConfiguration: {wire: ok, errors: ok (fixed), state: ok, persist: ok}
  ListTargetAccountConfigurations: {wire: ok, errors: ok (fixed), state: ok, persist: ok}
  GetExperimentTargetAccountConfiguration: {wire: ok, errors: ok (fixed), state: ok, persist: n/a (derived from template's target-account-configs)}
  ListExperimentTargetAccountConfigurations: {wire: ok, errors: ok (fixed), state: ok, persist: n/a}
families:
  route_matcher: {status: ok, note: 'RouteMatcher/parseFISPath path+method map verified 1:1 against every serializers.go SplitURI+request.Method in the pinned SDK; all 25 ops match exactly (an extra non-AWS POST /experiments/{id}/stop alias is additive and does not collide with any real route)'}
  experiment_lifecycle: {status: ok, note: 'real background goroutine state machine: pending→initiating→running→completing/stopping→completed/stopped/failed; StopExperiment cancels via context; snapshot/restore cancels in-flight goroutines and marks non-terminal experiments failed (no stuck-pending disguised no-op)'}
  error_taxonomy: {status: fixed, note: 'see Notes — this was the main defect class found this sweep'}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - Experiment reports (ExperimentReportConfiguration / ExperimentReport / GetExperiment "experimentReport" + "experimentReportConfiguration" fields) are entirely unimplemented — not in models.go, not in wire DTOs. Real FIS added this feature after the original build-out.
  - Running-experiment ExperimentTarget DTO omits Filters/ResourceTags/SelectionMode (present on the real wire shape as informational metadata alongside the resolved ResourceArns); ExperimentAction DTO omits Description. Both are additive, non-breaking omissions (zero-value on absence), not incorrect on the fields that are present.
  - "startAfter" dependency waiting in executeActionsOrdered is a no-op (topoSortActions already produces a valid topological order, so the loop's `if !completed[dep] { continue }` never actually blocks) — functionally correct today because execution is sequential and single-threaded, but if actions ever run concurrently this would need a real wait/signal, not a comment-only guard.
  - Experiment/action provider quota-vs-lock race: StartExperiment reads experimentCount/leverEngaged under RLock, releases it, then re-locks to write — two concurrent StartExperiment calls could both pass the maxExperiments=1000 check before either writes. Low real-world impact (large headroom, not attacker-adjacent in an emulator), not fixed this sweep.
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - Experiment report configuration / report generation feature surface (see gaps)
  - Built-in action catalog completeness vs the full real AWS FIS action list (gopherstack ships a curated subset across EC2/RDS/ECS/EKS/DynamoDB/Lambda/SSM/network/CloudWatch/Kinesis + the aws:fis:inject-api-*/wait built-ins; real AWS has more actions per service and evolves this list independently of the API shape)
leaks: {status: clean, note: 'Restore() cancels in-flight experiment goroutines before replacing state; Shutdown() (service.Shutdowner) cancels all running experiments; janitor sweeps terminal experiments past TTL under the coarse lock with a pre-snapshotted slice so Delete-while-iterating is safe'}
---

## Notes

**Error taxonomy was the main defect class this sweep.** FIS's actual API model
(cross-checked against `aws-sdk-go-v2/service/fis@v1.37.18`'s `deserializers.go` and
`types/errors.go`, plus `aws-sdk-go@v1.55.5`'s `api-2.json` for HTTP status codes)
defines **exactly four** exception shapes for the *entire service*, and every
operation's generated per-op error deserializer only recognizes a subset of these by
`__type` string match — anything else falls through to `smithy.GenericAPIError`,
which breaks `errors.As(err, &types.ResourceNotFoundException{})`-style typed
handling in real SDK client code:

- `ValidationException` — HTTP 400
- `ResourceNotFoundException` — HTTP 404 (the **only** not-found shape; there is no
  `ExperimentTemplateNotFoundException` / `ExperimentNotFoundException` /
  `ActionNotFoundException` / `TargetResourceTypeNotFoundException` /
  `SafetyLeverNotFoundException` / `TargetAccountConfigurationNotFoundException` in
  the real model — FIS collapses every not-found case onto one shape)
- `ConflictException` — HTTP 409 (only declared for `CreateExperimentTemplate`,
  `CreateTargetAccountConfiguration`, `StartExperiment`, `UpdateSafetyLeverState` —
  **not** `StopExperiment`)
- `ServiceQuotaExceededException` — HTTP **402** (Payment Required — an unusual but
  confirmed choice per the model; not 429)

Before this sweep, `handler.go`'s `exceptionTypeFor`/`writeBackendError` fabricated
six non-existent per-resource `*NotFoundException` type names, a non-existent
`TooManyTagsException`, sent `ConflictException`/409 for `StopExperiment` on a
non-running experiment (unrecognized by that op's real deserializer), and used HTTP
429 instead of 402 for the experiment-count quota. Consolidated into a single
`classifyError` returning `{exceptionType, httpStatus}` so the two concerns can't
drift apart again.

**`ExperimentStatusError` wire field names were wrong and the field was dead code.**
The real `types.ExperimentError` shape serializes as `{"code", "location",
"accountId"}`; gopherstack emitted `{"exceptionName", "accountId"}` — a real SDK
client reading `experiment.State.Error.Code` would always see `nil` since the
deserializer silently drops unknown JSON keys. Worse, nothing in `backend.go` ever
populated `Status.Error` in the first place (only `Reason` was set on failure), so the
field was unreachable in both directions. Fixed: renamed to `Code`/`Location` to
match the wire shape, and `markExperimentFailed` now populates
`Code: "ActionExecutionFailed"` + `Location: <failing action name>` +
`AccountID` when an external action provider fails.

**`toUnix` was a local reimplementation of `pkgs/awstime.Epoch`** (byte-for-byte
identical formula, `UnixNano()/1e9`) minus the zero-time guard — `pkgs/awstime.Epoch`
returns `0` for a zero `time.Time`, `toUnix` would have returned a large negative
number. No live code path passes a zero `time.Time` to it today (all CreationTime/
StartTime fields are set from `time.Now()` at construction), so this was latent
rather than currently user-visible, but it duplicated a pkg the memory doc explicitly
calls out. Now delegates to `awstime.Epoch`.

**Duplicate `status`/`state` JSON keys are intentional and harmless, not a bug.** The
real wire shape only has a top-level `"state"` key on `Experiment`/`ExperimentAction`
(confirmed via `deserializers.go`'s `case "state":` in
`awsRestjson1_deserializeDocumentExperiment`/`...ExperimentAction`); gopherstack's
DTOs also emit an identical `"status"` key alongside it. Real SDK deserializers
silently ignore unrecognized keys (`default: _, _ = key, value` in every generated
`deserializeDocument*` function), so this doesn't break real clients — left as-is
rather than removed, since some non-SDK/raw-HTTP consumers in this codebase may read
`"status"`.

**Route matcher verified op-by-op.** `RouteMatcher`/`parseFISPath` were checked
against every `httpbinding.SplitURI(...)` + `request.Method = "..."` pair across all
25 operations in `serializers.go` — every path prefix, HTTP verb, and nested
sub-resource segment (`/experimentTemplates/{id}/targetAccountConfigurations/{accountId}`,
`/experiments/{id}/resolvedTargets`, `/safetyLevers/{id}/state`, `/tags/{resourceArn}`)
matches exactly. `parseFISSafetyLeverPath`/`parseFISTemplateSubPath` only inspect
`segs[1]` for the ID and ignore trailing segments like `/state`, which is
intentionally permissive (matches the real 3-segment `UpdateSafetyLeverState` path
without needing an exact-length check) rather than a bug.
