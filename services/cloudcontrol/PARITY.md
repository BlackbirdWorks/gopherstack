---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: cloudcontrol
sdk_module: aws-sdk-go-v2/service/cloudcontrol@v1.29.15
last_audit_commit: 0689b86e
last_audit_date: 2026-07-24
overall: A            # genuine wire/error-code fixes + an invented-field deletion found and applied this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "DesiredState now enforced as required (was silently accepted empty, matching CreateResourceInput.DesiredState 'This member is required'); ProgressEvent.ResourceModel populated; AlreadyExistsException/InvalidRequestException HTTP 400"}
  GetResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceNotFoundException HTTP 400"}
  UpdateResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "PatchDocument now enforced as required (was silently no-op'd by applyPatch on an empty/missing patch, matching UpdateResourceInput.PatchDocument 'This member is required'); ClientToken idempotency added (real UpdateResourceInput.ClientToken field was previously dropped entirely -- accepted on the wire but never passed to the backend); ProgressEvent.ResourceModel reflects post-patch Properties; RFC6902 patch applied in place"}
  DeleteResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "ClientToken idempotency added (real DeleteResourceInput.ClientToken field was previously dropped entirely)"}
  ListResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "pagination via pkgs/page; InvalidRequestException on malformed TypeName; now returns defensive copies (see leaks note) instead of live backend pointers"}
  GetResourceRequestStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "unknown token -> RequestTokenNotFoundException (the only error this op declares); output now includes HooksProgressEvent (real field on GetResourceRequestStatusOutput, always empty/omitted -- this backend has no Hooks concept)"}
  CancelResourceRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "unknown token -> RequestTokenNotFoundException; non-IN_PROGRESS status -> ConcurrentModificationException/HTTP 500 -- confirmed against live API reference"}
  ListResourceRequests: {wire: ok, errors: ok, state: ok, persist: ok, note: "INVENTED-FIELD FIX: ResourceRequestStatusFilter.TypeName was NOT a real field (confirmed against both aws-sdk-go-v2/service/cloudcontrol/types and botocore's service-2.json -- the real filter shape has exactly Operations + OperationStatuses, no TypeName) and was silently narrowing results below what real AWS would return for the same filter body; deleted the field and the filtering logic that used it. Operations/OperationStatuses enum validation confirmed correct -- both are closed Smithy string enums in the real model."}
families:
  progress_event_lifecycle: {status: ok, note: "every mutating op completes synchronously to a terminal SUCCESS (or CANCEL_COMPLETE) in the same call -- no PENDING/IN_PROGRESS hang risk since GetResourceRequestStatus/ListResourceRequests read the same requests table that was just written"}
  persistence: {status: ok, note: "Handler/InMemoryBackend both implement Snapshot/Restore (persistence.go), versioned, wired via store.Registry (store_setup.go); confirmed round-trips resources+requests+clientTokens in persistence_test.go. cloudcontrolSnapshotVersion left at 1 -- the new ProgressEvent fields (ErrorCode/HooksRequestToken/RetryAfter) are additive/optional and JSON-decode-compatible with any snapshot written before this pass."}
  client_token_idempotency: {status: ok, note: "NEW this pass: CreateResource's existing ClientToken idempotency (return the cached ProgressEvent on token replay instead of re-processing) is now also implemented for UpdateResource and DeleteResource, matching the real SDK: all three *Input shapes declare a ClientToken member. Previously only CreateResourceInput's ClientToken was wired through the handler at all -- updateResourceInput/deleteResourceInput had no ClientToken field, so the real SDK's ClientToken on those two ops was silently dropped on the wire. Shared cachedEventForToken/rememberClientToken helpers factor the now-3x-duplicated logic. ClientTokenConflictException (reuse of a token across a genuinely different request) is still NOT detected -- see deferred."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "cloudcontrol keeps its own generic resource store; it does NOT delegate to the real per-service backend (e.g. AWS::S3::Bucket via CreateResource does not create a row visible to services/s3's ListBuckets, and vice versa). This is explicitly allowed by the task brief (either design is parity-correct) but is a real cross-service gap for any test that mixes CloudControl and native-service calls against the same logical resource. No bd issue filed yet -- flagging for triage."
  - "TypeNotFoundException (extension not registered in the CFN registry) is unreachable: this backend has no type registry, so any well-formed TypeName (ns::svc::type) is implicitly accepted. Not fixed -- would require building a registry concept out of scope for this pass."
  - "ListResourcesInput.ResourceModel ('The resource model to use to select the resources to return') is a real input field this backend accepts on the wire (unknown-field JSON decode is a no-op, not an error) but never applies as a filter -- this backend has no secondary resource-model index to filter against. Low-impact/rarely-used field; not fixed this pass."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Full errCodeLookup coverage for the remaining documented-but-unreachable exceptions (ThrottlingException, ServiceLimitExceededException, HandlerFailureException, NotStabilizedException, NotUpdatableException, ResourceConflictException, PrivateTypeException, GeneralServiceException, NetworkFailureException, InvalidCredentialsException, HandlerInternalFailureException, ConcurrentOperationException, ClientTokenConflictException). None of these are currently producible by this backend's logic (no chaos-injection wiring specific to cloudcontrol beyond the generic ChaosServiceName/ChaosOperations hooks), so adding dead mapping cases was judged out of scope/gold-plating this pass. Revisit if chaos fault injection or a richer validation model is added."
  - "ClientTokenConflictException specifically: reusing the same ClientToken across a genuinely DIFFERENT request (different TypeName/Identifier/op) is not detected -- the cached ProgressEvent is returned unconditionally on any token match, same simplification CreateResource already made pre-existing this pass, now applied consistently to Update/Delete too. Real conflict detection would require persisting and diffing the full original request, out of scope."
leaks: {status: clean, note: "no goroutines/timers/janitors; InMemoryBackend is pure lockmetrics.RWMutex + store.Table state, no background work. FIXED this pass: ListResources/ListAllResources previously returned *Resource pointers live inside the backend's store.Table -- a caller mutating one directly corrupted backend state without the lock, a real (if previously unexploited -- no current caller retains/mutates the result) mutation-safety hole. Now copies on the way out, matching every other accessor. Locked in by TestBackend_ListAllResources_ReturnsCopiesNotLiveState / TestBackend_ListResources_ReturnsCopiesNotLiveState."}
---

## Notes

Protocol: awsjson1.0 (`application/x-amz-json-1.0`, `X-Amz-Target: CloudApiService.<Op>`).
Confirmed against the real SDK client package (target prefix, content-type, error envelope
`{"__type": "...", "message": "..."}`).

**Every op completes synchronously to a terminal status** (SUCCESS on Create/Update/Delete,
CANCEL_COMPLETE on Cancel). This is a deliberate, parity-acceptable design choice per the task
brief ("EITHER can be parity-correct") -- there is no PENDING/IN_PROGRESS hang risk because
GetResourceRequestStatus and ListResourceRequests read the same `requests` store.Table that
CreateResource/UpdateResource/DeleteResource just wrote to in the same call. The only way to
observe an IN_PROGRESS event is the test-only `AddProgressEvent` helper, used to exercise the
CancelResourceRequest "only PENDING/IN_PROGRESS is cancellable" rule.

**Fixed this pass (2026-07-24)**:

- `ProgressEvent` gained `ErrorCode`, `HooksRequestToken`, `RetryAfter` (all real fields on
  `types.ProgressEvent`, confirmed against `aws-sdk-go-v2/service/cloudcontrol/types/types.go`
  and its deserializer -- `RetryAfter` is epoch-seconds like `EventTime`, not an ISO8601
  string). All three are always empty/omitted today since this backend never leaves a request
  non-terminal or FAILED -- modeled for wire-shape parity, not gold-plated behavior.
- `GetResourceRequestStatusOutput` gained `HooksProgressEvent` (real field on the real output
  shape); always empty/omitted since this backend has no Hooks concept.
- **Deleted an invented field**: `ResourceRequestStatusFilter.TypeName`. Confirmed absent from
  both the aws-sdk-go-v2 types package and botocore's `service-2.json` -- the real filter has
  only `Operations` and `OperationStatuses`. The prior implementation filtered
  `ListResourceRequests` results by it, a genuine wire-shape bug (narrower results than real AWS
  would ever return for an equivalent filter body). `TestHandler_ListResourceRequests_TypeNameFilter`
  (which asserted the old, wrong filtering behavior) was rewritten as
  `TestHandler_ListResourceRequests_TypeNameFilterIsIgnored` to assert the correct (no-op)
  behavior.
- `CreateResource` now rejects a missing/empty `DesiredState` with `InvalidRequestException` --
  `DesiredState` is "This member is required" on the real `CreateResourceInput`; previously
  silently accepted and created a resource with empty `Properties`.
- `UpdateResource` now rejects a missing/empty `PatchDocument` with `InvalidRequestException` --
  `PatchDocument` is "This member is required" on the real `UpdateResourceInput`; previously
  silently accepted and `applyPatch` no-op'd on the unparseable empty string instead of the
  request being rejected.
- `UpdateResource`/`DeleteResource` now accept and honor `ClientToken` for idempotency (real
  `UpdateResourceInput.ClientToken` / `DeleteResourceInput.ClientToken` were previously entirely
  absent from gopherstack's input structs -- accepted-and-dropped on the wire, not merely
  unused). Shared `cachedEventForToken`/`rememberClientToken` helpers factor the now-3x logic
  that used to live only in `CreateResource`.
- `ListResources`/`ListAllResources` now return defensive copies of `*Resource` instead of the
  live pointers held inside the backend's `store.Table` (`pkgs/store.Table.Range`/`All` perform
  no copying themselves -- that is documented as the owning backend's responsibility). Every
  other accessor (`GetResource`, the `ProgressEvent` returned by Create/Update/DeleteResource)
  already copied; this closes the one remaining hole where a caller could mutate backend state
  without holding the lock, bypassing `UpdateResource`'s own patch semantics entirely.

**Error-code bugs fixed in a prior pass, still correct, don't re-flag**:

- `ValidationException` does not exist anywhere in CloudControl's error model (verified: absent
  from botocore's `service-2.json` shapes). Every operation instead declares
  `InvalidRequestException` ("invalid input from the user has generated a generic exception")
  as the generic input-validation error.
- `GetResourceRequestStatus` declares **only** `RequestTokenNotFoundException` as an error --
  an unrecognized RequestToken must not surface as `ResourceNotFoundException`.
  `CancelResourceRequest` declares the same plus `ConcurrentModificationException`.
- `CancelResourceRequest` on a non-PENDING/non-IN_PROGRESS request returns
  `ConcurrentModificationException` (HTTP 500), not a client validation error.
- HTTP status codes: per the live API reference, virtually every CloudControl client-fault
  exception (`ResourceNotFoundException`, `AlreadyExistsException`, `InvalidRequestException`,
  `RequestTokenNotFoundException`, etc.) is **HTTP 400**. Only a handful of server-fault
  exceptions (`ConcurrentModificationException`, `ServiceInternalErrorException`, etc.) are
  HTTP 500.

**This pass's field-diff method**: `aws-sdk-go-v2/service/cloudcontrol@v1.29.15` was already
present in the module cache (`go env GOMODCACHE`), so every `types.*` struct, every
`api_op_*.go` Input/Output struct, and the `awsAwsjson10_deserializeDocumentProgressEvent`
generated deserializer were read directly -- not inferred from documentation prose. The
`ResourceRequestStatusFilter.TypeName` deletion was additionally cross-checked against
botocore's `service-2.json` (gunzipped from the installed `botocore` package) to rule out a
version-lag false positive in the Go SDK snapshot; both sources agree the field does not exist.

**Traps for the next auditor** (already correct, don't re-flag):

- `Properties`/`DesiredState`/`PatchDocument`/`ResourceModel` are all JSON **strings**, never
  decoded objects, on the wire -- confirmed correct throughout.
- `EventTime` and `RetryAfter` are epoch-seconds as a JSON **number** via the local
  `unixEpochTime` wrapper, not a timestamp string -- correct, and covered by
  `TestHandler_EventTimeIsUnixNumber`. `RetryAfter` is `*unixEpochTime` (nullable) since real
  AWS only ever sets it on a non-terminal PENDING/IN_PROGRESS status, which this backend never
  produces -- always nil/omitted today, that is correct, not a bug.
- `identifierKeys` in resources.go is a best-effort heuristic (no CFN schema registry backs this
  emulator), documented inline with the specific resource types each key maps to. This is an
  intentional simplification, not a bug: real CloudControl derives the identifier from the
  resource type's schema-declared `primaryIdentifier`, which isn't tracked here.
- The backend is a single self-contained generic store, not a fan-out to per-service backends
  (see gaps above) -- this was independently verified against the task brief's explicit
  either-is-acceptable framing, not overlooked.
- `RoleArn`/`TypeVersionId` are real input members on every mutating op's `*Input` shape but are
  not modeled on gopherstack's decode structs. This is NOT a wire bug: `encoding/json` silently
  ignores unrecognized object members on decode, so a real client sending these fields degrades
  gracefully (accepted-and-ignored) rather than erroring. Not worth the struct-field noise across
  every op given neither field changes response shape or observable behavior in this emulator
  (no IAM role assumption, no private-type-version registry).
