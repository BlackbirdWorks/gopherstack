---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: cloudcontrol
sdk_module: aws-sdk-go-v2/service/cloudcontrol@v1.29.15
last_audit_commit: 0689b86e
last_audit_date: 2026-07-26
overall: A            # follow-up pass (gopherstack-c9yf): ResourceModel filter + ClientTokenConflict fixed for real
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "DesiredState now enforced as required (was silently accepted empty, matching CreateResourceInput.DesiredState 'This member is required'); ProgressEvent.ResourceModel populated; AlreadyExistsException/InvalidRequestException HTTP 400"}
  GetResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceNotFoundException HTTP 400"}
  UpdateResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "PatchDocument now enforced as required (was silently no-op'd by applyPatch on an empty/missing patch, matching UpdateResourceInput.PatchDocument 'This member is required'); ClientToken idempotency added (real UpdateResourceInput.ClientToken field was previously dropped entirely -- accepted on the wire but never passed to the backend); ProgressEvent.ResourceModel reflects post-patch Properties; RFC6902 patch applied in place"}
  DeleteResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "ClientToken idempotency added (real DeleteResourceInput.ClientToken field was previously dropped entirely)"}
  ListResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "pagination via pkgs/page; InvalidRequestException on malformed TypeName; now returns defensive copies (see leaks note) instead of live backend pointers; ResourceModel (real 'resource model to use to select the resources to return' field) is now applied as a real filter -- see gopherstack-c9yf fix below"}
  GetResourceRequestStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "unknown token -> RequestTokenNotFoundException (the only error this op declares); output now includes HooksProgressEvent (real field on GetResourceRequestStatusOutput, always empty/omitted -- this backend has no Hooks concept)"}
  CancelResourceRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "unknown token -> RequestTokenNotFoundException; non-IN_PROGRESS status -> ConcurrentModificationException/HTTP 500 -- confirmed against live API reference"}
  ListResourceRequests: {wire: ok, errors: ok, state: ok, persist: ok, note: "INVENTED-FIELD FIX: ResourceRequestStatusFilter.TypeName was NOT a real field (confirmed against both aws-sdk-go-v2/service/cloudcontrol/types and botocore's service-2.json -- the real filter shape has exactly Operations + OperationStatuses, no TypeName) and was silently narrowing results below what real AWS would return for the same filter body; deleted the field and the filtering logic that used it. Operations/OperationStatuses enum validation confirmed correct -- both are closed Smithy string enums in the real model."}
families:
  progress_event_lifecycle: {status: ok, note: "every mutating op completes synchronously to a terminal SUCCESS (or CANCEL_COMPLETE) in the same call -- no PENDING/IN_PROGRESS hang risk since GetResourceRequestStatus/ListResourceRequests read the same requests table that was just written"}
  persistence: {status: ok, note: "Handler/InMemoryBackend both implement Snapshot/Restore (persistence.go), versioned, wired via store.Registry (store_setup.go); confirmed round-trips resources+requests+clientTokens in persistence_test.go. cloudcontrolSnapshotVersion bumped 1->2 this pass: ClientTokens' value type changed from a bare requestToken string to clientTokenEntry{RequestToken,Fingerprint} to support ClientTokenConflictException detection (see client_token_idempotency below) -- a real shape change, so old snapshots are discarded cleanly rather than risking a partial/wrong decode."}
  client_token_idempotency: {status: ok, note: "CreateResource/UpdateResource/DeleteResource all implement ClientToken idempotency (return the cached ProgressEvent on token replay instead of re-processing), matching the real SDK: all three *Input shapes declare a ClientToken member. NEW this pass (gopherstack-c9yf): ClientTokenConflictException is now detected for real -- each cached entry also stores a deterministic fingerprint of the original request (op+TypeName+Identifier+DesiredState/PatchDocument); replaying a token with the SAME fingerprint still idempotently returns the cached event, but replaying it with a DIFFERENT fingerprint (a genuinely different request reusing someone else's token) now returns ClientTokenConflictException/HTTP 400. This is a real, deterministic check -- no fabrication, no fault injection needed. clientTokens' persisted value type changed from a bare string to {requestToken, fingerprint} (backendSnapshot.ClientTokens), so cloudcontrolSnapshotVersion bumped 1->2 (old snapshots discarded cleanly per the existing version-mismatch protocol, not partially decoded)."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "cloudcontrol keeps its own generic resource store; it does NOT delegate to the real per-service backend (e.g. AWS::S3::Bucket via CreateResource does not create a row visible to services/s3's ListBuckets, and vice versa). This is explicitly allowed by the task brief (either design is parity-correct) but is a real cross-service gap for any test that mixes CloudControl and native-service calls against the same logical resource. No bd issue filed yet -- flagging for triage."
  - "TypeNotFoundException (extension not registered in the CFN registry) is unreachable: this backend has no type registry, so any well-formed TypeName (ns::svc::type) is implicitly accepted. GENUINELY IMPOSSIBLE without fabrication (re-triaged gopherstack-c9yf, not fixed): real CloudFormation/CloudControl's registry spans thousands of AWS-published + arbitrarily many privately-registered third-party extension types, and whether a given TypeName is 'registered' is fundamentally an account-specific, mutable fact (types get (de)activated per account/region via RegisterType/DeactivateType, which cloudcontrol's own SDK surface doesn't even expose -- that's CloudFormation's API). Any registry gopherstack could build here would be one of: (a) an arbitrary hardcoded allowlist of 'known' AWS types, which would be incomplete by construction and would make ListResources/CreateResource start REJECTING valid TypeNames this emulator previously accepted -- a regression, not a fix, and itself a fabricated 'known types' dataset; or (b) accept-everything, which is exactly today's (correct, honest) behavior. There is no third option that adds real signal without inventing data. Not fixed."
chaos_coverage:           # errors reachable via pkgs/chaos fault injection rather than backend logic — verified, not a gap
  - "The remaining 12 documented-but-unreachable exceptions from gopherstack-c9yf (ThrottlingException, ServiceLimitExceededException, HandlerFailureException, NotStabilizedException, NotUpdatableException, ResourceConflictException, PrivateTypeException, GeneralServiceException, NetworkFailureException, InvalidCredentialsException, HandlerInternalFailureException, ConcurrentOperationException) are ALREADY COVERED by pkgs/chaos, not a gap needing backend code. Verified concretely: Handler implements service.ChaosProvider (ChaosServiceName()==\"cloudcontrol\", ChaosOperations()==GetSupportedOperations(), ChaosRegions()), so it is enumerated by GET /_gopherstack/chaos/targets. The chaos middleware (pkgs/chaos/middleware.go) runs as global Echo middleware registered via registry.Use(chaos.Middleware(...)) (cli.go:5754) OUTSIDE/BEFORE any service's own routing, and extracts service+operation from the same SigV4 Authorization header + X-Amz-Target header this service's own RouteMatcher/ExtractOperation already rely on (cloudcontrol is awsjson1.0 with X-Amz-Target: CloudApiService.<Op>, so extractOperationFromRequest's X-Amz-Target-after-the-dot parsing resolves the exact operation name, e.g. \"CreateResource\") -- so a fault rule {service: \"cloudcontrol\", operation: \"CreateResource\", error: {code: \"ThrottlingException\", statusCode: 400}} deterministically short-circuits that op with an arbitrary injected Code+StatusCode (FaultError carries both, pkgs/chaos/fault_response.go) before this handler ever runs. Synthesizing these from backend state instead (e.g. fabricating a request-rate counter under a single coarse lock with no real concurrency contention) would be exactly the kind of invented signal this project's honesty rules forbid; fault injection is the correct, non-fabricated mechanism for exceptions AWS only returns under real infrastructure conditions this emulator doesn't have."
deferred: []              # consciously not audited this pass (scope) — next pass targets; none this pass
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

**Fixed this pass (2026-07-26, bd issue gopherstack-c9yf)**:

- `ListResources`' `ResourceModel` field ("The resource model to use to select the resources
  to return", confirmed real on `ListResourcesInput` in `aws-sdk-go-v2/service/cloudcontrol/
  types/types.go` and its serializer, `serializers.go:702-704` -- a plain JSON string field,
  same convention as `DesiredState`/`PatchDocument`) is now decoded and applied as a real
  filter: `InMemoryBackend.ListResources` takes a `resourceModel` JSON-object string and
  `matchesResourceModel` requires every key/value pair in it to match the corresponding key in
  the resource's current `Properties`. An unparseable `ResourceModel` matches nothing (fails
  closed) rather than erroring or being silently ignored. Previously this field was not even
  present on the local decode struct, so it degraded to accepted-and-ignored -- a real gap now
  closed with a generic, non-fabricated match (no per-type schema knowledge needed: the filter
  works identically for any resource type since it only compares against propertydata this
  backend already stores).
- `ClientTokenConflictException` is now detected for real (previously deferred as "would
  require persisting and diffing the full original request, out of scope" -- re-triaged this
  pass and judged not out of scope, since the fingerprint needed is trivial: the op name plus
  the exact fields already being passed to Create/Update/DeleteResource). Each `ClientToken`'s
  cache entry (`clientTokenEntry`) now stores a deterministic fingerprint of the original
  request (`op + TypeName + Identifier + DesiredState/PatchDocument`) alongside the cached
  `RequestToken`. A replay with a matching fingerprint still idempotently returns the cached
  `ProgressEvent` (unchanged behavior); a replay with a *different* fingerprint (same token,
  genuinely different request) now returns `ClientTokenConflictException`/HTTP 400 (confirmed
  `ErrorFault() == smithy.FaultClient` on the real SDK's
  `types.ClientTokenConflictException`, so 400 not 500, matching every other client fault in
  this handler). `clientTokens`' persisted value type changed from a bare `string` to
  `clientTokenEntry{RequestToken, Fingerprint}`, so `cloudcontrolSnapshotVersion` bumped 1->2;
  an old (v1) snapshot is discarded cleanly by the existing version-mismatch path in
  `Restore`, exactly like the v0 (no-persistence-at-all) case before it -- never partially
  decoded as the new shape.
- `TypeNotFoundException` was re-triaged and confirmed GENUINELY IMPOSSIBLE without
  fabrication, not merely deferred -- see the `gaps` entry above for the full reasoning
  (no third design option between "arbitrary incomplete allowlist that regresses previously-
  accepted TypeNames" and "accept everything", which is already the correct, honest behavior).
- The remaining 12 previously-listed "unreachable without chaos" exceptions
  (`ThrottlingException`, `ServiceLimitExceededException`, `HandlerFailureException`,
  `NotStabilizedException`, `NotUpdatableException`, `ResourceConflictException`,
  `PrivateTypeException`, `GeneralServiceException`, `NetworkFailureException`,
  `InvalidCredentialsException`, `HandlerInternalFailureException`,
  `ConcurrentOperationException`) were reclassified from "deferred" to "ALREADY COVERED by
  chaos fault injection" -- see `chaos_coverage` above for the concrete verification (Handler
  implements `service.ChaosProvider`, chaos middleware runs globally ahead of this handler and
  resolves the exact operation via the same `X-Amz-Target` header this service's own routing
  uses, `FaultError` carries an arbitrary injected `Code`+`StatusCode`). These are correctly
  NOT implemented as backend logic: synthesizing them from this emulator's single coarse lock
  with no real concurrency/quota/IAM/network state would itself be fabrication.

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
