---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: cloudcontrol
sdk_module: aws-sdk-go-v2/service/cloudcontrol@v1.29.15
last_audit_commit: 0689b86e
last_audit_date: 2026-07-13
overall: A            # genuine wire/error-code fixes found and applied this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "ProgressEvent.ResourceModel now populated; AlreadyExistsException/InvalidRequestException now HTTP 400 (was 409/ValidationException)"}
  GetResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceNotFoundException now HTTP 400 (was 404)"}
  UpdateResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "ProgressEvent.ResourceModel now reflects post-patch Properties; RFC6902 patch applied in place"}
  DeleteResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "pagination via pkgs/page; InvalidRequestException on malformed TypeName"}
  GetResourceRequestStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "unknown token now RequestTokenNotFoundException (was ResourceNotFoundException) -- this op declares RequestTokenNotFoundException as its ONLY error"}
  CancelResourceRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "unknown token -> RequestTokenNotFoundException; non-IN_PROGRESS status -> ConcurrentModificationException/HTTP 500 (was ValidationException/400) -- confirmed against live API reference"}
  ListResourceRequests: {wire: ok, errors: ok, state: ok, persist: ok, note: "filter/sort/paginate; enum validation on Operations/OperationStatuses (undocumented for this op but harmless)"}
families:
  progress_event_lifecycle: {status: ok, note: "every mutating op completes synchronously to a terminal SUCCESS (or CANCEL_COMPLETE) in the same call -- no PENDING/IN_PROGRESS hang risk since GetResourceRequestStatus/ListResourceRequests read the same requests table that was just written"}
  persistence: {status: ok, note: "Handler/InMemoryBackend both implement Snapshot/Restore (persistence.go), versioned, wired via store.Registry (store_setup.go); confirmed round-trips resources+requests+clientTokens in persistence_test.go"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "cloudcontrol keeps its own generic resource store; it does NOT delegate to the real per-service backend (e.g. AWS::S3::Bucket via CreateResource does not create a row visible to services/s3's ListBuckets, and vice versa). This is explicitly allowed by the task brief (either design is parity-correct) but is a real cross-service gap for any test that mixes CloudControl and native-service calls against the same logical resource. No bd issue filed yet -- flagging for triage."
  - "ProgressEvent is still missing ErrorCode, RetryAfter, and HooksRequestToken (all real fields per types.ProgressEvent). Not fixed this pass: backend never produces a FAILED/async-pending terminal state (every op completes synchronously to SUCCESS), so these fields would always be empty/unused today. Worth adding if/when an async FAILED path is implemented."
  - "TypeNotFoundException (extension not registered in the CFN registry) is unreachable: this backend has no type registry, so any well-formed TypeName (ns::svc::type) is implicitly accepted. Not fixed -- would require building a registry concept out of scope for this pass."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Full errCodeLookup coverage for the remaining documented-but-unreachable exceptions (ThrottlingException, ServiceLimitExceededException, HandlerFailureException, NotStabilizedException, NotUpdatableException, ResourceConflictException, PrivateTypeException, GeneralServiceException, NetworkFailureException, InvalidCredentialsException, HandlerInternalFailureException, ConcurrentOperationException, ClientTokenConflictException). None of these are currently producible by this backend's logic (no chaos-injection wiring specific to cloudcontrol beyond the generic ChaosServiceName/ChaosOperations hooks), so adding dead mapping cases was judged out of scope/gold-plating this pass. Revisit if chaos fault injection or a richer validation model is added."
leaks: {status: clean, note: "no goroutines/timers/janitors; InMemoryBackend is pure lockmetrics.RWMutex + store.Table state, no background work"}
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

**Error-code bugs fixed this pass** (all confirmed against the live AWS API reference pages,
not just the aws-sdk-go-v2 Go types, since HTTP status codes aren't visible in the vendored
SDK source):

- `ValidationException` does not exist anywhere in CloudControl's error model (verified: absent
  from botocore's `service-2.json` shapes). Every operation instead declares
  `InvalidRequestException` ("invalid input from the user has generated a generic exception")
  as the generic input-validation error. The `ErrValidation` sentinel's wire code was renamed
  accordingly.
- `GetResourceRequestStatus` declares **only** `RequestTokenNotFoundException` as an error --
  an unrecognized RequestToken must not surface as `ResourceNotFoundException` (that describes
  a missing *resource*, this op has no resource in its request shape at all, only a token).
  `CancelResourceRequest` declares the same plus `ConcurrentModificationException`.
- `CancelResourceRequest` on a non-PENDING/non-IN_PROGRESS request returns
  `ConcurrentModificationException` (HTTP 500 -- "The resource is currently being modified by
  another operation"), confirmed by fetching the live API reference page's Errors section --
  NOT a client validation error. The prior code's own comment claimed intent to match
  `UnsupportedActionException` but actually implemented `ValidationException`; both were wrong.
- HTTP status codes: per the live API reference, virtually every CloudControl client-fault
  exception (`ResourceNotFoundException`, `AlreadyExistsException`, `InvalidRequestException`,
  `RequestTokenNotFoundException`, etc.) is **HTTP 400**, not a semantically-matched REST code
  (404/409). Only a handful of server-fault exceptions
  (`ConcurrentModificationException`, `ServiceInternalErrorException`, `HandlerFailureException`,
  etc.) are HTTP 500. gopherstack previously used 404 for ResourceNotFoundException and 409 for
  AlreadyExistsException; both are now 400. (Practical client impact is small since aws-sdk-go-v2
  identifies error types by the `__type` field, not HTTP status, but this is a genuine wire-shape
  divergence worth matching exactly.)
- The handler's default/unmapped-error branch previously used a bespoke `{"message": ...}` JSON
  shape via `c.JSON` instead of the service's `__type`/`message` envelope -- fixed to use
  `marshalError("ServiceInternalErrorException", ...)` for wire consistency, since a client
  parsing `__type` off an unrecognized-shape 500 would get nothing to match against.

**Wire-shape gap fixed**: `ProgressEvent.ResourceModel` (a JSON string of the resource's current
properties) was entirely absent from the struct, even though it's a documented field on every
real `ProgressEvent` and is commonly read directly by CDK/Pulumi/Terraform-provider-style
clients to avoid a follow-up `GetResource` round-trip after a `CreateResource`/`UpdateResource`
call. Now populated: `CreateResource` sets it to the stored `DesiredState`, `UpdateResource`
sets it to the post-patch `Properties`, and `CancelResourceRequest` carries the original event's
value forward. `DeleteResource` leaves it empty (resource no longer exists), matching that the
field is `omitempty`.

**Traps for the next auditor** (already correct, don't re-flag):

- `Properties`/`DesiredState`/`PatchDocument`/`ResourceModel` are all JSON **strings**, never
  decoded objects, on the wire -- confirmed correct throughout (`resourceDescription.Properties
  string`, etc.).
- `EventTime` is epoch-seconds as a JSON **number** via the local `unixEpochTime` wrapper, not a
  timestamp string -- correct, and covered by `TestHandler_EventTimeIsUnixNumber`.
- `identifierKeys` in backend.go is a best-effort heuristic (no CFN schema registry backs this
  emulator), documented inline with the specific resource types each key maps to. This is an
  intentional simplification, not a bug: real CloudControl derives the identifier from the
  resource type's schema-declared `primaryIdentifier`, which isn't tracked here.
- The backend is a single self-contained generic store, not a fan-out to per-service backends
  (see gaps above) -- this was independently verified against the task brief's explicit
  either-is-acceptable framing, not overlooked.
