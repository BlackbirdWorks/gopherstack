---
service: sagemakerruntime
sdk_module: aws-sdk-go-v2/service/sagemakerruntime@v1.39.3
last_audit_commit: 95ab0584
last_audit_date: 2026-07-24
overall: A            # genuine fixes found this pass (3): EndpointName existence/InService validation wired cross-service, NewSessionId Expires= wire-format bug, ClosedSessionId/session-expiry enforcement
ops:
  InvokeEndpoint: {wire: ok, errors: ok, state: ok, persist: n/a, note: "sync op; EndpointName is now validated against the wired services/sagemaker endpoint registry (existence + InService); NewSessionId's Expires= attribute now matches the SDK's RFC-3339 wire format; ClosedSessionId is now emitted when an expired session is touched. body is an opaque mock, other headers round-trip correctly"}
  InvokeEndpointAsync: {wire: ok, errors: ok, state: ok, persist: ok, note: "returns InferenceId (JSON body)/OutputLocation/FailureLocation headers correctly; EndpointName now validated like the other two ops"}
  InvokeEndpointWithResponseStream: {wire: ok, errors: ok, state: ok, persist: n/a, note: "event-stream framing (prelude/header/payload/CRC) verified against smithy-go wire format; EndpointName now validated; SessionId only touches, never creates (per SDK doc: sessions can't be created via this op), and InvokeEndpointWithResponseStreamOutput has no ClosedSessionId member so expiry-driven closure is a side effect only here, never surfaced on this response"}
families:
  sessions: {status: ok, note: "NEW_SESSION creation, FIFO eviction (maxSessions=1000), TouchSession on subsequent calls, ExpiresAt now enforced (session past its ExpiresAt is evicted and reported via ClosedSessionId on InvokeEndpoint; see SessionTouchOutcome) -- all covered."}
  invocation_history: {status: ok, note: "bounded FIFO (maxInvocationHistory=1000), persisted."}
  endpoint_validation: {status: ok, note: "EndpointLookup (endpoint_lookup.go) is a minimal interface satisfied directly by *sagemaker.InMemoryBackend's exported DescribeEndpoint method; wired at Provider.Init via wireEndpointLookup (provider.go), following the services/cloudwatchlogs/provider.go s3HandlerProvider precedent -- no change to services/sagemaker was needed, since DescribeEndpoint was already an exported, lock-safe read accessor. Unknown EndpointName and known-but-not-InService both surface real AWS's 'Endpoint <name> of account <account> not found.' ValidationError message (confirmed against real-world AWS error reports: an endpoint still Creating is reported as not-found from InvokeEndpoint's perspective too, since the runtime routing table only serves InService endpoints). When no lookup is wired (bare NewInMemoryBackend, e.g. every pre-existing test in this package), validation is a no-op, preserving standalone behaviour."}
gaps: []
deferred: []
leaks: {status: clean, note: "sessions/asyncInvocations/invocations are all FIFO-capped (maxSessions/maxAsyncInvocations/maxInvocationHistory=1000); no goroutines, no janitor (Shutdown is a documented no-op). New endpointLookup field is a plain interface reference (no goroutine, no owned resource); SetEndpointLookup/validateEndpoint both take/release the backend's own lock before calling out to the (separately-locked) sagemaker backend, so no lock is held across the cross-service call and no lock-ordering cycle is introduced."}
---

## Notes

Protocol: restjson1. Three ops, disambiguated purely by path suffix (no
X-Amz-Target header): `/endpoints/{EndpointName}/invocations`,
`/endpoints/{EndpointName}/async-invocations`,
`/endpoints/{EndpointName}/invocations-response-stream`. EndpointName is a
path segment (`extractEndpointName` cuts on the first `/` after the
`/endpoints/` prefix), never a query param or header.

**Bugs found and fixed this pass (2026-07-24; see git diff for exact lines):**

1. **EndpointName existence/InService was never validated.** Real AWS returns
   `ValidationError` ("Endpoint <name> of account <account> not found.") for
   both an unknown EndpointName and one that has not yet reached InService
   (confirmed against real-world AWS error reports: an endpoint still
   `Creating` is reported the same way, since the runtime's routing table
   only serves InService endpoints). The previous audit correctly identified
   this as a genuine gap but deferred it as requiring cross-service backend
   wiring "out of scope for a same-service-only pass." This pass wired it:
   `endpoint_lookup.go` defines a minimal `EndpointLookup` interface
   (`DescribeEndpoint(ctx, name) (*sagemaker.Endpoint, error)`), already
   satisfied by `*sagemaker.InMemoryBackend` with zero changes to
   `services/sagemaker` (its `DescribeEndpoint` was already an exported,
   lock-safe read accessor). `provider.go`'s `wireEndpointLookup` connects
   the two at `Provider.Init` time via a private `sagemakerHandlerProvider`
   interface type-asserted against `ctx.Config`, following the exact
   precedent of `services/cloudwatchlogs/provider.go`'s
   `s3HandlerProvider`/`wireExportSink` (NOT the much larger
   `services/cloudformation`-style `BackendsProvider`, which would have been
   overkill for a single dependency). When unwired (every pre-existing test
   in this package constructs a bare `NewInMemoryBackend`), validation is a
   no-op, preserving prior behaviour for standalone use.

2. **`NewSessionId`'s `Expires=` attribute used the wrong wire format.**
   `handler.go` formatted it as an RFC 1123 HTTP-date
   (`"Mon, 02 Jan 2006 15:04:05 GMT"`), but the SDK model's
   `NewSessionResponseHeader` shape declares the pattern
   `^[a-zA-Z0-9](-*[a-zA-Z0-9])*;\sExpires=[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$`
   (confirmed against botocore's `sagemaker-runtime` `service-2.json`) --
   i.e. an RFC 3339 timestamp with no fractional seconds. A client validating
   or parsing the header against that pattern would have rejected
   gopherstack's previous output. Fixed to
   `session.ExpiresAt.UTC().Format("2006-01-02T15:04:05Z")`; see
   `TestNewSessionHeaderFormat_MatchesSDKPattern`.

3. **`ClosedSessionId` was never emitted; session `ExpiresAt` was tracked but
   never enforced.** `TouchSession` now returns a `SessionTouchOutcome`: if
   the touched session has passed its `ExpiresAt`, it is evicted and
   `ClosedSessionID` is set, which `InvokeEndpoint` surfaces as the
   `X-Amzn-SageMaker-Closed-Session-Id` response header (matching
   `InvokeEndpointOutput.ClosedSessionId`). `InvokeEndpointWithResponseStream`
   calls `TouchSession` too but discards the outcome, since
   `InvokeEndpointWithResponseStreamOutput` has no `ClosedSessionId` member
   in the SDK model -- only `InvokeEndpointOutput` does. A test-only
   `ExpireSessionForTest` helper (`export_test.go`, following
   `services/sagemaker/export_test.go`'s precedent) forces a session's
   `ExpiresAt` into the past deterministically, since AWS provides no real
   API to trigger session closure (it's entirely model/container-driven).

**Traps for the next auditor (looks-wrong-but-correct):**

- `handleInvokeEndpointAsync` accepts a client-supplied
  `X-Amzn-Sagemaker-Outputlocation` *request* header and uses it verbatim. This
  header does **not** exist in the real `InvokeEndpointAsyncInput` wire shape
  (checked `serializers.go`'s `awsRestjson1_serializeOpHttpBindingsInvokeEndpointAsyncInput`:
  the only headers bound are Accept/Content-Type/CustomAttributes/Filename/
  InferenceId/InputLocation/InvocationTimeoutSeconds/RequestTTLSeconds/
  S3OutputPathExtension). It's a harmless gopherstack-only testing backdoor: real
  SDK callers never send it, so they always take the "generate a fake S3 path"
  branch. Not a parity bug, just don't mistake it for a real SDK field.
- `X-Amzn-Sagemaker-*` header constants use lowercase "maker" in the Go source
  (e.g. `headerCustomAttributes = "X-Amzn-Sagemaker-Custom-Attributes"`) instead
  of the docs' `X-Amzn-SageMaker-*` casing. This is **not** a bug: Go's
  `net/http` canonicalizes every header name via
  `textproto.CanonicalMIMEHeaderKey` on both `Set` and `Get`, which title-cases
  each hyphen-separated segment (`SageMaker` -> `Sagemaker`) regardless of the
  literal casing used in source -- so the wire bytes and the client's `Header.
  Get` lookups are unaffected either way. Confirmed by cross-referencing
  `deserializers.go`'s literal `response.Header.Values("X-Amzn-SageMaker-...")`
  calls, which resolve to the same canonical key. This also holds for
  botocore's `service-2.json`, which even declares `InvokeEndpointOutput.
  InvokedProductionVariant`'s `locationName` as `"x-Amzn-Invoked-Production-
  Variant"` (lowercase leading `x`) -- same reasoning, same non-issue.
- `InvokeEndpointWithResponseStream`'s `SessionId` header is only ever passed to
  `TouchSession`, never `StartSession` -- this is correct: the SDK doc for
  `InvokeEndpointWithResponseStreamInput.SessionId` explicitly states "You can't
  create a stateful session by using the InvokeEndpointWithResponseStream action."
- `pathToOperation`'s default `"Unknown"` branch (unmatched path suffix under
  `/endpoints/`) returns a non-SDK `"UnknownOperationException"` __type. Left
  as-is: unreachable by any real SDK call (the SDK only ever generates the three
  known suffixes), so it's defensive-only code, not a wire-parity concern.
- `TargetModel`/`TargetContainerHostname`/`EnableExplanations`/
  `InferenceComponentName` request headers (confirmed against
  `service-2.json`: `X-Amzn-SageMaker-Target-Model`,
  `X-Amzn-SageMaker-Target-Container-Hostname`,
  `X-Amzn-SageMaker-Enable-Explanations`,
  `X-Amzn-SageMaker-Inference-Component`) are intentionally accepted but not
  consumed by the handler: none of them appear in any operation's *output*
  shape, so there is nothing wire-visible to get wrong by ignoring them --
  they only affect real model-container routing/behaviour, which this
  emulator does not simulate (deterministic synthetic responses are the
  documented parity bar for this service).
- `InvokeEndpointWithResponseStreamInput.ContentType` is bound to the plain
  `Content-Type` header (same as the sync op), while its sibling `Accept` is
  bound to `X-Amzn-SageMaker-Accept` -- an intentional asymmetry in the real
  SDK model, not a copy-paste error to "fix": the *output* side is
  asymmetric too (`InvokeEndpointWithResponseStreamOutput.ContentType` binds
  to `X-Amzn-SageMaker-Content-Type`, unlike the sync op's plain
  `Content-Type` output). Confirmed against `service-2.json`; `handler.go`'s
  `headerAsyncAccept`/`headerStreamContentType` constants already matched
  this correctly before this pass.
- `ModelError`/`ModelNotReadyException`/`ServiceUnavailable`/
  `InternalDependencyException`/`ModelStreamError`/`InternalStreamFailure`
  error paths have no request-side trigger (there is no real model to
  fail). `InternalStreamFailure`/`ModelStreamError` additionally have no
  `httpStatusCode` in `service-2.json`'s `error` trait at all (unlike the
  other five, which map to 530/500/424/429/503/400 respectively) --
  confirmed they are delivered as in-band event-stream exception events, not
  top-level HTTP error responses, which is why they're absent from the
  status-code table. Chaos-injection (ChaosServiceName/ChaosOperations/
  ChaosRegions) is wired at the framework level (pkgs/chaos) and can inject
  arbitrary faults independent of handler code, so this is not a gap in the
  handler itself -- just no organic way to hit these paths without chaos
  rules configured.

**Verified correct without changes:** `InvokeEndpoint` request/response header
binding (Accept/ContentType negotiation, CustomAttributes echo,
X-Amzn-Invoked-Production-Variant default `"AllTraffic"` / target-variant
override), the event-stream binary frame format for
`InvokeEndpointWithResponseStream` (prelude length/headers-length/CRC32,
`:message-type`/`:event-type`/`:content-type` headers, payload-only
`PayloadPart.Bytes`), `InvokeEndpointAsyncOutput`'s wire shape
(`InferenceId` is a plain JSON body field with no `location` binding in the
SDK model -- unlike `OutputLocation`/`FailureLocation`, which are
header-bound -- and gopherstack's JSON-body-plus-headers response shape
matches exactly), FIFO eviction bounds for sessions/async-invocations/
invocation-history, and `Handler.Snapshot`/`Restore` delegation to the backend
(all exercised by `persistence_test.go`).
