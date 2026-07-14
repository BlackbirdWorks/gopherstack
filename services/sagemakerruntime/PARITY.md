---
service: sagemakerruntime
sdk_module: aws-sdk-go-v2/service/sagemakerruntime@v1.39.3
last_audit_commit: 95ab0584
last_audit_date: 2026-07-13
overall: A            # genuine fixes found (3): ValidationError code, Host matcher, missing FailureLocation
ops:
  InvokeEndpoint: {wire: ok, errors: ok, state: ok, persist: n/a, note: "sync op, no persisted state of its own beyond the invocation-history/session side effects; body is an opaque mock, headers round-trip correctly"}
  InvokeEndpointAsync: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns FailureLocation (was missing); InferenceId/OutputLocation already correct"}
  InvokeEndpointWithResponseStream: {wire: ok, errors: ok, state: ok, persist: n/a, note: "event-stream framing (prelude/header/payload/CRC) verified against smithy-go wire format; SessionId only touches, never creates (per SDK doc: sessions can't be created via this op)"}
families:
  sessions: {status: ok, note: "NEW_SESSION creation, FIFO eviction (maxSessions=1000), TouchSession on subsequent calls -- all covered. Expiry (ExpiresAt) is tracked but never enforced (TouchSession doesn't check it, no ClosedSessionId is ever emitted); see gaps."}
  invocation_history: {status: ok, note: "bounded FIFO (maxInvocationHistory=1000), persisted."}
gaps:
  - "Endpoint-name existence is never validated (real AWS returns ValidationError for an unknown EndpointName). The endpoint registry is owned by services/sagemaker's InMemoryBackend, which sagemakerruntime has no wiring to reach -- cross-service backend references in this codebase go through a BackendsProvider interface implemented in cli.go (see services/cloudformation/provider.go for the pattern), which is out of scope for a same-service-only pass. (bd: file a cross-service issue for wiring sagemakerruntime -> sagemaker.DescribeEndpoint through AppContext or a shared registry.)"
  - "ClosedSessionId (InvokeEndpointOutput) is never emitted. Real AWS sets it when the model container decides to close a stateful session -- this is inherently model-driven and can't be triggered by request semantics alone; separately, gopherstack's own session ExpiresAt is tracked but not enforced (TouchSession doesn't check expiry, so an 'expired' session just keeps working forever). Emulating expiry-driven closure would be a reasonable mock behavior but wasn't in scope for a header/wire-shape-focused audit; noting for a future pass."
  - "ModelError/ModelNotReadyException/ServiceUnavailable/InternalDependencyException/ModelStreamError/InternalStreamFailure error paths have no request-side trigger (there is no real model to fail). Chaos-injection (ChaosServiceName/ChaosOperations/ChaosRegions) is wired at the framework level (pkgs/chaos) and can inject arbitrary faults independent of handler code, so this is not a gap in the handler itself -- just no organic way to hit these paths without chaos rules configured."
deferred: []
leaks: {status: clean, note: "sessions/asyncInvocations/invocations are all FIFO-capped (maxSessions/maxAsyncInvocations/maxInvocationHistory=1000); no goroutines, no janitor (Shutdown is a documented no-op)."}
---

## Notes

Protocol: restjson1. Three ops, disambiguated purely by path suffix (no
X-Amz-Target header): `/endpoints/{EndpointName}/invocations`,
`/endpoints/{EndpointName}/async-invocations`,
`/endpoints/{EndpointName}/invocations-response-stream`. EndpointName is a
path segment (`extractEndpointName` cuts on the first `/` after the
`/endpoints/` prefix), never a query param or header.

**Bugs found and fixed this pass (see git diff for exact lines):**

1. **Wrong error `__type` for validation failures** (`handler.go`, method-not-allowed
   and missing-EndpointName branches). The handler returned `"ValidationException"`,
   copied from the bedrockruntime/most-JSON-services convention. But
   `aws-sdk-go-v2/service/sagemakerruntime/types/errors.go` declares
   `type ValidationError struct{...}` with `ErrorCode() == "ValidationError"`
   (no "Exception" suffix) -- confirmed against both `types/errors.go` and the
   `deserializers.go` switch (`strings.EqualFold("ValidationError", errorCode)`).
   A client doing `var ve *types.ValidationError; errors.As(err, &ve)` would have
   silently fallen through to a generic `smithy.GenericAPIError` instead. Fixed to
   `"ValidationError"`.

2. **Wrong Host-header route-matcher prefix** (`handler.go` `RouteMatcher`). Matched
   `"sagemaker-runtime."` but the real SageMaker Runtime endpoint hostname (per the
   SDK's endpoint resolver, `endpoints.go`) is `runtime.sagemaker.<region>.amazonaws.com`
   / `runtime.sagemaker-fips.<region>...` -- i.e. `"runtime.sagemaker."`. The old
   prefix would never match real traffic (path-prefix matching on `/endpoints/`
   happened to save it in every existing test/usage, since gopherstack is normally
   addressed by a single base endpoint with the real path). Fixed the prefix and
   added `TestHandler_RouteMatcher_Host` to cover it (matcher-routed, not just
   `Handler()`-bypassing).

3. **`InvokeEndpointAsync` never returned `FailureLocation`** (`backend.go`,
   `handler.go`). Confirmed against `deserializers.go`:
   `awsRestjson1_deserializeOpHttpBindingsInvokeEndpointAsyncOutput` binds
   `X-Amzn-SageMaker-FailureLocation` unconditionally (same call as
   `X-Amzn-SageMaker-OutputLocation`) -- real AWS always returns both on a
   successful 202, not just on later polling. Added `AsyncInvocation.
   FailureLocation`, a `deriveFailureLocation` helper that mirrors
   `OutputLocation` with a distinct suffix (real AWS derives both from the
   endpoint's `AsyncInferenceConfig`, which is cross-service/out of scope; this
   mirrors deterministically instead), and set the response header in
   `handleInvokeEndpointAsync`.

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
  calls, which resolve to the same canonical key.
- `InvokeEndpointWithResponseStream`'s `SessionId` header is only ever passed to
  `TouchSession`, never `StartSession` -- this is correct: the SDK doc for
  `InvokeEndpointWithResponseStreamInput.SessionId` explicitly states "You can't
  create a stateful session by using the InvokeEndpointWithResponseStream action."
- `pathToOperation`'s default `"Unknown"` branch (unmatched path suffix under
  `/endpoints/`) returns a non-SDK `"UnknownOperationException"` __type. Left
  as-is: unreachable by any real SDK call (the SDK only ever generates the three
  known suffixes), so it's defensive-only code, not a wire-parity concern.

**Verified correct without changes:** `InvokeEndpoint` request/response header
binding (Accept/ContentType negotiation, CustomAttributes echo,
X-Amzn-Invoked-Production-Variant default `"AllTraffic"` / target-variant
override, NEW_SESSION -> `X-Amzn-SageMaker-New-Session-Id` with `Expires=`
attribute), the event-stream binary frame format for
`InvokeEndpointWithResponseStream` (prelude length/headers-length/CRC32,
`:message-type`/`:event-type`/`:content-type` headers, payload-only
`PayloadPart.Bytes`), FIFO eviction bounds for sessions/async-invocations/
invocation-history, and `Handler.Snapshot`/`Restore` delegation to the backend
(all exercised by `persistence_test.go`).
