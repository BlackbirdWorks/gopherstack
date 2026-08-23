---
service: apigatewaymanagementapi
sdk_module: aws-sdk-go-v2/service/apigatewaymanagementapi@v1.32.4
last_audit_commit: f16ac0367
last_audit_date: 2026-08-20
overall: A            # zero-bug wrapper-key/nested-shape sweep this pass; re-verified every op field-by-field against the pinned SDK source, added a real-SDK-client wire round-trip test (previously only raw-HTTP unit tests existed)
ops:
  PostToConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-verified 2026-08-20 against aws-sdk-go-v2/service/apigatewaymanagementapi@v1.32.4: Input{ConnectionId *string (api_op_PostToConnection.go:32), Data []byte (api_op_PostToConnection.go:37)}/Output{} (empty) match; ConnectionId is a URI path param (serializers.go:224 awsRestjson1_serializeOpHttpBindingsPostToConnectionInput), Data is the raw httpPayload body (serializers.go: input.Data streamed directly as the request body, Content-Type: application/octet-stream) -- not a JSON field. Method POST /@connections/{ConnectionId} (serializers.go:183,186). Error set ForbiddenException/GoneException/LimitExceededException/PayloadTooLargeException matches awsRestjson1_deserializeOpErrorPostToConnection (deserializers.go:347). full downstream buffer returns LimitExceededException (429); PayloadTooLargeException (413) carries X-Amzn-Errortype header + __type body field. New this pass: TestAPIGwMgmt_SDKRoundTrip_PayloadTooLarge, TestAPIGwMgmt_SDKRoundTrip_LimitExceeded, TestAPIGwMgmt_SDKRoundTrip (wire_sdk_roundtrip_test.go) drive this op through the real aws-sdk-go-v2 client over pkgs/service's router."}
  GetConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-verified 2026-08-20 against the same pinned SDK: GetConnectionOutput{ConnectedAt *time.Time (api_op_GetConnection.go:40), Identity *types.Identity (api_op_GetConnection.go:42), LastActiveAt *time.Time (api_op_GetConnection.go:45)} -- no ConnectionId member (still correct, unchanged since the 2026-08-13 fix noted below). Method GET /@connections/{ConnectionId} (serializers.go:112,115). Document decode (deserializers.go:245 awsRestjson1_deserializeOpDocumentGetConnectionOutput) reads root-level JSON keys connectedAt/identity/lastActiveAt (flattened at the response root, no wrapper key -- Output has no httpPayload member). connectedAt/lastActiveAt are __timestampIso8601 parsed via smithytime.ParseDateTime, which tries RFC3339Nano/RFC3339 (smithy-go@v1.27.6 time/time.go:37-44) -- matches Go's default time.Time JSON marshaling used here, not epoch numbers. identity nests via awsRestjson1_deserializeDocumentIdentity (deserializers.go:523): sourceIp/userAgent, matching types.Identity{SourceIp,UserAgent}. connectedAt/lastActiveAt/identity are real backend-recorded state, not fabricated. New this pass: TestAPIGwMgmt_SDKRoundTrip asserts SourceIp/UserAgent/ConnectedAt/LastActiveAt through the real client's typed Identity struct, not just raw JSON bytes."}
  DeleteConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-verified 2026-08-20: Input{ConnectionId *string (api_op_DeleteConnection.go:30)}/Output{} (empty) match. Method DELETE /@connections/{ConnectionId} (serializers.go:41,44). Error set ForbiddenException/GoneException/LimitExceededException matches awsRestjson1_deserializeOpErrorDeleteConnection (deserializers.go:63) -- PayloadTooLargeException is correctly absent (not applicable to a bodyless delete). forcibly disconnects (closes the connection's real downstream transport) instead of only removing the registry entry."}
families:
  admin_diagnostics: {status: ok, note: "gopherstack-only /_gopherstack/apigwmgmt/* endpoints (list/broadcast/stats/prune/messages/timeline/ping) are not AWS API surface; audited only insofar as they share backend code paths with the 3 real ops. PruneIdle closes downstream on removal for consistency with DeleteConnection. fixed this pass: Broadcast now actually attempts delivery on each connection's real downstream channel (mirroring PostToConnection) instead of unconditionally reporting every active connection as having received the frame."}
gaps:
  - "IMPOSSIBLE (re-confirmed gopherstack-u3ie): EventDisconnected (models.go) is a defined-but-unused LifecycleEvent constant. Re-verified this pass against connections.go's DeleteConnection (line ~84-98): `delete(b.connections, connectionID)` removes the entire connState -- including its event timeline -- in the SAME locked critical section that would append an EventDisconnected entry, and admin's GetTimeline (store.go) looks up the timeline by connectionID from that same live map. This is not merely low-value, it is a true no-op: appending the event immediately before the delete would have zero externally observable effect through any code path (adminGetTimeline can't retrieve a timeline for a connectionID no longer in the map, and there is no separate disconnect-log store). Confirmed not worth implementing -- doing so would be dead code, not a real fix."
deferred:
  - "ALREADY COVERED BY CHAOS (verified gopherstack-u3ie): ForbiddenException (403, caller-not-authorized) is modeled by the real API for all 3 ops but never returned; gopherstack has no general IAM-authorization-check convention for this service (only eventbridge does something similar, for an unrelated resource-policy reason) and implementing a real cross-cutting auth model is out of scope for a single-service pass. Concretely verified this pass: apigatewaymanagementapi.Handler implements ChaosServiceName() -> \"apigatewaymanagementapi\" and ChaosOperations() -> h.GetSupportedOperations() -> [PostToConnection, GetConnection, DeleteConnection] (handler.go), and pkgs/chaos.Middleware is wired globally via registry.Use(chaos.Middleware(faultStore)) in cli.go, matching purely on the request's SigV4 service name + X-Amz-Target/path + region and injecting an arbitrary caller-specified FaultError{Code, StatusCode} without touching backend state. A fault rule such as {\"service\":\"apigatewaymanagementapi\",\"operation\":\"PostToConnection\",\"error\":{\"code\":\"ForbiddenException\",\"statusCode\":403}} deterministically returns that exact typed error to a real client, with zero backend code changes."
  - "PARTIALLY COVERED (re-confirmed gopherstack-u3ie): LimitExceededException's rate-limiting half (\"client sending more than the allowed number of requests per unit of time\") is not modeled -- only the \"WebSocket client-side buffer is full\" half is implemented (that half is directly reachable through the real downstream-channel wiring from apigatewayv2, and is exercised by both PostToConnection and admin Broadcast; see the fixed-bugs notes below). Building a real shared rate-limiter primitive (there is no `pkgs/ratelimit` or equivalent in this codebase -- checked pkgs-catalog.md) to emulate genuine request-per-second throttling is a cross-cutting feature spanning every service, not a fix local to this one, and remains out of scope for this pass. In the meantime a caller that specifically wants to exercise the rate-limiting __type value on demand can already do so via the same chaos fault-injection mechanism as ForbiddenException above."
leaks: {status: clean, note: "DeleteConnection/PruneIdle now close the downstream chan on removal, so the apigatewayv2 writer goroutine (services/apigatewayv2/proxy.go handleWebSocketProxy) that ranges over it terminates and closes the real *websocket.Conn -- previously this goroutine leaked forever (blocked on an unclosed channel with no more writers) whenever a connection was torn down via DeleteConnection/PruneIdle rather than via the client disconnecting first. Verified no double-close risk: the coarse lockmetrics.RWMutex fully serializes PostToConnection/DeleteConnection/PruneIdle/Broadcast, so a connState is removed from the map in the same critical section the channel is closed in, and no other code path can retrieve the same connState afterward. Broadcast (this pass) only ever sends on downstream, never closes it, and uses the same non-blocking select-with-default pattern as PostToConnection, so it cannot block the coarse lock nor race with a concurrent close."}
---

## apigatewaymanagementapi (this session, 2026-08-20)

Wrapper-key / nested-shape wire-parity sweep. **Zero new bugs.** All three ops
(PostToConnection, GetConnection, DeleteConnection) re-verified field-by-field
against the pinned `aws-sdk-go-v2/service/apigatewaymanagementapi@v1.32.4`
source (`api_op_*.go`, `serializers.go`, `deserializers.go`), plus
`smithy-go@v1.27.6` for the timestamp parser. Every prior claim in this file
(GetConnection's dropped `connectionId` field, the nested `identity` shape,
ISO 8601 timestamp encoding, the 4-exception error set with per-op variance,
`X-Amzn-Errortype` header handling) checked out unchanged against the live
SDK source, not just against the existing PARITY.md text.

New finding (not a bug, a documented SDK quirk worth recording): the pinned
SDK's `awsRestjson1_deserializeErrorGoneException` /
`...ErrorForbiddenException` / `...ErrorLimitExceededException`
(`deserializers.go:415,410,420`) never decode the response body at all --
literally `return &types.GoneException{}` with no field population -- so
`Message` is always empty on those three client-side regardless of what
gopherstack's `writeModeledError` body contains. Only
`PayloadTooLargeException`'s deserializer (`deserializers.go:425`) actually
decodes a body. This means classification is purely by error **code**
(`X-Amzn-Errortype` header, checked first, or body `code`/`__type` as
fallback via `restjson.GetErrorInfo`) -- the numeric HTTP status never gates
which typed exception the client constructs, only whether the error path
triggers at all (`status < 200 || status >= 300`). gopherstack's status
codes (410/429/413) are still correct for AWS-fidelity/realism even though
the Go client doesn't branch on them. The extra `connectionId` key
`writeModeledError` adds to every error body is harmless: `GetErrorInfo`
decodes into a struct with only `Code`/`Type`/`Message` fields, so unknown
keys are silently ignored by `encoding/json`.

Added `wire_sdk_roundtrip_test.go` -- this service previously had thorough
raw-HTTP-request unit tests (`handler_test.go`'s
`TestHandler_GoneExceptionWireShape`,
`TestHandler_PostToConnection_PayloadTooLarge_WireShape`, etc., which
assert on raw JSON bytes/headers) but no test driving the real
`aws-sdk-go-v2` client through `pkgs/service`'s router
(`newRoundTripClient`-style, per `services/dax/wire_sdk_roundtrip_test.go`).
New tests: `TestAPIGwMgmt_SDKRoundTrip` (happy path: GetConnection's typed
`Identity`/timestamps, PostToConnection delivery, DeleteConnection),
`TestAPIGwMgmt_SDKRoundTrip_GoneException` (all 3 ops against an unknown
connection ID, asserting `errors.As` into `*types.GoneException`),
`TestAPIGwMgmt_SDKRoundTrip_PayloadTooLarge`, and
`TestAPIGwMgmt_SDKRoundTrip_LimitExceeded` (a real unbuffered downstream
channel with no reader, proving the full-buffer condition maps to a typed
`*types.LimitExceededException` rather than a silent 200). All four pass
under `go test -race`.

Provenance: prior stamp (`last_audit_commit: 2d47b51d4`,
`last_audit_date: 2026-07-29`) checked via `git show -s --format=%ad
2d47b51d4` -> `2026-07-29`, exactly matching the recorded date -- no
gap/false-stamp issue found for that entry itself. However the stamp had
NOT advanced across the 2026-08-13 GetConnection fix documented in this same
file's `ops.GetConnection.note` (a fix dated two weeks after the
frontmatter's `last_audit_date`), so the stamp was stale by that gap. This
pass refreshes it to current HEAD (`f16ac0367`) and today (2026-08-20).

Gaps unchanged from prior passes (still correctly deferred, not fixed this
pass): `ForbiddenException` is never returned (no IAM-authorization-check
convention in this service; reachable via `pkgs/chaos` fault injection
instead) and `LimitExceededException`'s rate-limiting half (as opposed to
the implemented full-buffer half) is not modeled (no `pkgs/ratelimit`
primitive exists). Both remain out of scope for a single-service pass; see
the `deferred:` block above for the full justification, unchanged.

Structurally unverifiable: `PostToConnection`'s `Data` payload is an opaque
byte blob (the WebSocket message itself) with no AWS-defined internal
structure to check against -- gopherstack's job is only to preserve it
byte-for-byte and honor the 128 KB limit, both of which are verified by
`TestAPIGwMgmt_SDKRoundTrip` and the pre-existing
`TestHandler_PostToConnection_PayloadLimitBoundary`.

## Notes

Protocol: REST-JSON (restjson1), but routed by literal path prefix `@connections/`
(not a normal resource path) plus HTTP method, since PostToConnection, GetConnection,
and DeleteConnection all share the identical `/@connections/{connectionId}` path and
are distinguished purely by method (POST/GET/DELETE). Verified RouteMatcher matches the
literal `@` correctly and added test coverage for connectionIds containing `=`/`+`
(real connectionIds are base64url-ish and commonly contain `=` padding) — no bug found
there, existing `strings.HasPrefix`/`strings.TrimPrefix` handling is correct since Go's
`net/http` already percent-decodes `URL.Path` before the handler sees it.

`ConnectedAt`/`LastActiveAt` are modeled as `__timestampIso8601` (ISO 8601 strings) by
the real API, NOT epoch-seconds numbers — do not "fix" these to `awstime.Epoch()`,
that would be a regression. Go's default `time.Time` JSON marshaling (RFC3339Nano) is
correct here.

The connection registry is real and shared cross-service: `apigatewayv2`
(`services/apigatewayv2/proxy.go` `handleWebSocketProxy`) holds a
`StorageBackend` reference (wired via `SetManagementAPIBackend`, presumably in
provider/cli wiring — not audited, out of scope) and calls `CreateConnection`
with a live `downstream chan []byte` (buffered, size 10) when a real WebSocket
upgrades, and `DeleteConnection` when the socket's read loop exits. This means
the GoneException-on-unknown-id check in this package is exercised against
real, non-fabricated connection state for genuine WebSocket clients, not just
gopherstack's admin "simulate connection" UI feature.

Bugs fixed in the prior pass (file references below predate a later repo-wide
refactor that split the old single `backend.go` into `store.go` +
`connections.go`; kept for history, current locations noted where changed):

1. **Disguised no-op / missing LimitExceededException** — `connections.go`
   (formerly `backend.go`) `PostToConnection`: when `state.downstream` (the
   real WS transport channel)
   was full, the code did `select { case ...: default: /* dropped */ }` and
   then unconditionally recorded the message as posted and returned `nil`
   (HTTP 200 success). Real AWS documents this exact condition ("the
   WebSocket client side buffer is full") as `LimitExceededException` (429).
   Fixed: delivery is now attempted before any accounting is committed; a full
   buffer returns `ErrLimitExceeded`, mapped in `handler.go` to a proper
   rest-json `LimitExceededException` (`X-Amzn-Errortype` header + `__type`
   body field + HTTP 429), and the message is not recorded as delivered.

2. **Wire-shape bug: `PayloadTooLargeException` missing modeled-error markers** —
   `handler.go` `handlePostToConnection`: both the `http.MaxBytesReader`-triggered
   413 and the backend `ErrPayloadTooLarge` 413 previously returned a bare
   `{"message": "..."}` body with no `X-Amzn-Errortype` header and no `__type`
   field. Per the restjson1 protocol (verified against
   `aws-sdk-go-v2/service/apigatewaymanagementapi@v1.29.13/deserializers.go`),
   the SDK resolves the exception type from the header first, then the body's
   error-code field; without either, the client gets a generic/unknown error
   instead of `*types.PayloadTooLargeException`, breaking `errors.As` handling
   in caller code. Fixed via a shared `writeModeledError` helper used for
   `GoneException`, `LimitExceededException`, and `PayloadTooLargeException`.

3. **DeleteConnection didn't forcibly disconnect** — `connections.go`
   (formerly `backend.go`) `DeleteConnection` (and `store.go` `PruneIdle`,
   same class of bug) only deleted the
   registry map entry. Real AWS's DeleteConnection "forcibly disconnects" the
   client; here, a real WebSocket client proxied through `apigatewayv2` would
   stay connected indefinitely after an admin/API `DeleteConnection` call —
   its `downstream` channel simply stopped receiving new frames but was never
   closed, so `apigatewayv2`'s writer goroutine (`for msg := range downstream`)
   never observed a signal to `conn.Close()` the real socket, leaking that
   goroutine forever. Fixed: both `DeleteConnection` and `PruneIdle` now call
   `closeDownstream`, which closes the channel (only if non-nil) in the same
   locked critical section the map entry is removed in, causing the existing
   `apigatewayv2` writer goroutine to exit its range loop and close the
   socket — no `apigatewayv2` changes needed, it already handled a closed
   channel correctly; it just never received the signal.

Bug fixed this pass (2026-07-24 re-audit; local to this package):

4. **Disguised no-op: admin Broadcast never wrote to the real downstream
   channel** — `store.go` `Broadcast`: unconditionally pushed to every
   connection's message ring, bumped its stats/timeline, and counted it in
   the returned `delivered` total, without ever attempting a send on
   `state.downstream`. A real WebSocket client proxied through
   `apigatewayv2` (which owns a live `downstream` channel per connection --
   see the cross-service note above) never received broadcast frames even
   though the admin API reported them "delivered" to every active
   connection; a connection whose client-side buffer was full was
   (incorrectly) reported as delivered rather than skipped. This is the same
   bug class as fix #1 above (PostToConnection), just never applied to
   Broadcast. Fixed: `Broadcast` now mirrors `PostToConnection`'s pattern --
   for each connection with a non-nil `downstream`, delivery is attempted via
   a non-blocking send before that connection's accounting (message ring,
   `LastActiveAt`/`PostedMessages`/`BytesSent`, timeline entry, `delivered`
   count) is committed; a full buffer causes that connection to be skipped
   entirely for this broadcast, exactly as a single `PostToConnection` call
   would report `LimitExceededException` for it. Broadcast has no
   per-connection error channel back to the caller (it only returns an
   aggregate count), so a skipped connection is simply not counted rather
   than surfaced as an error -- there is no AWS API this maps to since
   Broadcast is a gopherstack-only admin extension, not real API surface.
   Covered by `TestBackend_BroadcastDeliversToRealDownstream` and
   `TestBackend_BroadcastSkipsConnectionWithFullDownstreamBuffer` in
   `ringbuffer_test.go`.

Not bugs (verified, do not re-flag):
- `GetConnection` returning nested `identity: {sourceIp, userAgent}` — this
  *is* correct per the real `Identity` shape (real AWS omits `connectionId`
  from the response since it's the request key). The wire response's extra
  `connectionId` field (present until 2026-08-13) has since been deleted --
  see the `GetConnection` ops entry above.
- `maxPayloadBytes` boundary is `> 128*1024`, i.e. exactly 128 KiB is allowed
  and only the 129th KiB triggers `PayloadTooLargeException` — matches real
  AWS's "exceeded" (not "at") semantics; test table already covers both the
  at-limit and over-limit cases.
- The backend-level `len(data) > maxPayloadBytes` check in `PostToConnection`
  is unreachable from the HTTP handler path (the body is already capped by
  `http.MaxBytesReader` at the same threshold before reaching the backend) —
  this is intentional defense-in-depth for direct backend callers/tests, not
  dead code to delete.
