---
service: apigatewaymanagementapi
sdk_module: aws-sdk-go-v2/service/apigatewaymanagementapi@v1.29.13
last_audit_commit: be69d5ece
last_audit_date: 2026-07-24
overall: A            # re-verified field-diff against downloaded SDK source this pass; 1 additional bug fixed (admin Broadcast non-delivery)
ops:
  PostToConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-verified this pass against aws-sdk-go-v2/service/apigatewaymanagementapi@v1.29.13 deserializers.go: PostToConnectionInput{ConnectionId,Data}/Output{} (empty) match; error set (ForbiddenException/GoneException/LimitExceededException/PayloadTooLargeException) and X-Amzn-ErrorType header + body __type/message resolution order match awsRestjson1_deserializeOpErrorPostToConnection. full downstream buffer returns LimitExceededException (429); PayloadTooLargeException (413) carries X-Amzn-Errortype header + __type body field"}
  GetConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-verified this pass: GetConnectionOutput{ConnectedAt,Identity,LastActiveAt} (no ConnectionId member -- gopherstack's extra connectionId field is a harmless addition) confirmed against api_op_GetConnection.go; connectedAt/lastActiveAt are __timestampIso8601 parsed via smithytime.ParseDateTime (RFC3339-family) in deserializers.go, matching Go's default time.Time JSON marshaling used here -- not epoch numbers. identity correctly nested per types.Identity{SourceIp,UserAgent}. connectedAt/lastActiveAt/identity are real backend-recorded state, not fabricated"}
  DeleteConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-verified this pass: DeleteConnectionInput{ConnectionId}/Output{} (empty) match; error set (ForbiddenException/GoneException/LimitExceededException) matches awsRestjson1_deserializeOpErrorDeleteConnection. forcibly disconnects (closes the connection's real downstream transport) instead of only removing the registry entry"}
families:
  admin_diagnostics: {status: ok, note: "gopherstack-only /_gopherstack/apigwmgmt/* endpoints (list/broadcast/stats/prune/messages/timeline/ping) are not AWS API surface; audited only insofar as they share backend code paths with the 3 real ops. PruneIdle closes downstream on removal for consistency with DeleteConnection. fixed this pass: Broadcast now actually attempts delivery on each connection's real downstream channel (mirroring PostToConnection) instead of unconditionally reporting every active connection as having received the frame."}
gaps:
  - ForbiddenException (403, caller-not-authorized) is modeled by the real API for all 3 ops but never returned; gopherstack has no general IAM-authorization-check convention for this service (only eventbridge does something similar, for an unrelated resource-policy reason). Implementing would require a cross-cutting auth model, not a fix local to this service. Not filed as a bd issue by this pass -- flagging for triage.
  - LimitExceededException's rate-limiting half ("client sending more than the allowed number of requests per unit of time") is not modeled -- only the "WebSocket client-side buffer is full" half is implemented (that half is directly reachable through the real downstream-channel wiring from apigatewayv2, and is exercised by both PostToConnection and, as of this pass, admin Broadcast). Adding request-rate throttling would need a shared rate-limiter primitive; out of scope for this pass.
  - EventDisconnected (types.go) is a defined-but-unused LifecycleEvent constant: DeleteConnection/PruneIdle discard the whole connState (including its event timeline) rather than ever appending it. Cosmetic (timeline is a UI-only diagnostic, not AWS surface, and the connState -- including any event appended immediately before deletion -- is discarded in the same step regardless) -- not fixed.
deferred: []
leaks: {status: clean, note: "DeleteConnection/PruneIdle now close the downstream chan on removal, so the apigatewayv2 writer goroutine (services/apigatewayv2/proxy.go handleWebSocketProxy) that ranges over it terminates and closes the real *websocket.Conn -- previously this goroutine leaked forever (blocked on an unclosed channel with no more writers) whenever a connection was torn down via DeleteConnection/PruneIdle rather than via the client disconnecting first. Verified no double-close risk: the coarse lockmetrics.RWMutex fully serializes PostToConnection/DeleteConnection/PruneIdle/Broadcast, so a connState is removed from the map in the same critical section the channel is closed in, and no other code path can retrieve the same connState afterward. Broadcast (this pass) only ever sends on downstream, never closes it, and uses the same non-blocking select-with-default pattern as PostToConnection, so it cannot block the coarse lock nor race with a concurrent close."}
---

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
  from the response since it's the request key; gopherstack's extra
  `connectionId` field is harmless/ignored by the SDK, a deliberate UI
  convenience, not a wire bug).
- `maxPayloadBytes` boundary is `> 128*1024`, i.e. exactly 128 KiB is allowed
  and only the 129th KiB triggers `PayloadTooLargeException` — matches real
  AWS's "exceeded" (not "at") semantics; test table already covers both the
  at-limit and over-limit cases.
- The backend-level `len(data) > maxPayloadBytes` check in `PostToConnection`
  is unreachable from the HTTP handler path (the body is already capped by
  `http.MaxBytesReader` at the same threshold before reaching the backend) —
  this is intentional defense-in-depth for direct backend callers/tests, not
  dead code to delete.
