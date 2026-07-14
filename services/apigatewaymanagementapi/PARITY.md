---
service: apigatewaymanagementapi
sdk_module: aws-sdk-go-v2/service/apigatewaymanagementapi@v1.29.13
last_audit_commit: 142c3c28
last_audit_date: 2026-07-13
overall: A            # 3 genuine bugs found and fixed this pass
ops:
  PostToConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: full downstream buffer now returns LimitExceededException (429) instead of silently dropping the frame and reporting success; fixed: PayloadTooLargeException (413) now carries X-Amzn-Errortype header + __type body field"}
  GetConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "connectedAt/lastActiveAt/identity are real backend-recorded state, not fabricated; identity correctly nested"}
  DeleteConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now closes the connection's real downstream transport (forcibly disconnects) instead of only removing the registry entry"}
families:
  admin_diagnostics: {status: ok, note: "gopherstack-only /_gopherstack/apigwmgmt/* endpoints (list/broadcast/stats/prune/messages/timeline/ping) are not AWS API surface; audited only insofar as they share backend code paths with the 3 real ops. PruneIdle now also closes downstream on removal for consistency with DeleteConnection."}
gaps:
  - ForbiddenException (403, caller-not-authorized) is modeled by the real API for all 3 ops but never returned; gopherstack has no general IAM-authorization-check convention for this service (only eventbridge does something similar, for an unrelated resource-policy reason). Implementing would require a cross-cutting auth model, not a fix local to this service. Not filed as a bd issue by this pass -- flagging for triage.
  - LimitExceededException's rate-limiting half ("client sending more than the allowed number of requests per unit of time") is not modeled -- only the "WebSocket client-side buffer is full" half was fixed this pass (that half is directly reachable through the real downstream-channel wiring from apigatewayv2). Adding request-rate throttling would need a shared rate-limiter primitive; out of scope for this pass.
  - admin Broadcast (a gopherstack UI-only, non-AWS extension) records messages/stats but never actually writes to a connection's `downstream` channel, so real WebSocket clients proxied through apigatewayv2 never receive broadcast frames even though the admin API reports them "delivered". Not an AWS-parity bug (Broadcast isn't part of the real API), so left unfixed this pass; noted for a future admin-feature cleanup pass.
  - EventDisconnected (types.go) is a defined-but-unused LifecycleEvent constant: DeleteConnection/PruneIdle discard the whole connState (including its event timeline) rather than ever appending it. Cosmetic (timeline is a UI-only diagnostic, not AWS surface) -- not fixed.
deferred: []
leaks: {status: clean, note: "DeleteConnection/PruneIdle now close the downstream chan on removal, so the apigatewayv2 writer goroutine (services/apigatewayv2/proxy.go handleWebSocketProxy) that ranges over it terminates and closes the real *websocket.Conn -- previously this goroutine leaked forever (blocked on an unclosed channel with no more writers) whenever a connection was torn down via DeleteConnection/PruneIdle rather than via the client disconnecting first. Verified no double-close risk: the coarse lockmetrics.RWMutex fully serializes PostToConnection/DeleteConnection/PruneIdle, so a connState is removed from the map in the same critical section the channel is closed in, and no other code path can retrieve the same connState afterward."}
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

Bugs fixed this pass (all local to this package; no cross-service edits):

1. **Disguised no-op / missing LimitExceededException** — `backend.go`
   `PostToConnection`: when `state.downstream` (the real WS transport channel)
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

3. **DeleteConnection didn't forcibly disconnect** — `backend.go`
   `DeleteConnection` (and `PruneIdle`, same class of bug) only deleted the
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
