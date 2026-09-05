---
service: azurequeue
sdk_module: azure-sdk-for-go/sdk/storage/azqueue@v1.0.1
last_audit_commit: c7cf3aec
last_audit_date: 2026-09-04
overall: C
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  ListQueues: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /<account>?comp=list. No prefix/marker/maxresults pagination support yet -- returns all queues in one page with an always-empty NextMarker. No queue metadata (x-ms-meta-*) is stored or returned."}
  CreateQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /<account>/<queue>. No metadata (x-ms-meta-*) support -- a pre-existing queue of any prior state is treated as identical metadata and returns 204 (idempotent), never 409 QueueAlreadyExists; that code is wired but currently unreachable."}
  DeleteQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /<account>/<queue>. No lease/If-Match conditional support."}
  PutMessage: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /<account>/<queue>/messages. visibilitytimeout/messagettl query params supported; messagettl=-1 (Azure's 'never expire' sentinel) is accepted but modeled as a 100-year TTL rather than true infinite retention. MessageText is stored verbatim -- never base64 decoded/re-encoded."}
  GetMessages: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /<account>/<queue>/messages?numofmessages=N&visibilitytimeout=T. Dequeues up to numofmessages (1-32) visible, non-expired messages in insertion order, hiding each until its new visibilitytimeout elapses, rotating PopReceipt, and incrementing DequeueCount."}
  PeekMessages: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /<account>/<queue>/messages?peekonly=true&numofmessages=N. Read-only: no visibility change, no PopReceipt assigned/returned, DequeueCount unchanged."}
  DeleteMessage: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /<account>/<queue>/messages/<id>?popreceipt=. popreceipt is mandatory (400 InvalidQueryParameterValue if absent); a stale/wrong value is rejected with 400 PopReceiptMismatch, an unknown message id with 404 MessageNotFound."}
  UpdateMessage: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /<account>/<queue>/messages/<id>?popreceipt=&visibilitytimeout=. Both query params are mandatory. Rotates PopReceipt and returns it plus TimeNextVisible via x-ms-popreceipt/x-ms-time-next-visible response headers. An optional body replaces MessageText."}
  ClearMessages: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /<account>/<queue>/messages (no sub-path). Removes every message from the queue unconditionally."}
families:
  auth: {status: partial, note: "Identical stance to services/azureblob: checkAuth parses a present Authorization header via pkgs/azureauth.ParseAuthorizationHeader (structural only). azureauth.VerifySharedKey exists and is unit-tested but is not called from checkAuth -- verification enforcement is deliberately deferred, matching services/s3's PresignSecret-opt-in philosophy. An absent or invalid header is still accepted."}
  queue_body_headers: {status: ok, note: "x-ms-version, x-ms-request-id, and Date are set on every response (success and error paths) via setCommonHeaders, so azure-sdk-for-go's response parsing does not error on missing headers. Every error response also carries x-ms-error-code, matching real Azure Storage."}
  routing_isolation: {status: ok, note: "Runs on its own dedicated *http.Server, bound synchronously in StartWorker to a fixed port (default 10001 via --azure-queue-port/AZURE_QUEUE_PORT, matching Azurite's own Queue service port; no fallback pool -- fails fast if unavailable, mirroring services/azureblob and services/iot's MQTT broker), never registered into the shared AWS single-port Router or multiplexed onto Azure Blob's own dedicated port -- see provider.go's Provider doc comment and AZURE.md section 4 for the full rationale. cli.go's reserveFixedServicePorts additionally reserves this port in the shared PortAlloc pool (pkgs/portalloc.Allocator.Reserve) at startup, since 10001 sits inside --port-range-start/--port-range-end's own default range and would otherwise be handed to an unrelated Acquire caller."}
  visibility_and_ttl: {status: ok, note: "Visibility timeout (default 30s on Get Messages, 0/immediate on Put Message) and message TTL (default 7 days) are modeled with an injectable clock (InMemoryBackend.nowFunc, see export_test.go's SetNowFunc) rather than wall-clock sleeps, so unit tests exercise expiry/visibility deterministically. A Janitor background sweep (janitor.go, mirroring services/sqs's MessageRetentionPeriod sweeper) removes TTL-expired messages across all queues on a timer; visibility-timeout expiry itself is checked lazily at read time (Get/Peek), not swept."}
  observability: {status: ok, note: "StartWorker wraps its Echo handler with telemetry.WrapEchoHandler so ExtractOperation/ExtractResource feed Prometheus metrics, and derives its listener logger via logger.WithWorker(ctx, \"azurequeue\", \"listener\"). InMemoryBackend and the server-lifecycle mutex both use *lockmetrics.RWMutex instead of raw sync.RWMutex/Mutex, matching repo convention."}
gaps:
  - "No queue metadata (x-ms-meta-*) support -- neither stored on Create nor returned on List; Create is therefore always idempotent (204) for a pre-existing queue, and QueueAlreadyExists (409) is unreachable in this MVP."
  - "messagettl=-1 (Azure's 'never expire' sentinel) is modeled as a 100-year TTL, not true infinite retention."
  - "List Queues returns every result in one page; no prefix/marker/maxresults pagination."
  - "No SetQueueMetadata/GetQueueMetadata, no queue ACL (Set/Get Queue ACL / SAS-scoped access policies)."
  - "No poison-message / dead-letter handling -- DequeueCount is tracked and returned but nothing acts on it (contrast services/sqs's DLQ)."
  - "Visibility-timeout expiry is checked lazily at read time (Get/Peek Messages), not proactively swept -- functionally correct (a message becomes visible again exactly when TimeNextVisible passes, regardless of sweep timing) but differs from message-TTL expiry, which the Janitor does proactively sweep."
  - "Auth verification is not enforced -- see families.auth. pkgs/azureauth.VerifySharedKey exists and is unit-tested but checkAuth does not call it yet."
  All gaps above are intentional MVP scope per AZURE.md's M1/M2 split (see AZURE.md section 8's M2 entry), not oversights.
deferred:
  - "Initial implementation pass (2026-09-04): seeded this service from scratch per AZURE.md M2 (see AZURE.md section 8; note the milestone numbering there differs from this task's internal M1 naming). Structurally mirrors services/azureblob's M0 implementation and PARITY.md format; no prior audit history to reconcile."
leaks: {status: clean, note: "The dedicated *http.Server started by StartWorker is stopped by Shutdown via srv.Shutdown(ctx) (falling back to srv.Close() on a graceful-shutdown error, both logged), mirroring services/azureblob and cli.go's own top-level server lifecycle. The Janitor's single background goroutine (started from StartWorker, mirroring services/xray's WithJanitor pattern) runs on a worker.Group ticker that stops when its context is cancelled -- verified by TestStartWorker_WithJanitorRuns starting and immediately tearing one down without leaking."}
---

## Notes

### Why Azure Queue gets its own port, separate from Azure Blob's
Azure Queue's REST path shape (`/<account>/<queue>[/messages[/<id>]]`) shares
the same `/<account>/<resource>` ambiguity with Azure Blob and Table that
motivated Azure Blob's own dedicated port in the first place (see
`services/azureblob/PARITY.md`'s identical note and AZURE.md section 4) --
multiplexing Queue onto Blob's port would reintroduce exactly that
collision. `StartWorker` synchronously binds a fixed port (default `10001`,
Azurite's own Queue-service default, overridable via
`--azure-queue-port`/`AZURE_QUEUE_PORT`) before standing up its own
`*echo.Echo` + `*http.Server`, with no fallback into the shared
`--port-range-start`/`--port-range-end` `PortAlloc` pool if the bind fails
(`StartWorker` returns the bind error directly instead). `cli.go`'s
`reserveFixedServicePorts` additionally reserves `10001` in that shared pool
at startup so an unrelated `Acquire` caller never collides with it.

### Auth
Identical stance to `services/azureblob`: the `Authorization` header is
parsed via `pkgs/azureauth.ParseAuthorizationHeader` (structural only), but a
malformed or absent header is still accepted -- matching this repo's
permissive-by-default philosophy. Real HMAC verification
(`azureauth.VerifySharedKey`) exists and is unit-tested but is not called
from `checkAuth` yet.

### Message text is opaque
Every real Azure SDK base64-encodes `MessageText` client-side by default
before sending it, and decodes it client-side on the way back. This backend
never touches that encoding either way -- it stores and returns exactly the
bytes it received in `<MessageText>`, so a round-trip through a real SDK
still decodes correctly (the SDK undoes its own encoding), while a raw XML
client sending plain text also gets exactly that text back unchanged.

### Visibility timeout and pop receipts
`PutMessage`/`GetMessages`/`UpdateMessage` all thread through
`InMemoryBackend.nowFunc` (`store.go`), an injectable clock overridable via
`export_test.go`'s `SetNowFunc`, so tests can assert visibility-timeout and
TTL-expiry behavior by advancing a fake clock instead of sleeping in real
time (`TestInMemoryBackend_GetMessages_HidesUntilVisibilityTimeoutElapses`,
`TestInMemoryBackend_SweepExpired`). Every `GetMessages`/`UpdateMessage` call
rotates a message's `PopReceipt`; `DeleteMessage`/`UpdateMessage` reject a
stale or mismatched value with `PopReceiptMismatch` rather than silently
accepting it.

## More

- [Full parity audit](PARITY.md)
- [All services](../../README.md#services)
