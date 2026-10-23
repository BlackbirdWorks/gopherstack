---
service: azureservicebus
sdk_module: azure-sdk-for-go/sdk/messaging/azservicebus@v1.11.0   # go.mod dependency (test-only): drives the AMQP 1.0 listener in amqp_sdk_test.go
last_audit_commit: a5f048bc5
last_audit_date: 2026-10-20
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /<queue>. Entity-kind (queue vs topic) resolution order: ?type=topic/?type=queue query param (either value wins outright), then a real Atom+XML parse of the request body (yielding LockDuration/MaxDeliveryCount/DefaultMessageTimeToLive too), then a case-insensitive substring sniff as a last resort, then default-to-queue -- see families.entity_kind_detection. Per-entity LockDuration/MaxDeliveryCount/DefaultMessageTimeToLive are now stored and honored (see families.lock_duration_and_long_poll); an absent/unparseable property falls back to the package-level default. LockDuration above MaxLockDuration (5 minutes) is rejected 400 Bad Request rather than silently accepted -- see the Notes section."}
  DeleteQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /<queue>. Also matches a topic name (DeleteEntity tries queue then topic) -- see DeleteTopic."}
  GetQueue: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /<queue>. Returns an Atom+XML entry with a QueueDescription (LockDuration/MaxDeliveryCount/DefaultMessageTimeToLive plus MessageCount and CountDetails Active/DeadLetter counts). 404 if the name exists as neither a queue nor a topic (getEntity tries queue then topic, matching deleteEntity's order)."}
  ListQueues: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /$Resources/Queues, real Service Bus's management-API listing endpoint. Returns an Atom+XML feed of entries, sorted by name. No pagination -- returns every queue in one page, matching services/azurequeue's/azuretable's List fidelity bar (see PARITY.md's Get/List fidelity note)."}
  CreateTopic: {wire: ok, errors: ok, state: ok, persist: ok, note: "Same PUT /<name> endpoint as CreateQueue; disambiguated by the same resolution order, see CreateQueue's note. Only DefaultMessageTimeToLive is meaningful for a topic (applied as the per-message TTL cap at Send time, since a topic never itself holds messages)."}
  DeleteTopic: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /<topic>, deletes all subscriptions and their messages too."}
  GetTopic: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /<topic>. Returns an Atom+XML entry with a TopicDescription (DefaultMessageTimeToLive)."}
  ListTopics: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /$Resources/Topics. Same shape/fidelity as ListQueues."}
  CreateSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /<topic>/subscriptions/<name>. Request body may carry a SqlFilter rule (RuleDescription); it is read (via the same Atom+XML parse used for entity-kind detection, to also extract LockDuration/MaxDeliveryCount) and the filter itself discarded -- every subscription still behaves as Service Bus's default TrueFilter (match-all). See families.filter_evaluation. LockDuration above MaxLockDuration (5 minutes) is rejected 400 Bad Request, same as CreateQueue -- see the Notes section."}
  DeleteSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /<topic>/subscriptions/<name>."}
  GetSubscription: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /<topic>/subscriptions/<name>. Returns an Atom+XML entry with a SubscriptionDescription (LockDuration/MaxDeliveryCount)."}
  ListSubscriptions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /<topic>/subscriptions (list) -- fell out naturally from the path parser alongside $Resources, so it's included even though it wasn't explicitly required. Same feed shape/fidelity as ListQueues/ListTopics, scoped to one topic."}
  SendMessage: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /<queue-or-topic>/messages. Message body is opaque bytes; BrokerProperties request header (JSON) supplies MessageId/Label/CorrelationId/ReplyTo/SessionId/TimeToLive (seconds). Sending to a topic fans the message out to every current subscription as an independent copy (services/sns's topic/subscription bookkeeping used as the structural reference). Non-reserved request headers are stored as application properties (CustomHeaders) and echoed on receive/peek. Sending directly to a subscription path is rejected 405, matching real Service Bus (subscriptions are receive-only). An explicit TimeToLive is now capped at the target entity's configured DefaultMessageTimeToLive, matching real Service Bus; an absent one uses that TTL outright."}
  PeekLockMessage: {wire: ok, errors: ok, state: ok, persist: n/a, note: "POST /<queue-or-topic-subscription>[/$DeadLetterQueue]/messages/head. Destructive read: locks the oldest available message for the entity's own configured LockDuration (falling back to DefaultLockDuration, 60s, if unconfigured -- see families.lock_duration_and_long_poll) and returns it with a BrokerProperties response header (LockToken/LockedUntilUtc/SequenceNumber/DeliveryCount) plus a Location header giving the complete/abandon URI. The real API's long-poll \"?timeout=<seconds>\" query parameter is now implemented (clamped to MaxPeekLockWaitTimeout, 30s) -- see families.lock_duration_and_long_poll."}
  CompleteMessage: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /<queue-or-topic-subscription>[/$DeadLetterQueue]/messages/<id>/<locktoken>. A stale/unknown/expired lock returns 410 Gone (MessageLockLost), matching real Service Bus's MessageLockLostException mapping."}
  AbandonMessage: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT on the same path as CompleteMessage. Releases the lock immediately (no visibility delay, unlike Azure Queue's UpdateMessage) so the message becomes available for redelivery right away, waking any long-polling PeekLock waiter; if DeliveryCount has reached the entity's configured MaxDeliveryCount (falling back to 10 if unconfigured) the message is moved to the dead-letter sub-queue instead of being released."}
  DeadLetter: {wire: partial, errors: ok, state: ok, persist: ok, note: "Client dead-letter: PUT .../messages/<id>/<locktoken> with a BrokerProperties header carrying DeadLetterReason and/or DeadLetterErrorDescription moves the locked message to <entity>/$DeadLetterQueue, readable there (PeekLock/receive-and-delete/peek) with both properties surfaced as response headers and BrokerProperties fields. The real REST reference documents Unlock as bodyless, so this is an emulator extension on the same URI (wire: partial). Auto dead-lettering (MaxDeliveryCountExceeded, TTLExpiredException) now stamps its reason too. Dead-lettering from the DLQ itself is 400. Reason/description persist."}
  PeekMessages: {wire: partial, errors: ok, state: ok, persist: n/a, note: "GET .../messages[?fromSequenceNumber=N&maxMessages=M] (also under $DeadLetterQueue): non-locking peek returning a JSON array of {BrokerProperties, ContentType, Body (base64), CustomProperties}; does not lock, change DeliveryCount, or hide locked messages. Emulator extension (azservicebus peeks over AMQP; real REST has no peek-many)."}
  ReceiveAndDeleteMessage: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE .../messages/head (also under $DeadLetterQueue): removes and returns the oldest visible message as 200 with BrokerProperties and body, 204 when empty. Matches the documented Destructive Read."}
  AMQPSend: {wire: ok, errors: ok, state: ok, persist: ok, note: "AMQP 1.0 sender link to a queue, topic or <topic>/Subscriptions/<sub> (listener on --azure-servicebus-amqp-port 5672 plain / --azure-servicebus-amqp-tls-port 5671 TLS, same backend as REST). Single messages and the 0x80013700 batch-format envelope; properties (message-id, subject, content-type, correlation-id, reply-to, to, group-id), application properties (typed in memory), header ttl and x-opt-scheduled-enqueue-time are mapped onto the shared message. Unknown entity: attach refused with amqp:not-found. Driven by the real azservicebus client in amqp_sdk_test.go."}
  AMQPReceive: {wire: ok, errors: ok, state: ok, persist: n/a, note: "AMQP 1.0 receiver link on a queue or subscription, credit-based, PeekLock (rcv-settle-mode second) and ReceiveAndDelete (first/settled). Delivery tag is the lock token in .NET Guid byte order, as the SDK expects. Annotations carry x-opt-sequence-number/enqueued-time/locked-until/message-state; header delivery-count is count-1. <entity>/$DeadLetterQueue addresses the dead-letter sub-queue and surfaces DeadLetterReason/DeadLetterErrorDescription."}
  AMQPSettle: {wire: ok, errors: ok, state: ok, persist: ok, note: "Dispositions on a PeekLock link: accepted = complete, modified = abandon, modified with undeliverable-here = defer, rejected with the com.microsoft:dead-letter error = dead-letter with reason and description, released = abandon. Unsettled deliveries are abandoned when the link or connection drops."}
  AMQPManagement: {wire: ok, errors: ok, state: ok, persist: ok, note: "Request/response $management node (<entity>/$management, reply routed by reply-to): com.microsoft:peek-message, renew-lock, receive-by-sequence-number (deferred messages), schedule-message, cancel-scheduled-message, update-disposition. Also the session operations (see AMQPSession). Status 410 on a lost lock (message or session), 204 when nothing matches, 501 for unsupported operations. Scheduled messages are held back by the backend until due and, on a topic, cancelled across every subscription copy."}
  AMQPSession: {wire: ok, errors: partial, state: ok, persist: ok, note: "Session-enabled queues/subscriptions (RequiresSession, parsed from the Atom create body and echoed on Get/List). Receivers attach with the com.microsoft:session-filter source filter (named session, or null for the next session with deliverable messages; the wait honours the com.microsoft:timeout link property, default and cap 60s, then fails with com.microsoft:timeout). The attach reply echoes the locked session id in the filter and com.microsoft:locked-until-utc (.NET ticks) in link properties. The lock is owned by the receiver link name (also carried as associated-link-name on $management calls), lasts the entity LockDuration, and is released when the link closes or expires; a second accept of a locked session fails com.microsoft:session-cannot-be-locked. Delivery is FIFO within the locked session only. $management: renew-session-lock, get-session-state, set-session-state (256 KiB cap), and peek-message/update-disposition scoped by the link's session. Losing the session lock detaches the link with com.microsoft:session-lock-lost, settlement on it fails, and management settlement returns 410. Sending without SessionId to a session entity is rejected, and plain receivers on a session entity are refused; both use amqp:not-allowed, which is the best-known condition and not verified against the live service. Session state and locks are persisted in the queue/subscription snapshot (Sessions); locks are cleared by the Janitor once expired. Driven by the real azservicebus session API in amqp_session_sdk_test.go. Not implemented: com.microsoft:get-message-sessions (the SDK does not use it), session-scoped receive-by-sequence-number."}
  AMQPAuth: {wire: ok, errors: ok, state: ok, persist: n/a, note: "SASL ANONYMOUS and PLAIN, plus the $cbs put-token flow. With --azure-servicebus-validate-sas the SharedAccessSignature is verified with the SDK's own signing rule (HMAC-SHA256 over the raw sr + newline + se) against the same key the REST plane uses; PLAIN compares the password to that key; entity links and $management need a prior valid claim. Off by default, like REST."}
families:
  sdk_compat: {status: ok, note: "azservicebus (AMQP 1.0 only) connects to the AMQP listener: plain 5672 with UseDevelopmentEmulator=true in the connection string, or TLS 5671 with a self-signed dev certificate (supply it via ClientOptions.TLSConfig; the SDK uses host:port as the TLS server name, so set ServerName). test/integration/azureservicebus_test.go still covers REST directly."}
  entity_kind_detection: {status: ok, note: "Real Service Bus's management REST API disambiguates queue vs. topic creation by the root XML element name in an Atom+XML entry body (<QueueDescription> vs <TopicDescription>). handler.go's resolveEntityKind now does a real encoding/xml parse (atom.go's parseAtomEntityBody) of that shape, namespace-tolerant (see the Notes section below), before falling back to the previous case-insensitive substring sniff (kept only for the REST integration test's deliberately simplified bodies) and finally defaulting to a queue. The ?type= query-param escape hatch recognizes both \"topic\" and \"queue\" explicitly (a code-review pass caught that only \"topic\" was wired up, contradicting the doc comment). Malformed XML never fails the create -- it simply falls through the same resolution chain."}
  filter_evaluation: {status: deferred, note: "Subscription SQL-filter rules (SqlFilter/CorrelationFilter) are accepted structurally in CreateSubscription's request body but never parsed or evaluated -- every subscription receives every message sent to its topic, as if it had the default TrueFilter rule. Real filter evaluation (a SQL-subset parser/evaluator, following services/azuretable's $filter or services/cosmosdb's SQL-subset precedent) is deferred, per AZURE.md section 9's M5 scope note."}
  sessions: {status: ok, note: "Sessions are implemented on the AMQP plane (see ops.AMQPSession). The REST data plane has no session receive API in Service Bus, so over REST a session entity rejects a send without SessionId and peek-lock or destructive reads with 400; SessionId on a REST send is stored and delivered to AMQP session receivers."}
  amqp_transport: {status: ok, note: "pkgs/amqp1 is a server-side AMQP 1.0 implementation (type codec, framing, SASL, connection/session/link engine with session windows, link credit, multi-frame transfers and drain) behind a small Handler interface; no new module. Frame size and message size are capped (64 KiB frames, 1 MiB messages) and decoder allocations are bounded by the frame. Listeners are reserved in the shared port allocator. Not implemented: transactions, resume of unsettled state across reconnects, WebSocket transport, SASL EXTERNAL."}
  lock_duration_and_long_poll: {status: ok, note: "LockDuration/MaxDeliveryCount/DefaultMessageTimeToLive are now per-entity properties (EntityConfig, stored on storedQueue/storedTopic/storedSubscription and round-tripped through backendSnapshot), parsed from the Atom+XML create-request body as ISO 8601 duration strings (LockDuration/DefaultMessageTimeToLive) or a plain integer (MaxDeliveryCount) via a small hand-written parser (iso8601.go's parseISO8601Duration) -- no new module dependency. An absent/unparseable property falls back to the existing package-level default. LockDuration is additionally validated at CreateQueue/CreateSubscription time against MaxLockDuration (5 minutes, real Service Bus's documented maximum) and rejected with 400 Bad Request if it exceeds that -- a fidelity improvement over the initial pass. PeekLock's long-poll \"?timeout=<seconds>\" query parameter is now implemented: gopherstack waits up to that many seconds (clamped to MaxPeekLockWaitTimeout, 30s) for a message to arrive, via a broadcast notify channel woken by Send, by Abandon on EITHER of its two outcomes (released back to availability, or moved to dead-letter), and by the Janitor's sweep on any of its three outcomes (lock release, lock-expiry dead-letter, TTL-expiry dead-letter) -- so a long-poll against $DeadLetterQueue wakes immediately when the Janitor or an Abandon dead-letters a message, not just on the live-list path. Mirrors services/sqs's ReceiveMessage/pollReceive/receiveOnce shape but additionally honoring the request's context for prompt cancellation on client disconnect (an improvement over the SQS precedent), and correctly sizes its first recheck-timer tick to min(timeout, 1s) so sub-second timeouts aren't stretched to a full second. See the Notes section for the 30s cap's rationale (corrected from an earlier draft) and the ISO 8601 parser's overflow clamp."}
  dead_lettering: {status: ok, note: "A background Janitor (janitor.go, 10s default interval) proactively releases expired locks (dead-lettering on the entity's configured MaxDeliveryCount exhaustion) and moves TTL-expired live messages straight to dead-letter, mirroring services/azurequeue's Janitor shape but adapted to Service Bus's richer lock/delivery-count/dead-letter model instead of a flat TTL sweep. A message that expires while already sitting in the dead-letter sub-queue is dropped outright (nowhere further to move it). See the DeadLetter op note above for the still-unimplemented explicit client-initiated dead-letter operation."}
  auth: {status: partial, note: "Authorization: SharedAccessSignature sr=<resource>&sig=<hmac>&se=<expiry>&skn=<keyname> is always structurally parsed (sas.go's ParseSASAuthorization extracts key name, resource scope, and expiry). Cryptographic HMAC-SHA256 verification (VerifySAS) exists and is unit-tested but is opt-in via Handler.WithSASValidation / --azure-servicebus-validate-sas (mirroring services/s3's WithPresignValidation and Blob/Queue/Table's WithSharedKeyValidation) -- off by default, an absent/malformed/wrong-signature header is accepted."}
  routing_isolation: {status: ok, note: "Runs on its own dedicated *http.Server, bound synchronously in StartWorker to a fixed port (default 10003 via --azure-servicebus-port/AZURE_SERVICEBUS_PORT; no fallback pool -- fails fast if unavailable, mirroring services/azureblob/azurequeue/azuretable), never registered into the shared AWS single-port Router. cli.go's reserveFixedServicePorts additionally reserves this port in the shared PortAlloc pool at startup."}
  observability: {status: ok, note: "StartWorker wraps its Echo handler with telemetry.WrapEchoHandler so ExtractOperation/ExtractResource feed Prometheus metrics, and derives its listener logger via logger.WithWorker(ctx, \"azureservicebus\", \"listener\")."}
gaps:
  - "AMQP bodies are flattened: amqp-value and sequence bodies are stored as bytes and returned as a data section; application property types survive in memory but are strings after a persistence restore or when set through REST. See families.amqp_transport."
  - "No SQL-filter rule evaluation for subscriptions -- every subscription is effectively TrueFilter (match-all). See families.filter_evaluation."
  - "Client dead-letter and PeekMessages use emulator-extension REST shapes (BrokerProperties DeadLetterReason on the Unlock PUT; GET .../messages); the AMQP plane now does both natively."
  All gaps above are intentional MVP scope per AZURE.md section 9's M5 entry (or, for DeadLetter, a deliberate pause pending human input), not oversights.
deferred:
  - "Initial implementation pass (2026-09-06): seeded this service from scratch per AZURE.md M5 (see AZURE.md section 9). Structurally mirrors services/azurequeue's pop-receipt/visibility-timeout pattern for message locks and services/sns's topic/subscription fan-out bookkeeping; no prior audit history to reconcile."
  - "Follow-up pass (2026-09-07): closed five bounded parity gaps -- per-entity LockDuration/MaxDeliveryCount/DefaultMessageTimeToLive, PeekLock long-poll, full Atom+XML entity-kind parsing, and Get/List operations. Left the explicit client-initiated DeadLetter gap as-is after research found the wire shape unconfirmed (see gaps above); SQL-filter evaluation, sessions remained deferred per AZURE.md section 9."
  - "Sessions pass (2026-10-20): RequiresSession entities, session-filter receivers with locks, session state, renew-session-lock, FIFO per session, persisted in the snapshot."
  - "AMQP 1.0 pass (2026-10-20): added pkgs/amqp1 and the AMQP listener (plain 5672, TLS 5671) over the shared backend, with Defer, ReceiveDeferred, RenewLock, scheduled enqueue/cancel and lock-token lookup added to the backend; verified with the real azservicebus client."
leaks: {status: clean, note: "The dedicated *http.Server started by StartWorker is stopped by Shutdown via srv.Shutdown(ctx) (falling back to srv.Close() on a graceful-shutdown error, both logged), mirroring services/azurequeue. The Janitor's single background goroutine (started from StartWorker) runs on a worker.Group ticker that stops when its context is cancelled -- verified by leak_test.go starting and immediately tearing one down without leaking."}
---

## Notes

### Why Service Bus gets its own port, and why 10003 specifically
Azure Service Bus has no Azurite equivalent to mirror a port number from, so
`10003` is simply gopherstack's own next-available slot after Azure Table's
`10002`, following the same fixed-port, synchronous-bind, fail-fast pattern
as `services/azureblob`/`azurequeue`/`azuretable` (see AZURE.md section 4 and
section 9's M5 entry). `cli.go`'s `reserveFixedServicePorts` reserves it in
the shared `PortAlloc` pool at startup so an unrelated `Acquire` caller never
collides with it.

### AMQP 1.0 plane
azservicebus speaks only AMQP 1.0, so the service also listens on 5672
(plain; use `UseDevelopmentEmulator=true` in the connection string) and 5671
(TLS with a generated self-signed certificate). Both share the REST plane's
in-memory backend, so a message sent over REST can be received over AMQP and
back. `amqp_sdk_test.go` drives the listeners with the real SDK; the REST
integration test under `test/integration` still uses `net/http`. Set the
ports to 0 (`--azure-servicebus-amqp-port`, `--azure-servicebus-amqp-tls-port`)
to disable a listener.

### Queue/topic entity-kind detection
Real Service Bus's management REST API is Atom+XML and disambiguates a `PUT
/<name>` queue-vs-topic create by the request body's root element name
(`<QueueDescription>` vs `<TopicDescription>`). `createEntity`/
`resolveEntityKind` (`handler.go`) now does a real `encoding/xml` parse of
that shape (`atom.go`'s `parseAtomEntityBody`), in this resolution order:
(1) an explicit `?type=topic` query param always wins; (2) a successful
Atom+XML parse determines the kind and also yields the create body's
`LockDuration`/`MaxDeliveryCount`/`DefaultMessageTimeToLive` properties (see
the per-entity configuration note below); (3) the previous case-insensitive
substring sniff for `"topicdescription"`, kept as a last resort for the REST
integration test's deliberately simplified hand-built bodies (there's no SDK
that can generate a proper Atom+XML body against this REST-only emulator --
see `families.sdk_compat`); (4) default to a queue. Malformed XML never
fails the create -- `xml.Unmarshal` returning an error simply falls through
to (3)/(4), matching this repo's permissive-by-default philosophy. The same
parser serves `createSubscription` for a `SubscriptionDescription`'s
`LockDuration`/`MaxDeliveryCount`; any nested `<RuleDescription>`/`<SqlFilter>`
is structurally ignored by `encoding/xml` (no matching struct field), which
is exactly the existing "accept and discard" filter behavior.

**Namespace tolerance.** Real Service Bus's `QueueDescription`/
`TopicDescription`/`SubscriptionDescription` elements are seen in the wild
under two different namespace URIs --
`http://schemas.microsoft.com/netservices/2010/10/servicebus/connect` (newer)
and `http://schemas.microsoft.com/servicebus/2010/10/` (older). Rather than
picking one and rejecting the other, every parsing struct field tag in
`atom.go` omits the namespace prefix entirely (e.g. `` `xml:"QueueDescription"` ``
rather than a namespace-qualified tag); Go's `encoding/xml` matches such a
tag purely on local element name regardless of the element's actual
namespace, which is exactly the tolerance wanted here. `atom.go`'s own
*response*-side serialization (Get/List) has to commit to emitting one
namespace, and uses the newer one (`sbDescriptionNS`).

### Per-entity LockDuration/MaxDeliveryCount/DefaultMessageTimeToLive
`EntityConfig` (`interfaces.go`) now holds these three properties per queue/
topic/subscription (`storedQueue`/`storedTopic`/`storedSubscription`'s
`Config` field, round-tripped through `backendSnapshot` automatically since
it's a plain exported struct field), parsed out of the Atom+XML create-request
body (see above). A zero-valued field (absent from the body, or present but
unparseable) falls back to the existing package-level default
(`DefaultLockDuration`/`MaxDeliveryCount`/`DefaultMessageTTL`) via
`EntityConfig`'s unexported `lockDuration()`/`maxDeliveryCount()`/
`defaultMessageTTL()` accessor methods -- callers never see the zero value
directly. `PeekLock` uses the entity's `LockDuration` whenever the caller
passes a non-positive `lockDuration` (the backend resolves it, not the
handler -- see `StorageBackend.PeekLock`'s doc comment for why this shape was
chosen over threading it through the handler); `Abandon` and the Janitor's
lock-expiry sweep both use the entity's `MaxDeliveryCount` for the
dead-letter-on-exhaustion decision. `Send` uses the target entity's
`DefaultMessageTimeToLive` when the caller supplies no per-message
`TimeToLive`, and **caps** an explicit per-message `TimeToLive` at that same
value if it's larger -- matching real Service Bus's documented behavior that
a message's effective TTL is `min(requested TTL, entity's
DefaultMessageTimeToLive)`.

**The ISO 8601 duration parser and its overflow clamp.** `LockDuration` and
`DefaultMessageTimeToLive` are ISO 8601 duration strings on the wire (e.g.
`PT1M`, `PT30S`, `P14D`); `MaxDeliveryCount` is a plain integer and needs no
such parsing. Rather than add a new module dependency, `iso8601.go` implements
a small, purpose-built `parseISO8601Duration`/`formatISO8601Duration` pair
supporting exactly the `PnDTnHnMnS`/`PTnHnMnS` subset (with optional
fractional seconds) that Service Bus itself ever emits or expects for these
two properties -- weeks, months, and years are deliberately unsupported.
Real Service Bus uses `P10675199DT2H48M5.4775807S` as its documented
"infinite"/max-value sentinel for these fields (it's .NET's
`TimeSpan.MaxValue` spelled out in ISO 8601); parsed literally, that value
would overflow `time.Duration`'s int64-nanosecond range. `parseISO8601Duration`
detects this (the accumulated nanosecond total reaching or exceeding
`math.MaxInt64`) and clamps to the maximum representable `time.Duration`
(~292 years) rather than erroring -- a deliberate, documented approximation
rather than silent data loss, and unit-tested (`iso8601_internal_test.go`)
both for that exact sentinel string and for values beyond it. The boundary
comparison is deliberately `>=`, not `>`: `float64(math.MaxInt64)` rounds up
to exactly 2^63 (one more than the actual maximum, 2^63-1, which has no exact
float64 representation), so a `nanos == 2^63` input would fail a plain `>`
check and fall through to converting an out-of-range float64 to `int64` --
implementation-defined behavior that yields a negative duration on common
platforms. A code-review pass caught this off-by-one before it shipped;
`iso8601_internal_test.go` has a case for exactly the boundary value.

### LockDuration's 5-minute maximum
Real Service Bus caps a queue's or subscription's `LockDuration` at 5 minutes
(its documented default is 1 minute); a create request specifying more is
rejected. `handler.go`'s `validateEntityConfig` (called from `createEntity`
for a queue and unconditionally from `createSubscription`) now enforces the
same bound via the new `MaxLockDuration` constant, returning 400 Bad Request
-- matching real Service Bus's own validation instead of silently persisting
an unenforceable value. This is deliberately narrow: only `LockDuration`'s
upper bound and `MaxDeliveryCount`'s non-negativity are checked; no other
property is bounded, and nothing is clamped (out-of-range values are
rejected outright, not silently corrected).

### PeekLock long-poll ("?timeout=") and its 30-second cap
Real Service Bus clients pass `?timeout=<seconds>` on `POST .../messages/head`
to wait up to that long for a message before giving up, rather than getting
an immediate 204. This is now implemented: `messageQueue` (`store.go`) gained
a broadcast generation `notify chan struct{}`, closed and replaced under the
backend's write lock (`broadcastLocked`) by anything that makes a new message
newly available on *either* its live list or its dead-letter sub-queue --
`Send`, `Abandon` (both its release-back-to-availability outcome and its
dead-letter-on-exhaustion outcome), and the Janitor's sweep (both its
lock-release path and its lock-expiry/TTL-expiry dead-letter paths) all call
it. One channel is shared by both lists, so a dead-letter transition also
wakes a live-list waiter and vice versa; the wrong-list waiter simply
re-checks, finds nothing, and goes back to sleep -- a harmless spurious
wakeup, not a bug, and not worth a second channel. `PeekLockWait`
(`message_ops.go`) mirrors `services/sqs`'s
`ReceiveMessage`/`pollReceive`/`receiveOnce` long-poll shape closely: an
initial immediate attempt (`peekLockOnce`, which -- like SQS's `receiveOnce`
-- captures the entity's current `notify` channel under the *same* lock as
the failed read, closing the lost-wakeup race window), then a wait loop
selecting on that channel, a recheck-timer backstop (sized to
`min(timeout, 1s)` so a sub-second timeout isn't stretched out to a full
second), and the overall deadline (re-checked before every subsequent
`peekLockOnce` attempt, so a notification racing the deadline can't
resurrect an already-expired wait into a false success), with the same
careful timer stop-and-drain discipline SQS uses before each `Reset`.

**Why 30 seconds.** `MaxPeekLockWaitTimeout` is a deliberate cap on
server-side resource use per in-flight long-poll request (one goroutine and
one held connection for up to this long), chosen to match real Service Bus's
own documented 30-second long-poll/operation-timeout value rather than SQS's
unrelated 20-second SQS-specific maximum. This is **not** related to
`azureServiceBusReadTimeout` (60s): that setting is `http.Server.ReadTimeout`,
which only bounds how long reading the incoming request (headers/body) may
take -- it does not bound handler execution time, and this server sets
neither a `WriteTimeout` nor any handler-level deadline. A long-poll
handler blocking for the full 30s cap is not at risk of being torn down by
any `http.Server` timeout; the wait is bounded purely by the cap itself and
by request-context cancellation on client disconnect. (An earlier version of
this note incorrectly attributed the 30s choice to avoiding a race with
`ReadTimeout` -- that reasoning was wrong and has been corrected here.) A
`?timeout=` above the cap is clamped down to it (not rejected), matching
this repo's permissive-by-default stance elsewhere.

**A deliberate improvement over the SQS precedent: context-awareness.**
SQS's `pollReceive`/`receiveOnce` take no `context.Context` at all --the wait
loop only ever exits on deadline or message arrival. `PeekLockWait` instead
takes a `ctx context.Context` (the handler passes `c.Request().Context()`)
and selects on `ctx.Done()` too, so a client that disconnects mid-long-poll
releases the waiting goroutine immediately rather than holding it (and the
underlying connection's resources) until the full timeout elapses.

### Explicit client-initiated DeadLetter: research finding
The brief asked to verify, before implementing, whether real Service Bus's
Brokered Messaging REST API expresses an explicit "dead-letter this message
now" client operation as a `PUT` on the same lock URI Abandon uses
(`.../messages/<id>/<token>`), carrying `DeadLetterReason`/
`DeadLetterErrorDescription` in a `BrokerProperties` *request* header. That
research did not confirm this shape:

- The current official REST API reference page for this exact endpoint,
  [Unlock Message](https://learn.microsoft.com/en-us/rest/api/servicebus/unlock-message)
  (`PUT .../messages/{messageId}/{lockToken}`), documents its Request Body as
  **"None"** and lists no `BrokerProperties` (or any other) request header
  beyond `Authorization`. It does not mention dead-lettering as a possible
  outcome of this call at all.
- No separate "Deadletter Message" (or similarly named) REST operation page
  exists in Microsoft's current REST API reference for Service Bus; searching
  both the docs site and its source repository turned up nothing.
- The `DeadLetterReason`/`DeadLetterErrorDescription` *properties themselves*
  are real and well-documented -- they appear on a dead-lettered message once
  it's in the DLQ (e.g. `ServiceBusReceivedMessage.DeadLetterReason` in the
  .NET SDK) -- but every source found describes them as populated by the
  *SDK's* `DeadLetterMessageAsync`/`DeadLetter()` call, which is itself
  layered on AMQP 1.0's message-disposition mechanism (a `<modified>` or
  `<rejected>` outcome with annotations), not as a REST wire-level request
  header on a documented HTTP endpoint.

Given the brief's explicit instruction not to invent an unconfirmed
endpoint, **nothing was implemented for this gap.** It remains exactly as
before: dead-lettering is only reachable via the `$DeadLetterQueue` path
segment on PeekLock/Complete/Abandon, or automatically via the Janitor's
lock-expiry/TTL sweep. This is flagged back to a human to weigh in on: either
(a) accept that this really is an AMQP-only capability with no REST
equivalent (consistent with `families.sdk_compat`'s existing AMQP/REST
distinction), or (b) provide a primary source confirming the REST wire shape
so it can be implemented with confidence.

### Get/List operations and their fidelity bar
`GET /<queue-or-topic>`, `GET /<topic>/subscriptions/<name>`,
`GET /$Resources/Queues`, `GET /$Resources/Topics`, and (falling out
naturally from the same path-parsing work, so included even though only
"nice to have") `GET /<topic>/subscriptions` are all new. `$Resources/*` is
matched and routed *before* any generic `/<entity>` parsing in
`parseRequestPath` -- a queue or topic literally named `$Resources` is not a
real-world concern, but the precedence is explicit and commented rather than
accidental. Response `Content-Type` matches real Service Bus:
`application/atom+xml;type=entry;charset=utf-8` for a single Get,
`application/atom+xml;type=feed;charset=utf-8` for a List.

**Fidelity bar.** `services/azurequeue`'s `ListQueues` and
`services/azuretable`'s `ListTables`/`GetEntity`/`QueryEntities` are all
un-paginated, single-page, name-sorted listings with no metadata beyond a
few flat fields (see their own PARITY.md notes) -- there is no Atom+XML
envelope in either sibling, since Azure Storage's (Blob/Queue/Table) REST
surface isn't Atom+XML to begin with. Service Bus's *real* management API
genuinely is Atom+XML, unlike those siblings, so this MVP does implement a
real (if minimal) `<entry>`/`<feed>` envelope with a `<content
type="application/xml">` holding a `QueueDescription`/`TopicDescription`/
`SubscriptionDescription` -- matching Service Bus's actual wire shape more
closely than the Storage siblings' pattern would allow. Within that
envelope, though, the same "flat, un-paginated, sorted-by-name, only the
properties this MVP already models" simplification the siblings use is kept:
no `updated`/`id`/`author` Atom boilerplate beyond `<title>`, no
continuation tokens, and no properties beyond
`LockDuration`/`MaxDeliveryCount`/`DefaultMessageTimeToLive`. Duration
fields are serialized back to ISO 8601 via `formatISO8601Duration`
(`iso8601.go`), unit-tested for round-trip fidelity against
`parseISO8601Duration`.

### Auth
`Authorization: SharedAccessSignature sr=...&sig=...&se=...&skn=...` is
always structurally parsed (`sas.go`'s `ParseSASAuthorization`) for its key
name and resource scope, but a missing, malformed, or (when validation is
off) incorrect signature is still accepted -- matching this repo's
permissive-by-default philosophy for every other Azure service.
Cryptographic verification (`VerifySAS`) is opt-in via
`Handler.WithSASValidation` / `--azure-servicebus-validate-sas`, mirroring
`services/s3`'s `WithPresignValidation` and Blob/Queue/Table's
`WithSharedKeyValidation`. `DefaultNamespace`/`DefaultKeyName`/`DefaultKeyValue`
ship a fixed dev identity (analogous to Azurite's `devstoreaccount1`) so a
`Endpoint=sb://sbemulatorns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=...`-shaped
connection string has a stable, documented default to point at.

### Message locks reuse Azure Queue's pop-receipt pattern, adapted
A message's `LockToken` plays the same role as Azure Queue's `PopReceipt`:
issued fresh on every successful `PeekLock`, required to match on
`Complete`/`Abandon`, and rejected with a specific error
(`ErrLockTokenMismatch`/`ErrMessageNotLocked` here, `ErrPopReceiptMismatch`
there) otherwise. The key difference is what happens on release: Azure
Queue's `UpdateMessage` re-hides a message behind a new visibility timeout,
while Service Bus's `Abandon` makes the message immediately available again
(no re-hide delay) -- and additionally checks `DeliveryCount` against
`MaxDeliveryCount` to decide whether to dead-letter it instead of releasing
it, which Azure Queue has no equivalent of.

## More

- [Full parity audit](PARITY.md)
- [All services](../../README.md#services)
