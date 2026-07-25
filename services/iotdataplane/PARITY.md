---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: iotdataplane
sdk_module: aws-sdk-go-v2/service/iotdataplane@v1.35.0   # bumped from v1.32.20; +3 new ops (device connection/messaging introspection)
last_audit_commit: 058bf0373   # HEAD when this manifest was written (parity-4 branch, pre-commit of this pass)
last_audit_date: 2026-07-25
overall: A-           # downgraded from A: 2 new ops are honest but functionally thin (see gaps) -- nothing fabricated, but ListSubscriptions is unconditionally empty and SendDirectMessage doesn't truly target a single client
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  GetThingShadow: {wire: ok, errors: ok, state: ok, persist: ok, note: "404 on deleted (tombstoned) shadow now correctly excluded"}
  UpdateThingShadow: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException (was VersionConflictException); RequestEntityTooLargeException (was InvalidRequestException/plain-413) for >8KB doc; version continues across delete+recreate; state.desired/state.reported now enforce AWS's documented 8-level JSON nesting depth cap; invented maxShadowsPerThing=100 cap REMOVED (no such AWS quota exists -- see notes)"}
  DeleteThingShadow: {wire: ok, errors: ok, state: ok, persist: ok, note: "response now omits state (empty response state document, AWS-doc-confirmed); soft-delete tombstone preserves version continuity"}
  ListNamedShadowsForThing: {wire: ok, errors: ok, state: ok, persist: ok, note: "excludes tombstoned (deleted) named shadows"}
  Publish: {wire: ok, errors: ok, state: ok, persist: n/a, note: "now parses+validates the full PublishInput wire surface (contentType/messageExpiry/responseTopic as query params; correlationData/payloadFormatIndicator/userProperties as X-Amz-Mqtt5-* headers, per serializers.go); userProperties persists onto the retained message (see GetRetainedMessage); delivers via MQTTPublisher broker interface; ErrNoBroker path logs+drops, and none of contentType/correlationData/messageExpiry/payloadFormatIndicator/responseTopic reach the broker (documented follow-up, not a stub -- see gaps)"}
  DeleteConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL AWS op restored to its real wire path DELETE /connections/{clientId} (was regressed to /_admin/-only in a prior 'AWS-accuracy' pass); admin alias kept for test convenience; now returns ResourceNotFoundException (was an unconditional no-op) when clientId has no tracked connection -- real AWS models this error for DeleteConnection"}
  GetRetainedMessage: {wire: ok, errors: ok, state: ok, persist: ok, note: "response now includes userProperties (base64, null when unset) -- was missing entirely; confirmed against GetRetainedMessageOutput"}
  ListRetainedMessages: {wire: ok, errors: ok, state: ok, persist: ok, note: "summary now includes qos -- a prior audit incorrectly asserted RetainedMessageSummary excludes qos; the real deserializer (awsRestjson1_deserializeDocumentRetainedMessageSummary) proves it's present"}
  GetConnection: {wire: ok, errors: ok, state: partial, persist: ok, note: "NEW op (GET /connections/{clientId}, real path field-diffed against serializers.go/deserializers.go). Reuses the same connections table DeleteConnection already tracks (gopherstack-only RegisterConnection admin extension) -- an untracked clientId is ResourceNotFoundException (matches the real op's modeled error), a tracked one returns connected:true/clientId/connectedSince genuinely. cleanSession/disconnectReason/disconnectedSince/keepAliveDuration/sessionExpiry/sourcePort/targetIp/targetPort/thingName/vpcEndpointId have no real backing data in this emulator and are omitted from the response (not fabricated as zero values) -- see gaps"}
  ListSubscriptions: {wire: ok, errors: ok, state: gap, persist: n/a, note: "NEW op (GET /connections/{clientId}/subscriptions). Errors/not-found semantics reuse the connections table (consistent with GetConnection/DeleteConnection). subscriptions is unconditionally empty: gopherstack tracks no per-client MQTT subscription state anywhere reachable from this package -- see gaps for why the real data (which does exist in the mochi-mqtt broker) couldn't be wired up this pass"}
  SendDirectMessage: {wire: ok, errors: ok, state: partial, persist: n/a, note: "NEW op (POST /connections/{clientId}/messages, field-diffed against serializers.go's awsRestjson1_serializeOpHttpBindingsSendDirectMessageInput). Validates clientId/topic exactly like GetConnection/Publish and returns ResourceNotFoundException for an untracked clientId (413 RequestEntityTooLargeException on oversized payload -- unlike Publish, this IS modeled for SendDirectMessage, confirmed via its error case list). Delivers via the same broker-backed path as Publish rather than true per-client-targeted delivery -- see gaps"}
families:
  admin-only-extensions: {status: ok, note: "RegisterConnection/ListConnections/ListThingsWithShadows have NO real AWS iotdataplane equivalent (confirmed against the SDK's op file listing); correctly confined to gopherstack-only paths (/_admin/connections, /api/things/shadow/ListThingsWithShadows) so they cannot shadow real AWS traffic"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Publish with no MQTT broker wired logs a warning and silently drops the message (ErrNoBroker path in backend.go Publish()). This is intentional degradation, not a disguised no-op -- when a broker IS wired (see cli.go startup, out of scope for this service-only pass) the message is delivered for real, retain/qos forwarded. Additionally, the MQTTPublisher interface (services/iotdataplane/interfaces.go) only carries topic/payload/retain/qos -- contentType/correlationData/messageExpiry/payloadFormatIndicator/responseTopic are parsed and validated at the HTTP layer but never reach live MQTT subscribers, since forwarding them would require extending MQTTPublisher and its only real implementation (services/iot/broker.go, backed by mochi-mqtt), which is outside this service's own scope. No AWS-modeled response surface within iotdataplane echoes these fields back (GetRetainedMessageOutput only carries userProperties, which IS wired through), so this has no other observable wire-parity impact. No further work identified without cross-service broker changes."
  - "UnsupportedDocumentEncodingException (real AWS error, modeled for GetThingShadow/DeleteThingShadow/UpdateThingShadow) is never returned -- no Content-Encoding-based validation exists. Left unimplemented: re-verified this pass via targeted web search (AWS API reference, boto3 docs) and still found no documented trigger condition (e.g. which Content-Encoding values are rejected, or whether it's Accept-Encoding-driven). Speculative validation risks a wrong-shape fix. Candidate for a future audit pass with real-AWS verification first (e.g. a live AWS account probe)."
  - "ListSubscriptions always returns an empty subscriptions array, even for a tracked/connected client. Real per-client subscription state DOES exist elsewhere in the repo -- the mochi-mqtt broker (services/iot/broker.go, github.com/mochi-mqtt/server/v2) tracks each client's live subscriptions in cl.State.Subscriptions -- but it is not reachable from this package: the MQTTPublisher interface (interfaces.go) this backend depends on only exposes topic-broadcast Publish(), and extending it to expose subscription queries would require changing services/iot/broker.go (out of scope for this pass; that directory was explicitly off-limits). Returning an honestly empty list for a genuinely-tracked client was chosen over fabricating topic filters. Candidate follow-up: add a ListSubscriptions(clientID) method to MQTTPublisher backed by Broker.server.Load().Clients.Get(clientID).State.Subscriptions, then have InMemoryBackend.ListSubscriptions call through it when a broker is wired."
  - "SendDirectMessage delivers by broadcasting on the target topic through the same broker-backed path as Publish, not by sending directly to the named client the way real AWS does. Real SendDirectMessage explicitly does not require the receiving client to be subscribed to the topic (\"the receiving client does not need to subscribe to the topic\"); gopherstack's only broker primitive (MQTTPublisher.Publish, backed by mochi-mqtt's s.Publish) has no per-client-addressed send, so a client that isn't subscribed to the given topic will NOT observe a SendDirectMessage the way it would against real AWS. This is a deliberate, documented choice (wiring into the real delivery path so at least topic-subscribers observe it, rather than writing to a dead-end store no caller could ever observe) -- see InMemoryBackend.SendDirectMessage's doc comment. Fixing this for real would need mochi-mqtt's client-targeted write path (s.Clients.Get(clientID) + a raw PUBLISH write), which lives in services/iot/broker.go, out of scope here. confirmation/timeout (real AWS: wait for a QoS-1 PUBACK, HTTP 504 on timeout) select QoS 0-vs-1 on the outgoing publish but never actually block or time out, since MQTTPublisher.Publish is synchronous/fire-and-forget with no ack channel."
  - "GetConnection omits cleanSession/disconnectReason/disconnectedSince/keepAliveDuration/sessionExpiry/sourcePort/targetIp/targetPort/thingName/vpcEndpointId from its response for every client, tracked or not -- gopherstack's connections table (populated only by the gopherstack-only RegisterConnection admin extension) never had this data to begin with (no real MQTT CONNECT packet is parsed anywhere in this service). Omitted (not zero-valued) so a real SDK client decodes these exactly as if the server had never observed them, which is wire-compatible even though it under-reports what a live AWS endpoint would return."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Chaos fault-injection paths (ChaosServiceName/ChaosOperations) -- not part of AWS wire surface, no parity concern."
leaks: {status: clean, note: "no goroutines/timers introduced; tombstone rows are bounded by the same lifecycle as live shadow rows (same store.Table, same Reset/Snapshot/Restore path); removing the maxShadowsPerThing cap does not introduce unbounded growth risk beyond what already existed (shadows were never capped process-wide, only per-thing, and the per-thing cap had no eviction/GC of its own -- it only returned an error)"}
---

## Notes

Freeform: AWS-behavior specifics worth remembering (exact algorithms, wire quirks,
error-message text, protocol = query-XML / REST-XML / REST-JSON / json-1.0), and any
"looks-wrong-but-correct" traps so the next auditor doesn't re-flag them.

- **Protocol**: restjson1. Verified directly against the compiled
  `aws-sdk-go-v2/service/iotdataplane@v1.32.20` serializers/deserializers (most
  authoritative source available -- prefer this over doc prose when they conflict).

- **Real error codes per op** (confirmed via `deserializers.go`'s per-op
  `awsRestjson1_deserializeOpError*` case lists): `ConflictException` and
  `RequestEntityTooLargeException` are modeled **only** for `UpdateThingShadow` --
  no other op (including `Publish`) has them in its error set. Don't add
  `RequestEntityTooLargeException` handling to `Publish`'s oversized-body path
  without new evidence; its current generic 413 (no specific AWS error code) is
  the closest defensible behavior given it isn't a modeled exception there.

- **There is no `VersionConflictException`** in the real API -- it was a
  gopherstack-invented name. The real wire error code for a shadow version
  mismatch is `ConflictException` (`ErrVersionConflict`'s Go identifier is kept
  unchanged for API stability; only its wire string changed).

- **DeleteThingShadow response shape**: AWS docs (device-shadow-rest-api.html)
  say the body is an "Empty response state document" -- confirmed this means
  only `version` + `timestamp`, NOT `state`/`metadata`/`clientToken`. Do not
  "fix" this back to including state; a previous implementation had a *dead*
  fallback branch with the correct minimal shape that was never reached because
  the primary path always succeeded and returned the (wrong, too-rich) full
  shadow response.

- **Version does not reset on delete**: verbatim from AWS docs: "Note that
  deleting a shadow does not reset its version number to 0." Implemented via a
  tombstone (`shadowEntry.deleted`) that keeps the row (with state cleared) in
  the `shadows` table instead of physically removing it, so
  `nextShadowVersion` naturally continues from the pre-delete version when the
  shadow is recreated. Tombstones are excluded from `GetThingShadow`,
  `ListNamedShadowsForThing`, and `ListThingsWithShadows`, and don't count
  against `maxShadowsPerThing` (see `liveShadowCount`). Persisted via
  `shadowEntrySnap.Deleted` (additive `omitempty` field -- old snapshots decode
  fine with `deleted=false`, no `iotdataplaneSnapshotVersion` bump needed).

- **DeleteConnection is a real, published AWS op** (`DELETE
  /connections/{clientId}`, confirmed via `api_op_DeleteConnection.go` +
  serializer) -- unlike `RegisterConnection`/`ListConnections`, which do NOT
  exist in the real SDK at all (grep the SDK module's op file listing to
  reconfirm if in doubt). A prior "AWS-accuracy audit batch" commit
  (3f01eaf0) moved all three off `/connections` to `/_admin/connections` in
  one sweep to properly hide the two fake ops, but collaterally broke real
  wire compatibility for the one genuine op. Fixed by restoring `DELETE
  /connections/{clientId}` as an additional real route (kept the `/_admin/`
  alias too, since existing tests and tooling depend on it). If you see
  `/connections` show up again in a "cleanup", check whether DeleteConnection
  is being swept along with the fake ops before touching it.

- **Named/classic shadow key**: `shadowKey(thingName, shadowName)` = `"<thingName>#<shadowName>"`,
  classic shadow uses `shadowName == ""`. `#` cannot appear in either
  component given their validation regexes, so no collision risk.

- **Path-style named shadow route is NOT real AWS wire**: `handler.go` also
  accepts `/things/{thingName}/shadow/name/{shadowName}` in addition to the
  real `/things/{thingName}/shadow?name=...` query-param form. Confirmed via
  `httpbinding.SplitURI` call sites in `serializers.go` that the real SDK only
  ever generates the `?name=` form for Get/Update/DeleteThingShadow -- the
  path-style route has no equivalent in `aws-sdk-go-v2/service/iotdataplane`.
  This is pure test-convenience leniency (a superset of accepted request
  shapes, same op names, same responses): it never causes a real SDK client's
  traffic to be misrouted or misinterpreted, since a real client only ever
  sends the `?name=` form. Left in place (heavily used by existing tests), but
  noted here so a future audit doesn't mistake it for a modeled AWS op.

- **Shadow doc size limit**: `maxShadowDocumentBytes` = 8KB, matches
  `maxShadowBodyBytes` at the HTTP layer (`handler.go`'s `MaxBytesReader`), so
  in practice the HTTP-layer cutoff fires first and the backend's own check is
  a defensive backstop reachable only when the backend is invoked directly
  (bypassing the HTTP body limit) -- see
  `TestRefinement2_ShadowDocumentValidation_BackendSizeCheck`. Both paths now
  return `RequestEntityTooLargeException`/413 consistently.

- **Publish max size**: 128KB (`maxPublishBodyBytes`), matches real AWS IoT
  Core's documented MQTT/HTTP publish payload limit. No error-code fix applied
  here (see gaps) since `RequestEntityTooLargeException` isn't modeled for
  `Publish`.

- **`errMethodNotAllowed` constant** changed from the placeholder string
  `"method not allowed"` to the real AWS wire error code
  `"MethodNotAllowedException"` (used across every 405 response in this
  handler). No test depended on the old literal text (only status codes were
  asserted), so this was a safe, systemic wire-accuracy fix.

- **`maxShadowsPerThing=100` cap REMOVED**: the prior pass flagged this as
  low-confidence and left it in place. This pass re-verified against the
  authoritative AWS General Reference "AWS IoT Core endpoints and quotas"
  page: it documents shadow document size (8KB), shadow name length (64
  bytes), in-flight-unacknowledged-messages-per-thing (10), and
  requests-per-second-per-shadow (20) -- but **no limit at all on the number
  of named shadows per thing**. Community reports (AWS re:Post) describe
  200-10,000+ named shadows on a single thing without hitting any API-level
  cap. gopherstack's self-imposed 100-shadow cap was therefore a
  gopherstack-invented behavior that would reject `UpdateThingShadow` calls a
  real AWS account would accept -- the wrong direction for parity (stricter
  than AWS, not more lenient). Removed the check, the now-dead
  `liveShadowCount` helper, and the `MaxShadowsPerThing` test export; replaced
  the cap-boundary tests in shadows_test.go with
  `Test_ManyNamedShadowsPerThing_*` tests proving no artificial limit exists.

- **Shadow state JSON nesting depth cap ADDED**: the same AWS quotas page
  documents "Maximum depth of JSON device state documents: 8 levels (in both
  desired and reported sections)" -- this was previously unenforced entirely.
  Added `maxShadowStateDepth = 8` and `validateShadowStateDepth` (shadows.go),
  applied to `state.desired`/`state.reported` in `applyShadowStateSection`,
  returning `InvalidRequestException` when exceeded. The section's top-level
  object itself counts as depth 1 (i.e. `{"a":1}` is depth 1, `{"a":{"b":1}}`
  is depth 2). See `Test_ShadowStateDepth_*` in shadows_validation_test.go.

- **`ListRetainedMessages` summary was missing `qos`**: a prior audit pass
  asserted (incorrectly) that AWS's `RetainedMessageSummary` excludes `qos`
  and added tests locking that in. Directly reading
  `awsRestjson1_deserializeDocumentRetainedMessageSummary` in the real SDK's
  `deserializers.go` shows `qos` IS a recognized field on that shape. Fixed
  `handleListRetainedMessages` to include it and rewrote the tests that had
  asserted its absence (`Test_ListRetainedMessages_SummaryIncludesQos`).

- **`GetRetainedMessage` was missing `userProperties`**: `GetRetainedMessageOutput`
  in the real SDK carries a `UserProperties []byte` field (base64-encoded MQTT5
  user properties JSON array, or absent/null when unset) that gopherstack's
  response never included. Added `RetainedMessage.UserProperties` (types.go),
  threaded it through `StoreRetainedMessage`'s new `userProperties []byte`
  parameter, and included it in the `GetRetainedMessage` JSON response.

- **`Publish` was missing its entire MQTT5-property wire surface**: real
  `PublishInput` (per `awsRestjson1_serializeOpHttpBindingsPublishInput` in
  `serializers.go`) carries `contentType`/`messageExpiry`/`responseTopic` as
  query params and `correlationData`/`payloadFormatIndicator`/`userProperties`
  as `X-Amz-Mqtt5-*` headers (the last base64-encoded). None of these were
  even read from the request before this pass -- a real SDK client setting
  any of them got silently ignored with no validation. Added
  `parseMQTT5PublishParams` (handler_publish.go) plus validators in publish.go
  (`validatePayloadFormatIndicator`, `validateResponseTopic`,
  `parseMessageExpiry`, `decodeUserProperties`) so malformed values now
  produce the correct `InvalidRequestException`. Of these fields, only
  `userProperties` has an AWS-visible effect reachable within this service
  (persisted onto the retained message, see above) -- the rest are
  accepted/validated but not forwarded anywhere further; see gaps for why.

- **`DeleteConnection` never returned `ResourceNotFoundException`**: real AWS
  models this error for `DeleteConnection` (confirmed via
  `awsRestjson1_deserializeOpErrorDeleteConnection`'s case list in
  `deserializers.go`), but gopherstack's implementation unconditionally
  succeeded even for a `clientId` with no tracked connection ("idempotent
  delete"). Since gopherstack's only concept of "connected" is the
  `connections` table (populated via the gopherstack-only `RegisterConnection`
  admin extension -- see admin-only-extensions family), a real AWS SDK client
  that never calls the admin endpoint will always get 404 from
  `DeleteConnection`, which is the intended test-convenience contract: use
  `/_admin/connections/{clientId}` (POST) to simulate a connected client
  first. Added `ErrConnectionNotFound` (errors.go) wired to
  `ResourceNotFoundException` in `handleError`. `cleanSession`/
  `preventWillMessage` (the two other real `DeleteConnectionInput` query
  params) are still not parsed -- there is no live per-client session state
  modeled in this service to react to them, and `DeleteConnectionOutput` has
  no fields that would surface a difference either way, so this is left as an
  accepted no-op akin to the Publish/broker gap, not tracked as a separate
  gap entry since it has zero observable wire impact.

- **New family this pass: device connection/messaging introspection**
  (`GetConnection`, `ListSubscriptions`, `SendDirectMessage` -- SDK bumped to
  `v1.35.0`, `+3` ops). All three are real, published AWS iotdataplane
  operations rooted at `/connections/{clientId}` (confirmed via
  `api_op_{GetConnection,ListSubscriptions,SendDirectMessage}.go` and their
  serializers). Routing required generalizing the old
  `isDeleteConnectionPath`/`extractConnectionClientID` DELETE-only helpers
  (`handler.go`) into `splitConnectionsWirePath`/`connectionsWireOperation`,
  which classify any `/connections/{clientId}[/subscriptions|/messages]`
  method+path combination into the right op (or `""` for the one
  non-AWS combination that must keep 404ing: bare `POST
  /connections/{clientId}`, which is `RegisterConnection`'s territory and
  only exists at `/_admin/connections/{clientId}`).

- **Real-state survey (why the grade moved to A-, not why it stayed A):**
  this service already had one piece of genuine, if gopherstack-only,
  connection state -- the `connections` table, populated exclusively via the
  `RegisterConnection` admin extension (see `admin-only-extensions`) and
  already used by `DeleteConnection` for its 404 semantics. `GetConnection`
  reuses this table honestly: a registered client's `connected`/`clientId`/
  `connectedSince` are real, tracked values, not fabricated, and an
  unregistered client correctly 404s. But this repo's *other* candidate
  source of real state -- `services/iot`'s `mochi-mqtt`-backed broker, which
  genuinely tracks live subscriptions per client
  (`cl.State.Subscriptions` in `github.com/mochi-mqtt/server/v2`) -- is not
  reachable from `iotdataplane` without extending the `MQTTPublisher`
  interface boundary and its only implementation, `services/iot/broker.go`,
  which this pass was explicitly barred from touching (a concurrent agent's
  scope). That is a real, structural limitation, not a shortcut: it's why
  `ListSubscriptions` is unconditionally empty and why `SendDirectMessage`
  broadcasts on-topic instead of truly addressing one client. Both are
  documented in `gaps` with the exact follow-up (a `ListSubscriptions`/
  client-targeted-write method on `MQTTPublisher`, backed by
  `Broker.server.Load().Clients.Get(clientID)`) for whenever `services/iot`
  is back in scope.

- **`GetConnection`/`ListSubscriptions`/`SendDirectMessage` share
  `ErrConnectionNotFound`** (`errors.go`) with `DeleteConnection` rather than
  inventing per-op not-found errors: all four ops are modeled with
  `ResourceNotFoundException` in their real error case lists
  (`deserializers.go`), and gopherstack's one and only concept of "is this
  clientId connected" is the same `connections` table for all of them --
  reusing the sentinel keeps that consistent instead of accidentally
  diverging.

- **`SendDirectMessage`'s `RequestEntityTooLargeException` is real, unlike
  `Publish`'s**: confirmed via `awsRestjson1_deserializeOpErrorSendDirectMessage`'s
  case list (`GatewayTimeoutException`, `RequestEntityTooLargeException`,
  `UnauthorizedException` are modeled here but *not* on `Publish`'s case
  list -- see the existing "Real error codes per op" note above). Reused
  the existing `ErrRequestTooLarge` sentinel (413) for an oversized message
  body, capped at the same `maxPublishBodyBytes` (128KB) as `Publish`, since
  AWS IoT Core doesn't document a different MQTT/HTTP payload limit specific
  to `SendDirectMessage`. `GatewayTimeoutException`/`UnauthorizedException`
  are left unimplemented: the former needs real PUBACK-wait semantics this
  emulator's fire-and-forget broker interface can't provide (see gaps), and
  the latter has no IAM/permission model in this service to ever trigger it
  -- both are genuine impossibilities given current scope, not oversights.
