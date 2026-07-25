---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: iotdataplane
sdk_module: aws-sdk-go-v2/service/iotdataplane@v1.32.20
last_audit_commit: b88208cbf
last_audit_date: 2026-07-24
overall: A            # genuine fixes found and verified against SDK source + AWS docs
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
families:
  admin-only-extensions: {status: ok, note: "RegisterConnection/ListConnections/ListThingsWithShadows have NO real AWS iotdataplane equivalent (confirmed against the SDK's op file listing); correctly confined to gopherstack-only paths (/_admin/connections, /api/things/shadow/ListThingsWithShadows) so they cannot shadow real AWS traffic"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Publish with no MQTT broker wired logs a warning and silently drops the message (ErrNoBroker path in backend.go Publish()). This is intentional degradation, not a disguised no-op -- when a broker IS wired (see cli.go startup, out of scope for this service-only pass) the message is delivered for real, retain/qos forwarded. Additionally, the MQTTPublisher interface (services/iotdataplane/interfaces.go) only carries topic/payload/retain/qos -- contentType/correlationData/messageExpiry/payloadFormatIndicator/responseTopic are parsed and validated at the HTTP layer but never reach live MQTT subscribers, since forwarding them would require extending MQTTPublisher and its only real implementation (services/iot/broker.go, backed by mochi-mqtt), which is outside this service's own scope. No AWS-modeled response surface within iotdataplane echoes these fields back (GetRetainedMessageOutput only carries userProperties, which IS wired through), so this has no other observable wire-parity impact. No further work identified without cross-service broker changes."
  - "UnsupportedDocumentEncodingException (real AWS error, modeled for GetThingShadow/DeleteThingShadow/UpdateThingShadow) is never returned -- no Content-Encoding-based validation exists. Left unimplemented: re-verified this pass via targeted web search (AWS API reference, boto3 docs) and still found no documented trigger condition (e.g. which Content-Encoding values are rejected, or whether it's Accept-Encoding-driven). Speculative validation risks a wrong-shape fix. Candidate for a future audit pass with real-AWS verification first (e.g. a live AWS account probe)."
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
