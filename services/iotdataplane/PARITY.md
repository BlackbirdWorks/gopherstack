---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: iotdataplane
sdk_module: aws-sdk-go-v2/service/iotdataplane@v1.32.20
last_audit_commit: 57398ee1
last_audit_date: 2026-07-13
overall: A            # genuine fixes found and verified against SDK source + AWS docs
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  GetThingShadow: {wire: ok, errors: ok, state: ok, persist: ok, note: "404 on deleted (tombstoned) shadow now correctly excluded"}
  UpdateThingShadow: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException (was VersionConflictException); RequestEntityTooLargeException (was InvalidRequestException/plain-413) for >8KB doc; version continues across delete+recreate"}
  DeleteThingShadow: {wire: ok, errors: ok, state: ok, persist: ok, note: "response now omits state (empty response state document, AWS-doc-confirmed); soft-delete tombstone preserves version continuity"}
  ListNamedShadowsForThing: {wire: ok, errors: ok, state: ok, persist: ok, note: "excludes tombstoned (deleted) named shadows"}
  Publish: {wire: ok, errors: ok, state: ok, persist: n/a, note: "delivers via MQTTPublisher broker interface; ErrNoBroker path logs+drops (documented follow-up, not a stub -- see gaps)"}
  DeleteConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL AWS op restored to its real wire path DELETE /connections/{clientId} (was regressed to /_admin/-only in a prior 'AWS-accuracy' pass); admin alias kept for test convenience"}
  GetRetainedMessage: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRetainedMessages: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  admin-only-extensions: {status: ok, note: "RegisterConnection/ListConnections/ListThingsWithShadows have NO real AWS iotdataplane equivalent (confirmed against the SDK's op file listing); correctly confined to gopherstack-only paths (/_admin/connections, /api/things/shadow/ListThingsWithShadows) so they cannot shadow real AWS traffic"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Publish with no MQTT broker wired logs a warning and silently drops the message (ErrNoBroker path in backend.go Publish()). This is intentional degradation, not a disguised no-op -- when a broker IS wired (see cli.go startup, out of scope for this service-only pass) the message is delivered for real, retain/qos forwarded. No further work identified without broker wiring changes, which live outside services/iotdataplane/."
  - "UnsupportedDocumentEncodingException (real AWS error, modeled for GetThingShadow/DeleteThingShadow/UpdateThingShadow) is never returned -- no Content-Encoding-based validation exists. Left unimplemented: no clear trigger condition was verified against real AWS behavior, and speculative validation risks a wrong-shape fix. Candidate for a future audit pass with real-AWS verification first."
  - "maxShadowsPerThing=100 (backend.go) is a soft self-imposed cap, not verified against an authoritative AWS quota number -- left unchanged this pass (low confidence either way, non-blocking)."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Chaos fault-injection paths (ChaosServiceName/ChaosOperations) -- not part of AWS wire surface, no parity concern."
leaks: {status: clean, note: "no goroutines/timers introduced; tombstone rows are bounded by the same lifecycle as live shadow rows (same store.Table, same Reset/Snapshot/Restore path)"}
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
