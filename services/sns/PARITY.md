---
service: sns
sdk_module: aws-sdk-go-v2/service/sns@v1.41.0
last_audit_commit: 3d4de4f9
last_audit_date: 2026-07-11
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateTopic: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotent on existing name; FIFO/CBD/Kms validation correct"}
  DeleteTopic: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: now also drops topicMessageArchive (was leaking + could resurrect stale archive on ARN reuse)"}
  ListTopics: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTopicAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "computed attrs (Owner/TopicArn/EffectiveDeliveryPolicy/SubscriptionsConfirmed|Pending|Deleted) correct"}
  SetTopicAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  Subscribe: {wire: ok, errors: ok, state: ok, persist: ok, note: "all 9 protocols; pending-confirmation literal 'pending confirmation'; firehose requires SubscriptionRoleArn; dedup on existing confirmed sub"}
  ConfirmSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  Unsubscribe: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSubscriptions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSubscriptionsByTopic: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSubscriptionAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  SetSubscriptionAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "FilterPolicy/FilterPolicyScope/RedrivePolicy(+DLQ existence check)/DeliveryPolicy/ReplayPolicy/RawMessageDelivery/SubscriptionRoleArn"}
  Publish: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: Lambda/Firehose/SQS-emitter now share one signed envelope (buildPublishedEvent) instead of Lambda fabricating a random-UUID signature"}
  PublishBatch: {wire: partial->ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: per-entry MessageAttributes field prefix was missing '.MessageAttributes' segment (verified against serializers.go) — every batch entry's attributes were silently dropped, breaking FilterPolicy matching for PublishBatch"}
  PublishToTargetArn (TargetArn publish): {wire: ok, errors: ok, state: ok, persist: n/a, note: "EndpointDisabled enforced"}
  PublishSMS (PhoneNumber publish): {wire: ok, errors: ok, state: ok, persist: n/a, note: "opt-out + sandbox-unverified enforced"}
  CreatePlatformApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPlatformApplicationAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  SetPlatformApplicationAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPlatformApplications: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePlatformApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePlatformEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEndpointAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  SetEndpointAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEndpointsByPlatformApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  AddPermission: {wire: ok, errors: ok, state: ok, persist: ok, note: "stored on Topic.Permissions, travels with topic snapshot"}
  RemovePermission: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSMSSandboxAccountStatus/CreateSMSSandboxPhoneNumber/DeleteSMSSandboxPhoneNumber/ListSMSSandboxPhoneNumbers/VerifySMSSandboxPhoneNumber: {wire: ok, errors: ok, state: ok, persist: ok}
  CheckIfPhoneNumberIsOptedOut/ListPhoneNumbersOptedOut/OptInPhoneNumber: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: ErrOptedOut sentinel text was the unrelated copy-pasted string 'KMSOptInRequired'"}
  GetSMSAttributes/SetSMSAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDataProtectionPolicy/PutDataProtectionPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "backed by topic/resource attribute; not deep-audited against a real customer-managed-data-identifier grammar"}
  ListOriginationNumbers: {wire: ok, errors: ok, state: ok, persist: ok, note: "AWS has no public create API; empty by default, SeedOriginationNumber for tests"}
  TagResource/UntagResource/ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "pkgs/tags-backed"}
families:
  filter_policy_matching: {status: ok, note: "prefix/suffix/equals-ignore-case/anything-but(+nested)/exists/numeric(6 ops)/wildcard/cidr/$or, MessageBody vs MessageAttributes scope, String.Array expansion, 150-condition cap, 256KiB size cap — read in full, no gaps found"}
  fifo_topics: {status: ok, note: "MessageGroupId required, ContentBasedDeduplication (SHA-256 body digest) vs explicit MessageDeduplicationId mutually exclusive, 5-min dedup window with bounded+swept map, 20-digit zero-padded monotonic SequenceNumber per topic, PublishBatch per-entry dedup"}
  delivery_lambda_firehose_sms_application: {status: ok, note: "fixed this pass: (1) Lambda envelope now carries the real per-publish Timestamp/Signature/SigningCertURL/UnsubscribeURL instead of a fabricated random-UUID signature and empty cert/unsub URLs; (2) Firehose now respects RawMessageDelivery (envelopes as JSON when false, matching AWS default, previously always sent the bare message); DLQ redrive on failure now forwards the same body that was attempted"}
  replay_policy_archive: {status: ok, note: "fixed this pass: replay previously only reached HTTP/HTTPS (direct call) and SQS (via the publish emitter) — Lambda/Firehose/SMS/Application subscriptions with a ReplayPolicy silently replayed nothing. Now fans out through the same per-protocol delivery functions Publish uses. NOT investigated: real AWS restricts archive/replay to FIFO topics + SQS/Lambda/Firehose only; this backend allows ArchivePolicy on standard topics and replays to HTTP/email/sms/application too (see gaps)"}
  http_https_delivery: {status: ok, note: "RSA-2048 self-signed cert + SignatureVersion=2 SHA256 signing, retry via DeliveryPolicy/EffectiveDeliveryPolicy, DLQ redrive, concurrency-capped worker semaphore, ctx-cancel on shutdown"}
  error_codes: {status: ok, note: "NotFound/TopicAlreadyExists/PlatformApplicationAlreadyExists/InvalidParameter/EndpointDisabled/OptedOut/AuthorizationError(permission label) all map to correct AWS code strings; 400 vs 500 split verified in handleBackendError"}
gaps:
  - "ArchivePolicy/ReplayPolicy are accepted on any topic and fan out to any protocol (HTTP/email/sms/application); real AWS restricts message archiving/replay to FIFO topics with SQS/Lambda/Firehose subscribers only. Not fixed this pass — no doc access to confirm the exact restriction text/error code, and existing tests exercise HTTP replay deliberately. (bd: gopherstack-bz6)"
  - "Subscribe does not validate that an 'sqs' protocol endpoint is a well-formed SQS queue ARN (any string is accepted); AWS rejects malformed endpoint ARNs per-protocol. Low value — SDK-driven callers always pass valid ARNs. (bd: gopherstack-bz6)"
  - "SignatureVersion topic/subscription attribute is accepted and stored (isKnownTopicAttribute) but delivery always signs with SHA-256 (AWS 'SignatureVersion 2'); a topic explicitly set to SignatureVersion=1 should sign with SHA-1 instead. Not fixed — no consumer in this codebase verifies signatures, so behavior is unobservable, and getting this wrong is worse than leaving it (bd: gopherstack-bz6)"
deferred:
  - "GetDataProtectionPolicy/PutDataProtectionPolicy: not verified against the real data-identifier grammar (e.g. built-in identifiers like 'Name', 'Address'); only checked that the policy round-trips as an opaque JSON string"
  - "Cross-service integration (test/integration/*_parity_test.go) was not run this pass — see parity-principles.md note that unit tests are not parity proof; recommend running the SDK-driven integration suite in a follow-up"
leaks: {status: clean, note: "fixed this pass: (1) topicMessageArchive was never persisted (Snapshot/Restore) and was never cleaned up on DeleteTopic (both leak + ARN-reuse resurrection bug); (2) smsDeliveries/emailDeliveries/applicationDeliveries observability buffers had no cap and grew unboundedly under sustained publish traffic without a Drain* call — added appendBounded with maxRecordedDeliveries=100k; (3) notificationSigner.certURL was read/written without synchronization (SetSigningCertBaseURL vs concurrent delivery reads) — added a dedicated RWMutex. HTTP delivery goroutines already had proper ctx-cancel + semaphore + deliveryWg cleanup (unchanged, verified correct)."}
---

## Notes

Freeform notes for the next auditor — AWS-behavior specifics worth remembering, and
"looks-wrong-but-correct" traps.

### 2026-07-11 re-audit (parity-4)
No code changes made — no genuine bugs found. `services/sns/` had zero commits between
the prior ledger's `last_audit_commit` and current HEAD (that prior audit's own commit,
`ce30166a`, is what's actually recorded in the ledger, so there was no local drift to
re-check). SDK bumped `v1.40.3` -> `v1.41.0`: changelog is serialization-snapshot-test
and dependency-only, zero new/changed SNS operations. Re-verified: `buildActions()`
still routes all 42 real SDK operations 1:1 (no stubs, no missing ops); the four fixes
called out in the prior pass are intact (`topicMessageArchive` cleanup on DeleteTopic +
persistence round-trip, PublishBatch per-entry `MessageAttributes.` prefix, `ErrOptedOut`
sentinel text, `appendBounded`-capped delivery buffers). All gates
(build/vet/`-race` test/`go fix -diff`/golangci-lint) pass clean with zero issues.

### Protocol
SNS is the **AWS query (XML) protocol** (`Version=2010-03-31`, form-encoded request,
XML response with a `ResponseMetadata`/`RequestId` wrapper). PublishBatch entries and
per-entry MessageAttributes use the query-protocol list/map indexing convention:
`Parent.member.N.Field`, and nested maps use `Parent.member.N.MapField.entry.M.Key/Value`.
**Trap**: it is easy to drop a nesting segment when building one of these prefixes by
hand (see the PublishBatch fix this pass) — always cross-check against
`serializers.go` in the vendored SDK (`go list -m -f '{{.Dir}}' .../service/sns` to find
the extracted module, or `go get` into a scratch module) rather than trusting a
hand-derived guess.

### FilterPolicy matching semantics
- Default `FilterPolicyScope` is `MessageAttributes`; `MessageBody` requires the
  published message to parse as a JSON object, otherwise nothing matches.
- A `String.Array` message attribute (JSON array in `StringValue`) expands to one
  candidate per element; the condition matches if ANY element matches (OR).
- `$or` is only treated as the OR operator when its array has ≥2 elements, every
  element is a JSON object, and no element uses a reserved operator keyword
  (`prefix`/`suffix`/`equals-ignore-case`/`anything-but`/`exists`/`numeric`/`wildcard`/
  `cidr`) as a top-level field — otherwise `$or` is an ordinary attribute name.
- `numeric` conditions are `[op, num, op, num, ...]` pairs, ANDed together.
- Non-existent attributes still get a *single* match attempt against `exists:false`
  and `anything-but` conditions — do not special-case "attribute missing" as "no match"
  without checking those two operators first.
- 150 total attribute conditions across a policy (including `$or` sub-policies), and a
  256 KiB serialized-policy size cap, both enforced at Subscribe/SetSubscriptionAttributes
  time (fail fast) not at match time.

### Per-protocol message envelope shapes
- **HTTP/HTTPS and Firehose** (when `RawMessageDelivery=false`, the AWS default): full
  `Notification` JSON envelope — `Type, MessageId, TopicArn, Subject?, Message, Timestamp,
  SignatureVersion, Signature, SigningCertURL, UnsubscribeURL`. `RawMessageDelivery=true`
  delivers the bare message body only.
- **Lambda**: ALWAYS the full envelope wrapped in `{"Records":[{"EventVersion":"1.0",
  "EventSource":"aws:sns","EventSubscriptionArn":...,"Sns":{...}}]}` — Lambda does NOT
  support `RawMessageDelivery` (unconditional envelope, unlike HTTP/SQS/Firehose).
- **SQS**: delivered via the `pkgs/events` emitter (`SetPublishEmitter`/`SubscribeToSNS`
  in cli.go, out of scope for this service) — SNS's job ends at emitting
  `events.SNSPublishedEvent` with the same Timestamp/Signature/SigningCertURL used
  everywhere else for this publish.
- **SMS/Application(mobile push)**: no real external sink in this mock; delivery is
  recorded (`SMSDelivery`/`ApplicationDelivery`) and drained via `DrainSMSDeliveries`/
  `DrainApplicationDeliveries` for test/dashboard observability. These buffers are now
  bounded (`maxRecordedDeliveries`) — do not remove the cap thinking it is dead code.
- **Trap**: as of this pass, every channel for a single `Publish` call shares ONE
  `Timestamp`/`Signature`/`SigningCertURL`, built once in `buildPublishedEvent` and
  threaded through to the SQS emitter, Lambda, and Firehose. Do not reintroduce a
  second, independent `time.Now()`/sign call per channel — that was the bug (Lambda
  used to fabricate `uuid.NewString()` as a fake "signature").

### Signature / MD5
- SNS signs the **Notification envelope** (RSA, canonical string of
  Message/MessageId/Subject?/Timestamp/TopicArn/Type sorted by field name,
  newline-separated) — this is NOT the same as SQS's `MD5OfMessageBody`. **SNS's
  `Publish`/`PublishBatch` responses do NOT include any MD5 field** — that's an
  SQS-only concept; don't add one here, it would be a fabricated field AWS never
  returns.
- This backend always signs with SHA-256 ("SignatureVersion 2") regardless of the
  topic's `SignatureVersion` attribute value. Real AWS defaults to SignatureVersion 1
  (SHA-1) and only uses SHA-256 when the topic attribute is explicitly "2". Left
  as a known gap (see `gaps:`) rather than guessed at, since nothing in this codebase
  verifies the signature and getting the SHA-1 codepath subtly wrong (e.g. real AWS's
  exact PKCS1v15 padding/hash combination) is worse than a consistently-SHA-256 mock.

### Subscription ARN format
- Confirmed subscription ARN: `arn:{partition}:sns:{region}:{account}:{topicName}:{uuid}`
  (built via `arn.Build("sns", region, accountID, topicName+":"+uuid.New().String())`).
- **Pending** (unconfirmed) HTTP/HTTPS/email/email-json subscriptions return the
  **literal string** `"pending confirmation"` (lowercase, with a space) as the
  `SubscriptionArn` in `Subscribe`'s response AND in `ListSubscriptions`/
  `ListSubscriptionsByTopic` — this is NOT a placeholder ARN, it is the exact string
  AWS returns. `Subscribe`'s `ReturnSubscriptionArn=true` parameter overrides this and
  always returns the real ARN even while pending.
- SQS, Lambda, Firehose, SMS, and Application subscriptions are auto-confirmed
  (`PendingConfirmation=false` immediately); only HTTP, HTTPS, email, and email-json
  require confirmation.

### FIFO topics
- `MessageGroupId` is required on every Publish/PublishBatch entry to a `.fifo` topic.
- `ContentBasedDeduplication=true` forbids an explicit `MessageDeduplicationId` (uses
  SHA-256 hex of the message body instead); `false` (default) requires one.
- Dedup window is 5 minutes, keyed by `topicArn + "/" + dedupID`; a duplicate within
  the window returns a **new** synthesized MessageId/SequenceNumber without
  re-publishing or re-delivering — this mirrors real AWS (dedup is silent, not an error).
- `SequenceNumber` is a 20-digit zero-padded, per-topic monotonic counter — not derived
  from message content or timestamp.

### Locking
`InMemoryBackend` uses one coarse `lockmetrics.RWMutex` per the pkgs-catalog rule.
`notificationSigner` additionally has its own small `sync.RWMutex` guarding just its
mutable `certURLValue` field (set once via `SetSigningCertBaseURL` when the mock
server's address becomes known, read on every signed delivery) — this is a
single-field auxiliary lock, not a second backend-resource lock, so it does not
violate the "one coarse lock per backend" rule.
