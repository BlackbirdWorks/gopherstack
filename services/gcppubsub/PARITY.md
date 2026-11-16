---
service: gcppubsub
sdk_module: cloud.google.com/go/pubsub/v2@v2.7.0
last_audit_commit: f5ea09414
last_audit_date: 2026-10-18
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateTopic: {wire: ok, errors: ok, state: ok, persist: ok, note: "Validates the resource name (3-255 chars, letter first, no goog prefix), labels (max 64), message_retention_duration (10m-31d) and schema_settings (schema must exist, encoding required). ingestion_data_source_settings and message_transforms are rejected UNIMPLEMENTED."}
  GetTopic: {wire: ok, errors: ok, state: ok, persist: n/a, note: "NOT_FOUND for unknown topics."}
  UpdateTopic: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask is required; labels, message_storage_policy, kms_key_name, schema_settings, message_retention_duration, satisfies_pzs and tags are mutable, name and unknown paths are INVALID_ARGUMENT. Unmasked fields are preserved."}
  ListTopics: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name, page_size/page_token via pkgs/page."}
  DeleteTopic: {wire: ok, errors: ok, state: ok, persist: ok, note: "Subscriptions survive and report topic _deleted-topic_; snapshots of the topic are dropped."}
  Publish: {wire: ok, errors: ok, state: ok, persist: ok, note: "1-1000 messages, 10 MB cap, each needs data or attributes. Numeric microsecond message ids, publish_time, ordering_key. Fans out to every attached, non-detached subscription whose filter matches. Topics with schema_settings validate every payload."}
  ListTopicSubscriptions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Detached subscriptions are not listed."}
  ListTopicSnapshots: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Unexpired snapshots of the topic."}
  DetachSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "Marks the subscription detached: it stops receiving new messages and can drain its backlog."}
  CreateSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "Defaults ack_deadline 10s (valid 10-600), retention 7d (10m-31d), expiration ttl 31 days. Validates filter, dead_letter_policy (topic must exist, 5-100 attempts), retry_policy, push_config endpoint, labels. BigQuery, Cloud Storage and Bigtable delivery and message_transforms are rejected UNIMPLEMENTED."}
  GetSubscription: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Returns topic_message_retention_duration from the topic and detached/state."}
  UpdateSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask required; push_config, ack_deadline_seconds, retain_acked_messages, message_retention_duration, labels, expiration_policy, dead_letter_policy, retry_policy, enable_exactly_once_delivery and tags are mutable. topic, filter and enable_message_ordering are immutable (INVALID_ARGUMENT)."}
  ListSubscriptions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Per project, sorted, paged."}
  DeleteSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "Wakes blocked pulls, which then fail NOT_FOUND."}
  ModifyPushConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "An empty push_config switches the subscription back to pull."}
  Pull: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Leases up to max_messages (cap 1000) for the ack deadline. Blocks up to 10s for messages unless return_immediately. Push subscriptions are FAILED_PRECONDITION."}
  StreamingPull: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Bidirectional stream used by the official client's Receive. First response carries SubscriptionProperties; acks, modacks, stream_ack_deadline and MaxOutstandingMessages/Bytes flow control are honoured per stream; exactly-once subscriptions get Acknowledge/ModifyAckDeadline confirmations with invalid ids. Leases survive a disconnect until their deadline."}
  Acknowledge: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Unknown or expired ids are ignored, except on exactly-once subscriptions where they are INVALID_ARGUMENT."}
  ModifyAckDeadline: {wire: ok, errors: ok, state: ok, persist: n/a, note: "0 releases the lease immediately (nack), honouring retry_policy backoff; 1-600 extends."}
  Seek: {wire: ok, errors: ok, state: ok, persist: ok, note: "To a time or an unexpired snapshot of the same topic. Time seeks also pull in topic-retained messages the subscription lacks. Acked messages are only available to replay while retain_acked_messages, topic retention or a snapshot on the topic keeps them."}
  CreateSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "Captures the unacknowledged message ids; expires 7 days minus the age of the oldest unacknowledged message, and is refused (FAILED_PRECONDITION) when that leaves under an hour."}
  GetSnapshot: {wire: ok, errors: ok, state: ok, persist: n/a, note: "NOT_FOUND once expired."}
  ListSnapshots: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Per project."}
  UpdateSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "Mutable labels and expire_time (future, within 7 days of creation)."}
  DeleteSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "NOT_FOUND for unknown snapshots."}
  CreateSchema: {wire: ok, errors: ok, state: ok, persist: ok, note: "AVRO definitions are parsed (records, enums, arrays, maps, unions, fixed, named refs); PROTOCOL_BUFFER definitions only get a lexical check (syntax, a message, balanced braces)."}
  GetSchema: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Name may carry @revision; view BASIC omits the definition."}
  ListSchemas: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Latest revision per schema, BASIC view by default."}
  ListSchemaRevisions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Newest first."}
  CommitSchema: {wire: ok, errors: ok, state: ok, persist: ok, note: "Adds an 8-char revision; the type cannot change."}
  RollbackSchema: {wire: ok, errors: ok, state: ok, persist: ok, note: "Creates a new revision copying the named one."}
  DeleteSchemaRevision: {wire: ok, errors: ok, state: ok, persist: ok, note: "The last remaining revision cannot be deleted."}
  DeleteSchema: {wire: ok, errors: ok, state: ok, persist: ok, note: "Topics that referenced the schema point at _deleted-schema_ and stop validating."}
  ValidateSchema: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Same definition checks as CreateSchema."}
  ValidateMessage: {wire: ok, errors: ok, state: partial, persist: n/a, note: "AVRO is fully validated in JSON (union-wrapped Avro JSON) and binary encodings. PROTOCOL_BUFFER binary is checked for well-formed wire format and JSON for being an object, not against the message fields."}
  SetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "google.iam.v1.IAMPolicy on topics, subscriptions, snapshots and schemas. A non-empty etag must match (ABORTED otherwise); empty etag overwrites; bindings need role and members; conditional bindings need version 3 (INVALID_ARGUMENT otherwise); policies drop with their resource."}
  GetIamPolicy: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Empty policy with etag 0x002001 when none is set. Policies without conditions are version 1; conditional ones are version 3 only when requested_policy_version is 3, else version 1 with conditions dropped and _withcond_<hash> role suffixes (docs.cloud.google.com/iam/docs/policies)."}
  TestIamPermissions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Empty permission list is INVALID_ARGUMENT; NOT_FOUND for missing resources."}
families:
  delivery: {status: ok, note: "Per-subscription message state with ack ids per delivery, lease expiry redelivery, retry_policy exponential backoff, ordering keys (one outstanding message per key, in publish order), dead-letter forwarding after max_delivery_attempts (with CloudPubSubDeadLetterSource* attributes and delivery_attempt on received messages) and expiration of idle subscriptions."}
  filtering: {status: ok, note: "attributes.k = / != \"v\", attributes:k, hasPrefix(attributes.k, \"p\"), NOT, AND, OR and parentheses; invalid filters are INVALID_ARGUMENT. Filtered-out messages are never stored for the subscription."}
  push: {status: partial, note: "HTTP POST of the JSON envelope (message.data base64, attributes, messageId, publishTime, subscription, deliveryAttempt) or the raw payload with X-Goog-Pubsub-* headers for no_wrapper; 102/200/201/202/204 ack, anything else retries after at least 1s or the retry_policy backoff. OIDC tokens are minted per delivery and sent as a Bearer header (see the body)."}
  routing_isolation: {status: ok, note: "Own gRPC server bound in StartWorker to --gcp-pubsub-port / GCP_PUBSUB_PORT (default 8085, fails fast if taken); never registered on the shared AWS router. cli.go reserves the port in the shared PortAlloc pool. Point clients at it with PUBSUB_EMULATOR_HOST=localhost:8085."}
  auth: {status: partial, note: "No request is authenticated or authorised; IAM policies are stored but not enforced."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; RPC failures are logged at debug. gRPC calls are not fed to the shared telemetry operation metrics."}
items_still_open:
  - "TestIamPermissions grants every requested pubsub.<collection>.* permission; policies are stored, not evaluated, and role names are not checked against the gcpiam catalog."
  - "BigQuery, Cloud Storage and Bigtable subscriptions, topic ingestion sources and message transforms are rejected UNIMPLEMENTED."
  - "PROTOCOL_BUFFER schemas are not compiled: definitions get a lexical check and messages a wire-format or JSON-object check only."
leaks: {status: clean, note: "Background resources are the grpc.Server and the push dispatcher goroutine (plus its in-flight requests), all stopped and joined by Shutdown. StreamingPull reader goroutines exit when the stream context ends."}
---

Initial implementation (2026-10-18): Publisher, Subscriber and SchemaService gRPC verified against cloud.google.com/go/pubsub/v2 v2.7.0 through the official client in sdk_test.go.

Push OIDC: each delivery to a subscription with oidc_token carries Authorization: Bearer <JWT>, RS256 over claims iss https://accounts.google.com, aud (the audience, else the push endpoint URL per the PushConfig.OidcToken field comment), sub and azp (the service account unique id from gcpiam when wired, else a stable 21-digit id derived from the email), email, email_verified, iat and exp one hour later (docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions). The signing key is generated per process and exported as a JWKS by InMemoryBackend.PushTokenKeySet; it is not persisted and not served over HTTP.

Push delivery: a redirect answer is a negative acknowledgment and is not followed, the client ignores proxy settings, and response bodies are read up to 64 KiB. With no_wrapper write_metadata the x-goog-pubsub-subscription-name, -message-id, -publish-time and -ordering-key headers are written and message attributes become plain headers, with no default Content-Type (docs.cloud.google.com/pubsub/docs/payload-unwrapping); attributes that are not valid header fields, that start with x-goog-pubsub-, or that name Authorization, Host, Content-Length, Transfer-Encoding, Connection, Upgrade, TE or Trailer are not written.

Fixed limits enforced (docs.cloud.google.com/pubsub/quotas): 10000 topics, 10000 subscriptions, 5000 snapshots and 10000 schemas per project, 20 revisions and 300 KB per schema, 100 attributes per message with 256-byte keys and 1024-byte values, 512 KB of ack ids per Acknowledge or ModifyAckDeadline, 10 MB per unary Pull response.

## Emulator choices where the vendor is silent

- The default OIDC audience (the push endpoint) and the key set (never published; PushTokenKeySet stays internal) are not documented: authenticate-push-subscriptions says the audience is optional and that signing certificates are tightly controlled.
- Push attributes are written as raw headers per payload-unwrapping; the emulator additionally drops hop-by-hop, proxy, Host, Content-Length, Expect and x-goog-pubsub-* names and any value httpguts rejects. Push endpoints are not restricted to loopback.

- Push endpoints are not verified at subscription creation: neither docs.cloud.google.com/pubsub/docs/push nor authenticate-push-subscriptions describes an endpoint verification or domain ownership step.
- The pages name no per-subscription server-side outstanding-message limit ("the service may limit outstanding messages per connection", no number), so flow control stays per stream.
- RESOURCE_EXHAUSTED for the count limits, INVALID_ARGUMENT for the size limits and FAILED_PRECONDITION for a snapshot that would live under an hour are the emulator's choice: the quotas page and the Snapshot.expire_time comment name no status codes.
- The regional throughput quotas (publisher, pull, acknowledger, push, StreamingPull kB per minute and open StreamingPull connections) are not applied: they depend on the region size, and the emulator has no regions.
