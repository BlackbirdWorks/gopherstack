---
service: ocistreaming
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every operation in the service tests
last_audit_commit: 2d5590dc6
last_audit_date: 2026-10-25
overall: B
ops:
  CreateStreamPool: {wire: ok, errors: partial, state: ok, persist: ok, note: "CREATING then ACTIVE after the transition window on the injectable clock. kafkaSettings defaults (autoCreateTopicsEnable false, logRetentionHours 24, numPartitions 1) are validated (24 to 168, 1 to 1000). customEncryptionKeyDetails.kmsKeyId must be an OCID of an ENABLED OCI Vault key when the Vault backend is wired (keyState ACTIVE, NONE without a key). privateEndpointDetails are stored and echoed as privateEndpointSettings. Returns opc-work-request-id."}
  GetStreamPool: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; DELETED pools stay readable."}
  ListStreamPools: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; id, name, lifecycleState filters; sortBy NAME or TIMECREATED; paged with opc-next-page."}
  UpdateStreamPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "name, kafkaSettings (merged), customEncryptionKeyDetails (re-validated), tags; If-Match gives 412 NoEtagMatch. Applies immediately, no UPDATING state."}
  DeleteStreamPool: {wire: ok, errors: partial, state: ok, persist: ok, note: "202 with opc-work-request-id; DELETING then DELETED. A pool that still holds non-deleted streams answers 409 IncorrectState."}
  ChangeStreamPoolCompartment: {wire: ok, errors: partial, state: ok, persist: ok, note: "Moves the pool record only; its streams keep their compartment."}
  CreateStream: {wire: ok, errors: partial, state: ok, persist: ok, note: "Needs streamPoolId or compartmentId (a per-compartment DefaultPool is created on first use). partitions 1 to 1000, retentionInHours 24 to 168 (default 24), names unique per pool among live streams (409 Conflict). messagesEndpoint is the base URL the client addressed. CREATING until the transition window passes; messages are rejected with 409 until ACTIVE."}
  GetStream: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; DELETED streams stay readable but hold no messages."}
  ListStreams: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId, streamPoolId or id required; name and lifecycleState filters; sortBy NAME or TIMECREATED; paged."}
  UpdateStream: {wire: ok, errors: partial, state: ok, persist: ok, note: "streamPoolId (move, name collision checked) and tags; partitions and retention are immutable as in the SDK."}
  DeleteStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "202 with opc-work-request-id; DELETING then DELETED, at which point messages and consumer groups are dropped."}
  ChangeStreamCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Sets the stream compartment."}
  CreateConnectHarness: {wire: ok, errors: partial, state: ok, persist: ok, note: "Name and compartment; lifecycle follows the same injectable-clock transitions. Kafka Connect itself is not emulated."}
  GetConnectHarness: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListConnectHarnesses: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; id, name, lifecycleState filters; paged."}
  UpdateConnectHarness: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags only, as in the SDK."}
  DeleteConnectHarness: {wire: ok, errors: ok, state: ok, persist: ok, note: "202 with work request; DELETING then DELETED."}
  ChangeConnectHarnessCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  GetWorkRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "IN_PROGRESS until the resource transition time, then SUCCEEDED; resources carry the entity and action."}
  ListWorkRequests: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId or workRequestId, resourceId filter; the items envelope the SDK collection expects."}
  ListWorkRequestErrors: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Always empty: no emulated operation fails asynchronously."}
  ListWorkRequestLogs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Accepted and succeeded entries."}
  PutMessages: {wire: ok, errors: partial, state: ok, persist: ok, note: "Key and value are base64 byte arrays. The partition is murmur2(key) mod partitions (Kafka default partitioner); keyless messages go round-robin. Per-entry results carry partition, offset and timestamp; a key over 256 bytes fails that entry. A request over 1 MiB is 413, more than 1 MiB per partition within one clock second is 429 TooManyRequests (checked before anything is appended)."}
  GetMessages: {wire: ok, errors: partial, state: ok, persist: n/a, note: "Plain cursors read one partition from the cursor offset and return opc-next-cursor; limit 1 to 10000 (default 10000). Group cursors read from a reserved partition (see CreateGroupCursor). Reads below the retained base skip forward. The 2 MiB/s consumer and per-group request rate limits are not enforced."}
  CreateCursor: {wire: ok, errors: partial, state: ok, persist: n/a, note: "TRIM_HORIZON, LATEST, AT_OFFSET, AFTER_OFFSET (offset must lie in the retained range, else 400) and AT_TIME. Cursors are opaque base64 JSON bound to the stream and never expire."}
  CreateGroupCursor: {wire: ok, errors: partial, state: ok, persist: ok, note: "Creates the group on first use (type TRIM_HORIZON, LATEST or AT_TIME fixes the starting offsets; later calls ignore it), joins the instance (instanceName defaults to a UUID), timeoutInMs 1 to 300000 (default 60000), commitOnGet default true. Partitions are balanced to ceil(partitions / live instances); an instance gives up its excess at its next call."}
  GetGroup: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Per partition: committedOffset, reservedInstance and timeReservedUntil for live reservations."}
  UpdateGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "Resets every partition's committed offset to TRIM_HORIZON, LATEST or AT_TIME."}
  ConsumerCommit: {wire: ok, errors: partial, state: ok, persist: ok, note: "Applies the offsets a commitOnGet=false cursor accumulated, only for partitions the instance still reserves (409 IncorrectState otherwise); returns a fresh cursor."}
  ConsumerHeartbeat: {wire: ok, errors: ok, state: ok, persist: ok, note: "Extends the instance's reservations and re-balances; returns the cursor."}
families:
  auth: {status: ok, note: "pkgs/ociauth verifies draft-cavage signatures for API keys registered through POST /_gopherstack/oci/keys; accept-any otherwise, --oci-streaming-enforce-auth rejects unsigned and unknown keys."}
  error_envelope: {status: partial, note: "Errors are {code,message} with opc-request-id. InvalidParameter, MissingParameter, NotAuthenticated, NotAuthorizedOrNotFound, NoEtagMatch and IncorrectState follow the OCI reference; Conflict for duplicate stream names, RequestEntityTooLarge, TooManyRequests and CannotParseRequest are unverified."}
  messages_endpoint: {status: ok, note: "Admin and messages APIs share the one listener and the /20180418 base path, so a stream's messagesEndpoint is the URL the client addressed and works as the StreamClient host."}
  lifecycle: {status: ok, note: "CREATING and DELETING last 2 seconds on the injectable backend clock; transitions, retention and consumer reservations are applied on every request and by a one-minute sweeper."}
  retention: {status: ok, note: "Messages older than retentionInHours are dropped from the front of each partition; offsets keep counting and TRIM_HORIZON moves to the oldest retained offset."}
  consumer_groups: {status: ok, note: "Group offsets, reservations, balancing, heartbeats and timeouts persist with the snapshot and follow the injectable clock."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-streaming-port / OCI_STREAMING_PORT (default 8152); never on the shared AWS router; cli.go reserves the port in the shared pool."}
  vault_integration: {status: ok, note: "When OCI Vault runs in the same process, customEncryptionKey references are checked against its keys (must exist and be ENABLED); standalone, any ocid1.key. OCID is accepted."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards all state."}
items_still_open:
  - "The Kafka compatibility API (bootstrap servers, SASL and the Kafka wire protocol) is not implemented; kafkaSettings are stored and returned and bootstrapServers is whatever the caller set."
  - "Create, delete and update responses report the transitional state but updates and compartment moves complete immediately; there is no UPDATING state and work requests only model CREATE, UPDATE and DELETE completion times."
  - "Cursors never expire, and the 2 MiB/s consumer throughput and per-group request-rate limits are not enforced; only the 1 MiB/s per-partition ingest, 1 MiB request and 256 byte key limits are."
  - "Partition and stream counts per tenancy, partition limits per stream (1 to 1000 here) and the exact error code strings for failed PutMessages entries are unverified against the live service."
  - "Error codes and statuses for a duplicate stream name (409 Conflict), deleting a non-empty pool (409 IncorrectState), a lost group reservation on commit (409 IncorrectState) and an unusable customEncryptionKey (400 InvalidParameter) are unverified."
  - "Private stream pools only store their subnet and NSG settings; endpoints are not network-isolated and endpointFqdn is the addressed host."
  - "Messages and consumer group state are held in memory and in snapshots, so large retained volumes grow the snapshot."
  - "opc-retry-token idempotency, IAM policy checks, resource principals and instance principals are not applied; only API-key signatures are verified."
  - "Tag values and defined-tag namespaces are stored verbatim, not validated against a tag namespace."
---
