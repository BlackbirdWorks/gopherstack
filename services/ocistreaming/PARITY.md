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
  UpdateStreamPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "name, kafkaSettings (merged), customEncryptionKeyDetails (re-validated), tags; If-Match gives 412 NoEtagMatch. UPDATING for the transition window, then ACTIVE; the work request finishes with it."}
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
  PutMessages: {wire: ok, errors: partial, state: ok, persist: ok, note: "Key and value are base64 byte arrays. The partition is murmur2(key) mod partitions (Kafka default partitioner); keyless messages go round-robin. Per-entry results carry partition, offset and timestamp; a key over 256 bytes fails that entry. A request over 1 MiB is 400 InvalidParameter "Request size is limited to 1 MiB.", more than 1 MiB per partition within one clock second is 429 TooManyRequests (checked before anything is appended)."}
  GetMessages: {wire: ok, errors: partial, state: ok, persist: n/a, note: "Plain cursors read one partition from the cursor offset and return opc-next-cursor; limit 1 to 10000 (default 10000). Group cursors read from a reserved partition (see CreateGroupCursor). An expired plain cursor is rejected 400. Reads below the retained base skip forward. More than 5 GetMessages requests per second per partition per consumer group (plain cursors count as one group) is 429 TooManyRequests, per docs.oracle.com Limits on Streaming Resources. A stream holds at most 50 consumer groups (400 InvalidParameter beyond, code unverified)."}
  CreateCursor: {wire: ok, errors: partial, state: ok, persist: n/a, note: "TRIM_HORIZON, LATEST, AT_OFFSET, AFTER_OFFSET (offset must lie in the retained range, else 400) and AT_TIME. Cursors are opaque base64 JSON bound to the stream; a plain cursor expires 5 minutes after it was issued (docs: the cursor returned by GetMessages \"expires in five minutes\"), each opc-next-cursor renews it, and an expired one is 400 InvalidParameter (code not documented by Oracle, unverified). Group cursors are governed by timeoutInMs instead."}
  CreateGroupCursor: {wire: ok, errors: partial, state: ok, persist: ok, note: "Creates the group on first use (type TRIM_HORIZON, LATEST or AT_TIME fixes the starting offsets; later calls ignore it), joins the instance (instanceName defaults to a UUID), timeoutInMs 1 to 300000 (default 60000), commitOnGet default true. Partitions are balanced to ceil(partitions / live instances); an instance gives up its excess at its next call."}
  GetGroup: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Per partition: committedOffset, reservedInstance and timeReservedUntil for live reservations."}
  UpdateGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "Resets every partition's committed offset to TRIM_HORIZON, LATEST or AT_TIME."}
  ConsumerCommit: {wire: ok, errors: partial, state: ok, persist: ok, note: "Applies the offsets a commitOnGet=false cursor accumulated, only for partitions the instance still reserves (400 InvalidParameter "Trying to commit unreserved partition" otherwise); returns a fresh cursor."}
  ConsumerHeartbeat: {wire: ok, errors: ok, state: ok, persist: ok, note: "Extends the instance's reservations and re-balances; returns the cursor."}
families:
  auth: {status: ok, note: "pkgs/ociauth verifies draft-cavage signatures for API keys registered through POST /_gopherstack/oci/keys; accept-any otherwise, --oci-streaming-enforce-auth rejects unsigned and unknown keys."}
  error_envelope: {status: partial, note: "Errors are {code,message} with opc-request-id. InvalidParameter, MissingParameter, NotAuthenticated, NotAuthorizedOrNotFound, NoEtagMatch and IncorrectState follow the OCI reference; Conflict for duplicate stream names and CannotParseRequest are unverified."}
  messages_endpoint: {status: ok, note: "Admin and messages APIs share the one listener and the /20180418 base path, so a stream's messagesEndpoint is the URL the client addressed and works as the StreamClient host."}
  lifecycle: {status: ok, note: "CREATING and DELETING last 2 seconds on the injectable backend clock; transitions, retention and consumer reservations are applied on every request and by a one-minute sweeper."}
  retention: {status: ok, note: "Messages older than retentionInHours are dropped from the front of each partition; offsets keep counting and TRIM_HORIZON moves to the oldest retained offset."}
  consumer_groups: {status: ok, note: "Group offsets, reservations, balancing, heartbeats and timeouts persist with the snapshot and follow the injectable clock."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-streaming-port / OCI_STREAMING_PORT (default 8152); never on the shared AWS router; cli.go reserves the port in the shared pool."}
  vault_integration: {status: ok, note: "When OCI Vault runs in the same process, customEncryptionKey references are checked against its keys (must exist and be ENABLED); standalone, any ocid1.key. OCID is accepted."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards all state."}
items_still_open:
  - "The Kafka compatibility API (bootstrap servers, SASL and the Kafka wire protocol) is not implemented; kafkaSettings are stored and returned and bootstrapServers is whatever the caller set."
  - "The documented tenancy partition allowance (default 200 on Monthly Universal Credits, 50 on Pay-as-You-Go or Promo) is not enforced and the status for exceeding it is undocumented; the 1 to 1000 partitions per stream range, stream counts per tenancy and the error code strings of failed PutMessages entries have no documentation."
  - "Per-operation error mapping for a duplicate stream name (409 Conflict), deleting a non-empty pool (409 IncorrectState) and an unusable customEncryptionKey (400 InvalidParameter) is undocumented; the common errors page lists the codes but not which operation returns them."
  - "Private stream pools only store their subnet and NSG settings; endpoints are not network-isolated and endpointFqdn is the addressed host."
  - "Messages and consumer group state are held in memory and in snapshots, so large retained volumes grow the snapshot."
  - "IAM policy checks, resource principals and instance principals are not applied; only API-key signatures are verified."
---

## 2026-11-13 pass

- Stream, pool and harness updates and compartment moves report UPDATING for the transition window; the SDK enums list UPDATING and its only work request types are CREATE, UPDATE and DELETE (streaming/operation_type.go).
- Reads are limited to 2 MiB/s per partition across consumers, answered 429: Kafka overview table "Write throughput per partition 1 MB per second and Read throughput per partition 2 MB per second" (https://docs.oracle.com/en-us/iaas/Content/kafka/overview.htm).
- PutMessages over 1 MiB is 400 InvalidParameter "Request size is limited to 1 MiB." and an uncommittable group cursor is 400 InvalidParameter "Trying to commit unreserved partition" (https://docs.oracle.com/en-us/iaas/Content/Streaming/Reference/troubleshootingstreaming.htm).
- CreateStreamPool and CreateConnectHarness replay on opc-retry-token for 24 hours (SDK request docs, streaming/create_stream_pool_request_response.go) and answer 409 InvalidatedRetryToken once the resource was deleted (https://docs.oracle.com/en-us/iaas/Content/API/References/apierrors.htm); tokens persist in the snapshot as retryTokens. CreateStream has no retry token in the SDK.
- Free-form and defined tags are checked by pkgs/ocitags (10 free-form and 64 defined tags, keys up to 100 and values up to 256 characters, 5 K total, retired tags rejected, unknown namespaces 404 NotAuthorizedOrNotFound "The following tag namespaces / keys are not authorized or not found", ENUM validators) against OCI Identity when it runs in the same process (https://docs.oracle.com/en-us/iaas/Content/Tagging/Concepts/taggingoverview.htm, troubleshooting page above).
