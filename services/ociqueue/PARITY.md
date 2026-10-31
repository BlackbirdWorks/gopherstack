---
service: ociqueue
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every operation in the service tests
last_audit_commit: 3a4fe760a
last_audit_date: 2026-10-27
overall: B
ops:
  CreateQueue: {wire: ok, errors: partial, state: ok, persist: ok, note: "202 with opc-work-request-id and no body, as the SDK response declares. CREATING then ACTIVE after the transition window on the injectable clock. retentionInSeconds 10 to 604800 (default 86400), visibilityInSeconds 0 to 43200 (default 30), timeoutInSeconds 0 to 30 (default 30), deadLetterQueueDeliveryCount 0 to 20 (default 5, 0 disables the DLQ), channelConsumptionLimit 1 to 100 (default 100). customEncryptionKeyId must be an OCID of an ENABLED OCI Vault key when the Vault backend is wired. A non-empty capabilities list is rejected 400 (see items_still_open)."}
  GetQueue: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; messagesEndpoint is the base URL the client addressed, so it works as the QueueClient host; DELETED queues stay readable."}
  ListQueues: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId or id required; displayName, lifecycleState filters; sortBy timeCreated or displayName; items envelope, paged with opc-next-page."}
  UpdateQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "202 with work request; displayName, visibility, timeout, DLQ count, channel limit, key and tags apply immediately and the queue reports UPDATING for the transition window while data plane calls keep working. If-Match gives 412 NoEtagMatch."}
  DeleteQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "202 with work request; DELETING then DELETED, at which point all messages are dropped. Parked long polls are released and answer 409 IncorrectState."}
  ChangeQueueCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Work request MOVE_QUEUE; sets the compartment immediately."}
  PurgeQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "purgeType NORMAL, DLQ or BOTH, optionally limited to channelIds; applied immediately, the PURGE_QUEUE work request completes after the transition window. consumerGroupId is rejected 400."}
  GetWorkRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "IN_PROGRESS until the resource transition time, then SUCCEEDED; resources carry the entity and action."}
  ListWorkRequests: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId or workRequestId, resourceId filter; items envelope."}
  ListWorkRequestErrors: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Always empty: no emulated operation fails asynchronously."}
  ListWorkRequestLogs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Accepted and succeeded entries."}
  PutMessages: {wire: ok, errors: partial, state: ok, persist: ok, note: "Up to 20 messages and 512 KiB per request, 128 KiB per message, channelId up to 64 characters (metadata requires channelId). Returns per message id and expireAfter (now + retention). Wakes parked long polls."}
  GetMessages: {wire: ok, errors: partial, state: ok, persist: ok, note: "limit 1 to 20 (default 1), visibilityInSeconds 0 to 43200 (default queue value; 0 peeks but still counts a delivery), timeoutInSeconds 0 to 30 (default queue value; 0 is a short poll). Long polling parks on a per-queue wake channel plus timers for the poll deadline and the next message becoming visible, so it never spins; it ends on a put, a visibility change, the deadline, client disconnect, queue deletion or listener shutdown. channelFilter matches a channel id exactly."}
  UpdateMessage: {wire: ok, errors: partial, state: ok, persist: ok, note: "Sets the visibility of the message that holds the receipt (0 to 43200 seconds from now); returns id and visibleAfter. Unknown or superseded receipt is 404."}
  UpdateMessages: {wire: ok, errors: partial, state: ok, persist: ok, note: "Up to 20 entries; each entry reports id and visibleAfter or errorCode 404 with errorMessage, with clientFailures counted."}
  DeleteMessage: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; unknown receipt is 404."}
  DeleteMessages: {wire: ok, errors: partial, state: ok, persist: ok, note: "Up to 20 entries; failed entries carry errorCode 404 and errorMessage, successful ones are empty objects."}
  GetStats: {wire: ok, errors: ok, state: ok, persist: n/a, note: "visibleMessages, inFlightMessages and sizeInBytes (content bytes) for the queue and for the DLQ, optionally for one channelId."}
  ListChannels: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Channels that currently hold messages; channelFilter is a substring match; sorted and paged with opc-next-page."}
families:
  auth: {status: ok, note: "pkgs/ociauth verifies draft-cavage signatures for API keys registered through POST /_gopherstack/oci/keys or OCI Identity; accept-any otherwise, --oci-queue-enforce-auth rejects unsigned and unknown keys."}
  error_envelope: {status: partial, note: "Errors are {code,message} with opc-request-id. InvalidParameter, MissingParameter, NotAuthenticated, NotAuthorizedOrNotFound, NoEtagMatch and IncorrectState follow the OCI reference; the status used for oversize messages (400 InvalidParameter) and for a stale receipt (404) is unverified."}
  messages_endpoint: {status: ok, note: "Admin and messages APIs share one listener and the /20210201 base path, so a queue's messagesEndpoint is the URL the client addressed."}
  lifecycle: {status: ok, note: "CREATING, UPDATING and DELETING last 2 seconds on the injectable backend clock; transitions, retention and dead lettering are applied on every request and by a one-minute sweeper."}
  visibility_and_receipts: {status: ok, note: "A delivery hides the message until now + visibility and issues a fresh receipt; the previous receipt stops working. Receipts stay valid after the visibility window lapses until the message is delivered again."}
  dead_letter: {status: ok, note: "A message that has been delivered deadLetterQueueDeliveryCount times and becomes visible again moves to the DLQ instead of being delivered once more; DLQ messages follow retention, show in GetStats and are cleared by PurgeQueue DLQ or BOTH."}
  long_polling: {status: ok, note: "Event driven; the poll timer is injectable (SetTimer) so tests fire it without sleeping."}
  retention: {status: ok, note: "Messages and DLQ entries past retentionInSeconds are dropped on the next request or sweep."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-queue-port / OCI_QUEUE_PORT (default 8156); never on the shared AWS router; cli.go reserves the port in the shared pool."}
  vault_integration: {status: ok, note: "When OCI Vault runs in the same process, customEncryptionKeyId is checked against its keys (must exist and be ENABLED); standalone any ocid1.key. OCID is accepted."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards all state."}
items_still_open:
  - "Consumer groups (CreateConsumerGroup, GetConsumerGroup, ListConsumerGroups, UpdateConsumerGroup, DeleteConsumerGroup) are not routed, the consumerGroupId query parameter is ignored by the data plane and the CONSUMER_GROUPS capability is rejected."
  - "The LARGE_MESSAGES capability is rejected; only the standard 128 KiB message limit exists."
  - "The DLQ cannot be consumed: the SDK has no API to read dead lettered messages, so they are only counted by GetStats and cleared by PurgeQueue."
  - "Limits (retention 10 to 604800 s, visibility 0 to 43200 s, poll timeout 0 to 30 s, DLQ count 0 to 20, batch of 20 messages, 128 KiB per message, 512 KiB per request, 64 character channel ids) and the channelConsumptionLimit semantics are recalled from the Oracle documentation and SDK comments, not verified against the live service."
  - "channelFilter is an exact match on GetMessages and a substring match on ListChannels; the live matching rules are unverified."
  - "Throughput limits and per-queue storage quotas (and therefore channelConsumptionLimit enforcement) are not applied."
  - "Messages are held in memory and in snapshots, so large backlogs grow the snapshot."
  - "opc-retry-token idempotency, IAM policy checks, resource principals and instance principals are not applied; only API-key signatures are verified."
  - "Tag values and defined-tag namespaces are stored verbatim, not validated against a tag namespace; systemTags are never set."
---
