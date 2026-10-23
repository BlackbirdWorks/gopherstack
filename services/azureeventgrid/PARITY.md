---
service: azureeventgrid
sdk_module: azure-sdk-for-go/sdk/messaging/eventgrid/azeventgrid@v1.0.0   # go.mod dependency (test-only): drives the custom-topic publish endpoint in sdk_test.go; aznamespaces@v1.0.0 drives namespace topics
last_audit_commit: be2302298
last_audit_date: 2026-10-21
overall: B
ops:
  PublishEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /api/events on the HTTPS port with EventGridSchema events (array or single object); id, subject, eventType, eventTime and dataVersion are required (400 BadRequest otherwise); CloudEvents on an EventGridSchema topic are rejected. Events are stamped with topic and metadataVersion, then fanned out to matching event subscriptions."}
  PublishCloudEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "Same endpoint for topics created with inputSchema CloudEventSchemaV1_0: structured application/cloudevents+json and batched application/cloudevents-batch+json (specversion 1.0, id, source, type required). Binary content mode (ce-* headers) is not implemented."}
  SendEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /topics/{topic}:publish for namespace topics, single or batched CloudEvents, fanned out to every pull subscription whose filter matches."}
  ReceiveEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: ":receive with maxEvents (1-100) and maxWaitTime long poll; returns value[].brokerProperties {lockToken, deliveryCount} and the CloudEvent. Locked events reappear when the lock lapses (injected clock in tests)."}
  AcknowledgeEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: ":acknowledge returns succeededLockTokens and failedLockTokens (BadRequest for unknown or expired tokens)."}
  ReleaseEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: ":release with releaseDelayInSeconds makes the event available again after the delay."}
  RejectEvents: {wire: partial, errors: ok, state: partial, persist: ok, note: ":reject removes the event; namespace dead-letter destinations are not implemented, so rejected and over-delivered events are dropped."}
  RenewEventLocks: {wire: ok, errors: ok, state: ok, persist: ok, note: ":renewLock extends the lock by the subscription's receiveLockDurationInSeconds."}
  ValidateSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /validate?id=<code> completes manual webhook validation (the validationUrl handed to a webhook that did not echo the validationCode), moving the subscription from AwaitingManualAction to Succeeded."}
families:
  arm_integration: {status: ok, note: "services/azurearm Microsoft.EventGrid: topics (inputSchema, endpoint) with listKeys and regenerateKey, topic-scoped eventSubscriptions (getFullUrl), namespaces with listKeys/regenerateKey, namespaces/topics and namespaces/topics/eventSubscriptions. Topics, keys, subscriptions and queued events live in this service; ARM keeps location, tags and unmodelled properties. Applied, re-planned with no drift and destroyed by test/terraform/azure/eventgrid_test.go."}
  sdk_compat: {status: ok, note: "azeventgrid v1: PublishEvents, PublishCustomEventEvents, PublishCloudEvents with aeg-sas-key and aeg-sas-token credentials; aznamespaces v1: SendEvent(s), ReceiveEvents, Acknowledge/Release/Reject/RenewEventLocks with the SharedAccessKey credential. The HTTPS listener uses a self-signed or configured certificate."}
  filters: {status: ok, note: "includedEventTypes, subjectBeginsWith/EndsWith (isSubjectCaseSensitive), and all advanced operators: NumberIn/NotIn/LessThan/LessThanOrEquals/GreaterThan/GreaterThanOrEquals/InRange/NotInRange, BoolEquals, StringIn/NotIn/BeginsWith/NotBeginsWith/EndsWith/NotEndsWith/Contains/NotContains, IsNullOrUndefined, IsNotNull; dotted or slashed key paths; enableAdvancedFilteringOnArrays."}
  webhook_delivery: {status: ok, note: "Validation handshake on create or endpoint change: SubscriptionValidationEvent POST (validationResponse echo, else manual validationUrl) and the CloudEvents OPTIONS abuse-protection handshake. Notifications carry aeg-event-type, aeg-subscription-name and aeg-delivery-count; 2xx succeeds, 400 and 413 dead-letter at once, everything else retries."}
  sink_delivery: {status: ok, note: "ServiceBusQueue, ServiceBusTopic, EventHub and StorageQueue destinations deliver into the emulator's own azureservicebus, azureeventhubs and azurequeue backends through adapters in internal/app. Service Bus and queue messages carry the event object, Event Hubs the event in a JSON array."}
  retry_dead_letter: {status: ok, note: "Retries follow Event Grid's schedule (10s, 30s, 1m, 5m, 10m, 30m, 1h, 3h, 6h, 12h repeating) on an injected clock, bounded by retryPolicy maxDeliveryAttempts (1-30) and eventTimeToLiveInMinutes (1-1440). Exhausted events are written to the StorageBlob dead-letter container as topic/subscription/yyyy/MM/dd/HH/id.json with deadLetterReason, deliveryAttempts, lastDeliveryOutcome, publishTime and lastDeliveryAttemptTime."}
  auth: {status: ok, note: "Off by default (any credential is accepted and the topic is picked by host label, key or the single topic). With --azure-eventgrid-validate-sas the aeg-sas-key must equal a topic key and SAS tokens (r, e, s; HMAC-SHA256 over the base64-decoded key) must verify and be unexpired; namespace requests use SharedAccessKey or SAS against the namespace keys."}
gaps:
  - "Domains and domain topics are not implemented."
  - "System topics, partner topics and partner namespaces are not implemented."
  - "Custom input schemas and input schema mappings are not implemented."
  - "Azure Function, Hybrid Connection and Partner destinations are not implemented."
  - "Batched webhook delivery (maxEventsPerBatch) and delivery attributes are not implemented; each event is one request."
  - "Delivery with managed identity and AAD bearer validation are not implemented."
  - "Namespace topic subscriptions do not support dead-letter destinations or webhook push delivery."
  - "Event subscriptions scoped to a resource group, subscription or other resource types are not implemented; only custom topics are scopes."
  - "Dead-letter blob write failures are dropped."
deferred: []
items_still_open:
  - "Domains and domain topics (Microsoft.EventGrid/domains) are not implemented."
  - "System topics, partner topics and partner configurations are not implemented."
  - "Custom input schemas and input schema mappings are not implemented."
  - "Azure Function, Hybrid Connection and Partner destinations are not implemented."
  - "Batched webhook delivery (maxEventsPerBatch, preferredBatchSizeInKilobytes) and delivery attribute mappings are not implemented."
  - "Managed-identity delivery and AAD bearer validation are not implemented."
  - "Namespace topic subscriptions have no dead-letter destination, push (webhook) delivery mode or ARM-level topic spaces."
  - "Event subscriptions at resource group, subscription or non-topic resource scope are not implemented."
  - "Dead-letter blob write failures are not retried."
  - "No dashboard page."
leaks: {status: clean, note: "Shutdown cancels the delivery worker and waits for it, then closes the HTTPS server; the outbound HTTP client owns its transport (pkgs/httpclient)."}
---

## Notes

Subscription properties cross the ARM seam as ARM property maps, so the service keeps the exact wire shape (destination, filter, retryPolicy, deadLetterDestination, eventDeliverySchema, labels, expirationTimeUtc) and renders it back with defaults and provisioningState. WebHook endpoint query strings are withheld from reads and returned by `getFullUrl`.

A delivery's delivery schema always equals the topic's input schema (Event Grid does not convert between them).
