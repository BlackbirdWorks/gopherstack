---
service: azureeventgrid
sdk_module: azure-sdk-for-go/sdk/messaging/eventgrid/azeventgrid@v1.0.0   # go.mod dependency (test-only): drives the custom-topic publish endpoint in sdk_test.go; aznamespaces@v1.0.0 drives namespace topics; armeventgrid/v2@v2.3.0 drives domains in internal/app
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
  domains: {status: ok, note: "POST /api/events with a domain key (or the domain's host label): EventGridSchema events need a topic property naming the domain topic, CloudEvents use source. Events fan out to the domain's own subscriptions and to those of the named domain topic; delivered events carry topic = the domain topic's resource ID. Publishing to a domain topic that does not exist still delivers to domain-scope subscriptions. The first subscription of a missing domain topic creates it when autoCreateTopicWithFirstSubscription is true (default); the last subscription's deletion removes it when autoDeleteTopicWithLastSubscription is true (default); a self-managed domain answers 404 until the topic is created."}
  arm_integration: {status: ok, note: "services/azurearm Microsoft.EventGrid: topics (inputSchema, endpoint) with listKeys and regenerateKey, topic-scoped eventSubscriptions (getFullUrl, getDeliveryAttributes), domains (inputSchema, autoCreateTopicWithFirstSubscription, autoDeleteTopicWithLastSubscription) with listKeys/regenerateKey, domains/topics (list, get, put, delete), domain and domain-topic event subscriptions in both the armeventgrid path form (domains/{d}/[topics/{t}/]eventSubscriptions/{s}) and the generic scope form (.../providers/Microsoft.EventGrid/eventSubscriptions/{s}), namespaces with listKeys/regenerateKey, namespaces/topics and namespaces/topics/eventSubscriptions. Topics, keys, subscriptions and queued events live in this service; ARM keeps location, tags and unmodelled properties. Applied, re-planned with no drift and destroyed by test/terraform/azure/eventgrid_test.go."}
  sdk_compat: {status: ok, note: "azeventgrid v1: PublishEvents, PublishCustomEventEvents, PublishCloudEvents with aeg-sas-key and aeg-sas-token credentials; aznamespaces v1: SendEvent(s), ReceiveEvents, Acknowledge/Release/Reject/RenewEventLocks with the SharedAccessKey credential. The HTTPS listener uses a self-signed or configured certificate."}
  filters: {status: ok, note: "includedEventTypes, subjectBeginsWith/EndsWith (isSubjectCaseSensitive), and all advanced operators: NumberIn/NotIn/LessThan/LessThanOrEquals/GreaterThan/GreaterThanOrEquals/InRange/NotInRange, BoolEquals, StringIn/NotIn/BeginsWith/NotBeginsWith/EndsWith/NotEndsWith/Contains/NotContains, IsNullOrUndefined, IsNotNull; dotted or slashed key paths; enableAdvancedFilteringOnArrays."}
  webhook_delivery: {status: ok, note: "Validation handshake on create or endpoint change: SubscriptionValidationEvent POST (validationResponse echo, else manual validationUrl) and the CloudEvents OPTIONS abuse-protection handshake. Notifications carry aeg-event-type, aeg-subscription-name and aeg-delivery-count; 2xx succeeds, 400 and 413 dead-letter at once, everything else retries. maxEventsPerBatch (1-5000, default 1) and preferredBatchSizeInKilobytes (1-1024, default 64) group due events of one subscription into one JSON array request (CloudEvents: application/cloudevents-batch+json when batching is configured); a failed batch retries as a whole. deliveryAttributeMappings (Static, Dynamic from the first event of the request) become request headers; secret static values are withheld from reads and returned by DeliveryAttributes."}
  sink_delivery: {status: ok, note: "ServiceBusQueue, ServiceBusTopic, EventHub and StorageQueue destinations deliver into the emulator's own azureservicebus, azureeventhubs and azurequeue backends through adapters in internal/app. Service Bus and queue messages carry the event object, Event Hubs the event in a JSON array."}
  retry_dead_letter: {status: ok, note: "Retries follow Event Grid's schedule (10s, 30s, 1m, 5m, 10m, 30m, 1h, 3h, 6h, 12h repeating) on an injected clock, bounded by retryPolicy maxDeliveryAttempts (1-30) and eventTimeToLiveInMinutes (1-1440). Exhausted events are written to the StorageBlob dead-letter container as topic/subscription/yyyy/MM/dd/HH/id.json with deadLetterReason, deliveryAttempts, lastDeliveryOutcome, publishTime and lastDeliveryAttemptTime."}
  arm_scopes: {status: ok, note: "Subscription, resource group and non-Event-Grid resource scope event subscriptions (PUT, GET, DELETE, getFullUrl, getDeliveryAttributes; the scope must exist). A successful ARM PUT, DELETE or POST action in services/azurearm (event subscriptions excluded, so a subscription never sees its own creation) is reported through azurearm.ResourceObserver and delivered as Microsoft.Resources.ResourceWriteSuccess, ResourceDeleteSuccess or ResourceActionSuccess (dataVersion 2) with subject = resource ID and data authorization, claims, correlationId, httpRequest, resourceProvider, resourceUri, operationName, status, subscriptionId, tenantId, to subscriptions on /subscriptions/{id} (topic = that ID) and on the resource's resource group (topic = the group ID); a resource group's own write/delete is subscription level. All armeventgrid list operations (global and regional, by subscription or resource group, ForTopicType, ByResource, ByDomainTopic) with $filter on name (contains, eq, ne, not, and, or) and $top (1-100, default 20) paging via nextLink. Global lists hold subscription, resource group and other resource scopes; regional lists hold custom topic and domain subscriptions matched on the topic's location. Delivery schema is EventGridSchema."}
  auth: {status: ok, note: "Off by default (any credential is accepted and the topic is picked by host label, key or the single topic). With --azure-eventgrid-validate-sas the aeg-sas-key must equal a topic key and SAS tokens (r, e, s; HMAC-SHA256 over the base64-decoded key) must verify and be unexpired; namespace requests use SharedAccessKey or SAS against the namespace keys."}
gaps:
  - "System topics, partner topics and partner namespaces are not implemented."
  - "Custom input schemas and input schema mappings are not implemented."
  - "Azure Function, Hybrid Connection and Partner destinations are not implemented."
  - "Delivery with managed identity and AAD bearer validation are not implemented."
  - "Namespace topic subscriptions do not support dead-letter destinations or webhook push delivery."
  - "Resource-scope event subscriptions receive no events; event subscription PATCH is not implemented."
  - "Dead-letter blob write failures are dropped."
deferred: []
items_still_open:
  - "System topics, partner topics and partner configurations are not implemented."
  - "Custom input schemas and input schema mappings are not implemented."
  - "Azure Function, Hybrid Connection and Partner destinations are not implemented."
  - "Managed-identity delivery and AAD bearer validation are not implemented."
  - "Namespace topic subscriptions have no dead-letter destination, push (webhook) delivery mode or ARM-level topic spaces."
  - "Resource-scope event subscriptions (e.g. a storage account) are stored and listed but receive no events; ARM lifecycle events at subscription and resource group scope are the only system event source, and only the Succeeded outcomes (write, delete, action) are emitted."
  - "Event subscription update (PATCH) is not implemented."
  - "Dead-letter blob write failures are not retried."
  - "No dashboard page."
leaks: {status: clean, note: "Shutdown cancels the delivery worker and waits for it, then closes the HTTPS server; the outbound HTTP client owns its transport (pkgs/httpclient)."}
---

## Notes

Subscription properties cross the ARM seam as ARM property maps, so the service keeps the exact wire shape (destination, filter, retryPolicy, deadLetterDestination, eventDeliverySchema, labels, expirationTimeUtc) and renders it back with defaults and provisioningState. WebHook endpoint query strings are withheld from reads and returned by `getFullUrl`.

A delivery's delivery schema always equals the topic's input schema (Event Grid does not convert between them).
