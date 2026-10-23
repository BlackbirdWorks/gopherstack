---
service: azureeventhubs
sdk_module: azure-sdk-for-go/sdk/messaging/azeventhubs/v2@v2.0.2   # go.mod dependency (test-only): drives the AMQP 1.0 listener in sdk_test.go
last_audit_commit: e332a0eae
last_audit_date: 2026-10-20
overall: B
ops:
  CreateEventHub: {wire: partial, errors: ok, state: ok, persist: ok, note: "PUT /<hub> Atom entry (MessageRetentionInDays, PartitionCount); also reached through ARM Microsoft.EventHub eventhubs. Partition count may only grow; the legacy Atom surface is a small subset of the real one."}
  GetEventHub: {wire: partial, errors: ok, state: ok, persist: n/a, note: "GET /<hub> returns an EventHubDescription with PartitionIds and CreatedAt."}
  DeleteEventHub: {wire: ok, errors: ok, state: ok, persist: ok, note: "Also removes hub-scoped rules and disconnects receivers."}
  ListEventHubs: {wire: partial, errors: ok, state: ok, persist: n/a, note: "GET /$Resources/EventHubs across namespaces, no pagination."}
  CreateConsumerGroup: {wire: partial, errors: ok, state: ok, persist: ok, note: "PUT /<hub>/consumergroups/<name>; $Default exists on every hub."}
  GetConsumerGroup: {wire: partial, errors: ok, state: ok, persist: n/a, note: "GET /<hub>/consumergroups/<name>."}
  DeleteConsumerGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "$Default is protected."}
  ListConsumerGroups: {wire: partial, errors: ok, state: ok, persist: n/a, note: "GET /<hub>/consumergroups."}
  SendEvent: {wire: partial, errors: ok, state: ok, persist: ok, note: "POST /<hub>/messages, /<hub>/partitions/<n>/messages, /<hub>/publishers/<p>/messages; raw body or application/vnd.microsoft.servicebus.json batch; PartitionKey from BrokerProperties."}
  AMQPSend: {wire: ok, errors: ok, state: ok, persist: ok, note: "Sender links to <hub>, <hub>/Partitions/<n>, <hub>/Publishers/<p>; single messages and the 0x80013700 batch envelope; x-opt-partition-key hashed (FNV-1a, emulator-defined) to a partition, keyless events round-robin; a batch lands in one partition. Driven by the real azeventhubs producer."}
  AMQPReceive: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Receiver links on <hub>/ConsumerGroups/<cg>/Partitions/<n> with apache.org:selector-filter:string on x-opt-offset (incl. -1 and @latest), x-opt-sequence-number and x-opt-enqueued-time (ms), > and >=. Events carry x-opt-sequence-number, x-opt-offset, x-opt-enqueued-time and x-opt-partition-key, are sent pre-settled."}
  AMQPEpoch: {wire: ok, errors: ok, state: ok, persist: n/a, note: "com.microsoft:epoch receivers: a higher or equal epoch disconnects older receivers with amqp:link:stolen; a lower epoch (or a plain receiver while an epoch receiver exists) attaches and is detached with amqp:link:stolen. Up to 5 plain receivers per partition and consumer group (amqp:resource-limit-exceeded beyond)."}
  AMQPManagement: {wire: ok, errors: ok, state: ok, persist: n/a, note: "$management READ of com.microsoft:eventhub (partition_ids, created_at) and com.microsoft:partition (begin/last sequence, last offset, last time, is_partition_empty); status-code/status-description keys."}
  AMQPAuth: {wire: ok, errors: ok, state: ok, persist: n/a, note: "SASL ANONYMOUS/PLAIN and $cbs put-token. With --azure-eventhubs-validate-sas the SAS signature is verified against authorization-rule keys (skn), expiry is enforced, and rights (Listen/Send/Manage) gate receive, send and management. Off by default."}
  Retention: {wire: n/a, errors: n/a, state: ok, persist: ok, note: "Events older than the hub retention are trimmed lazily on every read/write and by a 30s sweeper, using an injected clock in tests. Capped at 100000 events per partition."}
families:
  arm_integration: {status: ok, note: "services/azurearm Microsoft.EventHub: namespaces, eventhubs (partitionCount, messageRetentionInDays, retentionDescription), consumergroups, namespace- and hub-level authorizationRules with listKeys, networkRuleSets/default. Hubs, groups, rules and keys live in this service; ARM keeps only ARM metadata. Applied, re-planned with no drift and destroyed by test/terraform/azure/eventhubs_test.go."}
  sdk_compat: {status: ok, note: "azeventhubs v2: producer batches (key, explicit partition), partition client from earliest/latest/offset/sequence/enqueued time, hub and partition properties, epoch receivers, SAS valid/invalid, restricted rights, TLS. Connect with UseDevelopmentEmulator=true to the plain port or with a TLSConfig to the TLS port."}
  ports: {status: ok, note: "HTTP 10005, AMQP 5673 plain and 5674 TLS (Service Bus keeps 5672/5671). Distinct ports were chosen over a shared listener because the AMQP open frame carries no namespace and the engine binds one handler per server."}
  namespaces: {status: partial, note: "AMQP addresses carry no namespace, so a hub name is resolved to the first namespace (by name) that has it; identical hub names in two namespaces are ambiguous."}
gaps:
  - "Kafka endpoint is not implemented."
  - "Capture, geo-DR, schema registry, application groups, compaction and cluster features are not implemented."
  - "SAS claims are not scoped to an entity; hub-scoped rules authorize any hub."
  - "Hub names resolve across namespaces by first match."
deferred: []
items_still_open:
  - "Kafka endpoint for Event Hubs (port 9093) is not implemented."
  - "Capture, geo-replication, compaction and application-group features are not implemented."
  - "SAS claims are not scoped to the audience entity."
  - "ARM namespace identity (SystemAssigned/UserAssigned), regenerateKeys and disasterRecoveryConfigs are not implemented."
  - "No dashboard page."
leaks: {status: clean, note: "Shutdown closes the AMQP servers and HTTP listener; the sweeper stops with its context; receiver registrations are removed when links close."}
---

## Notes

Partition keys are hashed with FNV-1a; real Event Hubs uses its own hash, so key-to-partition placement differs, only same-key-same-partition holds.
