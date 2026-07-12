service: mq
sdk_module: aws-sdk-go-v2/service/mq@v1.34.17   # audited against; go.mod pins this version
last_audit_commit: d54e01ab99e95fd424c6787001bee5390eecb16b
last_audit_date: 2026-07-12
overall: A                # genuine fixes found (wire enum casing, HTTP status codes, validation gaps, missing response fields)

# Per-op status. wire=response/request shape vs SDK; errors=code+HTTP status;
# state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateBroker: {wire: ok, errors: ok, state: ok, persist: ok, note: "HTTP status fixed 202->200 this pass"}
  DescribeBroker: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBrokers: {wire: ok, errors: ok, state: ok, persist: ok, note: "opaque index pagination via pkgs/page"}
  UpdateBroker: {wire: ok, errors: ok, state: partial, persist: ok, note: "response now includes dataReplicationMode/-Metadata + pending counterparts (was missing); state mutation is IMMEDIATE rather than staged-until-reboot -- see gaps"}
  DeleteBroker: {wire: ok, errors: ok, state: ok, persist: ok, note: "async DELETION_IN_PROGRESS -> removed-on-next-read lifecycle, matches SDK poll pattern"}
  RebootBroker: {wire: ok, errors: ok, state: ok, persist: ok, note: "REBOOT_IN_PROGRESS -> RUNNING promoted on next Describe/List read"}
  Promote: {wire: ok, errors: ok, state: partial, persist: ok, note: "validates mode + broker existence; no-op beyond that (CRDR promote simulation not implemented, matches CreateBroker's partial CRDR support)"}
  CreateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "username charset fixed to allow '.' and '~' (ActiveMQ pattern); password validation fixed to reject ':' and '=' in addition to ','"}
  DescribeUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "pending/replicationUser fields omitted (see gaps), harmless per protocol (extra/omitted optional fields both safe)"}
  UpdateUser: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUsers: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "revision history capped at 50, matches AWS"}
  DeleteConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigurationRevision: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConfigurationRevisions: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeBrokerEngineTypes: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static catalog, not persisted resource state"}
  DescribeBrokerInstanceOptions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "storageType filter/values fixed to uppercase EFS/EBS this pass"}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "HTTP status fixed 200->204 this pass"}
  DeleteTags: {wire: ok, errors: ok, state: ok, persist: ok}

families:
  route_matching: {status: ok, note: "parseRoute/parseBrokerRoute/parseUserRoute/parseConfigurationRoute/parseRevisionRoute/parseTagRoute verified path-prefix+method against every aws-sdk-go-v2/service/mq serializer (CreateBroker POST /v1/brokers, DescribeBroker GET /v1/brokers/{id}, reboot/promote POST suffixes, users nested under brokers, revisions nested under configurations, tags keyed by escaped ARN). All 24 ops in the pinned SDK route correctly through the matcher, not just via direct Handler() calls in tests."}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to InMemoryBackend.Snapshot/Restore (persistence.go); registered generically via Provider in cli.go. No silent-unregistration risk found."}

gaps:
  - "UpdateBroker mutates EngineVersion/HostInstanceType/SecurityGroups/AuthenticationStrategy/LdapServerMetadata/Logs/Configurations.Current/DataReplicationMode IMMEDIATELY instead of staging them as Pending* and promoting only on a subsequent successful reboot. Real Amazon MQ requires a reboot for these fields to take effect (DescribeBrokerOutput/UpdateBrokerOutput both carry dedicated pendingEngineVersion/pendingHostInstanceType/pendingSecurityGroups/pendingAuthenticationStrategy/pendingLdapServerMetadata/Configurations.Pending/LogsSummary.Pending/pendingDataReplicationMode wire fields for exactly this purpose). The Broker struct already carries all these Pending* fields and brokerResponse/updateBrokerResponse already serialize them, but nothing in backend.go ever assigns them -- they are permanently zero-valued dead fields. Net effect: gopherstack is over-eager (changes apply instantly) rather than under-eager (no SDK poll-loop hang, the specific failure mode called out for other services), but it is not AWS-accurate. Fixing this properly requires reworking applyBrokerCoreFields/applyUpdateBrokerOptions to stage into Pending* fields and extending promoteRebootingToRunning (rename to something like promoteBrokerReboot) to apply staged changes atomically with the REBOOT_IN_PROGRESS->RUNNING transition, plus reworking ~15 existing tests across handler_parity_batch1_test.go/handler_test.go/parity_extension_test.go that currently assert immediate-apply as correct. Deferred this pass due to blast radius vs. payload (no client-breaking behavior); recommend a dedicated follow-up pass. Would also need to decide handling for User-level pending changes (UserPendingChanges/ChangeType CREATE|UPDATE|DELETE) which apply the same reboot-gated pattern to CreateUser/UpdateUser/DeleteUser for ActiveMQ brokers -- currently gopherstack applies those immediately too."
  - "Configuration name has no charset/length validation (real AWS: 1-150 chars, alphanumeric + dashes/periods/underscores/tildes); only a non-empty check exists. Low severity -- gopherstack is more permissive than AWS, does not reject valid SDK input."
  - "DescribeUser response omits the optional 'pending' (UserPendingChanges) and 'replicationUser' fields -- both are always absent since the underlying pending-changes/CRDR-replication-user features are not modeled. Harmless (optional fields), but linked to the UpdateBroker pending-fields gap above."
  - "DescribeSharedResources (added to the mq service-2.json botocore model) has no corresponding operation in the pinned aws-sdk-go-v2/service/mq@v1.34.17 client at all -- out of scope for aws-sdk-go-v2 wire-compat auditing since the Go SDK can't call it."

deferred:
  - "Full CRDR (cross-region data replication) simulation: Promote/DataReplicationMetadata population when dataReplicationMode=CRDR is not modeled beyond accepting/echoing the mode string."

leaks: {status: clean, note: "no goroutines, tickers, or background janitors in services/mq; purely synchronous in-memory backend guarded by a single lockmetrics.RWMutex."}
