---
service: lambda
sdk_module: aws-sdk-go-v2/service/lambda@v1.97.0
last_audit_commit: a007ec3e
last_audit_date: 2026-07-11
overall: B   # re-audit of local drift since c3b5d46a; no new bugs found, all gates green
protocol: REST-JSON
families:
  resource_policy: {status: ok, note: unchanged since c3b5d46a; PROVEN — RemovePermission StatementId from URI path, Qualifier scoping, EventSourceToken/PrincipalOrgID}
  event_source_mappings: {status: ok, note: unchanged since c3b5d46a; ARN parsing, pollers PROVEN — backoff, FilterCriteria, BisectBatchOnFunctionError, ReportBatchItemFailures, MaxRecordAge. Storage backing (b.eventSourceMappings) converted map->store.Table (ce30166a); re-verified — CreateEventSourceMapping/Get/List/Delete/Update and janitor.sweepESMs all correctly ported}
  datalayer_refactor: {status: ok, note: "ce30166a converted functions/functionURLConfigs/eventSourceMappings/aliases/permissions/codeSigningConfigs/capacityProviders/provisionedConcurrencies from raw maps to pkgs/store Table/Index (store_setup.go, new file). Re-verified every call site in backend.go, janitor.go, async_destinations.go, export_test.go: key derivation (functionURLConfigsKeyFn/aliasKeyFn/permissionKeyFn/provisionedConcurrencyKeyFn all pure + stable), index-returned-slice aliasing (ListAliases/GetPolicy copy into a fresh slice before returning, never leak the Index-owned backing slice), delete cascades (deleteAliasesForFunctionLocked/deletePermissionsForFunctionLocked/deleteProvisionedConcurrenciesForFunctionLocked). No behavior change found — mechanical, correct conversion. codeSigningConfigs/capacityProviders/provisionedConcurrencies correctly kept on b.ephemeralRegistry (not b.registry) preserving their pre-refactor not-persisted status; permissions correctly kept off both registries with a DTO round-trip (permissionSnapshot) since FunctionName/Qualifier are json:\"-\" on the live struct"}
  persistence:      {status: ok, note: "ce30166a added lambdaSnapshotVersion=1 gate (mirrors sqs/ec2 pilot) — an incompatible/absent Version discards to empty rather than partially decoding. Same known systemic trait as sqs/ec2: on a version-mismatch Restore, only b.registry + b.permissions are reset; raw non-Table fields (versions/layers/eventInvokeConfigs/layerPolicies/functionConcurrencies/accountID/region) are left as-is. Not a lambda-specific regression — identical to services/sqs and services/ec2's Restore; Restore only ever runs once against a freshly-constructed backend in practice. Not flagging as a new bug; tracked here for awareness only"}
  runtime_lifecycle: {status: ok, note: unchanged since c3b5d46a; PROVEN — LRU eviction, async cleanup semaphore, container stop/remove, port release, dir cleanup. Real Docker exec}
  durable_execution: {status: ok, note: unchanged since c3b5d46a; PROVEN — reads/writes real durableExecutionStore despite handler_stubs.go filename}
gaps: []
deferred:
  - AddPermission FunctionUrlAuthType / InvokedViaFunctionUrl / RevisionId optimistic-concurrency (lower value)
  - function CRUD/versions/aliases/layers/provisioned-concurrency/URLs/tags — skimmed, tests green, not exhaustively re-verified
  - SDK v1.94.1->v1.97.0 added two new optional fields to existing shapes, no new ops — TelemetryConfig on Managed-Instances Capacity Provider, and a customer-managed-KMS-key field on Durable Config. Both accept-and-echo-only in a real AWS sense would need modeling; not present yet. Low value for an in-memory emulator (no real KMS/log-group enforcement) — left unimplemented (bd: file if a client depends on echoing these fields back)
leaks: {status: clean, note: event-source pollers + janitor + container lifecycle all leak-conscious; go test -race passes (includes new persistence_test.go round-trip coverage of the store.Table conversion)}
---

## Notes
- InvocationType is a type alias (type InvocationType = string) so lambda backend satisfies sns.LambdaInvoker directly.
- ARN-parsing anti-pattern "take last colon segment" recurs — watch for it elsewhere.
- Trap: RemovePermission wire = DELETE /2015-03-31/functions/{name}/policy/{StatementId} (path, not query).
- ce30166a (Parity sweep 3, unrelated commit that swept in a large dependency+datalayer PR) converted most lambda backend maps to pkgs/store Table/Index. eventInvokeConfigs, versions, layers, versionCounters, functionConcurrencies, layerVersionCounters, layerPolicies, activeConcurrencies, fnCodeSigningConfigs, fisFaults, runtimeManagementConfigs, functionRecursionConfigs, functionScalingConfigs, versionIndex, esmByFunctionARN, runtimes, functionURLServers were deliberately left as plain maps (documented per-field in store_setup.go's package doc) — each has a concrete reason (no pure identity in the value, one-to-many shape, or live non-serializable state). Read that doc comment before "fixing" any of them into a Table.
- pkgs/store.Table/Index perform NO internal locking (by design — see pkgs/store package doc); every lambda call site still takes b.mu itself. Index.Get() returns a slice OWNED BY THE INDEX — never return it directly from a public method without copying first (ListAliases/GetPolicy both copy correctly; verified).
