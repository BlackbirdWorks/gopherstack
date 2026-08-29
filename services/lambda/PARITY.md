---
service: lambda
sdk_module: aws-sdk-go-v2/service/lambda@v1.101.2
last_audit_commit: a007ec3e
last_audit_date: 2026-07-25
overall: A   # durable_execution wire-shape rewrite closed the last open gap; all gates green
protocol: REST-JSON
families:
  resource_policy: {status: ok, note: "PROVEN — RemovePermission StatementId from URI path, Qualifier scoping, EventSourceToken/PrincipalOrgID. This sweep closed the AddPermission deferred item: FunctionUrlAuthType/InvokedViaFunctionUrl are now accepted and rendered as IAM Condition entries (StringEquals lambda:FunctionUrlAuthType, Bool lambda:InvokedViaFunctionUrl — verified against real AWS docs/terraform-provider-aws issue #44829), and RevisionId optimistic concurrency is enforced on AddPermission/RemovePermission/GetPolicy (was hardcoded RevisionId:\"1\" — now a real content-hash of the statement-ID set, changing on every mutation, stable otherwise). Same RevisionId + duplicate-StatementId (ResourceConflictException) treatment extended to AddLayerVersionPermission/RemoveLayerVersionPermission/GetLayerVersionPolicy (layers.go), which had the identical hardcoded-\"1\" bug and silently overwrote a duplicate StatementId instead of rejecting it."}
  event_source_mappings: {status: ok, note: unchanged since c3b5d46a; ARN parsing, pollers PROVEN — backoff, FilterCriteria, BisectBatchOnFunctionError, ReportBatchItemFailures, MaxRecordAge. Storage backing (b.eventSourceMappings) converted map->store.Table (ce30166a); re-verified — CreateEventSourceMapping/Get/List/Delete/Update and janitor.sweepESMs all correctly ported}
  datalayer_refactor: {status: ok, note: "ce30166a converted functions/functionURLConfigs/eventSourceMappings/aliases/permissions/codeSigningConfigs/capacityProviders/provisionedConcurrencies from raw maps to pkgs/store Table/Index (store_setup.go, new file). Re-verified every call site in backend.go, janitor.go, async_destinations.go, export_test.go: key derivation (functionURLConfigsKeyFn/aliasKeyFn/permissionKeyFn/provisionedConcurrencyKeyFn all pure + stable), index-returned-slice aliasing (ListAliases/GetPolicy copy into a fresh slice before returning, never leak the Index-owned backing slice), delete cascades (deleteAliasesForFunctionLocked/deletePermissionsForFunctionLocked/deleteProvisionedConcurrenciesForFunctionLocked). No behavior change found — mechanical, correct conversion. codeSigningConfigs/capacityProviders/provisionedConcurrencies correctly kept on b.ephemeralRegistry (not b.registry) preserving their pre-refactor not-persisted status; permissions correctly kept off both registries with a DTO round-trip (permissionSnapshot) since FunctionName/Qualifier are json:\"-\" on the live struct"}
  persistence:      {status: ok, note: "ce30166a added lambdaSnapshotVersion=1 gate (mirrors sqs/ec2 pilot) — an incompatible/absent Version discards to empty rather than partially decoding. Same known systemic trait as sqs/ec2: on a version-mismatch Restore, only b.registry + b.permissions are reset; raw non-Table fields (versions/layers/eventInvokeConfigs/layerPolicies/functionConcurrencies/accountID/region) are left as-is. Not a lambda-specific regression — identical to services/sqs and services/ec2's Restore; Restore only ever runs once against a freshly-constructed backend in practice. Not flagging as a new bug; tracked here for awareness only. Note: PublishVersion's new RevisionId precondition check deliberately reuses fn.RevisionID (already persisted as part of FunctionConfiguration) rather than adding new persisted state, so this is unaffected."}
  runtime_lifecycle: {status: ok, note: unchanged since c3b5d46a; PROVEN — LRU eviction, async cleanup semaphore, container stop/remove, port release, dir cleanup. Real Docker exec}
  function_crud_versions_aliases_layers_concurrency_urls_tags: {status: ok, note: "Field-diffed this sweep (was 'skimmed, not exhaustively re-verified'). Real bug found + fixed: FunctionEventInvokeConfig.LastModified was a time.Time (ISO8601-string wire shape) but the real deserializer (PutFunctionEventInvokeConfig/GetFunctionEventInvokeConfig 'LastModified' case in deserializers.go) parses a json.Number — unlike FunctionConfiguration.LastModified, which IS an ISO8601 string. Fixed to float64 via pkgs/awstime.Epoch, matching the exact bug class documented in parity-principles.md. Also found + fixed a latent double-write bug in handleUpdateFunctionCode/handleUpdateFunctionConfiguration: applyFunctionCodeUpdate returned h.writeError(...)'s own return value as its error signal, but c.JSON (and so writeError) returns nil on ANY successful write — including a written error response — so the `!= nil` check could never detect a validation failure and would silently fall through to a second, conflicting 200 write. Converted to the bool-return convention (see checkRevisionID's doc comment in handler.go). RevisionId optimistic concurrency (previously only on AddPermission) extended to UpdateFunctionConfiguration/UpdateFunctionCode (checked against fn.RevisionID before mutating), UpdateAlias (against alias.RevisionID), and PublishVersion (new PublishVersionWithRevision atomic backend method — kept the existing 2-arg PublishVersion signature untouched since it has ~20 call sites across tests + a CFN caller; the revision check and the publish happen under one lock acquisition via a shared internal publishVersion(name, description, revisionID) to avoid a check-then-act race). Other families (function URL configs, tags, reserved/provisioned concurrency, code signing) spot-checked against the SDK's Output shapes/timestamp wire formats — no further gaps found; CreateFunctionUrlConfig/GetFunctionUrlConfig's CreationTime/LastModifiedTime and ProvisionedConcurrencyConfig.LastModified are correctly ISO8601 strings (verified against deserializers.go), not epoch numbers."}
  durable_execution: {status: ok, note: "CLOSED (was gap) — dedicated rewrite of durable_execution.go/handler_durable_execution.go, field-diffed against api_op_GetDurableExecution.go, api_op_GetDurableExecutionHistory.go, api_op_GetDurableExecutionState.go, api_op_ListDurableExecutionsByFunction.go, api_op_StopDurableExecution.go, api_op_CheckpointDurableExecution.go, api_op_SendDurableExecutionCallback{Success,Failure,Heartbeat}.go and their types.go/serializers.go/deserializers.go on the installed aws-sdk-go-v2/service/lambda@v1.101.2 module (unchanged for these ops/types between v1.97.0 and v1.101.2). All 9 ops confirmed present in the SDK (not a gopherstack-invented family). Fixed: (1) GetDurableExecutionOutput splits DurableExecutionArn/DurableExecutionName (was one merged ExecutionArn), uses Unix-epoch StartTimestamp/EndTimestamp (was ISO8601 StartTime/StopTime), and adds the previously-entirely-absent DurableConfig echo, Error, ExecutionDataIncluded (honors ?IncludeExecutionData=, default true), InputPayload, Result, TraceHeader, Version; (2) DurableExecutionStatus gained TIMED_OUT; (3) GetDurableExecutionHistory's Events use real types.Event field names/types (EventId/epoch EventTimestamp/EventType/Id/Name/ParentId/SubType + the 5 Execution*Details subtypes this emulator's checkpoint-driven state machine can produce), honors IncludeExecutionData (redacts payload/result/error sub-fields via fresh copies, never mutating the stored event) and ReverseOrder, paginates via Marker/MaxItems (pkgs/page) — previously emitted one invented 'Checkpoint' EventType (not a real enum value) with no pagination; (4) GetDurableExecutionState returns real types.Operation-shaped Operations (Id/Type/Status/StartTimestamp/EndTimestamp/Name/ParentId/SubType) tracked through a new CheckpointDurableExecution Updates state machine (Action START/SUCCEED/FAIL/CANCEL/RETRY on STEP/WAIT/CALLBACK/CONTEXT/CHAINED_INVOKE operations, each mapped to its real EventType via a verified (Type,Action)->EventType table) — CheckpointDurableExecutionInput/Output were previously dead types (handler read an untyped map and discarded it; GetDurableExecutionState always echoed only raw StateData with no Operations). Also found (via the required field-diff) and fixed two real ROUTING bugs beyond the named field-shape gap: StopDurableExecution was wired as DELETE on the bare execution path returning the full execution object — real wire is POST .../stop returning {StopTimestamp} (epoch), and an unknown-ARN Stop silently 200'd 'idempotent' — now 404 ResourceNotFoundException matching Get/GetState; ListDurableExecutionsByFunction was wired at GET /2025-12-01/durable-executions?FunctionArn= — the real op is GET /2025-12-01/functions/{FunctionName}/durable-executions, a completely different path family, now correctly routed with DurableExecutionName/Statuses/StartedAfter/StartedBefore/ReverseOrder/Marker/MaxItems all wired. Also fixed: SendDurableExecutionCallback{Success,Failure,Heartbeat} were routed under the durable-executions ARN prefix with suffixes /callback/success|failure|heartbeat — the real wire is a wholly separate resource, POST /2025-12-01/durable-execution-callbacks/{CallbackId}/{succeed|fail|heartbeat} (note succeed/fail, NOT success/failure) keyed by CallbackId alone; now correctly routed, resolved via a callbackOwner index populated when a checkpoint Update starts a CALLBACK operation, and 404s on an unknown CallbackId (previously silently 200'd regardless). Locking hardened as part of the rewrite: durableExecutionStore's raw sync.RWMutex replaced with lockmetrics.RWMutex (pkgs-catalog.md's 'one coarse instrumented mutex per invariant' rule — this file was the one remaining raw-mutex holdout in the package), and every read method now builds its complete wire response — deep-copying any *DurableOperation it returns — while still holding the lock, rather than handing the handler a live internal pointer to read unsynchronized (previously a genuine, if not test-triggered, data race between a concurrent Get and Checkpoint/Stop on the same execution). Deliberately unchanged, pre-existing, out-of-gap-scope limitation: gopherstack has no StartDurableExecution entry point (correctly — neither does the real API; AWS starts an execution implicitly on Invoke) and this emulator's Invoke path does not model durable-execution semantics, so it still auto-creates the execution record on its first CheckpointDurableExecution call. FunctionArn/DurableConfig/InputPayload/Version are therefore wire-correct (right name, right type, will round-trip through the real SDK client) but always empty/nil today, since no caller threads them through that never-built entry point — this is an entry-point/architecture gap, not a wire-shape gap, and rewiring Invoke was out of this task's scope. Also intentionally not populated: the ~19 CONTEXT/STEP/WAIT/CALLBACK/CHAINED_INVOKE *Details sub-objects the real types.Event/types.Operation declare (no step-function-style replay engine exists to produce their contents) — the generic Id/Name/ParentId/SubType/EventType/Status fields ARE populated for those operation types via the Updates state machine, only the type-specific Details payloads are omitted."}
  capacity_providers: {status: ok, note: "gopherstack-m53b (required-member sweep pass 4). CreateCapacityProvider read a top-level \"Name\" field that does not exist on the wire -- the real required field is CapacityProviderName (api_op_CreateCapacityProvider.go:28-45 vs the old models.go CreateCapacityProviderInput) -- so every real client request 400'd with \"Name is required\" before ever reaching the backend; PermissionsConfig and VpcConfig, both also required, were dropped entirely. Full-shape read (per this sweep's standing instruction) found the drop was worse than the three named fields: CapacityProvider/CreateCapacityProviderInput/UpdateCapacityProviderInput had a wholesale-fabricated shape -- a TargetOnDemandConcurrency field that appears nowhere in the real API (removed), Status/LastModifiedTime field names that are actually State/LastModified on the wire (renamed), an ACTIVE status value where the real CapacityProviderState enum is title-cased Active/Pending/Failed/Deleting (fixed), and CapacityProviderScalingConfig/InstanceRequirements/KmsKeyArn/PropagateTags/TelemetryConfig(partially)/VpcConfig were entirely un-modeled despite being real CapacityProvider members. Rebuilt CreateCapacityProviderInput/UpdateCapacityProviderInput/CapacityProvider field-for-field against types.CapacityProvider (types/types.go:206-249) and its nested types (CapacityProviderPermissionsConfig/VpcConfig/ScalingConfig/TelemetryConfig, InstanceRequirements, PropagateTags, TargetTrackingScalingPolicy); UpdateCapacityProvider (not itself one of the five named bugs, but sharing the same CapacityProvider model and left broken by a narrower fix) was corrected alongside it -- CapacityProviderName is a URI label there, not a body field (serializers.go:7098-7113), matching the existing name-from-path handler wiring. Get/List now correctly echo the real state instead of a fabricated shape. Existing tests (capacity_providers_test.go) encoded the broken \"Name\"/TargetOnDemandConcurrency shape end to end (3 create/update/list tests + 1 telemetry test); corrected to the real field names, and a Test_SDKRoundTrip_CreateCapacityProvider/Test_SDKRoundTrip_UpdateCapacityProvider pair added, driving the real aws-sdk-go-v2 lambda client end to end -- both fail against the unfixed decode (hand-reverted and confirmed). TestHandlerReset_ClearsState (dispatch_test.go) also encoded the old \"Name\" shape and was corrected. gopherstack-r80d (required-OUTPUT-member sweep): DeleteCapacityProvider returned bare 204 No Content, but DeleteCapacityProviderOutput.CapacityProvider is required on the wire (api_op_DeleteCapacityProvider.go:44-46) -- real AWS returns 200 with the deleted provider's state. The real SDK deserializer treats an empty 204 body as JSON-decode-EOF (not an error), so the old code produced a client-side success with CapacityProvider left nil -- exactly the zero-value-on-success-path bug class. Fixed: DeleteCapacityProvider now returns the pre-deletion snapshot, handler responds 200 with {CapacityProvider}. Test_SDKRoundTrip_DeleteCapacityProvider added, driving the real client; fails against the unfixed handler with 'Expected value not to be nil' on CapacityProvider (hand-reverted and confirmed). Full sweep of the other 20 required-output-member ops in this service's SDK surface (CheckpointDurableExecution, Create/Get/List/UpdateCapacityProvider, Create/Get/UpdateCodeSigningConfig, GetDurableExecution/-History/-State, GetFunctionCodeSigningConfig, Create/Get/List/UpdateFunctionUrlConfig, ListFunctionVersionsByCapacityProvider, PutFunctionCodeSigningConfig, PutRuntimeManagementConfig, StopDurableExecution) found all correctly populated on their success paths -- this was the only miss."}
  route_reachability: {status: ok, note: "gopherstack-l5ir (2026-08-13). All 85 real lambda ops extracted from serializers.go (request.Method + httpbinding.SplitURI in each op's awsRestjson1_serializeOp<Op>.HandleSerialize) and diffed against the route table. Found and fixed 12 ops that were unreachable or misrouted at their true path/method, beyond the two routing bugs durable_execution's rewrite already caught (see that family's note): GetLayerVersionByArn was wired to a fictional literal path /2018-10-31/layers-by-arn -- the real op shares ListLayers' bare /2018-10-31/layers path, disambiguated only by a ?find=LayerVersion query flag (the query-parameter-discriminator class this sweep was told to watch for specifically); ListFunctionEventInvokeConfigs checked a fictional plural suffix /event-invoke-configs instead of the real /event-invoke-config/list; GetFunctionRecursionConfig/PutFunctionRecursionConfig used date 2024-08-28 instead of the real 2024-08-31; GetFunctionScalingConfig/PutFunctionScalingConfig used date 2023-10-26 AND path segment scaling-config instead of the real 2025-11-30 and function-scaling-config (both wrong, independently); ListTags/TagResource/UntagResource used date 2015-03-31 instead of the real 2017-03-31 -- all three tagging operations were unreachable; InvokeAsync's suffix predicate required a trailing slash (/invoke-async/) the real client never sends (real path has none); ListLayerVersions/PublishLayerVersion resolved via a separate parallel implementation (extractLayerOperation, used by ExtractOperation and IAMAction, NOT by the real HTTP dispatch table which was already correct) that left its discriminating segment empty for exactly this path shape, so both ops always fell through to empty/Unknown -- a real IAM-action and CloudTrail-naming gap even though the request itself was correctly handled. Also corrected, not a bug: ExtractOperation previously returned the lambdaOpRoutes table's first-matching entry for POST .../invocations, which was the literal string \"InvokeFunction\" -- that is the correct IAM *action* name for this op (a documented AWS naming quirk where the IAM action differs from the API operation name) but the wrong *operation* name; ExtractOperation now special-cases this path to return the real op name \"Invoke\" while IAMAction is untouched and still correctly returns lambda:InvokeFunction. ExtractOperation, previously covering only ~30 of 85 ops (CRUD, layers, durable exec), was extended to mirror dispatchSpecialRoutes/lambdaOpRoutes/layerOpTable op-for-op so TestExtractOperation_SDKRouteTable (handler_paths_sdk_diff_test.go, one subtest per op) exercises the real dispatch tree directly -- 85/85 pass. Existing tests that encoded the old wrong paths/dates/expected-op-names (tags_test.go, handler_tags_iam_test.go, function_settings_test.go, event_invoke_config_test.go, layers_http_test.go, invocation_test.go, handler_routing_test.go) were corrected to the real shapes rather than preserved."}
gaps: []
deferred: []
leaks: {status: clean, note: "event-source pollers + janitor + container lifecycle all leak-conscious; go test -race passes. New PublishVersionWithRevision path adds no new goroutines/locks (reuses the existing PublishVersion lock); layerPolicyRevisionID/policyRevisionID are pure functions with no new backend state (derived from already-persisted b.permissions / b.layerPolicies, so no new persistence surface either). durable_execution rewrite: durableExecutionStore starts no goroutines and holds no live resources (pure in-memory map + mutex), so Shutdown has nothing to drain; every Lock/RLock is immediately followed by a deferred Unlock/RUnlock with no intervening early return; b.durableExecs.reset() (lifecycle.go) clears both the executions map and the callbackOwner index together, so no ghost callbackOwner entries survive a Reset."}
---

## Notes
- InvocationType is a type alias (type InvocationType = string) so lambda backend satisfies sns.LambdaInvoker directly.
- ARN-parsing anti-pattern "take last colon segment" recurs — watch for it elsewhere.
- Trap: RemovePermission wire = DELETE /2015-03-31/functions/{name}/policy/{StatementId} (path, not query).
- ce30166a (Parity sweep 3, unrelated commit that swept in a large dependency+datalayer PR) converted most lambda backend maps to pkgs/store Table/Index. eventInvokeConfigs, versions, layers, versionCounters, functionConcurrencies, layerVersionCounters, layerPolicies, activeConcurrencies, fnCodeSigningConfigs, fisFaults, runtimeManagementConfigs, functionRecursionConfigs, functionScalingConfigs, versionIndex, esmByFunctionARN, runtimes, functionURLServers were deliberately left as plain maps (documented per-field in store_setup.go's package doc) — each has a concrete reason (no pure identity in the value, one-to-many shape, or live non-serializable state). Read that doc comment before "fixing" any of them into a Table.
- pkgs/store.Table/Index perform NO internal locking (by design — see pkgs/store package doc); every lambda call site still takes b.mu itself. Index.Get() returns a slice OWNED BY THE INDEX — never return it directly from a public method without copying first (ListAliases/GetPolicy both copy correctly; verified).
- Policy RevisionId (function-policy and layer-version-policy) is deliberately a pure content-hash of the sorted StatementId set (policyRevisionID in permissions.go, layerPolicyRevisionID in layers.go), NOT a stored uuid.New()-per-mutation field like Function/Version/Alias RevisionID. This works because statement content is immutable once added (no UpdatePermission op exists — a StatementId can only be added once, then removed), so the ID set alone detects every real mutation, and it stays correct across Snapshot/Restore without adding new persisted state.
- writeError's return value is NOT a reliable "did this write an error response" signal — c.JSON (which it wraps) returns nil on any successful write, including a written error, so `if xErr := h.writeError(...); xErr != nil` can never trigger. Handler helpers that write an error and need the caller to stop must return bool (true=continue), matching validateMemoryAndTimeout/checkRevisionID/applyFunctionCodeUpdate. A stale `!= nil` check on such a helper is a latent double-write bug (found + fixed in applyFunctionCodeUpdate this sweep) — grep for this pattern before trusting any "returns error, checked with != nil" helper that calls writeError internally.
- Durable-execution family spans THREE independent path prefixes, not one — do not assume everything nests under `/2025-12-01/durable-executions/{DurableExecutionArn}/...`: GetDurableExecution/History/State + CheckpointDurableExecution + StopDurableExecution do; ListDurableExecutionsByFunction is `/2025-12-01/functions/{FunctionName}/durable-executions` (a `/functions` path, verified against api_op_ListDurableExecutionsByFunction.go); SendDurableExecutionCallback{Success,Failure,Heartbeat} is `/2025-12-01/durable-execution-callbacks/{CallbackId}/{succeed|fail|heartbeat}` keyed by CallbackId, not DurableExecutionArn (note succeed/fail, not success/failure — trap for anyone guessing the suffix). See handler_paths.go's prefix constants and handler_durable_execution.go's `isDurableExecPath`/`dispatchDurableExecRoutes`.
- Lambda's REST API is spread across a dozen+ date-versioned path prefixes (2015-03-31, 2017-03-31, 2017-10-31, 2018-10-31, 2019-09-25, 2019-09-30, 2020-04-22, 2020-06-30, 2021-07-20, 2021-10-31, 2021-11-15, 2024-08-31, 2025-11-30, 2025-12-01 all appear). gopherstack-l5ir found 4 of these constants carrying a wrong date (tags: 2015-03-31 vs real 2017-03-31; recursion-config: 2024-08-28 vs real 2024-08-31; scaling-config: 2023-10-26 vs real 2025-11-30) that made every op under that prefix unreachable. When adding or auditing any lambda op, verify its date prefix against `httpbinding.SplitURI(...)` in serializers.go directly -- do not assume a "close enough" date is correct, and do not trust an existing constant's date without checking it against the SDK source at least once.
- durable_execution is intentionally NOT wired into Snapshot/Restore (durableExecutionStore isn't touched by persistence.go) — this predates the wire-shape rewrite and is unrelated to it; durable executions were never persisted, only cleared on Reset (lifecycle.go's `b.durableExecs.reset()`). Not flagged as a bug: no entry point exists to repopulate FunctionArn/DurableConfig/InputPayload after a restore anyway (see durable_execution family note above), so persisting the store today would only round-trip empty shells.
- `ListLayers` and `ListLayerVersions` summary narrowing: `LayerVersion.Content` was previously populated on `ListLayers` and `ListLayerVersions` responses. In `aws-sdk-go-v2/service/lambda@v1.101.2`, `types.LayerVersionsListItem` does not contain `Content` (only `GetLayerVersion` / `PublishLayerVersion` returns `Content`). Fixed: `ListLayers` and `ListLayerVersions` omit `Content`.


## 2026-08-23: pagination bug sweep (ListLayerVersions, ListProvisionedConcurrencyConfigs, ListCodeSigningConfigs, ListFunctionsByCodeSigningConfig)

Discovered while auditing the pagination bug class found in medialive.
`handleListLayerVersions`, `handleListProvisionedConcurrencyConfigs`,
`handleListCodeSigningConfigs`, and `handleListFunctionsByCodeSigningConfig`
all ignored the real `Marker`/`MaxItems` request members (lambda@v1.101.2:
`ListLayerVersionsInput`, `ListProvisionedConcurrencyConfigsInput`,
`ListCodeSigningConfigsInput`, `ListFunctionsByCodeSigningConfigInput`) and
always returned every item in one unbounded page with no `NextMarker`,
despite `NextMarker` already existing (unused) on all four output structs.
Fixed using the existing `parsePaginationParams` + `pkgs/page.New` +
`lambdaDefaultMaxItems` pattern already used by `ListFunctions`/`ListLayers`
in this package. `ListLayerVersions`, `ListProvisionedConcurrencyConfigs`,
and `ListFunctionsByCodeSigningConfig` are unexported `*InMemoryBackend`
methods (not part of a public interface) but changed return type from a
bare slice to `page.Page[T]`; `go build ./...` confirmed clean, and two
pre-existing test call sites (persistence_test.go, layers_test.go) updated
for the new `ListLayerVersions` signature. Proven with four
`Test*_SDKRoundTrip_Pagination` tests (`list_pagination_ignored_test.go`),
each driving the real SDK client across two 10-item pages of 25 seeded
items and asserting the pages are disjoint; all four fail against the
unfixed handlers (`should have 10 item(s), but has 25`), hand-reverted
and confirmed.

Audited but NOT fixed: `handleListFunctionURLConfigs` also ignores
Marker/MaxItems, but the route is always called with a non-empty
`{name}` path segment, and the per-function code path
(`GetFunctionURLConfig(name)`) can only ever return 0 or 1 items — this
service's data model has no per-qualifier function URL configs, so the
unbounded branch is dead code with zero real blast radius. Not fixed.

## 2026-08-23: DeleteFunction's Qualifier discarded — every delete removed the whole function

Read `serializeOpHttpBindings<Op>Input` directly for `DeleteFunctionInput`
(lambda@v1.101.2 serializers.go:1690,
`awsRestjson1_serializeOpHttpBindingsDeleteFunctionInput`): `FunctionName`
is URI-bound, `Qualifier` is query-bound
(`encoder.SetQuery("Qualifier")`). `handleDeleteFunction`
(`handler_functions.go`) never read the query string at all — it called
`h.Backend.DeleteFunction(name)` unconditionally, so a client asking to
delete one published version (`DeleteFunctionInput{FunctionName,
Qualifier: "2"}`) instead had the entire function deleted: every version,
every alias, every event source mapping. `api_op_DeleteFunction.go`'s doc
comment is explicit: "To delete a specific function version, use the
Qualifier parameter. Otherwise, all versions and aliases are deleted", and
"You can't delete a version that an alias references." The backend already
tracked exactly the state this needed (`b.versionIndex`/`b.versions` for
published versions, `b.aliasesByFunction` for the alias-reference check) —
only `DeleteFunction`'s dispatch ignored the qualifier.

Fixed via the existing `QualifierInvoker`/`QualifierResolver`
optional-extension pattern (`store.go`) rather than changing
`StorageBackend.DeleteFunction`'s existing signature (would have required
touching `services/cloudformation/resources.go:2150`, the one out-of-package
caller, and running `make build-check`): added `QualifierDeleter` with
`DeleteFunctionVersion(name, qualifier string) error`, implemented on
`InMemoryBackend` (`functions.go`). `handleDeleteFunction` now reads
`Qualifier` off the query string; when present it type-asserts
`QualifierDeleter` and calls `DeleteFunctionVersion`, which deletes only the
targeted `b.versionIndex[name][qualifier]` entry (and its `b.versions[name]`
slice element) after checking `b.aliasesByFunction` for a referencing alias
(`ErrVersionReferencedByAlias`, new sentinel → 409 ResourceConflictException)
and rejecting `Qualifier=$LATEST` (`ErrInvalidParameterValue` → 400 — $LATEST
has no separate version resource; omit Qualifier to delete the whole
function). An empty Qualifier still calls the original unqualified
`DeleteFunction` path unchanged. Function tags are only released when the
whole function is deleted (`qualifier == ""`).

`TestDeleteFunction_Qualifier` (`delete_function_version_test.go`) drives
the real `aws-sdk-go-v2` lambda client, table-driven across three cases:
qualified delete removes only the targeted version ($LATEST and the other
version survive, `GetFunctionConfiguration(Qualifier: v1)` now 404s);
qualified delete is rejected with `ResourceConflictException` when an alias
still references that version (and the version survives the rejected
delete); unqualified delete still removes the whole function. Hand-reverted
`handleDeleteFunction` back to its pre-fix unconditional
`h.Backend.DeleteFunction(name)` call: both the "removes only that version"
and "blocked by alias reference" subtests failed exactly as predicted (the
whole function vanished instead of just the targeted version, so
`GetFunctionConfiguration` against the survivor 404'd and the
expected-error assertion against the alias-referenced delete saw no error
at all); restored and confirmed byte-identical via `md5sum`.

**Modelling gaps found in the same header sweep, not implemented**:
`InvokeInput`'s `TenantId` (lambda@v1.101.2 serializers.go:3859,
`awsRestjson1_serializeOpHttpBindingsInvokeInput`) is a real
`X-Amz-Tenant-Id` header for Lambda's multi-tenant-function feature —
gopherstack has no tenant concept anywhere in this service, so this is a
genuine unmodeled feature, not a discarded-but-tracked field; reported, not
attempted. `InvokeInput.DurableExecutionName` (request header) and
`InvokeOutput.DurableExecutionArn` (response header, deserializers.go:8744,
`awsRestjson1_deserializeOpHttpBindingsInvokeOutput`) are likewise never
wired on the `Invoke` path — consistent with, not a new instance of, the
already-documented durable_execution family gap above ("gopherstack has no
StartDurableExecution entry point... this emulator's Invoke path does not
model durable-execution semantics").

Gates: `go build ./...`, `go vet ./services/lambda/...`, `go test -race
-count=1 ./services/lambda/...`, `go fix -diff ./services/lambda/...` (no
diff), `gofmt -l services/lambda/` (no output), `golangci-lint run
./services/lambda/...` (1 finding — `godot` on the new
`DeleteFunctionVersion` doc comment's closing quoted sentence, fixed by
rewording so the comment's last line ends outside the quote; 0 issues after,
no `//nolint` added), `go test ./pkgs/persistence/...` (no persisted struct
changed) all clean. No exported method signature was changed —
`StorageBackend.DeleteFunction` is untouched — so `make build-check` was not
required; `go build ./...` (whole repo) confirmed clean regardless.


## 2026-08-28: UpdateAlias didn't validate FunctionVersion, unlike CreateAlias

gopherstack-huyl (Create-vs-Update precondition sweep). `UpdateAlias`
(versions_aliases.go) set `alias.FunctionVersion = input.FunctionVersion`
unconditionally, so an alias could be repointed at a version number that was
never published — `CreateAlias` validates the target version against
`b.versions[name]` (or accepts `$LATEST`), but `UpdateAlias` had no
equivalent check. lambda@v1.101.2 deserializers.go's
`deserializeOpErrorUpdateAlias` models `ResourceNotFoundException` (the same
code `ErrVersionNotFound` already maps to on the `CreateAlias` path), so the
fix mirrors `CreateAlias`'s `versionInList` check and reuses the existing
sentinel error. `handleUpdateAlias` (handler_versions_aliases.go) previously
had no `ErrVersionNotFound` case at all — added one, matching `handleCreateAlias`'s.
New real-SDK-client proof: `TestUpdateAlias_UnknownVersionSurfacesResourceNotFoundException`
(`$LATEST` still exempted, proven by `TestUpdateAlias_LatestVersionSucceeds`)
in `wire_field_fixes_test.go`; hand-reverted `versions_aliases.go` +
`handler_versions_aliases.go`, confirmed both tests fail
(`ResourceNotFoundException` never surfaced), restored.
