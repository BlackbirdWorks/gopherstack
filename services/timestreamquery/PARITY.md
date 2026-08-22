---
service: timestreamquery
sdk_module: aws-sdk-go-v2/service/timestreamquery@v1.39.4
last_audit_commit: a98a164d                    # NOT updated this pass -- git commands are off-limits (gopherstack-r80d batch 25)
last_audit_date: 2026-08-21
overall: A            # this pass: DescribeScheduledQuery's ScheduledQuery.TargetConfiguration.
                       # TimestreamConfiguration was missing 2 of its 4 required members
                       # (TimeColumn/DimensionMappings) -- CreateScheduledQuery's request
                       # parsing never read them at all. Fixed. Previous pass: CreateScheduledQuery.KmsKeyId was accepted nowhere and returned
                       # nowhere (a silent-drop, not just "no encryption layer") -- now stored and
                       # echoed on DescribeScheduledQuery. Fixed LastRunSummary.RunStatus/
                       # ScheduledQueryListEntry.LastRunStatus always claiming AUTO_TRIGGER_SUCCESS
                       # even though this emulator has no scheduler -- every run is produced by an
                       # explicit (manual) ExecuteScheduledQuery call, so the claim contradicted the
                       # only mechanism that ever populates it; now MANUAL_TRIGGER_SUCCESS.
ops:
  Query: {wire: fixed, errors: ok, state: ok, persist: ok, note: "deterministic mock rows/columns inferred from SQL projection (documented; no real Timestream Write data source exists to query against). ClientToken TTL fixed 8h->4h to match documented window. marshalColumnInfos (shared with PrepareQuery) previously hand-picked only Type.ScalarType out of ColumnInfo, silently dropping ArrayColumnInfo/RowColumnInfo/TimeSeriesMeasureValueColumnInfo whenever set; now passes the full ColumnType struct through so the nested union marshals correctly (types.Type)."}
  CancelQuery: {wire: ok, errors: ok, state: fixed, persist: ok, note: "was non-idempotent: 2nd CancelQuery on the same QueryId 404'd (ValidationException) instead of succeeding per documented idempotent-cancel contract. Now marks Cancelled in place instead of deleting."}
  PrepareQuery: {wire: fixed, errors: ok, state: fixed, persist: n/a, note: "disguised no-op: ValidateOnly=true (the ONLY mode real Timestream documents as supported) returned an EMPTY Columns/Parameters list, discarding the inferred result for the one mode real clients use. Now returns the same inferred Columns/Parameters regardless of ValidateOnly. Shares the marshalColumnInfos nested-union fix noted under Query."}
  DescribeEndpoints: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateScheduledQuery: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "ClientToken was parsed by the SDK request but never read by the handler/backend, so an SDK-auto-retried create (aws-sdk-go-v2 auto-generates ClientToken via idempotency-token-autofill middleware) hit ConflictException instead of replaying the original success. Now caches ClientToken->Arn for 8h and replays. Also: KmsKeyId (types.go:659) was accepted nowhere on the request and returned nowhere on ScheduledQueryDescription (types.go:659, same field name) -- a silent-drop, not just missing encryption. Now parsed, stored, and echoed back on DescribeScheduledQuery (still no actual at-rest encryption -- see gaps)."}
  DeleteScheduledQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeScheduledQuery: {wire: fixed, errors: ok, state: ok, persist: ok, note: "LastRunSummary.ExecutionStats used the misspelled wire field ExecutionTimeInMillisecs; real field is ExecutionTimeInMillis (no trailing ecs) per awsAwsjson10_deserializeDocumentExecutionStats. gopherstack-r80d batch 25: ScheduledQueryDescription.TargetConfiguration.TimestreamConfiguration (types/types.go, TimestreamConfiguration struct) requires DatabaseName/DimensionMappings/TableName/TimeColumn once TargetConfiguration itself is present (validators.go:651-666, validateTimestreamConfiguration) -- CreateScheduledQuery's request parsing only ever read DatabaseName/TableName from the request body, silently dropping TimeColumn/DimensionMappings (no backing struct field at all). A real client's CreateScheduledQuery request setting a full, client-side-valid TargetConfiguration (the SDK's own validator requires all 4 once TargetConfiguration is set at all) got back a DescribeScheduledQuery response missing 2 required members. Fixed: added TargetTimeColumn/TargetDimensionMappings to the ScheduledQuery domain model, threaded through CreateScheduledQuery's request parsing and the StorageBackend interface/InMemoryBackend signature, and echoed on DescribeScheduledQuery's TargetConfiguration.TimestreamConfiguration view."}
  ExecuteScheduledQuery: {wire: ok, errors: ok, state: fixed, persist: ok, note: "documented as running a scheduled query manually (api_op_ExecuteScheduledQuery.go: 'You can use this API to run a scheduled query manually'). This emulator has no scheduler goroutine, so ExecuteScheduledQuery is the ONLY code path that ever populates a run -- yet LastRunSummary.RunStatus and ScheduledQueryListEntry.LastRunStatus always claimed AUTO_TRIGGER_SUCCESS, asserting an automatic trigger that never happened. Fixed to MANUAL_TRIGGER_SUCCESS."}
  ListScheduledQueries: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateScheduledQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAccountSettings: {wire: fixed, errors: ok, state: ok, persist: ok, note: "DELETED an invented LastUpdatedTime field: neither DescribeAccountSettingsOutput nor UpdateAccountSettingsOutput (real SDK) defines one. Now also surfaces QueryCompute.ProvisionedCapacity.NotificationConfiguration when set (see UpdateAccountSettings)."}
  UpdateAccountSettings: {wire: fixed, errors: fixed, state: fixed, persist: fixed, note: "QueryCompute was accepted on the wire but never parsed/applied -- an account could never actually transition to PROVISIONED even though DescribeAccountSettings always echoed a QueryCompute field. Also fixed ProvisionedCapacity's response field name (ActiveQueryTCU, not the request-side TargetQueryTCU) and added QueryCompute to the account-settings snapshot (previously dropped across Snapshot/Restore). This pass: (1) DELETED the invented LastUpdatedTime response field (no equivalent in the real API shape); (2) added the documented TCU bounds (min 4, max 1000, multiple of 4) to both MaxQueryTCU and QueryCompute.ProvisionedCapacity.TargetQueryTCU, replacing the too-loose \"> 0\" check; (3) modeled QueryCompute.ProvisionedCapacity.NotificationConfiguration (types.AccountSettingsNotificationConfiguration: RoleArn + nested SnsConfiguration.TopicArn) end-to-end -- parsed from the request, validated (RoleArn required when present, SnsConfiguration.TopicArn required when SnsConfiguration present, mirroring the real SDK's client-side validateAccountSettingsNotificationConfiguration), applied only when ComputeMode is PROVISIONED, returned on the response, and persisted across Snapshot/Restore."}
families:
  tags: {status: deferred, note: "TagResource/UntagResource/ListTagsForResource are in GetSupportedOperations() and have working handlers/backend methods (own ARN-keyed tag map), but RouteMatcher intentionally excludes them (writeServiceTagOps) so production traffic is routed to the TimestreamWrite handler's unified cross-resource tag store instead. Verified TimestreamWrite's TagResource treats ResourceARN as an opaque key (no resource-type-specific lookup), so scheduled-query ARNs tag correctly there. This package's own tag handlers are dead code in production, reachable only via direct unit tests / Handler() bypassing RouteMatcher -- confirmed intentional, not a routing bug."}
  route_matching: {status: ok, note: "X-Amz-Target prefix Timestream_20181101., Content-Type application/x-amz-json-1.0 (awsjson1.0) verified against serializers.go (awsAwsjson10_*). DescribeEndpoints wired for SDK endpoint-discovery (fetchOpQueryDiscoverEndpoint calls DescribeEndpoints first)."}
gaps:
  - "CreateScheduledQueryInput.KmsKeyId is now stored and echoed on DescribeScheduledQuery (fixed this pass -- see CreateScheduledQuery note), but this emulator still has no at-rest encryption layer, so setting it has no observable effect on how results/error reports are protected. Honestly scoped: 'we do not encrypt', not 'we lose the setting'."
  - "ScheduledQueryDescription.RecentlyFailedRuns (up to 5 most recent failed runs) and ScheduledQueryRunSummary.QueryInsightsResponse are not modeled -- this emulator's ExecuteScheduledQuery always succeeds (see ExecuteScheduledQuery), so there is no failure path to populate RecentlyFailedRuns from, and no scheduled-query-run-level QueryInsights simulation exists. Both are optional response fields; omitting them is wire-safe (omitempty). Enum check: types.ScheduledQueryRunStatus has 4 documented values (enums.go:229-232) -- AUTO_TRIGGER_SUCCESS, AUTO_TRIGGER_FAILURE, MANUAL_TRIGGER_SUCCESS, MANUAL_TRIGGER_FAILURE. This package declares 3 as unexported consts (scheduled_queries.go) but is missing MANUAL_TRIGGER_FAILURE outright, and none of the *_FAILURE values are ever assigned (RunStatus is backend-generated output only, never client-supplied, so there is no exhaustiveness requirement over it) -- consistent with 'no failure path', not a separate drop. (bd: file follow-up if failure simulation is ever added)"
  - "gopherstack-r80d batch 25 reviewed, ruled OUT (not a bug): ScheduledQueryDescription.NotificationConfiguration/ScheduleConfiguration are required at the top level and, once present, their own SnsConfiguration.TopicArn/ScheduleExpression are required one level deeper -- scheduledQueryToView gates emitting each wrapper on the corresponding domain field being non-empty. The real SDK's own client-side validators (validators.go's validateNotificationConfiguration/validateScheduleConfiguration/validateSnsConfiguration/validateScheduleConfiguration) only reject a NIL pointer, not an empty string, so a real client COULD send TopicArn/ScheduleExpression as an explicit empty string and still pass client-side validation. But gopherstack's own handleCreateScheduledQuery independently rejects both as ValidationException if empty (\"NotificationConfiguration.SnsConfiguration.TopicArn is required\" / \"ScheduleConfiguration.ScheduleExpression is required\") -- stricter than the real SDK's client-side check, the same ruled-out class batch 23 established for codeconnections' RepositorySyncDefinition.Parent. Since gopherstack rejects the only path that would produce an empty value, the wrapper-omission gate is unreachable via any real client that gets past CreateScheduledQuery at all."
deferred:
  - "Query/CancelQuery against genuinely long-running or multi-page real query execution semantics -- this emulator's Query is synchronous and instantaneous (matches the mock-data-source design already documented in QueryWithOptions), so QueryExecutionException (a real error type for query engine failures) is never returned. Acceptable per the documented deterministic-mock design; revisit only if a real backing data source is added."
leaks: {status: clean, note: "clientTokens/scheduledQueryTokens/pageStore are self-contained caches with their own mutex, reset on Reset()/version-mismatch Restore(); queries table is bounded by maxRetainedQueries (10000) with arbitrary eviction, cancelled or not. No goroutines/tickers in this package -- nothing to ctx-parent or drain on Shutdown."}
---

## Notes

Protocol: awsjson1.0 (single POST endpoint, `X-Amz-Target: Timestream_20181101.<Op>`,
`Content-Type: application/x-amz-json-1.0`). Verified directly against
`aws-sdk-go-v2/service/timestreamquery@v1.39.4`'s serializers.go/deserializers.go/types
package (vendored under `~/go/pkg/mod`), not against this package's own output. The
`go.mod` pin was found stale at v1.36.16 in this pass and corrected to v1.39.4; `diff -r`
against the v1.36.16 module cache shows the version bump touched only client/middleware
plumbing (retry, tracing, clock-skew option) — `types/types.go` and `types/enums.go` are
byte-identical, so no prior wire claim in this file needed re-verification.

Real bugs fixed this pass (all under `services/timestreamquery/`):

1. **PrepareQuery disguised no-op** (`backend.go`, `PrepareQuery`): real Timestream
   documents `ValidateOnly=true` as the *only* supported mode for this operation, and
   `PrepareQueryOutput.Columns`/`Parameters` are both required response fields regardless
   of `ValidateOnly` — describing the query's shape is the entire point of the call. The
   emulator returned an **empty** Columns/Parameters list whenever `ValidateOnly` was
   true, i.e. for the one mode real clients actually use. Fixed to always return the
   inferred result. Renamed/updated the test that had asserted the old (wrong) behavior
   (`TestPrepareQuery_ValidateOnlyReturnsEmpty` -> `..._StillInfersColumns`).

2. **Wrong wire field name** (`backend_accuracy.go`, `ExecutionStats`): emitted
   `ExecutionTimeInMillisecs`; the real field (verified against
   `awsAwsjson10_deserializeDocumentExecutionStats`) is `ExecutionTimeInMillis` (no
   trailing "ecs"). A real client would silently fail to populate this field.

3. **CreateScheduledQuery ClientToken silently dropped** (`handler.go`, `backend.go`):
   the aws-sdk-go-v2 client auto-generates a `ClientToken` on *every* `CreateScheduledQuery`
   call via its idempotency-token-autofill middleware
   (`idempotencyToken_initializeOpCreateScheduledQuery`). The emulator never parsed or
   used it, so a retried request after a lost response (a plausible transient-network
   scenario) hit `ConflictException` ("already exists") instead of replaying the original
   success, as real Timestream documents ("After 8 hours, any request with the same
   ClientToken is treated as a new request"). Added an 8h ClientToken->Arn replay cache
   (`scheduledQueryTokens`), mirroring the existing `Query` ClientToken cache pattern.
   Also discovered and fixed the existing `Query` ClientToken cache's TTL: it was set to
   8h (`clientTokenTTL`), but `Query`'s own doc says "After 4 hours, any request with the
   same ClientToken is treated as a new request" — split into
   `queryClientTokenTTL` (4h) and `createScheduledQueryClientTokenTTL` (8h).

4. **CancelQuery non-idempotent** (`backend.go`, `CancelQuery`): `CancelQueryOutput`
   documents `CancellationMessage` as returned "when a CancelQuery request for the query
   ... has already been issued" — i.e. cancelling an already-cancelled query must
   succeed, not error. The emulator deleted the query result on first cancel, so a
   second `CancelQuery` for the same `QueryId` (e.g. a retried request) got
   `ValidationException` ("not found") instead. Fixed by marking `QueryResult.Cancelled`
   in place instead of deleting; the LRU retention cap (`maxRetainedQueries`) still
   bounds memory regardless of cancellation status. Updated
   `TestRefinement1_QueryCountTrack` and the persistence full-state test, which had
   asserted the old delete-on-cancel behavior.

5. **UpdateAccountSettings QueryCompute silently ignored** (`handler.go`, `backend.go`):
   `UpdateAccountSettingsInput.QueryCompute` (switch between `ON_DEMAND`/`PROVISIONED`
   compute mode) was never parsed from the request body at all, even though
   `DescribeAccountSettings` always echoed a `QueryCompute` field back (defaulting to
   `ON_DEMAND`) — a classic "never-populated field" disguised stub: the account could
   *never* actually transition to `PROVISIONED`. Wired up parsing + validation
   (`ComputeMode` required; `PROVISIONED` requires a positive
   `ProvisionedCapacity.TargetQueryTCU`) and application. While implementing this,
   found the response-side `ProvisionedCapacity` type used the *request*-side field name
   `TargetQueryTCU`; the real response type (`types.ProvisionedCapacityResponse`) uses
   `ActiveQueryTCU` plus a `LastUpdate{Status, TargetQueryTCU}` sub-object — fixed to
   match. Also added `QueryCompute` to the persisted `accountSettingsSnapshot` (it was
   entirely absent), since without that fix a `PROVISIONED` account would silently
   revert to `ON_DEMAND` across a Snapshot/Restore round trip (e.g. process restart) —
   this would have been a *new*, exercisable persistence bug introduced by fixing (5)
   without this companion change.

Real bugs fixed in the 2026-07-24 pass (all under `services/timestreamquery/`):

6. **Invented `LastUpdatedTime` field** (`models.go`, `account_settings.go`,
   `handler_account_settings.go`): `AccountSettings.LastUpdatedTime` was surfaced on both
   `DescribeAccountSettings` and `UpdateAccountSettings` responses, but neither
   `DescribeAccountSettingsOutput` nor `UpdateAccountSettingsOutput` in the real SDK
   defines any such field (verified directly against
   `aws-sdk-go-v2/service/timestreamquery@v1.36.16`'s `api_op_DescribeAccountSettings.go`
   / `api_op_UpdateAccountSettings.go`). Harmless to real clients (unknown JSON fields are
   ignored) but pure wire-shape drift with no real-API equivalent — deleted per the
   no-invented-fields rule. `TestAccountSettings_LastUpdatedTimeSetOnUpdate` (which
   asserted the old, wrong behavior) was replaced with
   `TestAccountSettings_NoInventedLastUpdatedTimeField`, which asserts the field is
   *absent*.

7. **TCU bounds not enforced** (`account_settings.go`, `validateTCU`): `MaxQueryTCU` and
   `QueryCompute.ProvisionedCapacity.TargetQueryTCU` only required `> 0`. Real AWS
   documents (`DescribeAccountSettingsOutput.MaxQueryTCU`'s doc comment): "you must set a
   minimum capacity of 4 TCU. You can set the maximum number of TCU in multiples of 4 ...
   The maximum value supported for MaxQueryTCU is 1000." Added `validateTCU` (min 4, max
   1000, multiple of 4) and applied it to both fields, replacing the too-loose positivity
   check. `TestUpdateAccountSettings_TCUMultipleOfFourValidation` covers the new bounds;
   all pre-existing tests already used multiple-of-4 TCU values (4/8/16/100) so none
   needed updating.

8. **`QueryCompute.ProvisionedCapacity.NotificationConfiguration` entirely unmodeled**
   (`models.go`, `account_settings.go`, `handler_account_settings.go`, `persistence.go`):
   real AWS's `types.ProvisionedCapacityRequest`/`ProvisionedCapacityResponse` both carry
   an optional `NotificationConfiguration` (`types.AccountSettingsNotificationConfiguration`:
   required `RoleArn` + optional nested `SnsConfiguration.TopicArn`) for SNS alerts on
   provisioned-capacity changes. Modeled end-to-end: new `AccountSettingsNotificationConfiguration`/
   `SnsConfiguration` types, request parsing in `handleUpdateAccountSettings`, client-side-style
   validation mirroring the real SDK's `validateAccountSettingsNotificationConfiguration`
   (RoleArn required when the block is present; `SnsConfiguration.TopicArn` required when
   `SnsConfiguration` is present), application only when `ComputeMode` is `PROVISIONED`
   (matching "This field is only visible when the compute mode is PROVISIONED"), inclusion
   on the response, and persistence across Snapshot/Restore (new
   `accountSettingsNotificationConfigSnapshot`) — without the persistence half this would
   have been a new, exercisable round-trip bug identical in shape to bug #5's QueryCompute
   fix. `KmsKeyId` and `RecentlyFailedRuns`/`ScheduledQueryRunSummary.QueryInsightsResponse`
   remain deliberately unmodeled; see `gaps`.

9. **`marshalColumnInfos` dropped the `ColumnInfo.Type` nested union**
   (`handler_query_execution.go`): shared by both `Query` and `PrepareQuery` responses,
   this function hand-built `{"ScalarType": c.Type.ScalarType}` instead of marshalling the
   full `ColumnType` struct, silently discarding `ArrayColumnInfo`/`RowColumnInfo`/
   `TimeSeriesMeasureValueColumnInfo` (`types.Type`'s other three union members) whenever
   any of them was set. `inferColumnsFromSQL` currently only ever produces scalar columns,
   so this was latent rather than actively firing, but the `ColumnType`/`ColumnInfo` Go
   types already model the full union correctly (see `models.go`) — the marshaling helper
   just wasn't using them. Fixed to pass `c.Type` straight through (its own json tags are
   already wire-correct) and to omit `Name` when empty (matching the real `*string`-typed,
   optional field, unset for array-element columns per the API doc). Covered by the new
   `column_info_marshal_test.go` (`TestMarshalColumnInfos_PreservesNestedUnion`,
   `TestMarshalColumnInfos_OmitsEmptyName`).

Real bugs fixed in the 2026-08-10 pass (gopherstack-kpi6, all under `services/timestreamquery/`):

10. **`CreateScheduledQueryInput.KmsKeyId` silently dropped** (`handler_scheduled_queries.go`,
    `scheduled_queries.go`, `models.go`): real `CreateScheduledQueryInput.KmsKeyId` and
    `types.ScheduledQueryDescription.KmsKeyId` (`aws-sdk-go-v2/service/timestreamquery@v1.39.4`
    `api_op_CreateScheduledQuery.go:100` / `types/types.go:659`) were previously not modeled
    at all: the field was accepted nowhere on create and returned nowhere on describe. This
    is the silent-drop class, not merely "no encryption layer" — a client configuring a KMS
    key had no way to confirm it took. Fixed: `KmsKeyID` added to `ScheduledQuery`, parsed
    from the request, stored, and echoed on `DescribeScheduledQuery` (`KmsKeyId,omitempty`,
    matching the real optional field). This emulator still performs no actual at-rest
    encryption — that half of the gap is honestly scoped, not fixed (see `gaps`). Persists
    across Snapshot/Restore for free via the existing generic `store.Table[ScheduledQuery]`
    JSON round trip (`store.go`/`store_setup.go`) — covered by
    `TestInMemoryBackend_SnapshotRestore_FullState`.
11. **`LastRunSummary.RunStatus`/`ScheduledQueryListEntry.LastRunStatus` always claimed
    `AUTO_TRIGGER_SUCCESS`** (`scheduled_queries.go`, `buildLastRunSummary` /
    `buildScheduledQueryListEntry`): real `ExecuteScheduledQuery` is documented "You can use
    this API to run a scheduled query manually" (`api_op_ExecuteScheduledQuery.go:16`), and
    this emulator has no scheduler goroutine (verified: no ticker/cron/background trigger
    anywhere in this package) — `ExecuteScheduledQuery` is the *only* code path that ever
    populates `LastRunTime`. Every run in this emulator is therefore manually triggered, yet
    the response always asserted an automatic one: a state claim contradicted by the only
    mechanism that could have produced it (the same class of bug as an op jumping to a
    terminal state it never did the work for). Fixed to `MANUAL_TRIGGER_SUCCESS`. Enum
    completeness checked separately: `types.ScheduledQueryRunStatus` has 4 documented values
    (`enums.go:229-232`); this package's unexported consts cover 3 (missing
    `MANUAL_TRIGGER_FAILURE`), and the `*_FAILURE` values are never assigned by any code
    path — consistent with the documented "no failure simulation" gap, not a further drop,
    since `RunStatus` is backend-generated output only and never parsed back from a request.
    Covered by `TestExecuteScheduledQuery_RunStatusIsManual`.

12. **`ScheduledQueryDescription.TargetConfiguration.TimestreamConfiguration` missing 2 of
    its 4 required members** (`handler_scheduled_queries.go`, `scheduled_queries.go`,
    `models.go` — gopherstack-r80d batch 25): once `TargetConfiguration` is present on a
    real `CreateScheduledQueryInput`, its `TimestreamConfiguration` is required
    (`validateTargetConfiguration`, `validators.go:632-649`), and once
    `TimestreamConfiguration` is present its own `DatabaseName`/`DimensionMappings`/
    `TableName`/`TimeColumn` are ALL required (`validateTimestreamConfiguration`,
    `validators.go:651-666`; `types/types.go`'s `TimestreamConfiguration` struct). The
    request-parsing struct in `handler_scheduled_queries.go` only ever declared
    `DatabaseName`/`TableName` on the nested `TimestreamConfiguration` shape —
    `TimeColumn`/`DimensionMappings` had no field to decode into at all, so a real client's
    fully valid `CreateScheduledQueryInput` (the SDK's own client-side validator requires
    all four once `TargetConfiguration` is set at all) silently lost both on the way in,
    and `DescribeScheduledQuery`'s response always omitted them. Fixed: added
    `TargetTimeColumn string`/`TargetDimensionMappings []DimensionMapping` to the
    `ScheduledQuery` domain model (`DimensionMapping` is a new type mirroring
    `types.DimensionMapping`'s `Name`/`DimensionValueType`, both required once a mapping is
    present), threaded through `createScheduledQueryInput`'s nested request shape, the
    `StorageBackend.CreateScheduledQuery` interface signature and `InMemoryBackend`'s
    implementation (2 new trailing params), `cloneScheduledQuery` (deep-copies the new
    slice, matching the existing `Tags` map treatment), and `scheduledQueryToView`'s
    `TargetConfiguration.TimestreamConfiguration` echo. Persistence needs no separate
    change: `Snapshot`/`Restore` JSON-marshal the whole `ScheduledQuery` struct through
    `store.Table`'s generic encoding, so the new tagged fields ride along automatically.
    Proven via a real `aws-sdk-go-v2/service/timestreamquery` client round trip
    (`wire_output_required_r80d_test.go`), hand-reverted (all 7 touched files reverted to
    `HEAD` together via `git show HEAD:<path>`, confirmed the test fails against the
    original code with "TimeColumn is required once TimestreamConfiguration is present"),
    restored, md5sum-verified byte-identical against the pre-revert working tree. All 13
    existing direct `backend.CreateScheduledQuery(...)` test call sites (across
    `isolation_test.go`/`persistence_test.go`/`scheduled_queries_test.go`) updated for the
    2 new trailing parameters; none needed logic changes since none previously exercised
    `TargetConfiguration` with real `TimeColumn`/`DimensionMappings` values.

    Reviewed and ruled OUT, not a bug: `SelectColumn` (wrapped by `PrepareQueryOutput.
    Columns`) and `ColumnInfo`/`ParameterMapping` (wrapped by `Query.ColumnInfo` and
    `PrepareQueryOutput.Parameters`) were also checked for the same nested-required-member
    shape. `SelectColumn` declares zero required members in the real Smithy model (Aliased/
    DatabaseName/Name/TableName/Type all optional). `ColumnInfo.Type` (required) and
    `ParameterMapping.Name`/`.Type` (both required) are already always populated
    unconditionally by `marshalColumnInfos`/`inferColumnsFromSQL` — `Name` is only
    conditionally omitted when empty, but `inferColumnsFromSQL` always assigns
    `"param%d"` for a real parameter, so the omission path is dead code for `Parameters`
    (a real parsed parameter is never empty-named) and harmless for `Columns`
    (`SelectColumn.Name` isn't required at all). `rekognition`'s
    `MediaAnalysisInput.S3Object`/`MediaAnalysisOutputConfig.S3Bucket` (the sibling
    nested-required-member class checked this same batch) were already correctly wired by
    a prior pass with an explicit doc-comment citing `validateOpStartMediaAnalysisJobInput`
    — re-confirmed still correct, no change needed.

Traps for the next auditor:

- `TagResource`/`UntagResource`/`ListTagsForResource` **are** in
  `GetSupportedOperations()` and have real, working handlers/backend methods in this
  package — but `RouteMatcher` deliberately excludes them (`writeServiceTagOps`) so
  production traffic for these ops is routed to the `TimestreamWrite` handler's single
  unified cross-resource-type tag store instead. This is **not** a routing bug: verified
  `services/timestreamwrite`'s `TagResource` treats `ResourceARN` as an opaque map key
  (no resource-type-specific existence check), so tagging a `scheduled-query` ARN there
  works correctly. The tag code in this package is unreachable via the real HTTP path and
  exists only so direct unit tests (which call `Handler()`/backend methods directly,
  bypassing `RouteMatcher`) can exercise it. Do not "fix" this by deleting the code or by
  changing the RouteMatcher without also auditing timestreamwrite's tag store.
- `Query`/`Query`-family responses are deterministic mock data (SQL-projection-inferred
  columns, always-empty `Rows`) since there is no real Timestream Write data source to
  execute against in this emulator. This is a documented, intentional design decision
  (see `QueryWithOptions`'s comments), not a stub — do not re-flag it without a plan for
  where real row data would come from.
- `DescribeScheduledQuery` returns `types.ScheduledQueryDescription` (full detail: State,
  ScheduleConfiguration, NotificationConfiguration, TargetConfiguration, LastRunSummary,
  ...); `ListScheduledQueries` returns the *different*, slimmer `types.ScheduledQuery`
  shape (Arn/Name/State/CreationTime/LastRunStatus/NextInvocationTime/
  PreviousInvocationTime/TargetDestination, no QueryString/full config). The emulator
  correctly uses two distinct response-builder functions (`scheduledQueryToView` vs
  `buildScheduledQueryListEntry`) for this — don't try to unify them.
