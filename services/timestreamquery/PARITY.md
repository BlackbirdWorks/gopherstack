---
service: timestreamquery
sdk_module: aws-sdk-go-v2/service/timestreamquery@v1.39.4
last_audit_commit: 0be795d3
last_audit_date: 2026-08-20
overall: A            # this pass (2026-08-20): CreateScheduledQueryInput.TargetConfiguration.
                       # TimestreamConfiguration silently dropped DimensionMappings and TimeColumn
                       # (both REQUIRED subfields whenever TargetConfiguration is set, per
                       # api_op_CreateScheduledQuery.go's TimestreamConfiguration struct) plus the
                       # optional MeasureNameColumn/MixedMeasureMappings/MultiMeasureMappings --
                       # accepted nowhere on create, echoed nowhere on describe. Now parsed, stored
                       # (ScheduledQueryTargetDetail), and echoed on DescribeScheduledQuery.
                       # Prior pass (2026-08-10): CreateScheduledQuery.KmsKeyId was accepted nowhere
                       # and returned nowhere (a silent-drop, not just "no encryption layer") -- now
                       # stored and echoed on DescribeScheduledQuery. Fixed LastRunSummary.RunStatus/
                       # ScheduledQueryListEntry.LastRunStatus always claiming AUTO_TRIGGER_SUCCESS
                       # even though this emulator has no scheduler -- every run is produced by an
                       # explicit (manual) ExecuteScheduledQuery call, so the claim contradicted the
                       # only mechanism that ever populates it; now MANUAL_TRIGGER_SUCCESS.
ops:
  Query: {wire: fixed, errors: ok, state: ok, persist: ok, note: "deterministic mock rows/columns inferred from SQL projection (documented; no real Timestream Write data source exists to query against). ClientToken TTL fixed 8h->4h to match documented window. marshalColumnInfos (shared with PrepareQuery) previously hand-picked only Type.ScalarType out of ColumnInfo, silently dropping ArrayColumnInfo/RowColumnInfo/TimeSeriesMeasureValueColumnInfo whenever set; now passes the full ColumnType struct through so the nested union marshals correctly (types.Type)."}
  CancelQuery: {wire: ok, errors: ok, state: fixed, persist: ok, note: "was non-idempotent: 2nd CancelQuery on the same QueryId 404'd (ValidationException) instead of succeeding per documented idempotent-cancel contract. Now marks Cancelled in place instead of deleting."}
  PrepareQuery: {wire: ok, errors: ok, state: fixed, persist: n/a, note: "disguised no-op: ValidateOnly=true (the ONLY mode real Timestream documents as supported) returned an EMPTY Columns/Parameters list, discarding the inferred result for the one mode real clients use. Now returns the same inferred Columns/Parameters regardless of ValidateOnly. Shares the marshalColumnInfos nested-union fix noted under Query. 2026-08-20: verified PrepareQueryOutput.Columns is actually types.SelectColumn (api_op_PrepareQuery.go), a WIDER sibling of types.ColumnInfo carrying three extra optional fields (Aliased/DatabaseName/TableName) this package's marshalColumnInfos reuse omits -- see gaps (Parameters is types.ParameterMapping, wire-identical {Name,Type} to ColumnInfo, so no gap there)."}
  DescribeEndpoints: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateScheduledQuery: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "ClientToken was parsed by the SDK request but never read by the handler/backend, so an SDK-auto-retried create (aws-sdk-go-v2 auto-generates ClientToken via idempotency-token-autofill middleware) hit ConflictException instead of replaying the original success. Now caches ClientToken->Arn for 8h and replays. Also: KmsKeyId (types.go:659) was accepted nowhere on the request and returned nowhere on ScheduledQueryDescription (types.go:659, same field name) -- a silent-drop, not just missing encryption. Now parsed, stored, and echoed back on DescribeScheduledQuery (still no actual at-rest encryption -- see gaps). 2026-08-20: TargetConfiguration.TimestreamConfiguration.DimensionMappings/TimeColumn (both required subfields of types.TimestreamConfiguration whenever TargetConfiguration is set, api_op_CreateScheduledQuery.go) plus MeasureNameColumn/MixedMeasureMappings/MultiMeasureMappings (optional) were accepted nowhere on create and echoed nowhere on describe -- confirmed with a failing real-SDK round-trip test before the fix. Now modeled end-to-end via ScheduledQueryTargetDetail, parsed in parseTargetConfiguration, and echoed in targetConfigurationView."}
  DeleteScheduledQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeScheduledQuery: {wire: fixed, errors: ok, state: ok, persist: ok, note: "LastRunSummary.ExecutionStats used the misspelled wire field ExecutionTimeInMillisecs; real field is ExecutionTimeInMillis (no trailing ecs) per awsAwsjson10_deserializeDocumentExecutionStats."}
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
  - "2026-08-20: PrepareQueryOutput.Columns is types.SelectColumn (api_op_PrepareQuery.go), not types.ColumnInfo -- a wider sibling adding Aliased/DatabaseName/TableName (all optional, no required markers) on top of Name/Type. This package's marshalColumnInfos (shared with Query's ColumnInfo response) only ever emits Name/Type, so PrepareQuery responses can never carry Aliased/DatabaseName/TableName. Wire-safe (all three are optional/omitempty), but this emulator's inferColumnsFromSQL has no real catalog to source DatabaseName/TableName from and no alias-detection logic, so filling them would require fabricating values rather than reflecting real inference -- left unmodeled rather than faked. types.ParameterMapping (PrepareQueryOutput.Parameters) is wire-identical to ColumnInfo ({Name,Type}), so no equivalent gap there."
  - "2026-08-20: ErrorReportConfiguration.S3Configuration.EncryptionOption (types.S3EncryptionOption: SSE_S3|SSE_KMS) and ObjectKeyPrefix are both optional members of the real S3Configuration (api_op_CreateScheduledQuery.go) that this package's createScheduledQueryInput parser never reads -- only BucketName is parsed/stored/echoed. Wire-safe to omit (both optional), but a client setting either gets no confirmation it took. Not fixed this pass (scope: TargetConfiguration's required-subfield drop was the higher-severity finding); flag for a follow-up pass alongside KmsKeyId's existing 'accepted, not enforced' pattern."
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

## timestreamquery (this session, 2026-08-20)

Full wrapper-key/nested-shape wire-parity sweep against
`aws-sdk-go-v2/service/timestreamquery@v1.39.4` (go.mod-pinned, unchanged this pass).
Protocol confirmed JSON-RPC 1.0 (`awsAwsjson10_*` prefix, `X-Amz-Target:
Timestream_20181101.<Op>`) directly from `api_client.go`/`serializers.go`, not from
`_PROTOCOLS.md`. All 15 ops enumerated from both `GetSupportedOperations()` and
`ls api_op_*.go` in the pinned SDK module cache; the two lists matched exactly (no
missing/extra ops).

`awsAwsjson10_deserializeOpDocument<Op>Output` helper-liveness check (`grep -c`, both
defined-and-called, then the function body read, not just the call site):
Query/CancelQuery/PrepareQuery/DescribeEndpoints/CreateScheduledQuery/
DescribeScheduledQuery/ListScheduledQueries/DescribeAccountSettings/UpdateAccountSettings/
TagResource/UntagResource/ListTagsForResource all count 2 (defined + called) and every
body does a real `map[string]interface{}` switch-decode of its own JSON keys straight
onto the output struct — no polly-`SynthesizeSpeech`-style disguised body. Void-output
ops (`DeleteScheduledQuery`, `ExecuteScheduledQuery`, `UpdateScheduledQuery`) count 0,
correctly: their `*Output` structs carry only `ResultMetadata`, so no document
deserializer is generated at all — not a gap. No REST-bound ops here (pure JSON-RPC), so
the httpPayload/cnhp REST trap does not apply to this service; confirmed rather than
assumed.

**Full-field-list diff, optional included, types checked** for every op's Input/Output
against `api_op_*.go` in the pinned SDK: no FABRICATED members found this pass (the one
prior fabrication, `AccountSettings.LastUpdatedTime`, was already deleted in the
2026-07-24 pass and stayed deleted). One real gap found and fixed (see below); two real
gaps found and disclosed, not fixed (SelectColumn's extra fields, S3Configuration's
EncryptionOption/ObjectKeyPrefix).

**`Datum`/`Row`/`TimeSeriesDataPoint`/`ColumnInfo`/`Type` recursion** — read
`awsAwsjson10_deserializeDocument{Datum,Row,TimeSeriesDataPoint,ColumnInfo,Type}` in
`deserializers.go` directly (not inferred from `types/types.go` alone) and diffed every
JSON key against `models.go`'s `Datum`/`Row`/`TimeSeriesDataPoint`/`ColumnType`/
`ColumnInfo`. Both directions verified:
- `Datum` → `ArrayValue []Datum` (self-recursive), `RowValue *Row` → `Row.Data []Datum`
  (back to `Datum`), `TimeSeriesValue []TimeSeriesDataPoint` → `{Time, Value *Datum}`
  (back to `Datum`), `ScalarValue *string`, `NullValue *bool`. All five keys and their
  JSON names match gopherstack's `Datum` exactly.
- `Type` (the `ColumnInfo.Type` union) → `ArrayColumnInfo *ColumnInfo` (recursive),
  `RowColumnInfo []ColumnInfo` (recursive, confirmed a **list** not a single struct —
  the brief's hint matched the SDK here), `TimeSeriesMeasureValueColumnInfo *ColumnInfo`
  (recursive), `ScalarType ScalarType`. Matches `models.go`'s `ColumnType` exactly,
  including `RowColumnInfo []ColumnInfo`.
- `marshalColumnInfos` (shared by `Query` and `PrepareQuery`) passes `ColumnType`
  straight through rather than hand-picking fields, so the recursion survives
  marshaling — verified this is still true (no regression since the 2026-07-24 fix that
  established it).

**Enums, both directions**, every enum named in the brief:
- `DimensionValueType`: 1 value (`VARCHAR`, enums.go:24-28) — gopherstack passes it
  through as an opaque string on the new `DimensionMapping.DimensionValueType` field
  (this pass); no gopherstack-side constant set to diverge.
- `MeasureValueType` (5: BIGINT/BOOLEAN/DOUBLE/VARCHAR/MULTI, enums.go:62-70) and
  `ScalarMeasureValueType` (5: BIGINT/BOOLEAN/DOUBLE/VARCHAR/TIMESTAMP, enums.go:144-152)
  — both passed through as opaque strings on the new `MixedMeasureMapping`/
  `MultiMeasureAttributeMapping` fields; same treatment as `ScalarType` already gets.
- `ScalarType`: 10 values (enums.go), passed through as opaque string via
  `ColumnType.ScalarType` — unchanged, still correct.
- `ScheduledQueryRunStatus`: 4 values (AUTO_TRIGGER_SUCCESS/AUTO_TRIGGER_FAILURE/
  MANUAL_TRIGGER_SUCCESS/MANUAL_TRIGGER_FAILURE, enums.go:229-232). gopherstack emits
  only MANUAL_TRIGGER_SUCCESS (the only reachable state, per the 2026-08-10 fix); the
  missing MANUAL_TRIGGER_FAILURE remains a disclosed, unreachable-without-a-failure-path
  gap (unchanged from last pass, re-verified not regressed).
- `ScheduledQueryState`: 2 values (ENABLED/DISABLED, enums.go) — both used
  (`scheduledQueryStateEnabled`/`scheduledQueryStateDisabled`), no drift.
- `S3EncryptionOption`: 2 values (SSE_S3/SSE_KMS) — NOT modeled at all (the
  `EncryptionOption` field itself is unparsed); see new gap this pass.
- `QueryInsightsMode`/`ScheduledQueryInsightsMode`: both 2 values
  (ENABLED_WITH_RATE_CONTROL/DISABLED) — `Query`'s `QueryInsights.Mode` compares against
  the literal `"DISABLED"` string (`query_execution.go:105`); no gopherstack-side enum
  constant to diverge from the SDK's two values.
- `LastUpdateStatus`: 3 values (PENDING/FAILED/SUCCEEDED) — `LastUpdate.Status` is
  always `SUCCEEDED` (synchronous apply, documented in `models.go`'s `LastUpdate`
  godoc); PENDING/FAILED unreachable without an async apply path, same class as the
  ScheduledQueryRunStatus gap, not separately re-disclosed.
- `ComputeMode`: 2 values (ON_DEMAND/PROVISIONED) — both used, validated as required on
  `UpdateAccountSettings`, no drift.
- `QueryPricingModel`: 2 values (BYTES_SCANNED/COMPUTE_UNITS) — both used, no drift.

**`ScheduledQuery` vs `ScheduledQueryDescription`** — re-verified distinct and each
still matching its own deserializer/builder: `ListScheduledQueries` decodes
`awsAwsjson10_deserializeDocumentScheduledQuery` (the slim 9-field summary type, built by
`buildScheduledQueryListEntry`); `DescribeScheduledQuery` decodes
`ScheduledQueryDescription` (built by `scheduledQueryToView`). No cross-contamination.

### Bug found and fixed

**`CreateScheduledQueryInput.TargetConfiguration.TimestreamConfiguration` silently
dropped `DimensionMappings`/`TimeColumn` (both REQUIRED subfields of
`types.TimestreamConfiguration` whenever `TargetConfiguration` is set —
`api_op_CreateScheduledQuery.go`) plus the optional `MeasureNameColumn`/
`MixedMeasureMappings`/`MultiMeasureMappings`** (`handler_scheduled_queries.go`,
`scheduled_queries.go`, `interfaces.go`, `models.go`): the emulator's
`createScheduledQueryInput` request parser only ever read `DatabaseName`/`TableName`
out of `TimestreamConfiguration`; every other member — including the two the real SDK
marks required whenever a client sets `TargetConfiguration` at all — was accepted
nowhere on create and echoed nowhere on `DescribeScheduledQuery`. Since
`TargetConfiguration` is how a scheduled query writes its results back to a Timestream
table (the primary real-world use of this op), any client actually wiring up write-back
would hit this drop. Bug class (a): members missing from a narrower type, matching this
campaign's dominant pattern.

Proved with a real-SDK round-trip test
(`wire_scheduledquery_targetconfig_test.go`,
`Test_SDKRoundTrip_ScheduledQuery_TargetConfigurationDetail`) written *before* the fix:
`CreateScheduledQuery` with a full `TimestreamConfiguration` (including
`DimensionMappings`/`TimeColumn`/`MeasureNameColumn`/`MixedMeasureMappings`) followed by
`DescribeScheduledQuery`, asserting every field round-trips through the real
`aws-sdk-go-v2` client. Against the unfixed code the test failed exactly as predicted
(`TimeColumn`/`MeasureNameColumn` empty, `DimensionMappings` len 0).

Fixed: new `ScheduledQueryTargetDetail`/`DimensionMapping`/`MixedMeasureMapping`/
`MultiMeasureAttributeMapping`/`MultiMeasureMappings` types (`models.go`); the request
parser now decodes the full nested `TimestreamConfiguration` shape via
`parseTargetConfiguration` (`handler_scheduled_queries.go`); `CreateScheduledQuery`
(`interfaces.go`, `scheduled_queries.go`) takes a new `targetDetail
*ScheduledQueryTargetDetail` parameter and stores it on `ScheduledQuery.TargetDetail`;
`scheduledQueryToView`'s new `targetConfigurationView` helper echoes it back on
`DescribeScheduledQuery`. Persists across Snapshot/Restore for free via the existing
`store.Table[ScheduledQuery]` JSON round trip (`TargetDetail` carries its own json tags)
— extended `TestInMemoryBackend_SnapshotRestore_FullState` with a `TargetDetail`
assertion to prove it, alongside the existing `KmsKeyId` round-trip assertion.

**Hand-revert proof** (`cp` method, no git): backed up
`models.go`/`interfaces.go`/`scheduled_queries.go`/`handler_scheduled_queries.go`/
`isolation_test.go`/`persistence_test.go`/`scheduled_queries_test.go` to scratch before
editing; after the fix, `cp`'d the pre-fix originals back over the working tree,
rebuilt, and re-ran `Test_SDKRoundTrip_ScheduledQuery_TargetConfigurationDetail` — it
failed with the identical `TimeColumn`/`MeasureNameColumn` empty-string /
`DimensionMappings` len-0 symptom. Restored the fixed versions via `cp` from scratch and
verified with `md5sum` that the restored files are byte-identical to the fixed state
before re-running the full gate suite.

Blast radius: `CreateScheduledQuery`'s backend signature gained one trailing parameter
(`targetDetail *ScheduledQueryTargetDetail`), so all 13 existing positional call sites in
`isolation_test.go`/`persistence_test.go`/`scheduled_queries_test.go` needed a trailing
argument added (`nil` for the 11 sites not exercising target config; the actual struct
for the two now covering it). No test assertions changed meaning — only the call
signature grew.

Decomposition note: `handleCreateScheduledQuery` and `scheduledQueryToView` both
exceeded gocognit's threshold once the new parsing/echo logic was inlined; extracted
`parseTargetConfiguration`/`convertMultiMeasureMappings`/
`convertMultiMeasureAttributeMappings` and `targetConfigurationView` as named helpers
(no `//nolint` used) — `golangci-lint run` is clean (0 issues) after the split.

### Gaps disclosed, not fixed

- `PrepareQueryOutput.Columns` is `types.SelectColumn`, not `types.ColumnInfo` — see
  `gaps` above. Not fixed: the extra fields (`Aliased`/`DatabaseName`/`TableName`) have
  no real backing data in this emulator's SQL-projection-based column inference, and
  fabricating them would trade a wire-shape gap for a functional-behavior lie.
- `ErrorReportConfiguration.S3Configuration.EncryptionOption`/`ObjectKeyPrefix` — see
  `gaps` above. Not fixed this pass: scoped out in favor of the higher-severity
  `TargetConfiguration` required-subfield drop; both are optional and independently
  fixable in a follow-up pass using the same pattern as `KmsKeyId`.

### Provenance

`last_audit_commit: a98a164d` (stamped `last_audit_date: 2026-08-10`) resolves to commit
`a98a164d4d98f7a6bcb7409b85a0ba2edecaca0f`, dated **2026-07-13** (`git show -s
--format=%ad`) — an mwaa parity commit, and **not an ancestor of HEAD**
(`git merge-base --is-ancestor` fails). A 28-day gap between the recorded commit's date
and the stamped audit date, with the commit predating the date, is the documented tell
for a stale/inaccurate provenance stamp (not a judgment based on the commit touching an
unrelated directory, which this campaign has separately established is not itself
evidence of anything). Reporting both dates as instructed; the actual audit content in
this file (five dated narrative passes: 2026-07-13 baseline through 2026-08-10, now
2026-08-20) is independently substantive and consistent with real work having happened,
so the stamp itself — not the underlying audits — is what's suspect. Refreshed this pass
to `last_audit_commit: 0be795d3` / `last_audit_date: 2026-08-20` (current HEAD at time of
this audit).

### Gates (verbatim)

```
$ go build ./services/timestreamquery/...
(clean)
$ go vet ./services/timestreamquery/...
(clean)
$ go fix -diff ./services/timestreamquery/...
(empty)
$ gofmt -l services/timestreamquery/
(empty)
$ go test -race -count=1 ./services/timestreamquery/...
ok  	github.com/blackbirdworks/gopherstack/services/timestreamquery	1.142s
$ golangci-lint run ./services/timestreamquery/...
0 issues.
$ awk 'length > 120 {print}' services/timestreamquery/*.go
(empty)
```

`git status --short` at session end shows only files under `services/timestreamquery/`
touched by this pass, plus unrelated concurrent changes under `services/scheduler/` and
`.claude/` from other activity in this shared checkout that this session did not make
and did not touch.
