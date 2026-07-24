---
service: timestreamquery
sdk_module: aws-sdk-go-v2/service/timestreamquery@v1.36.16
last_audit_commit: a98a164d
last_audit_date: 2026-07-24
overall: A            # this pass: deleted an invented LastUpdatedTime field, added the documented
                       # TCU (multiple-of-4, 4-1000) validation, modeled QueryCompute.ProvisionedCapacity
                       # .NotificationConfiguration end-to-end (parse/apply/respond/persist), and fixed
                       # marshalColumnInfos dropping the ColumnInfo.Type nested union (Array/Row/
                       # TimeSeriesMeasureValueColumnInfo) down to ScalarType only.
ops:
  Query: {wire: fixed, errors: ok, state: ok, persist: ok, note: "deterministic mock rows/columns inferred from SQL projection (documented; no real Timestream Write data source exists to query against). ClientToken TTL fixed 8h->4h to match documented window. marshalColumnInfos (shared with PrepareQuery) previously hand-picked only Type.ScalarType out of ColumnInfo, silently dropping ArrayColumnInfo/RowColumnInfo/TimeSeriesMeasureValueColumnInfo whenever set; now passes the full ColumnType struct through so the nested union marshals correctly (types.Type)."}
  CancelQuery: {wire: ok, errors: ok, state: fixed, persist: ok, note: "was non-idempotent: 2nd CancelQuery on the same QueryId 404'd (ValidationException) instead of succeeding per documented idempotent-cancel contract. Now marks Cancelled in place instead of deleting."}
  PrepareQuery: {wire: fixed, errors: ok, state: fixed, persist: n/a, note: "disguised no-op: ValidateOnly=true (the ONLY mode real Timestream documents as supported) returned an EMPTY Columns/Parameters list, discarding the inferred result for the one mode real clients use. Now returns the same inferred Columns/Parameters regardless of ValidateOnly. Shares the marshalColumnInfos nested-union fix noted under Query."}
  DescribeEndpoints: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateScheduledQuery: {wire: ok, errors: ok, state: fixed, persist: ok, note: "ClientToken was parsed by the SDK request but never read by the handler/backend, so an SDK-auto-retried create (aws-sdk-go-v2 auto-generates ClientToken via idempotency-token-autofill middleware) hit ConflictException instead of replaying the original success. Now caches ClientToken->Arn for 8h and replays."}
  DeleteScheduledQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeScheduledQuery: {wire: fixed, errors: ok, state: ok, persist: ok, note: "LastRunSummary.ExecutionStats used the misspelled wire field ExecutionTimeInMillisecs; real field is ExecutionTimeInMillis (no trailing ecs) per awsAwsjson10_deserializeDocumentExecutionStats."}
  ExecuteScheduledQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  ListScheduledQueries: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateScheduledQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAccountSettings: {wire: fixed, errors: ok, state: ok, persist: ok, note: "DELETED an invented LastUpdatedTime field: neither DescribeAccountSettingsOutput nor UpdateAccountSettingsOutput (real SDK) defines one. Now also surfaces QueryCompute.ProvisionedCapacity.NotificationConfiguration when set (see UpdateAccountSettings)."}
  UpdateAccountSettings: {wire: fixed, errors: fixed, state: fixed, persist: fixed, note: "QueryCompute was accepted on the wire but never parsed/applied -- an account could never actually transition to PROVISIONED even though DescribeAccountSettings always echoed a QueryCompute field. Also fixed ProvisionedCapacity's response field name (ActiveQueryTCU, not the request-side TargetQueryTCU) and added QueryCompute to the account-settings snapshot (previously dropped across Snapshot/Restore). This pass: (1) DELETED the invented LastUpdatedTime response field (no equivalent in the real API shape); (2) added the documented TCU bounds (min 4, max 1000, multiple of 4) to both MaxQueryTCU and QueryCompute.ProvisionedCapacity.TargetQueryTCU, replacing the too-loose \"> 0\" check; (3) modeled QueryCompute.ProvisionedCapacity.NotificationConfiguration (types.AccountSettingsNotificationConfiguration: RoleArn + nested SnsConfiguration.TopicArn) end-to-end -- parsed from the request, validated (RoleArn required when present, SnsConfiguration.TopicArn required when SnsConfiguration present, mirroring the real SDK's client-side validateAccountSettingsNotificationConfiguration), applied only when ComputeMode is PROVISIONED, returned on the response, and persisted across Snapshot/Restore."}
families:
  tags: {status: deferred, note: "TagResource/UntagResource/ListTagsForResource are in GetSupportedOperations() and have working handlers/backend methods (own ARN-keyed tag map), but RouteMatcher intentionally excludes them (writeServiceTagOps) so production traffic is routed to the TimestreamWrite handler's unified cross-resource tag store instead. Verified TimestreamWrite's TagResource treats ResourceARN as an opaque key (no resource-type-specific lookup), so scheduled-query ARNs tag correctly there. This package's own tag handlers are dead code in production, reachable only via direct unit tests / Handler() bypassing RouteMatcher -- confirmed intentional, not a routing bug."}
  route_matching: {status: ok, note: "X-Amz-Target prefix Timestream_20181101., Content-Type application/x-amz-json-1.0 (awsjson1.0) verified against serializers.go (awsAwsjson10_*). DescribeEndpoints wired for SDK endpoint-discovery (fetchOpQueryDiscoverEndpoint calls DescribeEndpoints first)."}
gaps:
  - "CreateScheduledQueryInput.KmsKeyId (customer-managed KMS key for at-rest encryption of the scheduled-query resource) is not modeled at all -- accepted nowhere, returned nowhere. Scoped out: this emulator has no at-rest encryption layer to attach it to, and no observable behavior depends on it. (bd: file follow-up if a KMS-integration test ever needs it)"
  - "ScheduledQueryDescription.RecentlyFailedRuns (up to 5 most recent failed runs) and ScheduledQueryRunSummary.QueryInsightsResponse are not modeled -- this emulator's ExecuteScheduledQuery always succeeds (see ExecuteScheduledQuery), so there is no failure path to populate RecentlyFailedRuns from, and no scheduled-query-run-level QueryInsights simulation exists. Both are optional response fields; omitting them is wire-safe (omitempty). (bd: file follow-up if failure simulation is ever added)"
deferred:
  - "Query/CancelQuery against genuinely long-running or multi-page real query execution semantics -- this emulator's Query is synchronous and instantaneous (matches the mock-data-source design already documented in QueryWithOptions), so QueryExecutionException (a real error type for query engine failures) is never returned. Acceptable per the documented deterministic-mock design; revisit only if a real backing data source is added."
leaks: {status: clean, note: "clientTokens/scheduledQueryTokens/pageStore are self-contained caches with their own mutex, reset on Reset()/version-mismatch Restore(); queries table is bounded by maxRetainedQueries (10000) with arbitrary eviction, cancelled or not. No goroutines/tickers in this package -- nothing to ctx-parent or drain on Shutdown."}
---

## Notes

Protocol: awsjson1.0 (single POST endpoint, `X-Amz-Target: Timestream_20181101.<Op>`,
`Content-Type: application/x-amz-json-1.0`). Verified directly against
`aws-sdk-go-v2/service/timestreamquery@v1.36.16`'s serializers.go/deserializers.go/types
package (vendored under `~/go/pkg/mod`), not against this package's own output.

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
