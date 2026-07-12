---
service: athena
sdk_module: aws-sdk-go-v2/service/athena@v1.57.2
last_audit_commit: c4a90472
last_audit_date: 2026-07-12
overall: A            # genuine wire-shape fixes found in a previously well-built, well-tested service
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  BatchGetPreparedStatement: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — request field was StatementNames, real wire is PreparedStatementNames; response field was UnprocessedStatementNames, real wire is UnprocessedPreparedStatementNames. Op was silently non-functional for real SDK clients (request always parsed as an empty name list)."}
  GetSessionEndpoint: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED — response was {SessionEndpoint: url}; real shape is {EndpointUrl, AuthToken, AuthTokenExpirationTime} (all three required). Client previously got a fully empty result."}
  CreatePresignedNotebookUrl: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED — response was {NotebookSessionUrl: url}; real shape is {NotebookUrl, AuthToken, AuthTokenExpirationTime} (all three required). Same class of bug as GetSessionEndpoint; both now share backend.newSessionAuthToken()."}
  GetResourceDashboard: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED — was a disguised no-op ignoring the required ResourceARN input and returning {ResourceDashboard: {}}; real shape is {Url: string}. Now validates ResourceARN is non-empty (InvalidRequestException otherwise) and returns a synthesized dashboard URL."}
  StartQueryExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  StopQueryExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  GetQueryExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "Query lifecycle is synchronous (QUEUED/RUNNING never observed) — StartQueryExecution runs the statement inline and stores a terminal SUCCEEDED/FAILED state before returning, so SDK poll loops never hang."}
  GetQueryResults: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResultSet/Row/Datum/ColumnInfo shapes verified against awsAwsjson11 deserializers; header row only on first page, matching AWS."}
  ListQueryExecutions: {wire: ok, errors: ok, state: ok, persist: ok, note: "opaque-token pagination via pkgs' page-token codec"}
  BatchGetQueryExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  WorkGroup (Create/Get/List/Update/Delete): {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — ResultConfiguration.ACLConfiguration was tagged json:\"ACLConfiguration\"; real wire key is \"AclConfiguration\" (exact-case switch in the generated deserializer). Affects GetWorkGroup/GetQueryExecution/StartQueryExecution responses and requests carrying ResultConfiguration."}
  NamedQuery (Create/Get/List/BatchGet/Delete/Update): {wire: ok, errors: ok, state: ok, persist: ok}
  DataCatalog (Create/Get/List/Update/Delete): {wire: ok, errors: ok, state: ok, persist: ok, note: "gap — CreateDataCatalogOutput/DeleteDataCatalogOutput in SDK v1.57.2 carry an optional DataCatalog object gopherstack does not populate; harmless (field is optional, client just gets nil) so left as a follow-up rather than an in-scope fix."}
  PreparedStatement (Create/Get/List/BatchGet/Delete/Update): {wire: ok, errors: ok, state: ok, persist: ok}
  CapacityReservation (Create/Get/List/Update/Cancel/Delete): {wire: ok, errors: ok, state: ok, persist: ok}
  CapacityAssignmentConfiguration (Put/Get): {wire: ok, errors: ok, state: ok, persist: ok}
  Notebook (Create/Delete/Export/Import/Update/UpdateMetadata/GetMetadata/ListMetadata): {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePresignedNotebookUrl: {wire: ok, errors: ok, state: ok, persist: n/a}
  Session (Start/Get/GetStatus/Terminate/List/ListNotebookSessions): {wire: ok, errors: ok, state: ok, persist: ok}
  Calculation (Start/Get/GetStatus/GetCode/Stop/List): {wire: ok, errors: ok, state: ok, persist: ok}
  Database/TableMetadata (Get/List): {wire: ok, errors: ok, state: ok, persist: ok, note: "'dirty' tables round-trip through the DTO registry in persistence.go; verified by store_setup_test.go"}
  Tags (Tag/Untag/ListTagsForResource): {wire: ok, errors: partial, state: ok, persist: ok, note: "gap — TagResource/UntagResource/ListTagsForResource never validate the ResourceARN actually corresponds to an existing resource (real AWS throws InvalidRequestException for an unknown ARN); ListTagsForResource also ignores MaxResults/NextToken pagination inputs entirely (always returns everything, no NextToken emitted). Deferred: broad behavior change across every resource kind, not a wire-shape break — no test currently depends on either behavior."}
  ListEngineVersions: {wire: partial, errors: ok, state: ok, persist: n/a, note: "gap — EngineVersionDescriptor includes a fabricated AuthEngineVersion field that does not exist on the real EngineVersion type (only SelectedEngineVersion/EffectiveEngineVersion). Harmless (extra field is ignored by the client) so left as a follow-up."}
  ListApplicationDPUSizes: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListExecutors: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetQueryRuntimeStatistics: {wire: ok, errors: ok, state: ok, persist: n/a}
families:
  pagination: {status: ok, note: "WorkGroups/NamedQueries/DataCatalogs/PreparedStatements all sort + NextToken/MaxResults correctly; an unrecognized/stale NextToken silently restarts from offset 0 instead of erroring (real AWS would likely reject it) — pre-existing, consistent pattern across all four, not touched this pass."}
  janitor/leaks: {status: clean, note: "worker.Group-based ticker with ctx cancellation; sweeps queryExecutions+queryResults, sessions, calculations under RLock-collect/Lock-delete with re-verification to avoid racing a concurrent revival. No goroutine leak risk found."}
gaps:
  - TagResource/UntagResource/ListTagsForResource do not validate resource existence, and ListTagsForResource ignores MaxResults/NextToken (bd: unfiled — flag for future sweep)
  - CreateDataCatalog/DeleteDataCatalog do not return the optional DataCatalog object the real API (SDK v1.57.2) now includes (bd: unfiled)
  - ListEngineVersions.EngineVersionDescriptor carries a fabricated AuthEngineVersion field not present on the real type (bd: unfiled)
  - Pagination NextToken across WorkGroups/NamedQueries/DataCatalogs/PreparedStatements silently resets to offset 0 on an unrecognized token instead of erroring (bd: unfiled)
deferred:
  - none — full routed-op surface audited this pass (base + extended dispatch tables, 70 ops total)
leaks: {status: clean, note: "janitor uses pkgs/worker.Group with proper ctx.Done() teardown; no raw goroutines spawned elsewhere in the service"}
---

## Notes

**Protocol**: awsjson1.1 (`application/x-amz-json-1.1`, single POST endpoint,
`X-Amz-Target: AmazonAthena.<Op>` dispatch). Route matcher
(`strings.HasPrefix(target, "AmazonAthena")`) is correct and was verified against
the real SDK's target prefix.

**Bugs fixed this pass** (all genuine wire-shape breaks verified against
`aws-sdk-go-v2/service/athena@v1.57.2`'s generated `serializers.go`/`deserializers.go`,
not against gopherstack's own output):

1. **`ResultConfiguration.ACLConfiguration` JSON tag was `"ACLConfiguration"`**;
   the real deserializer switches on the exact-case key `"AclConfiguration"`.
   Any GetWorkGroup/GetQueryExecution/StartQueryExecution response carrying this
   field was silently dropped by a real SDK client. `backend.go`.

2. **`BatchGetPreparedStatement` request field was `"StatementNames"`**; real
   wire key is `"PreparedStatementNames"` — a real client's request always
   parsed to an empty name list, so the op was non-functional. The response's
   `"UnprocessedStatementNames"` was likewise wrong (`"UnprocessedPreparedStatementNames"`
   is correct). `handler.go`.

3. **`GetSessionEndpoint` returned `{"SessionEndpoint": url}`**; the real
   `GetSessionEndpointOutput` has three *required* fields:
   `EndpointUrl`, `AuthToken`, `AuthTokenExpirationTime` (epoch seconds). A real
   client got a completely empty result. `handler_extra.go` + `backend_extra.go`.

4. **`CreatePresignedNotebookUrl` returned `{"NotebookSessionUrl": url}`**; the
   real `CreatePresignedNotebookUrlOutput` has three *required* fields:
   `NotebookUrl`, `AuthToken`, `AuthTokenExpirationTime`. Same bug class as #3;
   both now share `backend.newSessionAuthToken()` (a stateless synthesized
   bearer-token helper — neither op models real authentication server-side).
   `handler.go` + `backend.go`.

5. **`GetResourceDashboard` ignored its `ResourceARN` input entirely** and
   returned a fabricated `{"ResourceDashboard": {}}` envelope; the real op's
   input requires `ResourceARN` and its output is a single required `Url`
   string field. This was a disguised no-op per the no-stub rule — fixed to
   validate `ResourceARN` and return a synthesized dashboard URL.
   `handler_extra.go` + `backend_extra.go` (new `GetResourceDashboard` method,
   added to `StorageBackend`).

**Looks-wrong-but-correct traps** (do not re-flag):

- `StartQueryExecution` runs the statement *synchronously* and stores a
  terminal `SUCCEEDED`/`FAILED` state before the op returns — there is no
  `QUEUED`/`RUNNING` window. This is intentional: it guarantees an SDK poll
  loop against `GetQueryExecution` never hangs, and matches how a fast local
  emulator should behave.
- `handleGetQueryResults`'s `UpdateCount` is always `0` at the top level of the
  response (not nested under `ResultSet`) — this matches
  `GetQueryResultsOutput`'s real shape (`UpdateCount` is a sibling of
  `ResultSet`, not a child).
- `TagResource`/`UntagResource`/`CreateDataCatalog`-style ops returning
  `struct{}{}` (empty JSON object) is correct for AWS Athena's void outputs —
  confirmed against each op's deserializer (no case statements at all = an
  empty response body is fully valid).
- Prepared-statement and calculation/session lookups use
  `ResourceNotFoundException` (`ErrResourceNotFound`); workgroup/named-query/
  data-catalog/query-execution/capacity-reservation/notebook lookups use
  `InvalidRequestException` (`ErrNotFound`). This split is intentional and
  matches AWS's documented per-resource exception types, not an inconsistency.

**Persistence**: `persistence.go` (added recently, verified intact) round-trips
all "clean" `store.Table`-backed resources through `b.registry.SnapshotAll`/
`RestoreAll`, and the two "dirty" tables (`databases`, `tables`, which need a
`Catalog`/`Database` identity their value types deliberately exclude from JSON)
through an ephemeral DTO registry. `queryResults`, `tableData`, and
`resourceTags` are persisted explicitly as documented on `backendSnapshot`. No
new state was added by this pass that isn't already covered — the three fixed
ops (`GetSessionEndpoint`, `CreatePresignedNotebookUrl`, `GetResourceDashboard`)
are all pure/derived (no new backend fields), so nothing new needed wiring into
`Snapshot`/`Restore`.
