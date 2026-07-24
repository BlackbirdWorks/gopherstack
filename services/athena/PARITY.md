---
service: athena
sdk_module: aws-sdk-go-v2/service/athena@v1.57.2
last_audit_commit: c47d785b7
last_audit_date: 2026-07-23
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
  WorkGroup (Create/Get/List/Update/Delete): {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-07-23) — WorkGroup carried an invented Tags field (real GetWorkGroupOutput.WorkGroup has none; tags are TagResource/ListTagsForResource-only) that also went stale the moment TagResource/UntagResource were called, since those never touched it. Field removed; CreateWorkGroup's Tags input now flows only into resourceTags. Also FIXED (previous pass) — ResultConfiguration.ACLConfiguration was tagged json:\"ACLConfiguration\"; real wire key is \"AclConfiguration\"."}
  NamedQuery (Create/Get/List/BatchGet/Delete/Update): {wire: ok, errors: ok, state: ok, persist: ok}
  DataCatalog (Create/Get/List/Update/Delete): {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-07-23) — CreateDataCatalogOutput/DeleteDataCatalogOutput now populate the optional DataCatalog object (SDK v1.57.2) with the created/just-deleted record. Also FIXED — DataCatalog carried the same invented Tags field as WorkGroup (see above); removed, CreateDataCatalog's Tags input now flows only into resourceTags."}
  PreparedStatement (Create/Get/List/BatchGet/Delete/Update): {wire: ok, errors: ok, state: ok, persist: ok}
  CapacityReservation (Create/Get/List/Update/Cancel/Delete): {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-07-23) — CapacityReservation carried the same invented Tags field as WorkGroup/DataCatalog, but worse: CreateCapacityReservation had never built an ARN or written to resourceTags at all, so a capacity reservation's tags were previously unreachable via TagResource/ListTagsForResource entirely (no arn.Build call existed for this resource kind). Added InMemoryBackend.capacityReservationARN and wired Create/Delete to mirror/cascade-clean resourceTags like WorkGroup/DataCatalog already did."}
  CapacityAssignmentConfiguration (Put/Get): {wire: ok, errors: ok, state: ok, persist: ok}
  Notebook (Create/Delete/Export/Import/Update/UpdateMetadata/GetMetadata/ListMetadata): {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-07-23) — CreateNotebookInput carried an invented Tags field; the real CreateNotebookInput has only Name/WorkGroup/ClientRequestToken (unlike WorkGroup/DataCatalog/CapacityReservation, notebooks cannot be tagged at creation in the real API). Removed; a client sending Tags anyway (as no real SDK client would) is now harmlessly ignored rather than silently accepted. A notebook remains taggable after creation via TagResource against its ARN."}
  CreatePresignedNotebookUrl: {wire: ok, errors: ok, state: ok, persist: n/a}
  Session (Start/Get/GetStatus/Terminate/List/ListNotebookSessions): {wire: ok, errors: ok, state: ok, persist: ok}
  Calculation (Start/Get/GetStatus/GetCode/Stop/List): {wire: ok, errors: ok, state: ok, persist: ok}
  Database/TableMetadata (Get/List): {wire: ok, errors: ok, state: ok, persist: ok, note: "'dirty' tables round-trip through the DTO registry in persistence.go; verified by persistence_test.go (the store_setup_test.go filename this note previously cited does not exist in the tree — stale reference, the coverage itself is real and passing)"}
  Tags (Tag/Untag/ListTagsForResource): {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-07-23) — TagResource/UntagResource/ListTagsForResource now validate ResourceARN resolves to a currently existing taggable resource (workgroup/datacatalog/capacity-reservation/notebook, parsed from the ARN's kind/id resource segment), returning InvalidRequestException (ErrNotFound) otherwise instead of silently no-oping or returning an empty tag list. ListTagsForResource now also honors MaxResults/NextToken pagination (previously ignored both, always returning every tag in one response)."}
  ListEngineVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED (2026-07-23) — EngineVersionDescriptor carried a fabricated AuthEngineVersion field that does not exist on the real EngineVersion type (only SelectedEngineVersion/EffectiveEngineVersion); removed."}
  ListApplicationDPUSizes: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListExecutors: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetQueryRuntimeStatistics: {wire: ok, errors: ok, state: ok, persist: n/a}
families:
  pagination: {status: ok, note: "FIXED (2026-07-23) — WorkGroups/NamedQueries/DataCatalogs/PreparedStatements/(new) ListTagsForResource all sort + NextToken/MaxResults correctly, and an unrecognized/stale NextToken (e.g. its boundary item was deleted between calls) now resumes at the first surviving item at-or-after the boundary (pagination.paginationStart, mutation-stable via sort.Search) instead of silently restarting the page from offset 0 and re-emitting already-consumed results. Locked in by TestListWorkGroups_Pagination_StaleTokenResumesStably."}
  janitor/leaks: {status: clean, note: "worker.Group-based ticker with ctx cancellation; sweeps queryExecutions+queryResults, sessions, calculations under RLock-collect/Lock-delete with re-verification to avoid racing a concurrent revival. No goroutine leak risk found."}
gaps:
  - DeleteDataCatalogInput.DeleteCatalogOnly (real SDK v1.57.2 field, FEDERATED-catalog-only) is not modeled as a request input; gopherstack does not simulate the underlying CFN Stack/Lambda/Glue Connection resources a FEDERATED catalog's deletion would otherwise need to selectively preserve, so the flag would have no observable effect either way in this emulator. Not a wire-shape break (an extra unrecognized request field is harmlessly ignored). (bd: unfiled)
deferred:
  - none — full routed-op surface re-audited this pass (base + extended dispatch tables, 70 ops total)
leaks: {status: clean, note: "janitor uses pkgs/worker.Group with proper ctx.Done() teardown; no raw goroutines spawned elsewhere in the service. New capacityReservationARN-based resourceTags entries are cascade-deleted on DeleteCapacityReservation (TestInMemoryBackend_DeleteCapacityReservation_CascadesTags), matching the existing WorkGroup/DataCatalog cascade-delete behavior — no ghost tag rows after delete."}
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

**Bugs fixed 2026-07-23** (this pass; verified against
`aws-sdk-go-v2/service/athena@v1.57.2`'s `types/types.go` and per-op
`api_op_*.go` files, not against gopherstack's own output). Four of these are
gopherstack-INVENTED fields with no counterpart on the real SDK type — the
no-stub/no-invented-field rule requires deleting them, not documenting them
as harmless:

6. **`WorkGroup`/`DataCatalog`/`CapacityReservation` each carried an invented
   `Tags map[string]string` field**, serialized into `GetWorkGroup`/
   `ListWorkGroups`/`GetDataCatalog`/`ListDataCatalogs`/`GetCapacityReservation`/
   `ListCapacityReservations` responses. AWS's real `types.WorkGroup`,
   `types.DataCatalog`, and `types.CapacityReservation` carry no `Tags` field
   at all — real Athena manages tags exclusively through `TagResource`/
   `UntagResource`/`ListTagsForResource`, never echoing them back on the
   resource itself. Worse than a harmless extra field: gopherstack's copy went
   stale the instant `TagResource`/`UntagResource` were called against the
   same resource, since those ops only ever touched the separate
   `resourceTags` map, never the resource's own `.Tags` field — so
   `GetWorkGroup` and `ListTagsForResource` could disagree about a
   workgroup's tags. All three `Tags` fields removed; `Create*`'s `Tags`
   input now flows only into `resourceTags`. `models.go`, `work_groups.go`,
   `data_catalogs.go`, `capacity_reservations.go`.

7. **`CreateNotebookInput` carried an invented `Tags []Tag` field.** The real
   `CreateNotebookInput` has only `Name`/`WorkGroup`/`ClientRequestToken` — no
   real AWS SDK client can populate `Tags` on notebook creation (unlike
   `CreateWorkGroup`/`CreateDataCatalog`/`CreateCapacityReservation`, which do
   accept `Tags` in the real API and were left as-is). Removed the field and
   the `tags` parameter from `InMemoryBackend.CreateNotebook`; a notebook
   remains taggable after creation via `TagResource` against its ARN.
   `handler_notebooks.go`, `notebooks.go`, `interfaces.go`.

8. **`EngineVersionDescriptor` carried an invented `AuthEngineVersion`
   field.** The real `types.EngineVersion` has exactly two fields
   (`EffectiveEngineVersion`, `SelectedEngineVersion`); `AuthEngineVersion`
   does not exist on it. Removed. `models.go`, `sessions.go`.

9. **`CreateDataCatalogOutput`/`DeleteDataCatalogOutput` never populated their
   optional `DataCatalog` field.** The real SDK v1.57.2 output types for both
   ops carry `DataCatalog *types.DataCatalog` (the created record for Create,
   the just-deleted record for Delete); gopherstack returned an empty
   `struct{}{}` body, so a real client's `output.DataCatalog` was always
   `nil`. `CreateDataCatalog`/`DeleteDataCatalog` on `InMemoryBackend` now
   return `(*DataCatalog, error)` and the handler wires the result into the
   response. `data_catalogs.go`, `handler_data_catalogs.go`, `interfaces.go`.

10. **`TagResource`/`UntagResource`/`ListTagsForResource` never validated that
    `ResourceARN` resolved to an existing resource**, and `ListTagsForResource`
    ignored its `MaxResults`/`NextToken` inputs entirely. Real AWS returns
    `InvalidRequestException` for an unknown/malformed `ResourceARN` and
    paginates `ListTagsForResource` like every other List op. Added
    `resourceExistsForARN`, which parses the ARN's `kind/id` resource segment
    and checks the corresponding table (`workgroup`, `datacatalog`,
    `capacity-reservation`, or `notebook`); wired into all three ops. This
    also surfaced (and fixed) that capacity reservations had never been wired
    into `resourceTags` via an ARN at all — `CreateCapacityReservation` built
    no ARN and wrote nothing to `resourceTags`, so a capacity reservation's
    tags were previously unreachable through `TagResource`/
    `ListTagsForResource` regardless of validation. `tags.go`,
    `handler_tags.go`, `capacity_reservations.go`, `store.go`.

11. **`ListWorkGroups`/`ListNamedQueries`/`ListDataCatalogs`/
    `ListPreparedStatements` (and the new `ListTagsForResource`) silently
    restarted pagination from offset 0 whenever `NextToken` did not exactly
    match an existing item's key** (e.g. the boundary item was deleted
    between calls) — re-emitting already-consumed results to the caller
    instead of erroring or resuming correctly. Added
    `pagination.paginationStart` (a `sort.Search`-based mutation-stable
    resume, mirroring `pageTokenCodec.paginateQueryExecutionIDs`'s existing
    approach for `ListQueryExecutions`) and switched all five call sites to
    it. `pagination.go`, `work_groups.go`, `named_queries.go`,
    `data_catalogs.go`, `prepared_statements.go`, `tags.go`.

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
- `TagResource`/`UntagResource`-style ops returning `struct{}{}` (empty JSON
  object) is correct for AWS Athena's void outputs — confirmed against each
  op's deserializer (no case statements at all = an empty response body is
  fully valid). `CreateDataCatalog`/`DeleteDataCatalog` are NOT in this
  category as of 2026-07-23 — see bug #9 above; they carry an optional
  `DataCatalog` field gopherstack now populates.
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
`resourceTags` are persisted explicitly as documented on `backendSnapshot`.
The three ops fixed in the previous pass (`GetSessionEndpoint`,
`CreatePresignedNotebookUrl`, `GetResourceDashboard`) are all pure/derived (no
new backend fields), so nothing new needed wiring into `Snapshot`/`Restore`.
This pass (2026-07-23) removed the `Tags` field from `WorkGroup`/
`DataCatalog`/`CapacityReservation` (round-trip-safe: `encoding/json` silently
drops a removed struct field on both marshal and unmarshal, so an
old-shaped snapshot restores cleanly) and added `capacityReservationARN`-keyed
entries to the already-persisted `resourceTags` map (no new top-level
`backendSnapshot` field required) — round trip verified by
`TestInMemoryBackend_TagResource_CapacityReservation` and
`TestInMemoryBackend_DeleteCapacityReservation_CascadesTags`.
