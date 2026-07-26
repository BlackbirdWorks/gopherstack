---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: redshiftdata
sdk_module: aws-sdk-go-v2/service/redshiftdata@v1.43.0   # version audited against
last_audit_commit: 2b2a22e6d                             # HEAD when this audit began (working tree, uncommitted)
last_audit_date: 2026-07-25
overall: A            # genuine wire-shape/field gaps found and fixed this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  ExecuteStatement: {wire: ok, errors: ok, state: ok, persist: ok, note: >
    Sets Status=FINISHED synchronously (deterministic mock, acceptable per parity rules).
    Fixed this pass: Parameters were already stored but never echoed back via
    DescribeStatement.QueryParameters -- now round-trips. SessionId is now real: accepted
    on the wire, persisted on Statement, and echoed on ExecuteStatementOutput and every
    downstream DescribeStatement/ListStatements read, as pure passthrough of whatever the
    caller supplied (no session-scoped state exists to gate minting a fresh id when absent,
    see gaps). ClientToken/SessionKeepAliveSeconds now accepted on the wire (previously not
    even unmarshalled) but intentionally inert -- see gaps. DbGroups still not returned
    (optional field, gap).}
  BatchExecuteStatement: {wire: ok, errors: ok, state: ok, persist: ok, note: >
    QueryString=Sqls[0] matches AWS; sub-statements built with fixed HasResultSet=false
    (AWS doesn't run real SQL so this is a simplification, see gaps). Fixed this pass: the
    real BatchExecuteStatementInput.Parameters field (shared across all SQL statements in
    the batch) was not unmarshalled from the request body at all -- every parameter a batch
    caller sent was silently dropped. Now parsed, stored, and echoed via
    DescribeStatement.QueryParameters, matching ExecuteStatement. SessionId/ClientToken/
    SessionKeepAliveSeconds: same treatment as ExecuteStatement, see above.}
  DescribeStatement: {wire: ok, errors: ok, state: ok, persist: ok, note: >
    Fixed this pass: QueryParameters was never emitted (dropped whatever Parameters the
    creating ExecuteStatement/BatchExecuteStatement call stored) -- now conditionally
    included when non-empty. SessionId now conditionally included (see ExecuteStatement
    note). RedshiftPid still not returned (optional field, always absent instead of 0, gap).
    Prior-pass fixes retained: Duration in nanoseconds, SubStatements RedshiftQueryId=0.}
  GetStatementResult: {wire: ok, errors: ok, state: ok, persist: n/a, note: "unchanged this pass; ColumnMetadata key length / Records stringValue union previously field-diffed correct. Records/ColumnMetadata are deterministic mock data (acceptable, no real query engine)"}
  GetStatementResultV2: {wire: ok, errors: ok, state: ok, persist: n/a, note: "unchanged this pass; Records []{CSVRecords} union / columnSize->length previously field-diffed correct"}
  CancelStatement: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass; always errors with ErrTerminalState in practice because ExecuteStatement/BatchExecuteStatement finish synchronously -- see gaps"}
  ListStatements: {wire: ok, errors: ok, state: ok, persist: ok, note: >
    Fixed this pass: list items were missing QueryStrings (batch statements), QueryParameters,
    and SessionId, all three real StatementData members -- now conditionally included
    alongside the already-correct Id/Status/QueryString/IsBatchStatement/CreatedAt/UpdatedAt/
    ResultFormat/StatementName/SecretArn. Extra non-real fields (ClusterIdentifier/
    WorkgroupName/Database/DbUser/HasResultSet/Duration) still sent beyond StatementData;
    harmless, SDK ignores unknown keys (see gaps). RoleLevel accepted but unused (see gaps).}
  ListDatabases: {wire: ok, errors: ok, state: gap, persist: n/a, note: >
    Fixed this pass: (1) Database is a required ExecuteStatementInput-style member on the
    real ListDatabasesInput (confirmed against api_op_ListDatabases.go's "This member is
    required" doc comment) but was never validated -- a request omitting it now correctly
    400s with ValidationException instead of silently returning the full demo list. (2)
    NextToken was unconditionally emitted as "" even when no more pages existed (the map
    literal always set the key); real NextToken is an optional *string that's nil, not an
    empty string, once fully paginated -- now conditionally included like every other
    List* op in this package. Still a static demo list, not backed by any real database
    registry (acceptable per parity rules, deterministic mock).}
  ListSchemas: {wire: ok, errors: ok, state: gap, persist: n/a, note: >
    Fixed this pass: Database-required validation added (same reasoning as ListDatabases).
    ConnectedDatabase field added to the request struct (previously entirely unhandled,
    a real ListSchemasInput member) -- accepted but does not affect filtering since this
    mock's demo schema list isn't modeled per-database, consistent with how ClusterIdentifier/
    WorkgroupName/DbUser/SecretArn are already accepted-but-unused identity/auth fields here.
    Static demo list + SQL LIKE pattern filter, unchanged otherwise.}
  ListTables: {wire: ok, errors: ok, state: gap, persist: n/a, note: >
    Fixed this pass: DELETED the invented TableType request field and its filter logic --
    field-diffed against api_op_ListTables.go/serializers.go and confirmed TableType does
    not exist anywhere in the real SDK (ListTablesInput only has ClusterIdentifier/
    ConnectedDatabase/Database/DbUser/MaxResults/NextToken/SchemaPattern/SecretArn/
    TablePattern/WorkgroupName). Removed TestHandler_ListTables_TableType_FiltersCorrectly
    (tested invented behavior) along with it. ConnectedDatabase field added (real member,
    was missing) and Database-required validation added (same reasoning as ListDatabases).}
  DescribeTable: {wire: ok, errors: ok, state: gap, persist: n/a, note: >
    Fixed this pass: ConnectedDatabase field added (real DescribeTableInput member, was
    missing) and Database-required validation added (same reasoning as ListDatabases).
    Prior-pass fix retained: TableName is a plain string (was a nested object). ColumnList
    is static demo data ignoring req.Schema/req.Table (acceptable mock).}
  ListSessions: {wire: ok, errors: ok, state: partial, persist: n/a (derived), note: >
    NEW this pass (SDK added this op since v1.41.0; confirmed target
    "RedshiftData.ListSessions" against awsAwsjson11_serializeOpListSessions in
    aws-sdk-go-v2/service/redshiftdata@v1.43.0's serializers.go). This backend has no
    CreateSession/CloseSession API and does not store sessions as a first-class resource
    -- ListSessions derives SessionData by grouping stored Statement records that share a
    non-empty SessionID (groupSessions in sessions.go): ClusterIdentifier/WorkgroupName/
    Database/DbUser/UpdatedAt come from the most-recently-updated statement in the group,
    CreatedAt from the earliest. Field-diffed request/response against
    awsAwsjson11_serializeOpDocumentListSessionsInput and
    awsAwsjson11_deserializeDocumentSessionData in the same module. Validation added for
    the two documented mutual-exclusivity constraints (SessionId can't combine with
    Status/ClusterIdentifier/WorkgroupName/Database; ClusterIdentifier and WorkgroupName
    can't both be set) and for the Status enum. Pagination follows this package's
    NextToken-is-the-last-item's-ID convention (sessionPageStart), same as ListStatements.
    Status is real but always AVAILABLE in practice: BUSY is unreachable because
    ExecuteStatement/BatchExecuteStatement complete synchronously (same root cause as the
    pre-existing CancelStatement gap), and CLOSED is unreachable because
    SessionAliveSeconds/SessionTtl are not tracked (see gaps) so no expiry can ever fire.
    RoleLevel is accepted on the wire but not applied as a filter, identical to the
    pre-existing ListStatements RoleLevel gap. Sessions only appear here when the caller
    explicitly supplied SessionId to ExecuteStatement/BatchExecuteStatement -- this mock
    does not mint a session id when only SessionKeepAliveSeconds is given (pre-existing
    gap, see ExecuteStatement note above), so such sessions are invisible to ListSessions
    too; that is a real, if narrow, gap, hence state: partial rather than ok.}
# Families audited as a group (when per-op is impractical):
families:
  statement-lifecycle: {status: ok, note: "unchanged this pass -- SUBMITTED/PICKED/STARTED never observable -- ExecuteStatement/BatchExecuteStatement complete to FINISHED synchronously within the same call, so no client ever polls a non-terminal state (no hang bug). CancelStatement is real code but practically unreachable given synchronous completion (see gaps)."}
  persistence: {status: ok, note: >
    Handler.Snapshot/Restore delegate to InMemoryBackend.Snapshot/Restore (persistence.go);
    versioned snapshot (redshiftdataSnapshotVersion), ring buffer round-trips correctly
    (persistence_test.go). Statement gained a new SessionID field this pass
    (json:"sessionID,omitempty") and BatchExecuteStatement now populates the pre-existing
    Parameters field -- both are additive/omitempty on an already-JSON-serialized struct,
    so old snapshots decode safely with SessionID="" and no version bump was needed.}
  error_codes: {status: ok, note: >
    ValidationException and ResourceNotFoundException (types/errors.go) are the only two
    error types this backend can actually produce, and both are field-diffed this pass:
    ErrorFault Client -> HTTP 400 for both (confirmed against types/errors.go's
    ErrorFault() methods), matching handler.go's handleError mapping exactly.
    InternalServerException (ErrorFault Server -> HTTP 500) is used as the generic fallback
    for unclassified errors in handleError's default case -- also correct. See gaps for why
    ActiveStatementsExceededException/ActiveSessionsExceededException/
    DatabaseConnectionException/ExecuteStatementException/BatchExecuteStatementException/
    QueryTimeoutException are real modeled exceptions in the SDK but unreachable by design.}
gaps:
  - CancelStatement can never succeed against this backend: ExecuteStatement/BatchExecuteStatement set Status=FINISHED synchronously, so by the time a client calls CancelStatement the statement is always already terminal and CancelStatement always returns ErrTerminalState (ValidationException). This matches real AWS semantics ("To be canceled, a query must be running") given the backend's synchronous-completion design. Not fixed this pass -- would require modeling async statement execution (a state machine with a delay before reaching FINISHED), which is a larger behavioral change beyond a wire-shape/bug-fix pass.
  - ValidateConnectionTarget (models.go) enforces "exactly one of ClusterIdentifier/WorkgroupName" per real AWS constraints but is never called from any handler. ExecuteStatement/BatchExecuteStatement handlers are intentionally permissive (see TestHandler_ExecuteStatement_AllowsBothClusterAndWorkgroup / AllowsNeitherClusterNorWorkgroup in handler_statements_validation_test.go) -- this looks like a deliberate relaxation for ease of testing rather than an oversight, so left as-is. Re-review if strict AWS-parity validation becomes a priority.
  - DescribeStatement does not return RedshiftPid (optional field, always absent instead of 0); DbGroups not returned by ExecuteStatement/BatchExecuteStatement. Both are optional wire fields the real client zero-values when absent, so not a functional gap, just lower fidelity -- no group/pid registry exists in this mock to source real values from.
  - ClientToken and SessionKeepAliveSeconds are accepted on ExecuteStatement/BatchExecuteStatement's wire (unmarshalled into the request struct) but are not behaviorally significant. ClientToken idempotency (returning the same statement for a retried request with the same token) and session keep-alive/expiry both require modeling request-retry dedup and time-bounded session lifetimes this in-memory backend does not have; inventing either risks fabricating undocumented AWS behavior not verifiable without a live cluster (same reasoning as rdsdata's typeHint gap). Relatedly, this mock does NOT mint a fresh SessionId when SessionKeepAliveSeconds>0 and no SessionId is supplied (real AWS would start a new session and return its id) -- SessionId here is pure passthrough of whatever the caller already provided, since there's no session-scoped state (temp tables, transaction visibility, etc.) that a minted id would actually gate.
  - RoleLevel is parsed on ListStatements' request body but never applied as a filter: real semantics are "true (default) = all statements this IAM role has run, false = only this IAM session's statements," but this mock has no per-caller-identity or per-session model of statement ownership, so there is no signal to filter on. All statements are visible regardless of RoleLevel, matching the "true" default in effect at all times.
  - ActiveStatementsExceededException, ActiveSessionsExceededException, DatabaseConnectionException, ExecuteStatementException, BatchExecuteStatementException, and QueryTimeoutException are all real modeled exception types in aws-sdk-go-v2/service/redshiftdata/types/errors.go but are unreachable by design in this backend: ExecuteStatement/BatchExecuteStatement always complete synchronously and successfully against in-memory demo data (no real cluster connection to fail, no concurrent-statement or concurrent-session limit tracked). Deliberately NOT implemented this pass: inventing trigger conditions (e.g. an arbitrary "N active statements" cap, or making some ClusterIdentifier/SecretArn values fail with DatabaseConnectionException) would fabricate gopherstack-only behavior with no real-AWS trigger to field-diff against -- consistent with rdsdata's precedent of leaving unreachable-by-design SDK exceptions undone rather than guessing.
  - ListStatements items include several fields (ClusterIdentifier, WorkgroupName, Database, DbUser, HasResultSet, Duration) that don't exist on the real StatementData shape at all. The AWS SDK's JSON deserializer silently discards unknown keys, so this is harmless today, but flagged in case a future SDK version repurposes one of those key names.
  - ListSessions (new this pass) never returns Status=BUSY or Status=CLOSED, and never returns SessionAliveSeconds/SessionTtl/CurrentStatementId at all: this backend executes every statement synchronously to a terminal state (no mid-flight window to observe BUSY/CurrentStatementId) and does not track SessionKeepAliveSeconds expiry (no SessionTtl to compare "now" against, so CLOSED can never be derived). Modeling any of these would require the same async-execution and keep-alive state machine already flagged as out-of-scope for CancelStatement/ClientToken/SessionKeepAliveSeconds above -- not invented here for the same reason. ListSessions also can't see sessions that were only ever referenced via SessionKeepAliveSeconds without an explicit SessionId (this mock doesn't mint one, see ExecuteStatement's note).
deferred:
  - none
leaks: {status: clean, note: "Janitor uses pkgs/worker.Group with TaskTimeout bounding; ticker stops cleanly via ctx.Done(); ring buffer + statements map bounded by maxStatementHistory and EvictExpiredStatements TTL sweep. Unchanged this pass -- no new goroutines/tickers/locks introduced (SessionID/Parameters changes are pure data threaded through the existing lock scopes in statements.go)."}
---

## Notes

Protocol: awsjson1.1 (single POST endpoint, `X-Amz-Target: RedshiftData.<Op>`). Verified
the exact target prefix against `aws-sdk-go-v2/service/redshiftdata@v1.41.0`'s
`serializers.go` (`httpBindingEncoder.SetHeader("X-Amz-Target").String("RedshiftData.<Op>")`)
-- `redshiftDataTargetPrefix = "RedshiftData."` in handler.go matches exactly.

### This pass (2026-07-25): AWS SDK bump to v1.43.0 exposed one new operation,

`ListSessions`, which `TestSDKCompleteness` flagged as unhandled. Implemented for real
(not added to a `notImplemented` list): see the `ListSessions` row above for the full
field-diff and derivation summary. Confirmed the wire target string
(`RedshiftData.ListSessions`) and every request/response field name directly against
`aws-sdk-go-v2/service/redshiftdata@v1.43.0`'s `serializers.go`
(`awsAwsjson11_serializeOpDocumentListSessionsInput`) and `deserializers.go`
(`awsAwsjson11_deserializeDocumentSessionData`, `awsAwsjson11_deserializeOpDocumentListSessionsOutput`)
rather than inferring from the operation name. New files: `sessions.go` (backend
derivation: `SessionData`/`ListSessionsFilter` live in `models.go`,
`ValidateListSessionsRequest`/`groupSessions`/`(*InMemoryBackend).ListSessions`/filter+
pagination helpers live in `sessions.go`) and `handler_sessions.go` (request parsing +
wire-shape response building), mirroring the existing `statements.go`/`handler_statements.go`
pairing. No `Statement`/backend-interface signature changes were needed for
`ExecuteStatement`/`BatchExecuteStatement` -- `SessionID` was already threaded through from
a prior pass, and `SessionKeepAliveSeconds` is deliberately not newly wired through those
two ops for this op alone (see the `ListSessions` gaps entry) to avoid inconsistent
partial modeling. No snapshot version bump: `ListSessions` adds no new persisted field to
`Statement` (sessions are purely derived at read time from a `regionStore`'s existing
`statements` map, already fully covered by `backendSnapshot`/`regionSnapshot`), so
`redshiftdataSnapshotVersion` stays 1 and old snapshots decode unchanged.

### Prior pass (2026-07-23): field-diffed every op's Input/Output against
`aws-sdk-go-v2/service/redshiftdata@v1.41.0`'s `api_op_*.go`, `types/types.go`,
`types/errors.go`, and the `serializers.go`/`deserializers.go` wire-key tables. Real gaps
found and fixed (all in `handler_statements.go` / `handler_tables.go` / `handler_databases.go`
/ `statements.go` / `models.go` / `interfaces.go`):

1. **Invented field: `ListTablesInput.TableType`.** gopherstack accepted and filtered on a
   `TableType` request field with no counterpart anywhere in the real SDK -- confirmed by
   grepping `TableType` across the entire `redshiftdata@v1.41.0` module (zero matches) and
   reading `awsAwsjson11_serializeOpDocumentListTablesInput`'s full field list. Deleted the
   field, its filter branch in `filterDemoTables`, and the test built around it
   (`TestHandler_ListTables_TableType_FiltersCorrectly`) that was asserting invented
   behavior.

2. **`BatchExecuteStatementInput.Parameters` was never unmarshalled.** The real API shares
   one `Parameters` list across every SQL statement in a batch (confirmed against
   `awsAwsjson11_serializeOpDocumentBatchExecuteStatementInput`'s `Parameters` key, same
   shape as `ExecuteStatementInput.Parameters`), but `handleBatchExecuteStatement`'s request
   struct didn't even have a `Parameters` field -- any parameters a batch caller sent were
   silently discarded before reaching the backend. Added the field, threaded it through
   `StorageBackend.BatchExecuteStatement` into `Statement.Parameters` (a field that already
   existed and was already used by the single-statement `ExecuteStatement` path).

3. **`DescribeStatementOutput.QueryParameters` was never emitted.** Both `ExecuteStatement`
   and `BatchExecuteStatement` already stored the caller's `Parameters` on the `Statement`
   (confirmed the field existed in `models.go`), but `statementToDescribeResponse` never
   read it back out -- a client that submitted parameterized SQL could never see its own
   parameters echoed back via `DescribeStatement`, unlike real AWS (confirmed wire key
   `"QueryParameters"` and shape `SqlParametersList` against
   `awsAwsjson11_deserializeOpDocumentDescribeStatementOutput`'s `case "QueryParameters":`).
   Also added to `statementToListItem` (`StatementData.QueryParameters` is a real member
   too) and to `StatementData.QueryStrings` (was in `DescribeStatement` but missing from
   `ListStatements` items).

4. **`SessionId` was not modeled anywhere.** `ExecuteStatementInput.SessionId`,
   `BatchExecuteStatementInput.SessionId`, `ExecuteStatementOutput.SessionId`,
   `DescribeStatementOutput.SessionId`, and `StatementData.SessionId` are all real wire
   fields (confirmed against the serializers/deserializers) that gopherstack neither
   accepted nor returned. Added a `Statement.SessionID` field, threaded a `sessionID string`
   parameter through `StorageBackend.ExecuteStatement`/`BatchExecuteStatement`, and
   conditionally echo it on `ExecuteStatementOutput`/`BatchExecuteStatementOutput`/
   `DescribeStatementOutput`/`StatementData` (ListStatements items) whenever the caller
   supplied one. Deliberately does NOT mint a new session id when `SessionKeepAliveSeconds`
   is set without a `SessionId` (see gaps) -- pure passthrough only, to avoid inventing
   session-lifecycle semantics this mock has no state to back up.

5. **`ListDatabasesInput.Database` / `ListSchemasInput.Database` / `ListTablesInput.Database`
   / `DescribeTableInput.Database` are all documented "This member is required"** in their
   respective `api_op_*.go` files, but none of the four handlers validated it -- a request
   omitting `Database` silently returned the full static demo list/columns instead of a
   `ValidationException`. Added the same `Database is required` check `ExecuteStatement`/
   `BatchExecuteStatement` already had.

6. **`ListDatabasesOutput.NextToken`** was unconditionally set to `""` in the response map
   even when the demo database list fit on one page. Real `NextToken` is an optional
   `*string`, `nil` (omitted) once there are no more pages, not an empty string --
   `ListSchemas`/`ListTables`/`ListStatements` already followed the omit-when-empty
   convention; `ListDatabases` was the one outlier. Fixed to match.

7. **`ListSchemasInput.ConnectedDatabase` / `ListTablesInput.ConnectedDatabase` /
   `DescribeTableInput.ConnectedDatabase`** are real wire fields (cross-database query
   support) that were entirely absent from the corresponding request structs -- any value
   sent there was silently ignored by Go's `json.Unmarshal` (not an error, just dropped).
   Added the field to each struct for wire-shape completeness; left unused for filtering
   since this mock's demo schema/table/column lists aren't modeled per-database, consistent
   with how `ClusterIdentifier`/`WorkgroupName`/`DbUser`/`SecretArn` are already
   accepted-but-unused identity/auth fields on these same ops.

None of the existing unit tests asserted the missing `QueryParameters`/`SessionId` fields or
the invented `TableType` filter as *not* present, so these were real, previously-undetected
gaps rather than known-and-tested tradeoffs (the closest test, `TestParameters_AcceptedAndStored`,
only checked "request with Parameters returns 200 and an Id," never that `DescribeStatement`
echoed them back -- extended this pass to assert the round-trip).

### Prior pass (2026-07-13) bug classes (all in `handler.go`, retained, still correct):

1. **`ColumnMetadata` wire key `columnSize` does not exist in the real API.** The real
   field is `length` (int32) -- confirmed against
   `aws-sdk-go-v2/service/redshiftdata/deserializers.go`'s
   `awsAwsjson11_deserializeDocumentColumnMetadata` (`case "length":`). Sending
   `columnSize` meant the real SDK's deserializer silently dropped the value (its
   `default:` case just discards unknown keys), so every client parsing
   `GetStatementResult`, `GetStatementResultV2`, or `DescribeTable` would see
   `ColumnMetadata[i].Length == 0` regardless of what gopherstack sent. Renamed the
   constant `keyColumnSize` -> `keyLength` = `"length"` and updated all 7 call sites
   (`handleGetStatementResult`, `handleGetStatementResultV2`, `buildDemoColumns`).

2. **`DescribeTable`'s `TableName` was a nested `{schema, name, type}` object; the real
   `DescribeTableOutput.TableName` is a plain string.** Confirmed against
   `api_op_DescribeTable.go` (`TableName *string`) and its deserializer
   (`case "TableName":` decodes a bare string). The nested object was silently dropped
   by the real SDK the same way as (1), leaving `TableName` unset. Fixed to
   `"TableName": req.Table`.

3. **`Duration` was emitted in milliseconds; the real field is nanoseconds.** Both
   `DescribeStatementOutput.Duration` and `SubStatementData.Duration` are documented as
   "The amount of time in nanoseconds that the statement ran" (confirmed in both
   `aws-sdk-go-v2` and legacy `aws-sdk-go` v1 doc comments, and the wire key itself,
   `"Duration"`, already matched). Added `durationNanos(ms int64) int64` and applied
   it at the three wire-marshaling call sites (`statementToListItem`,
   `statementToDescribeResponse` top-level and its `SubStatements` loop).

4. **`GetStatementResultV2`'s `Records` was `[]string`; the real field is
   `[]types.QueryRecords`, a union whose only member is `CSVRecords` (a string).**
   Confirmed against the union deserializer
   `awsAwsjson11_deserializeDocumentQueryRecords` (`case "CSVRecords":`, `default:`
   treats anything else as `UnknownUnionMember`). Fixed to
   `[]map[string]any{{"CSVRecords": "mock_value"}}`.

**Looks-wrong-but-correct traps** (don't re-flag):
- `ExecuteStatement`/`BatchExecuteStatement` complete synchronously to `Status=FINISHED`
  in the same call. This is intentional (no real Redshift cluster to run SQL against
  asynchronously) and satisfies the "must reach a terminal state" parity rule trivially
  -- it is not a disguised hang bug.
- `ListStatements` default (`Status` omitted) returns only `FINISHED` statements
  (`matchesStatementStatus`) -- this was a deliberate choice from a prior sweep to match
  documented AWS default filtering behavior; don't flip without re-verifying against
  AWS docs.
- `ValidateConnectionTarget` exists in `models.go` but is intentionally never called
  from `ExecuteStatement`/`BatchExecuteStatement` -- there are named tests
  (`TestHandler_ExecuteStatement_AllowsBothClusterAndWorkgroup`,
  `TestHandler_ExecuteStatement_AllowsNeitherClusterNorWorkgroup`,
  `TestHandler_BatchExecuteStatement_AllowsNeitherClusterNorWorkgroup`) asserting the
  permissive behavior. Treat as a deliberate relaxation, not dead code to wire up.
- `DescribeTable`/`ListDatabases`/`ListSchemas`/`ListTables` return static demo data
  regardless of the requested database/schema/table -- acceptable per the "deterministic
  mock data OK, no real query engine" parity rule; not a stub since the ops still apply
  real filtering/pagination logic to that demo data, and now (this pass) real
  required-field validation too.
- `SessionId` is real and round-trips, but is NOT minted by this mock when absent -- don't
  "fix" `ExecuteStatement` to generate one whenever `SessionKeepAliveSeconds > 0` without
  re-reading the gaps entry above; there is no session-scoped state that would make a
  synthetic id meaningfully different from omitting it.
- `ListSessions` always reports `Status: "AVAILABLE"` and never emits `SessionAliveSeconds`/
  `SessionTtl`/`CurrentStatementId`. This is not an oversight to "complete" -- it is the
  direct, correct consequence of ExecuteStatement/BatchExecuteStatement completing
  synchronously (no BUSY window) and SessionKeepAliveSeconds not being tracked anywhere
  (no TTL to compare against for CLOSED). See the `ListSessions` gaps entry before changing
  this.
