---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: redshiftdata
sdk_module: aws-sdk-go-v2/service/redshiftdata@v1.41.0   # version audited against
last_audit_commit: 1c45a3ba                              # HEAD when this audit began
last_audit_date: 2026-07-13
overall: A            # genuine wire-shape fixes found and applied
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  ExecuteStatement: {wire: ok, errors: ok, state: ok, persist: ok, note: "sets Status=FINISHED synchronously (deterministic mock, acceptable per parity rules); DbGroups/SessionId not returned (optional fields, gap)"}
  BatchExecuteStatement: {wire: ok, errors: ok, state: ok, persist: ok, note: "QueryString=Sqls[0] matches AWS; sub-statements built with fixed HasResultSet=false (AWS doesn't run real SQL so this is a simplification, see gaps)"}
  DescribeStatement: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Duration was emitted in ms instead of ns; SubStatements now include RedshiftQueryId=0 for consistency with top-level"}
  GetStatementResult: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: ColumnMetadata used wire key columnSize instead of length. Records/ColumnMetadata are deterministic mock data (acceptable, no real query engine)"}
  GetStatementResultV2: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: Records was []string, real shape is []{CSVRecords: string} (QueryRecords union); fixed columnSize->length"}
  CancelStatement: {wire: ok, errors: ok, state: ok, persist: ok, note: "always errors with ErrTerminalState in practice because ExecuteStatement/BatchExecuteStatement finish synchronously, so no statement is ever in a cancellable non-terminal state -- see gaps"}
  ListStatements: {wire: ok, errors: ok, state: ok, persist: ok, note: "extra fields (ClusterIdentifier/WorkgroupName/Database/DbUser/HasResultSet/Duration) sent beyond real StatementData shape; harmless, SDK ignores unknown keys"}
  ListDatabases: {wire: ok, errors: ok, state: gap, persist: n/a, note: "static demo list, not backed by any real database registry -- acceptable per parity rules (deterministic mock)"}
  ListSchemas: {wire: ok, errors: ok, state: gap, persist: n/a, note: "static demo list + SQL LIKE pattern filter"}
  ListTables: {wire: ok, errors: ok, state: gap, persist: n/a, note: "static demo list + filters"}
  DescribeTable: {wire: ok, errors: ok, state: gap, persist: n/a, note: "fixed: TableName was a nested {schema,name,type} object, real shape is a plain string. ColumnList is static demo data ignoring req.Schema/req.Table (acceptable mock)"}
# Families audited as a group (when per-op is impractical):
families:
  statement-lifecycle: {status: ok, note: "SUBMITTED/PICKED/STARTED never observable -- ExecuteStatement/BatchExecuteStatement complete to FINISHED synchronously within the same call, so no client ever polls a non-terminal state (no hang bug). CancelStatement is real code but practically unreachable given synchronous completion (see gaps)."}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to InMemoryBackend.Snapshot/Restore (persistence.go); versioned snapshot (redshiftdataSnapshotVersion), ring buffer round-trips correctly (persistence_test.go)."}
gaps:
  - CancelStatement can never succeed against this backend: ExecuteStatement/BatchExecuteStatement set Status=FINISHED synchronously, so by the time a client calls CancelStatement the statement is always already terminal and CancelStatement always returns ErrTerminalState (ValidationException). This matches real AWS semantics ("To be canceled, a query must be running") given the backend's synchronous-completion design, and is already covered by TestRefinement3_CancelStatement_NotFound-style tests; flagged here only because a caller relying on "start statement then cancel it" integration pattern will never see a successful cancel. Not fixed this pass -- would require modeling async statement execution (a state machine with a delay before reaching FINISHED), which is a larger behavioral change beyond a wire-shape/bug-fix pass.
  - ValidateConnectionTarget (backend.go) enforces "exactly one of ClusterIdentifier/WorkgroupName" per real AWS constraints but is never called from any handler. ExecuteStatement/BatchExecuteStatement handlers are intentionally permissive (see TestHandler_ExecuteStatement_AllowsBothClusterAndWorkgroup / AllowsNeitherClusterNorWorkgroup in handler_parity_test.go, added in a prior sweep) -- this looks like a deliberate relaxation for ease of testing rather than an oversight, so left as-is. Re-review if strict AWS-parity validation becomes a priority.
  - DescribeStatement does not return RedshiftPid (optional field, always absent instead of 0); DbGroups/SessionId not returned by ExecuteStatement/BatchExecuteStatement. All are optional wire fields the real client zero-values when absent, so not a functional gap, just lower fidelity.
  - ListStatements items include several fields (ClusterIdentifier, WorkgroupName, Database, DbUser, HasResultSet, Duration) that don't exist on the real StatementData shape at all. The AWS SDK's JSON deserializer silently discards unknown keys, so this is harmless today, but flagged in case a future SDK version repurposes one of those key names.
deferred:
  - none
leaks: {status: clean, note: "Janitor uses pkgs/worker.Group with TaskTimeout bounding; ticker stops cleanly via ctx.Done(); ring buffer + statements map bounded by maxStatementHistory and EvictExpiredStatements TTL sweep."}
---

## Notes

Protocol: awsjson1.1 (single POST endpoint, `X-Amz-Target: RedshiftData.<Op>`). Verified
the exact target prefix against `aws-sdk-go-v2/service/redshiftdata@v1.41.0`'s
`serializers.go` (`httpBindingEncoder.SetHeader("X-Amz-Target").String("RedshiftData.<Op>")`)
-- `redshiftDataTargetPrefix = "RedshiftData."` in handler.go matches exactly.

Real bug classes found and fixed this pass (all in `handler.go`):

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
   `"Duration"`, already matched). The backend internally tracks `Statement.DurationMs`
   /`SubStatementData.DurationMs` in milliseconds (reasonable internal unit), but the
   handler was passing that value straight through to the wire, understating every
   duration by a factor of 1,000,000. Added `durationNanos(ms int64) int64` and applied
   it at the three wire-marshaling call sites (`statementToListItem`,
   `statementToDescribeResponse` top-level and its `SubStatements` loop).

4. **`GetStatementResultV2`'s `Records` was `[]string`; the real field is
   `[]types.QueryRecords`, a union whose only member is `CSVRecords` (a string).**
   Confirmed against the union deserializer
   `awsAwsjson11_deserializeDocumentQueryRecords` (`case "CSVRecords":`, `default:`
   treats anything else as `UnknownUnionMember`). A bare string element would hit the
   union's default case and become an unknown-member placeholder instead of a usable
   CSV row. Fixed to `[]map[string]any{{"CSVRecords": "mock_value"}}`.

None of the existing unit tests asserted the old (wrong) key names or exact `Records`
element shape -- they only checked "field is present / non-empty" -- so these were
real, previously-undetected wire bugs rather than known-and-tested tradeoffs.

**Looks-wrong-but-correct traps** (don't re-flag):
- `ExecuteStatement`/`BatchExecuteStatement` complete synchronously to `Status=FINISHED`
  in the same call. This is intentional (no real Redshift cluster to run SQL against
  asynchronously) and satisfies the "must reach a terminal state" parity rule trivially
  -- it is not a disguised hang bug.
- `ListStatements` default (`Status` omitted) returns only `FINISHED` statements
  (`matchesStatementStatus`) -- this was a deliberate choice from a prior sweep to match
  documented AWS default filtering behavior; don't flip without re-verifying against
  AWS docs.
- `ValidateConnectionTarget` exists in `backend.go` but is intentionally never called
  from `ExecuteStatement`/`BatchExecuteStatement` -- there are named tests
  (`TestHandler_ExecuteStatement_AllowsBothClusterAndWorkgroup`,
  `TestHandler_ExecuteStatement_AllowsNeitherClusterNorWorkgroup`,
  `TestHandler_BatchExecuteStatement_AllowsNeitherClusterNorWorkgroup`) asserting the
  permissive behavior. Treat as a deliberate relaxation, not dead code to wire up.
- `DescribeTable`/`ListDatabases`/`ListSchemas`/`ListTables` return static demo data
  regardless of the requested database/schema/table -- acceptable per the "deterministic
  mock data OK, no real query engine" parity rule; not a stub since the ops still apply
  real filtering/pagination logic to that demo data.
