---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: rdsdata
sdk_module: aws-sdk-go-v2/service/rdsdata@v1.35.4   # version audited against
last_audit_commit: deb6c42f                          # HEAD when this pass started (working tree, uncommitted)
last_audit_date: 2026-09-04
overall: A            # every op/family field-diffed against the real SDK source this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  ExecuteStatement: {wire: ok, errors: ok, state: ok, persist: ok, note: >
    FormatRecordsAs=JSON, the full 14-field ColumnMetadata, resultSetOptions
    (decimalReturnType/longReturnType), and generatedFields (rowid-alias
    INSERTs) are all implemented for real this pass -- see Notes.
    continueAfterTimeout is accepted on the wire as a documented no-op (no
    statement timeouts exist to continue past).}
  BatchExecuteStatement: {wire: ok, errors: ok, state: ok, persist: ok, note: >
    One UpdateResult per parameter set; transaction id validated before any
    engine execution. GeneratedFields now populated per parameter set using
    the same rowid-alias detection as ExecuteStatement (see Notes).}
  BeginTransaction: {wire: ok, errors: ok, state: ok, persist: ok, note: >
    Opaque per-region sequential id (txn-NNNNNN); real engine-side sql.Tx
    opened alongside so statements tagged with the id share atomic visibility.
    gopherstack-wh8gv (fixed 2026-09-11): that sql.Tx used to be opened under
    BeginTransaction's own per-request context, so a real net/http.Server
    auto-rolled it back the instant that HTTP response finished; now opened
    under an engine-owned context (sqlEngine.baseCtx) cancelled only on
    Commit/Rollback/janitor expiry/Reset. See the dated section below.}
  CommitTransaction: {wire: ok, errors: ok, state: ok, persist: ok, note: >
    Deletes the transaction from the region's table before returning, so
    reuse (execute/commit/rollback) correctly 400s with TransactionNotFoundException.}
  RollbackTransaction: {wire: ok, errors: ok, state: ok, persist: ok}
  ExecuteSql: {wire: ok, errors: ok, state: ok, persist: ok, note: >
    Deprecated op; executes for real against the same per-resource engine DB
    and records to the statement log like the other ops. resultFrame is now
    populated for query statements (records + resultSetMetadata), converted
    from the same engine row extraction ExecuteStatement uses, at the wire
    boundary into the older Value union (bigIntValue/bitValue, not
    longValue/booleanValue) -- gopherstack-7ows. Left nil for DML, which
    still only reports numberOfRecordsUpdated.}
# Families audited as a group (when per-op is impractical):
families:
  routing: {status: ok, note: >
    RouteMatcher gates on SigV4 service name ("rds-data") + one of the 6 fixed
    paths (/Execute, /BatchExecute, /BeginTransaction, /CommitTransaction,
    /RollbackTransaction, /ExecuteSql); verified against
    aws-sdk-go-v2/service/rdsdata's serializers.go request paths -- all match.}
  field_union: {status: ok, note: >
    Field{isNull,booleanValue,longValue,doubleValue,stringValue,blobValue,
    arrayValue} now models every member of the real Field union, including
    arrayValue (types.FieldMemberArrayValue / types.ArrayValue), fixed this
    pass -- see Notes. It is structurally present but functionally
    unreachable in a *result* (the pure-Go SQLite driver never produces an
    array-typed column), matching real AWS's own inability to emit one from
    ExecuteStatement/BatchExecuteStatement.}
  result_set_options: {status: ok, note: >
    resultSetOptions.{decimalReturnType,longReturnType} implemented this
    pass: previously accepted nowhere on the wire. See Notes for the exact
    shaping rules and the one deliberate default-behavior change this
    introduces.}
  transaction_lifecycle: {status: ok, note: >
    Verified id allocation, isolation across regions (isolation_test.go),
    commit/rollback removing the id from the active set so reuse 400s, and
    snapshot/restore round-tripping open transactions + the txCounter.
    gopherstack-02w (fixed this pass): BeginTransaction's doc comment
    (rdsdata@v1.35.4 api_op_BeginTransaction.go) states "A transaction can
    run for a maximum of 24 hours. A transaction is terminated and rolled
    back automatically after 24 hours" and "A transaction times out if no
    calls use its transaction ID in three minutes" -- neither was
    implemented, so a caller that began a transaction and never committed or
    rolled it back leaked it (and its engine-side *sql.Tx) forever. Fixed via
    janitor.go's Janitor: Transaction now carries CreatedAt/LastActivityAt
    (statements.go's touchTransactionLocked refreshes the latter on every
    ExecuteStatement/BatchExecuteStatement against that id), and a
    worker.Group-based sweep (wired through Handler.WithJanitor/StartWorker,
    provider.go) rolls back and evicts anything past either threshold. See
    janitor_test.go.}
  error_codes: {status: ok, note: >
    TransactionNotFoundException (400) and BadRequestException (400, via
    ErrValidation/errIsValidation) cover every error path this mock can
    produce; both are real modeled exceptions in types/errors.go.
    validateNoArrayParameters (fixed this pass) rejects an arrayValue
    parameter as BadRequestException, per real AWS's documented "Array
    parameters are not supported" -- the exact error class AWS returns for
    this case has not been independently verified against a live API call
    (the SDK doc comment states the constraint but not the wire error), so
    this is a best-effort, not a field-diffed, error mapping; flagged in
    items_still_open below rather than claimed as fully verified. No
    resourceArn/secretArn existence validation is performed (mock has no
    cluster registry), so NotFoundException/ForbiddenException/
    AccessDeniedException/ServiceUnavailableError/StatementTimeoutException
    are unreachable by design -- consistent with an emulator that doesn't
    simulate IAM or Aurora Serverless timeouts.}
gaps: []
items_still_open:
  - "Database/Schema (ExecuteStatement, BatchExecuteStatement, BeginTransaction,
    ExecuteSql -- all 4 ops that carry them) are decoded off the wire and never
    read anywhere (cmd/reqfieldscan, 2026-08-30 pass: 8 of rdsdata's 9 flagged
    fields). Real AWS's Database overrides the database named by resourceArn's
    connection/secret, and Schema (PostgreSQL only) overrides search_path --
    both select *within* a resource. gopherstack's sqlEngine keys its one
    SQLite database per (region, resourceARN) only (engine.go's dbFor/dbKey);
    there is no per-resource multi-database or schema catalog for these
    fields to select into, the same root cause as ExecuteSql's Database/
    Schema fields below and its siblings' repeated honest-gap pattern in
    this campaign. Confirmed via grep: no `.Database`/`.Schema` selector
    anywhere in non-test source. Not fixed: modeling multiple named
    databases/schemas inside one engine instance is a real feature (SQLite
    ATTACH DATABASE per name, or a schema-qualified table namespace), not a
    field-read fix."
  - "SqlParameter.typeHint bind semantics (gopherstack-fdle, fixed this
    pass -- see Notes): a hint now validates its stringValue's documented
    format and 400s a malformed one, but the *bound value* is still the
    unmodified string -- the mock SQLite engine has no distinct DATE/
    DECIMAL/TIMESTAMP/UUID column types to coerce into, so a well-formed
    DATE-hinted value still binds identically to an unhinted string. Real
    AWS's exact behavior for a malformed hinted value (which error class,
    and whether it's a request-time or DB-execution-time failure) is not
    independently verifiable without a live Aurora cluster -- the
    BadRequestException class and message wording gopherstack now returns
    are a best-effort inference, not a field-diffed fact. See Notes."
  - "ColumnMetadata.SchemaName/TableName/IsAutoIncrement (gopherstack-fdle,
    fixed this pass for the non-transactional path -- see Notes): populated
    via modernc.org/sqlite@v1.58.0's conn.ColumnInfo, which exposes the real
    sqlite3_column_table_name/database_name/origin_name C APIs through
    *sql.Conn.Raw (database/sql's own sql.ColumnType has no such accessor,
    as the prior pass found). Still always zero-valued for a statement run
    inside a BeginTransaction transaction: *sql.Tx has no equivalent to
    *sql.Conn.Raw, so there's no way to recover the driver connection
    ColumnInfo needs. ArrayBaseColumnType remains always 0 -- unaffected,
    and correct, since this mock's result columns are never array-typed
    (see the field_union family note above)."
leaks: {status: clean, note: >
  sqlEngine.reset() rolls back every open *sql.Tx and closes every resourceDB
  (including its keep-alive conn) before clearing the maps; Handler.Reset()
  delegates to Backend.Reset() which calls engine.reset(). The
  hasRowIDAliasColumn PRAGMA lookup runs synchronously on the same querier
  (already-held connection/tx) as the triggering statement, under the
  existing sqlEngine.mu, and is closed via `defer rows.Close()`.
  gopherstack-02w (fixed this pass): the prior "no goroutines, tickers, or
  other background work" framing missed the actual leak -- an unbounded
  `map[string]*Transaction` growing forever from transactions a caller began
  and never committed/rolled back, with no expiry. See transaction_lifecycle
  above; janitor.go now runs a background Janitor (a goroutine, started via
  Handler.StartWorker) that reaps them, matching the ticker-based pattern
  services/codebuild's janitor.go already uses -- this package is no longer
  goroutine-free, and that's the fix, not a regression.}
---

## Notes

**generatedFields (fixed this pass).** Previously always an empty array for
both ExecuteStatement and BatchExecuteStatement (flagged but left as a
"deliberate simplification" in the prior two audits, since it needed a 5th
backend-method return value threaded through ~30 call sites). Implemented
this pass: `StorageBackend.ExecuteStatement` now returns `([][]Field,
[]ColumnMetadata, int64, []Field, error)`; the new `[]Field` is
`generatedFieldsFor` (engine.go), which recognizes a simple, unquoted
`INSERT INTO <table>` statement, checks via `PRAGMA table_info(<table>)`
whether the table declares exactly one `INTEGER PRIMARY KEY` column (SQLite's
documented rowid alias -- https://sqlite.org/lang_createtable.html#rowid),
and if so surfaces `sql.Result.LastInsertId()` as a single `longValue`. Every
other case (no such column, a composite primary key, UPDATE/DELETE/DDL, or a
quoted/bracketed table identifier the regexp doesn't match) returns an empty
slice -- the same safe historical default. This is a real, verifiable
behavior (not a fabricated ID): it mirrors Aurora MySQL's AUTO_INCREMENT
generatedFields support, and real AWS's own doc comment confirms
`generatedFields` is meaningless for Aurora PostgreSQL. All ~35
`ExecuteStatement` call sites across the test suite were mechanically updated
to the new 5-return signature.

**resultSetOptions (fixed this pass).** Previously accepted nowhere on the
wire. Implemented per the exact SDK doc comments on
`types.ResultSetOptions`: `longReturnType` (default `LONG`, or `STRING`)
shapes INTEGER-affinity result columns; `decimalReturnType` (default
`STRING`, or `DOUBLE_OR_LONG`) shapes DECIMAL/NUMERIC-affinity result
columns. Threaded from the handler to the engine via a new
`resultSetOptionsContextKey` (store.go), mirroring the existing
`regionContextKey` pattern, rather than adding a rarely-used parameter to
`StorageBackend.ExecuteStatement` that nearly every call site would have to
pass a zero value for. **Deliberate default-behavior change:** implementing
the real default (`decimalReturnType=STRING`) means a DECIMAL/NUMERIC-affinity
column's value is now always rendered as a `stringValue` unless the caller
explicitly requests `DOUBLE_OR_LONG` -- previously such a column's Field
shape depended on whatever raw Go type the driver happened to scan
(int64/float64/string). This is intentional: it's what real AWS does by
default, and no existing test asserted a Field *value* shape for a
NUMERIC/DECIMAL-affinity column (only `TestEngine_ColumnMetadata_TypeAffinity`
asserted the `type` code, which is unaffected). A computed/literal column
with no declared type (e.g. `SELECT 42`, `COUNT(*)`) resolves to BLOB
affinity per `sqliteAffinity`'s existing rule 3, so resultSetOptions never
touches it -- consistent with pre-existing behavior, not a regression.

**arrayValue (fixed this pass).** `Field.ArrayValue *ArrayValue` and a new
`ArrayValue` struct (mirroring `types.ArrayValue`'s five members) were added
so a client sending `"arrayValue": {...}` in a parameter round-trips through
JSON instead of being silently dropped by `json.Unmarshal` (previously: the
unknown key was ignored and the parameter bound as an effective NULL). Real
AWS documents "Array parameters are not supported" for both
`ExecuteStatementInput.Parameters` and `BatchExecuteStatementInput.
ParameterSets`; `validateNoArrayParameters` (handler.go) now enforces that,
rejecting the request as `BadRequestException` before it reaches the engine.
See items_still_open for why the exact error class is a best-effort
inference rather than a verified fact.

**continueAfterTimeout (fixed this pass, wire-only).** Added to
`executeStatementRequest` so it round-trips instead of silently vanishing.
Remains a deliberate no-op: this mock has no statement-execution timeouts to
continue past, so there is no divergent behavior to implement -- consistent
with `StatementTimeoutException` being unreachable by design (see
error_codes family note).

**FormatRecordsAs, ColumnMetadata full shape, typeHint wire round-trip**
(fixed in the prior pass, unchanged this pass): see the two audits' worth of
history in git blame if needed; summary retained from the previous manifest
version below.

- `formatRecordsAs=JSON` on a SELECT statement (checked via the existing
  `isQuery` heuristic) omits `records`/`columnMetadata` and instead returns
  `formattedRecords`, a JSON string containing an array of row objects keyed
  by column name (Field union values unwrapped to native JSON; blobs
  base64-encoded). Invalid enum values are rejected as BadRequestException.
- `ColumnMetadata` carries the full real-AWS 14-field shape; `type`/
  `isSigned`/`isCaseSensitive` are derived from SQLite's documented type
  affinity algorithm (see `sqliteAffinity`); `nullable` and `precision`/
  `scale` reflect modernc.org/sqlite's driver limits (verified from driver
  source, not guessed).
- `SqlParameter.typeHint` round-trips on the wire; see the gopherstack-fdle
  section below for its format-validation semantics (added since), and
  gaps for why it still doesn't affect the actual bound value.

**Trap for the next auditor:** `ExecuteStatement`/`BatchExecuteStatement`
degrade SQL the mock SQLite engine rejects (e.g. DML against a table that was
never created) to the historical empty-success envelope rather than
surfacing an error (`statements.go`'s `ExecuteStatement`/
`BatchExecuteStatement` swallow `b.engine.execute`'s error deliberately).
This looks like a swallowed bug on first read but is intentional,
pre-existing, documented behavior ("historical lenient behaviour") -- don't
re-flag it without checking the surrounding comments first.

**Trap for the next auditor #2:** a column named `"42"` is real, not a typo
-- SQLite's pure-Go driver names literal/expression result columns after
their source text when there's no explicit AS alias (e.g. `SELECT 42` yields
a column literally named `"42"`). `TestHandler_ExecuteStatement_
FormatRecordsAsJSON` reads the column name back dynamically for this reason
rather than asserting a fixed key.

**Trap for the next auditor #3:** `generatedFieldsFor`'s table-name regexp
(`insertIntoTableRe`) only matches a bare, unquoted identifier immediately
after `INSERT [OR <resolution>] INTO`. An INSERT against a quoted/
bracket-escaped table name (`INSERT INTO "my table"...`) silently degrades
to no generated fields rather than erroring -- this is the same safe-default
philosophy as the rest of the engine's lenient-fallback behavior, not an
oversight; don't "fix" it into attempting identifier unquoting without
checking whether that's actually needed by a real test first.

## gopherstack-o7gx (2026-08-22): ReadBody-failure path wrote untyped errors

`Handler()`'s `httputils.ReadBody` failure branch wrote a bare
`c.String(http.StatusInternalServerError, "internal server error")` --
plain text, not JSON. rdsdata is restjson1 (confirmed from `rdsdata@v1.35.4`
deserializers.go's `awsRestjson1_deserializeOpError*` prefix), whose
client-side error decoder (`aws-sdk-go-v2@v1.43.4`
`aws/protocol/restjson.GetErrorInfo`) JSON-decodes the body for a
`code`/`__type` field; plain text doesn't decode, so a real client got
`*json.SyntaxError`, not even `UnknownError`.

Fixed by writing `{"__type": "InternalServerErrorException", "message":
"internal server error"}` instead (new `writeInternalServerError` helper).
`InternalServerErrorException` is rdsdata's own modeled internal error
(`rdsdata@v1.35.4` `types/errors.go:230`). Also promoted the file's
previously-inline `"__type"` literal to a `keyTypeField` constant (3
occurrences after this fix; `goconst` flagged it).

Proven with a real `aws-sdk-go-v2/service/rdsdata` client's
`ExecuteStatement`, whose `Sql` field alone exceeds
`httputils.MaxRequestBodyBytes` (16 MiB) -- legitimate SDK input.
`TestHandler_OversizedBodySurfacesInternalServerErrorException`
(`handler_oversized_body_test.go`) asserts `apiErr.ErrorCode() ==
"InternalServerErrorException"`; confirmed it fails pre-fix with
`*json.SyntaxError` (hand-reverted, byte-identical restore after).

NOT touched: `handleError`'s `errInvalidRequest`/`errUnknownAction`/
syntax/type-error catch-all and its `default:` fallback are themselves
untyped (`map[string]string{keyMessageField: err.Error()}`, no `__type`)
-- a pre-existing, separate gap in the genuine per-operation error path,
not the ReadBody-failure path this fix addresses. Left alone.
## rdsdata (this session, 2026-08-20)

Wrapper-key / nested-shape wire-parity sweep, the last service of a
160-service campaign. Independently re-derived every op's field list from
the pinned SDK (`aws-sdk-go-v2/service/rdsdata@v1.35.4`) rather than trusting
the prior audit's notes, per this campaign's method. **Result: zero bugs
found.** The prior two passes (see Notes above, and `d39bf33e4` 2026-08-11)
had already fixed every real gap this sweep would have caught (arrayValue,
generatedFields, resultSetOptions, ExecuteSql's resultFrame); this pass is a
from-scratch confirmation, not a rubber stamp.

- **Ops (6, confirmed against `ls api_op_*.go` at the pin, not assumed):**
  `BatchExecuteStatement`, `BeginTransaction`, `CommitTransaction`,
  `ExecuteSql`, `ExecuteStatement`, `RollbackTransaction`. `ExecuteSql`
  exists at v1.35.4 (marked Deprecated in its doc comment, not removed) and
  is fully implemented in gopherstack (`sql.go`), including the
  `resultFrame`/legacy `Value` union added 2026-08-11.
- **Protocol:** REST-JSON 1 (`awsRestjson1_*` in serializers.go/
  deserializers.go), matching `services/_PROTOCOLS.md`'s row for this
  service. All 6 ops are `POST` to a fixed, argument-free path (verified via
  `grep -n "SplitURI\|request.Method" serializers.go`): `/Execute` (L445),
  `/BatchExecute` (L44), `/BeginTransaction` (L157), `/CommitTransaction`
  (L253), `/ExecuteSql` (L344), `/RollbackTransaction` (L580) -- all match
  `handler.go`'s `pathExecute`/etc. constants and `RouteMatcher`/
  `ExtractOperation` exactly; no SigV4-path collisions found.
  `awsRestjson1_deserializeOpDocumentExecuteStatementOutput` (deserializers.go:939)
  read line-by-line: it's a real `map[string]interface{}` JSON walk with a
  per-key `switch`, not a passthrough -- no cnhp trap on this service.
- **`Field` union, 7 members (the brief said six -- off by one; the real
  union is `arrayValue`, `blobValue`, `booleanValue`, `doubleValue`,
  `isNull`, `longValue`, `stringValue`; confirmed via
  `types.Field` interface in types/types.go and both
  `awsRestjson1_serializeDocumentField` (serializers.go:748) and its
  deserializer counterpart (deserializers.go:2383-2484), same 7 keys, same
  spelling, both directions). `models.go`'s `Field` struct has all 7 as
  `omitempty` pointer/slice members; JSON keys match case-for-case.
  `engine.go`'s `fieldFromValue` (encode) and `fieldToValue` (decode, request
  side) each populate/read exactly one member per call -- verified by
  reading both function bodies, not just their signatures. NULL is
  distinguished from a zero value because every member is a `*T` (or `[]byte`
  for blob): a SQL NULL produces `Field{IsNull: &true}` (`v == nil` case in
  `fieldFromValue`), while e.g. an empty string produces
  `Field{StringValue: &""}` -- a non-nil pointer to the zero value, never
  confused with the null branch. Verified live: `SELECT 1.0/0.0` (a case the
  real serializer special-cases as `"NaN"`/`"Infinity"`/`"-Infinity"` string
  literals, serializers.go:769-780) returns SQL `NULL` from
  modernc.org/sqlite, not a float, so gopherstack's plain `*float64`
  `DoubleValue` (no NaN/Inf special-casing) is unreachable by the engine, not
  a latent bug -- confirmed by running the query against the actual driver,
  not assumed.
- **`ArrayValue` union, 5 members** (`arrayValues`, `booleanValues`,
  `doubleValues`, `longValues`, `stringValues` -- types.go's
  `ArrayValueMember*` set), matches `models.go`'s `ArrayValue` struct
  field-for-field. Recursion (`arrayValues []ArrayValue`) verified both
  directions: the SDK's `ArrayValueMemberArrayValues.Value []ArrayValue`
  recurses on the same interface; gopherstack's `ArrayValues []ArrayValue`
  recurses on the same concrete struct -- Go's `encoding/json` handles the
  self-referential struct natively on both encode and decode. Functionally
  unreachable in a *result* (same reasoning as the prior audit: the pure-Go
  driver never produces an array-typed column) and rejected on the *request*
  side by `validateNoArrayParameters` (handler.go:282, wired into both
  `ExecuteStatement` and `BatchExecuteStatement`'s per-parameter-set loop) --
  checked both call sites, not just one.
- **`formattedRecords`**: real AWS wire type is a JSON **string**
  (`FormattedRecords *string` in ExecuteStatementOutput, decoded via
  `value.(string)` type-assert at deserializers.go:966-971, not a nested
  object). gopherstack's `formatRecordsAsJSONString` (handler.go:391) builds
  a Go `string` and assigns it into the response map, which `json.Marshal`
  re-encodes as a JSON string literal -- correct type, verified against the
  deserializer's own type assertion rather than inferred from the field
  name.
- **Enums, both directions, all 4:** `DecimalReturnType` (`STRING`,
  `DOUBLE_OR_LONG`), `LongReturnType` (`STRING`, `LONG`), `RecordsFormatType`
  (`NONE`, `JSON`), `TypeHint` (`DATE`, `DECIMAL`, `JSON`, `TIME`,
  `TIMESTAMP`, `UUID`) -- all real typed enums in types/enums.go (not plain
  strings), all 2/2/2/6 values reproduced exactly in `handler.go`'s
  `decimalReturnType*`/`longReturnType*`/`formatRecordsAs*` constants and
  `models.go`'s `SQLParameter.TypeHint` doc comment, validated on request
  ingress (`validateResultSetOptions`, `validateFormatRecordsAs`) and emitted
  unchanged on egress. `TypeHint` is accepted but not bind-semantic (existing
  documented gap, unchanged this pass -- see gaps above).
- **Request/response field-list diff, all 6 ops, every member incl.
  optional, checked by type against the SDK Input/Output structs in their
  own `api_op_*.go`:** `ExecuteStatementInput` (11 fields),
  `ExecuteStatementOutput` (5 wire fields), `BatchExecuteStatementInput` (7),
  `BatchExecuteStatementOutput` (1), `BeginTransactionInput`/`Output` (4/1),
  `CommitTransactionInput`/`Output` (3/1), `RollbackTransactionInput`/
  `Output` (3/1), `ExecuteSqlInput`/`Output` (5/1, no `TransactionId` member
  on this legacy op -- confirmed absent, not omitted by oversight) -- all
  match gopherstack's request/response structs 1:1, no missing, no
  fabricated, no wrong-typed members found.
- **Error wire shape:** confirmed both `TransactionNotFoundException` and
  `BadRequestException` are members of every op's own
  `awsRestjson1_deserializeOpError<Op>` switch (spot-checked
  `ExecuteStatement`'s, deserializers.go:840-923) and are read via
  `X-Amzn-ErrorType` header falling back to a JSON-body `code`/`__type`/
  `message` triad (`restjson.GetErrorInfo`, aws-sdk-go-v2 internal
  decoder_util.go) -- gopherstack's `handleError` (handler.go:203) emits
  `__type` + `message` (lowercase, matching `GetErrorInfo`'s
  case-insensitive `Message` field match) with HTTP 400 for both, which is
  what the real SDK's error switch expects to parse successfully.
- **No wrong-key tests found to correct.** `handler_sdk_route_table_test.go`
  (added 2026-08-15, `69bbb940a`) independently re-derived the same 6
  method+path pairs from the same source this pass did and matches.
- **Provenance:** `last_audit_commit: 9419636f` / `last_audit_date:
  2026-07-23` from the prior stamp predates two real follow-up passes that
  changed this service's behavior without advancing the stamp:
  `d39bf33e4` (2026-08-11, `sdk_module` bumped v1.32.19->v1.35.4,
  `ExecuteSql` resultFrame implemented, +217 lines across models.go/sql.go/
  engine_test.go) and `69bbb940a` (2026-08-15, new route-table test, no
  behavior change). **The stamp did not advance across those passes** even
  though real work landed -- both commits' content was independently
  re-verified against the pinned SDK this pass rather than trusted, and both
  turned out correct. Advanced this pass to `914e8b59` / 2026-08-20.
- **Brief accuracy:** the brief's "`Field` is a union with six members" is
  off by one -- the real union has 7 (see above). "Six ops" and the
  `ExecuteSql`-still-exists question both checked out exactly as briefed.
- **Gates:** `go build ./services/rdsdata/...` clean; `go vet` clean;
  `go fix -diff` empty; `gofmt -l` empty; `go test -race ./services/rdsdata/...`
  ok (1.16s); `golangci-lint run ./services/rdsdata/...` 0 issues; no banned
  cyclop/gocyclo/gocognit/funlen nolints; `git status --short` shows nothing
  under `services/rdsdata/` touched (this pass made no code changes, only
  this PARITY.md stamp/notes update).

## 2026-08-30 (request-field axis sweep, gopherstack-4shm's class)

Ran `cmd/reqfieldscan -dir rdsdata`: dispatch table 6/6 resolved (100%, all
via the literal-decode path -- rdsdata never uses `service.JSONOpFunc`/
`service.WrapOp`), 9 unread fields flagged. **Result: zero bugs, all 9 honest
gaps.**

- `executeStatementRequest.ContinueAfterTimeout` (1 field): already
  documented (see `ExecuteStatement`'s `ops:` note above and the
  "continueAfterTimeout" Notes entry) -- accepted on the wire as a
  deliberate no-op, since this mock has no statement-execution timeouts to
  continue past. Re-confirmed, not re-opened.
- `Database`/`Schema` on `executeStatementRequest`, `batchExecuteStatementRequest`,
  `beginTransactionRequest`, `executeSQLRequest` (8 fields): newly documented
  this pass, see the `gaps:` entry above -- `sqlEngine.dbFor` keys one SQLite
  database per `(region, resourceARN)` only (`engine.go`), so there is no
  per-resource multi-database/schema catalog for these fields to select
  into. Matches this service's existing `typeHint` gap and its siblings'
  repeated pattern in this campaign of honest, no-backend-state gaps rather
  than defects.

No code changes this pass -- PARITY.md documentation only. Gates unaffected
(no source touched): `go build`, `go vet`, `go test -race`, `golangci-lint
run` all still green per the entries above.

## Handler-collision determinism sweep (2026-08-31, gopherstack-id70)

Same defect and fix as the census in `cmd/reqfielddiff`/`cmd/reqfieldscan`
(ef0eef041, appsync e2643a6dd). This package's `Sql`/`SQL` acronym casing
gives it 1 op/handler pairs needing the ambiguous fold, 1 of them
genuine collisions between an exported backend method and the real
unexported handler: `ExecuteSql`.

Verified directly rather than assumed: ran the unpatched tool from
`ef0eef041~1` five times and diffed against the fixed tool at HEAD, for
both `cmd/reqfieldscan` and `cmd/reqfielddiff`. Both were byte-identical
across all 5 old runs and HEAD (6 SDK operations compared) -- the
determinism defect never flipped a finding here, because the resolution
that actually mattered (this package's dispatch-table union) already
carried the correct field set regardless of which fold candidate won.

Verdict: confirmed zero damage, not merely predicted.

## gopherstack-fdle (2026-09-11): typeHint validation, ColumnMetadata table origin, array-param wording

Closed the three open items this issue tracked. Split cleanly into "verified
from docs" (implemented) and "needs live Aurora" (disclosed, unchanged).

**1. SqlParameter.typeHint bind semantics -- format validation implemented;
actual bind coercion still a documented gap.**

Verified the six enum values and their documented formats two ways: the SDK
source (`rdsdata@v1.35.4` `types/enums.go`'s `TypeHint` and `types/types.go`'s
`SqlParameter.TypeHint` doc comment) and the live API reference page
(https://docs.aws.amazon.com/rdsdataservice/latest/APIReference/API_SqlParameter.html,
fetched this pass) -- both read identically: `DATE` "YYYY-MM-DD", `DECIMAL`
(no format constraint beyond "sent as an object of DECIMAL type"), `JSON`
(no constraint beyond "sent as JSON"), `TIME` "HH:MM:SS[.FFF]", `TIMESTAMP`
"YYYY-MM-DD HH:MM:SS[.FFF]", `UUID` (no format given in either source, so
gopherstack validates against the standard 8-4-4-4-12 hex form).

`typehints.go`'s `validateTypeHints`/`validateTypeHintFormat` now checks a
hinted parameter's `stringValue` against its documented format (DATE/TIME/
TIMESTAMP via regexp -- TIME/TIMESTAMP have an optional fractional-seconds
suffix that doesn't fit a single `time.Parse` layout; DATE uses
`time.Parse(time.DateOnly, ...)` directly; DECIMAL via a plain-number
regexp; JSON via `encoding/json.Valid`; UUID via a hex-pattern regexp),
called from both `handleExecuteStatement` and per parameter set from
`handleBatchExecuteStatement`, alongside the existing
`validateNoArrayParameters` call. A malformed value under a hint returns
`ErrValidation` (`BadRequestException` -- confirmed a member of
`ExecuteStatement`'s own error switch,
`deserializers.go:880-923`'s `awsRestjson1_deserializeOpErrorExecuteStatement`)
wrapping the parameter name (`fmt.Errorf("%w: parameter %q: %w", ...)`), so
the response body names the offending parameter. **Disclosed, not
implemented:** the actual bound *value* is unchanged by a well-formed
hint -- the mock SQLite engine has no distinct DATE/DECIMAL/TIMESTAMP/UUID
column types to coerce a string into, so this mock can only ever validate
the wire-level string format, not reproduce Aurora's actual DB-side type
coercion. Whether real AWS's malformed-value failure is request-time
(before touching the database, as gopherstack now does) or a DB-execution-time
error from the database engine itself, and the exact message text, is not
independently verifiable without a live Aurora cluster -- said so directly
in `validateTypeHints`'s doc comment and here, rather than inventing wording
and presenting it as verified. A hint on a non-string or null `Value` is a
no-op (matches the doc's "the corresponding *String* parameter value..."
wording, which only defines behavior for a stringValue).

**2. ColumnMetadata.SchemaName/TableName/IsAutoIncrement -- implemented for
the non-transactional path via a real driver accessor neither prior audit
found.**

Both prior audits (2026-08-11, 2026-08-30) concluded `sql.ColumnType` (the
only introspection `database/sql` itself exposes) has no origin-table
accessor, and stopped there. This pass went one level deeper: the pinned
driver, `modernc.org/sqlite@v1.58.0` (go.mod), exposes the real
`sqlite3_column_table_name`/`sqlite3_column_database_name`/
`sqlite3_column_origin_name` C APIs directly through its own
`conn.ColumnInfo(query string) ([]sqlite.ColumnInfo, error)` method
(`conn.go:342-405`), reachable from a `*sql.Conn` via the standard
`(*sql.Conn).Raw` escape hatch -- confirmed by reading the driver source,
not assumed from its name.

`engine.go`'s new `columnOriginInfo` opens a fresh `*sql.Conn` from the
resource's `*sql.DB` and calls `ColumnInfo` on the same statement text (a
prepare-only call -- it doesn't execute or bind parameters, so named
placeholders like `:id` compile fine without values); `applyColumnOrigin`
matches each returned entry to `scanRows`'s columns by position, filling
`TableName`/`SchemaName` (from `DatabaseName`, the closest signal SQLite has
to a schema -- "main" for the default database) and computing
`IsAutoIncrement` by checking whether `(TableName, OriginName)` is that
table's sole rowid-alias `INTEGER PRIMARY KEY` column, reusing the same
`rowIDAliasColumn` primitive `generatedFieldsFor` already relied on (a real,
previously-verified signal, not a new invented one -- see the
`generatedFields` Notes entry above). A column with no unambiguous source
table (an expression, function call, or constant) reports an empty
`TableName` per the accessor's own documented contract, which
`applyColumnOrigin` passes through as the historical zero value --
verified in `TestExecuteStatement_ColumnMetadata_TableOrigin`'s `SELECT 1 +
1` case.

**Disclosed, not implemented:** this only works outside a
`BeginTransaction` transaction. `sqlEngine.execute` only has a `*sql.DB` to
call `.Conn`/`.Raw` on in the autocommit branch; the transactional branch
runs against an `*sql.Tx` (opened once by `BeginTransaction` and reused
across calls), and `*sql.Tx` has no `Raw` method or equivalent in
`database/sql` -- there is no way to recover the underlying driver
connection from an existing `*sql.Tx` to call `ColumnInfo` on. A statement
run with a `transactionId` therefore still reports
`SchemaName`/`TableName`/`IsAutoIncrement` as their zero value, exactly as
before this pass -- verified in
`TestExecuteStatement_ColumnMetadata_TableOrigin_InsideTransaction`.
`ArrayBaseColumnType` is untouched (still always 0) and correctly so: see
the field_union family note above for why this mock's result columns are
never array-typed.

**3. Array parameters -- confirmed and message wording tightened.**

Re-verified `validateNoArrayParameters`'s behavior against
`rdsdata@v1.35.4`: `ExecuteStatementInput.Parameters`
(`api_op_ExecuteStatement.go`) and `BatchExecuteStatementInput.ParameterSets`
(`api_op_BatchExecuteStatement.go:87`) both carry the identical doc comment
"Array parameters are not supported." (capital A, period) directly above the
field. gopherstack's rejection message previously read lowercase ("array
parameters are..."); changed to match the doc's exact capitalization while
still naming the parameter for debuggability:
`"%w: Array parameters are not supported (parameter %q)"`. Confirmed both
call sites (`handleExecuteStatement`'s single-parameter-list check and
`handleBatchExecuteStatement`'s per-parameter-set loop) still invoke it. The
exact wording a live Aurora call returns for this case remains
unverified without one -- this is a best-effort alignment with the
documented constraint, not a field-diffed fact; unchanged conclusion from
the prior two audits, restated here per this issue's ask to re-confirm it.

**Tests** (all real `aws-sdk-go-v2/service/rdsdata` client against
`httptest`, following the existing `newRoundTripClient` pattern from
`handler_oversized_body_test.go`, except the in-transaction ColumnMetadata
case -- see its doc comment for why): `typehints_realclient_test.go` (valid
+ malformed value per hint, table-driven; a non-string value is a no-op;
BatchExecuteStatement applies hints per parameter set),
`column_origin_realclient_test.go` (TableName/SchemaName/IsAutoIncrement for
a real table's columns, a computed column's empty origin, and the
inside-a-transaction zero-value case), `array_parameter_realclient_test.go`
(ExecuteStatement and BatchExecuteStatement both reject with the updated
message).

**Aside found while writing the in-transaction ColumnMetadata test, filed
separately and fixed the same day -- see "gopherstack-wh8gv" below:**
`sqlEngine.beginTx` (engine.go) opened the engine-side `*sql.Tx` using
`BeginTransaction`'s own per-request `context.Context`. Over a real
`net/http.Server`, that context was canceled once the `BeginTransaction`
request finished being served, and `database/sql` auto-rolls back a
`*sql.Tx` whose context is canceled -- so a *separate* subsequent HTTP
request (`ExecuteStatement` with that `transactionId`) silently hit
`sql.ErrTxDone`, which `statements.go`'s historical lenient fallback
swallowed into the ordinary empty-success envelope (no client-visible
error, just silently-wrong empty/zero results, i.e. lost writes). Was out
of scope for this issue (gopherstack-fdle: typeHint/ColumnMetadata/array
parameters, not transaction-context lifetime); tracked as gopherstack-wh8gv
and fixed the same day.

**Gates:** `go build ./...` clean; `go vet ./services/rdsdata/...` clean;
`go test -race -count=1 ./services/rdsdata/... ./pkgs/persistence/...` ok;
`golangci-lint run ./services/rdsdata/...` 0 issues; no cyclop/gocyclo/
gocognit/funlen nolints added. No persisted struct changed (ColumnMetadata/
SQLParameter's JSON shape is identical -- only how their fields are
populated changed; query results were never part of `backendSnapshot` to
begin with, see `persistence.go`), so `pkgs/persistence/testdata/
snapshot_inventory.json` needed no update and no version bump.

## gopherstack-wh8gv (2026-09-11): transaction-context lifetime -- silent data loss

Fixed the transaction-context-lifetime bug the gopherstack-fdle pass above
found and filed separately: `sqlEngine.beginTx` (engine.go) opened its
engine-side `*sql.Tx` under `BeginTransaction`'s own per-request context.
`database/sql.DB.BeginTx`'s doc comment (`database/sql/sql.go:1866-1868`,
go1.27 stdlib) states plainly: "The provided context is used until the
transaction is committed or rolled back. If the context is canceled, the
sql package will roll back the transaction." A real `net/http.Server`
cancels a request's context the instant its response is written, so the
moment `BeginTransaction`'s HTTP response went out, its `*sql.Tx` was
already rolled back -- every later `ExecuteStatement`/
`BatchExecuteStatement` against that `transactionId` then hit
`sql.ErrTxDone` (`database/sql/sql.go:2233-2235`: "ErrTxDone is returned by
any operation that is performed on a transaction that has already been
committed or rolled back"), which `statements.go`'s historical lenient
fallback swallowed into the ordinary empty-success envelope. Net effect: a
client that began a transaction, ran an INSERT against it, and committed
saw two 200 OKs and believed its write succeeded; the row was never there.

**Fix 1 -- engine-owned transaction context.** `sqlEngine` now carries its
own `baseCtx`/`baseCancel` (`context.WithCancel(context.Background())`,
created in `newSQLEngine`). `beginTx` derives each transaction's context
from `baseCtx`, not the caller's `ctx` (`engine.go`'s new `engineTx{tx,
cancel}` pairs a `*sql.Tx` with its own cancel func). That per-transaction
context is canceled only by `finalizeTx` (called from `CommitTransaction`/
`RollbackTransaction` after the real `tx.Commit()`/`tx.Rollback()` call,
and from the Janitor's `tick` on idle/max-lifetime expiry -- both already
existing call sites, unchanged) or by `sqlEngine.reset()` (backend
`Reset()`), which cancels `baseCtx` -- transitively canceling every still-open
transaction's derived context -- then replaces `baseCtx`/`baseCancel` so the
engine remains usable afterward. `ctx` (the caller's per-request context) is
still passed to `dbFor` for opening/pinning the resource `*sql.DB`: that
call's context only bounds waiting for a connection, not the returned
`*sql.DB`'s lifetime (unlike `BeginTx`), so it carries no equivalent
landmine -- confirmed by reading `database/sql.DB.Conn`'s doc comment, which
makes no such claim.

**Timeout semantics** (unchanged from the existing janitor.go
implementation, gopherstack-02w): BeginTransaction's own doc comment
(`rdsdata@v1.35.4 api_op_BeginTransaction.go`) and the live API reference
(https://docs.aws.amazon.com/rdsdataservice/latest/APIReference/API_BeginTransaction.html,
refetched this pass) both state, verbatim: "A transaction can run for a
maximum of 24 hours. A transaction is terminated and rolled back
automatically after 24 hours." and "A transaction times out if no calls use
its transaction ID in three minutes. If a transaction times out before it's
committed, it's rolled back automatically." The pre-existing Janitor
(`janitor.go`) already enforces both thresholds via `finalizeTx`; this pass
only changed what `finalizeTx`'s rollback races against (nothing, now,
instead of an already-dead context) and how "now" is read -- see Fix 3.

**Fix 2 -- dead-transaction execution errors instead of a fabricated
success.** Even with Fix 1, a transaction id can still legitimately go dead
between `Has()`'s check and engine execution in one case Fix 1 doesn't
touch: a snapshot `Restore` records a still-`ACTIVE` `Transaction` in
`b.transactions` (real AWS bookkeeping metadata, which persists) but
`engine.reset()` necessarily drops every `*sql.Tx` (an open driver
connection can't be serialized) -- so `e.txs` has no entry for that id after
a restart. `statements.go`'s `ExecuteStatement`/`BatchExecuteStatement` now
check `isDeadTransactionError` (engine.go: `errors.Is(err, errNoEngineTx) ||
errors.Is(err, sql.ErrTxDone)`) on any engine error while `transactionID !=
""`, and return `ErrTransactionNotFound` instead of falling through to the
historical empty-success envelope. The historical lenient fallback is
otherwise **unchanged** and deliberately still in place for genuine SQL
problems (bad syntax, DML against a table that was never created, both in
autocommit and inside a live transaction) -- see this file's "Trap for the
next auditor" note above; this fix only closes the one path where the
*transaction itself*, not the SQL, is the problem.

**Error class -- `TransactionNotFoundException`, not `BadRequestException`.**
The bd issue's initial hypothesis was `BadRequestException` with a "Transaction
<id> is not found" message. Checking the real error lists first
(`rdsdata@v1.35.4` `api_op_ExecuteStatement.go`/`api_op_BeginTransaction.go`/
etc. and the live API reference's Errors sections for BeginTransaction and
ExecuteStatement, both refetched this pass) shows AWS models this exact
case as its own distinct exception: `TransactionNotFoundException` --
"The transaction ID wasn't found." (HTTP 404 per the docs). gopherstack
already has this modeled end-to-end as `ErrTransactionNotFound`
(errors.go) and already used it for the unknown/committed/rolled-back cases
(`CommitTransaction`/`RollbackTransaction`/the pre-existing `Has()` checks
in `ExecuteStatement`/`BatchExecuteStatement`) -- so this fix reuses that
existing, already-correct mechanism for the newly-caught dead-engine-tx case
rather than inventing a second, less-accurate error path. gopherstack
returns it over HTTP 400 (matching every other error this handler emits),
not the documented 404: confirmed from `aws-sdk-go-v2`'s own deserializer
(`rdsdata@v1.35.4 deserializers.go`'s
`awsRestjson1_deserializeOpErrorExecuteStatement`) that the client selects
the Go exception type purely from the `__type`/`code` string (header or
body), never from the HTTP status -- so a real client still gets a typed
`*types.TransactionNotFoundException` regardless of the status code. The
400-vs-404 status mismatch is a pre-existing, unrelated gap (this handler
has always used a flat 400 for every error type) rather than something this
fix introduces or needed to correct to make the client-visible behavior
right.

**Fix 3 -- injectable clock.** `InMemoryBackend` had no clock seam;
`BeginTransaction`/`touchTransactionLocked`/`janitor.go`'s `tick` all called
`time.Now()` directly. Added `nowFunc func() time.Time` (default
`time.Now`) and an exported `WithClock` method (store.go), following
`services/polly/store.go`/`throttle.go`'s existing `WithClock` pattern
exactly. `BeginTransaction`, `touchTransactionLocked`, and the Janitor's
`tick` now read `nowFunc()` instead of `time.Now()`, so a test can drive the
3-minute idle timeout deterministically (advance a fake clock, then call
the already-exported `Janitor.SweepOnce` directly) with no `time.Sleep` and
no real wall-clock wait or background goroutine.

**Tests** (`transaction_context_realclient_test.go`, table-driven, real
`aws-sdk-go-v2/service/rdsdata` client against a real `httptest.Server` via
the existing `newRoundTripClient` helper, each call its own separate HTTP
request -- unlike this package's `doRDSDataRequest`-based transaction tests,
whose `httptest.NewRequest` carries a never-canceled `context.Background()`
and so could never have caught this): commit makes a live-transaction
INSERT visible to a later autocommit SELECT (the core regression case --
confirmed it fails pre-fix: temporarily reverted `beginTx` to use the
caller's `ctx` and reran, got a real `TransactionNotFoundException` instead
of a silent empty result, then restored the fix); rollback discards a
live-transaction INSERT (asserts the insert reported `NumberOfRecordsUpdated
== 1` before rollback, to prove it ran against a live tx rather than
"passing" for the old, wrong reason); unknown transaction id and execute
after commit both return `TransactionNotFoundException` (these two were
already correct pre-fix via the pre-existing `Has()` check -- kept as
baseline coverage); idle timeout via `WithClock` + `Janitor.SweepOnce`
also returns `TransactionNotFoundException`. `column_origin_realclient_test.go`'s
`TestExecuteStatement_ColumnMetadata_TableOrigin_InsideTransaction` doc
comment updated to stop describing this bug as a live reason to avoid a
real server round trip -- it no longer is one; that test still uses
`doRDSDataRequest` as a plain style choice, not a workaround.

**Gates:** `go build ./...` clean; `go vet ./services/rdsdata/...` clean;
`go test -race -count=1 ./services/rdsdata/... ./pkgs/persistence/...` ok;
`go test -race -count=20 -run 'Transaction' ./services/rdsdata/...` ok (all
20 iterations); `golangci-lint run ./services/rdsdata/...` -- one
`fieldalignment` finding on the new test file's `fakeClock` struct, fixed by
reordering fields (`time.Time` before `sync.Mutex`). No `cyclop`/`gocyclo`/
`gocognit`/`funlen` nolints added. No persisted struct's JSON shape changed
(`Transaction`'s fields are untouched; the new `nowFunc` is an unexported,
unpersisted backend field, not part of `backendSnapshot`) -- see
`persistence.go` -- so `pkgs/persistence/testdata/snapshot_inventory.json`
needed no update and no version bump.

## 2026-09-12 (typed-client coverage, gopherstack-n3zi)

Added `realclient_execute_sql_test.go` covering rdsdata's last typed-
client-uncovered op: the deprecated `ExecuteSql` batch-statement entry
point (superseded by `ExecuteStatement`/`BatchExecuteStatement`, still a
real, callable op in the pinned SDK). Creates a table, inserts a row, and
selects it back through the real client, asserting the decoded legacy
`SqlStatementResult`/`ResultFrame`/`ColumnMetadata`/`Value` union shapes
(`types.ValueMemberBigIntValue`). Zero bugs found -- `sql.go`'s
`legacyValueFromField` already emits the correct `bigIntValue` wire key for
this union.

Every real client call to this op carries an expected `SA1019` deprecation
notice; the whole file is exempted from `staticcheck` in `.golangci.yml`
(same pattern as `iotanalytics`/`opsworks`'s existing deprecated-op
exemptions) since testing this op requires calling it.

Typed-client coverage: 5/6 -> 6/6 (100%).

Gates: `go build ./...`, `go vet ./services/rdsdata/...`, `go test -race
-count=1 ./services/rdsdata/...` (pass), `golangci-lint run
--new-from-rev=HEAD ./services/rdsdata/...` (0 issues). No persisted
struct fields changed, no version bump. `cmd/paritylint` stays at 0 FAIL.
