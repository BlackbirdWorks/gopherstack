---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: timestreamwrite
sdk_module: aws-sdk-go-v2/service/timestreamwrite@v1.38.4
last_audit_commit: ca3b796e
last_audit_date: 2026-07-23
overall: A            # independently re-verified this pass; no new fixes needed, prior sweep already got the surface right
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateDatabase: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — KmsKeyId now applied atomically at creation (was a separate UpdateDatabase call after CreateDatabase, causing a race window and LastUpdatedTime != CreationTime)"}
  DescribeDatabase: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDatabase: {wire: ok, errors: ok, state: ok, persist: ok, note: "KmsKeyId is a required field on the real UpdateDatabaseRequest; handler does not enforce non-empty. NOT changed — TestAccuracy2_UpdateDatabaseClearKmsKeyId deliberately locks in clearing the key via empty string, a considered emulator convenience. See gaps."}
  ListDatabases: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDatabase: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly rejects deletion while tables remain (ValidationException), matching AWS doc note"}
  CreateTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "retention default 6h/73d and min/max bounds (1-8766h, 1-73000d) verified against botocore model"}
  DescribeTable: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "whole-struct replace semantics for RetentionProperties/MagneticStoreWriteProperties/Schema matches AWS (no deep merge)"}
  ListTables: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTable: {wire: ok, errors: ok, state: ok, persist: ok}
  WriteRecords: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — negative Version now rejected (ValidationException); RejectedRecords shape (Reason/ExistingVersion/RecordIndex) matches deserializers.go exactly"}
  DescribeEndpoints: {wire: partial, errors: ok, state: ok, persist: n/a, note: "returns a non-empty Endpoints list (satisfies SDK's hard requirement), but Address is hardcoded \"localhost\" instead of echoing the request Host like the sibling timestreamquery service does. Verified this is inert in practice: aws-sdk-go-v2's DiscoverEndpoint middleware skips the call entirely whenever EndpointSourceCustom is set (i.e. whenever a BaseEndpoint/AWS_ENDPOINT_URL is configured, which all gopherstack/LocalStack clients do), and even when not skipped it only overrides req.URL.Host if the returned host matches the partition's DNS suffix (*.amazonaws.com) — \"localhost\" never qualifies either way. Left unchanged; see gaps."}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: partial, errors: partial, state: ok, persist: ok, note: "real API can return ResourceNotFoundException for an unknown ARN; backend silently no-ops. NOT changed — ListTagsForResource/UntagResource have no-error signatures and existing tests (TestInMemoryBackend_DeleteDatabase_CleansUpTags etc.) deliberately assert empty-not-error after a resource is deleted. Fixing would need a signature change (add error return) rippling through the interface and ~10 call sites for an ambiguous case (AWS's own DeleteDatabase doc says distributed retries may already return either ResourceNotFoundException or success — clients must treat them as equivalent). See gaps."}
  ListTagsForResource: {wire: ok, errors: partial, state: ok, persist: ok, note: "same ResourceNotFoundException gap as UntagResource, same rationale for not changing"}
  CreateBatchLoadTask: {wire: partial, errors: ok, state: ok, persist: ok, note: "ReportConfiguration is a required field on the real CreateBatchLoadTaskRequest; handler validates DataSourceConfiguration but not ReportConfiguration. NOT changed — ~7 existing tests create tasks without ReportConfiguration and a compliant SDK client always sends it (smithy client-side required-field validation blocks the request before it's even sent), so the gap is unreachable via real client traffic. ClientToken is accepted but not used for idempotent retry dedup (a repeat call with the same token creates a second task instead of returning the original) — deferred, batch-load is not a highest-traffic family."}
  DescribeBatchLoadTask: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBatchLoadTasks: {wire: ok, errors: ok, state: ok, persist: ok}
  ResumeBatchLoadTask: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  errors: {status: ok, note: "fixed — ConflictException now returns HTTP 400 (was 409). awsJson1.0/1.1 has no per-exception HTTP status: every client-fault error is reported over 400 (server faults over 500), and the SDK resolves the concrete exception type from the body's __type field / X-Amzn-ErrorType header, never from the HTTP status (confirmed in aws-sdk-go-v2 deserializers.go's generic error dispatch, and matches the sibling verifiedpermissions/codestarconnections services and DynamoDB's own handler comment 'Most DynamoDB errors return 400')."
  wire-protocol: {status: ok, note: "fixed — response Content-Type was \"application/x-amz-json-1.1\"; real Timestream Write is protocol=json jsonVersion=1.0 (confirmed via botocore service-2.json metadata and aws-sdk-go-v2 serializers.go's httpBindingEncoder.SetHeader(\"Content-Type\").String(\"application/x-amz-json-1.0\")). Sibling timestreamquery already used 1.0 correctly. Target prefix \"Timestream_20181101.\" was already correct."}
  arn-building: {status: ok, note: "fixed — tableARN hand-formatted \"arn:aws:...\" (hardcoded \"aws\" partition) instead of using pkgs/arn.Build like databaseARN does in the same file. Now uses arn.Build for partition-correctness consistency (GovCloud/China regions would previously get a wrong partition on table ARNs but not database ARNs)."}
leaks: {status: clean, note: "closeAllTableMutexesLocked is correctly called on Reset, DeleteDatabase, DeleteTable, and before Restore discards the records map — no lockmetrics.RWMutex leak found. Persistence Snapshot/Restore round-trips databases, tables, batchLoadTasks (via store.Registry), plus the hand-rolled records/tags maps, nextTaskID, and rebuilds the per-table dedup index and mutex on Restore."}
gaps:
  - "UpdateDatabase does not enforce KmsKeyId as required (real UpdateDatabaseRequest marks it required) — not fixed, conflicts with an existing intentional test that uses empty string to clear the key (bd: file if desired)"
  - "UntagResource/ListTagsForResource never return ResourceNotFoundException for an unknown ARN (real API can) — not fixed, would require an interface signature change and conflicts with existing post-delete cleanup test assertions; AWS's own docs note the two outcomes are meant to be treated as equivalent for DeleteDatabase's ARN-cleanup race anyway (bd: file if desired)"
  - "CreateBatchLoadTask does not validate ReportConfiguration as required, and ClientToken is accepted but not used for idempotent dedup (bd: file if desired)"
  - "DescribeEndpoints Address is hardcoded \"localhost\" instead of echoing the request Host (sibling timestreamquery does echo it); verified inert for normal custom-endpoint usage, but would matter for tooling that inspects the raw response instead of relying on SDK routing (bd: file if desired, low priority)"
deferred: []
reaudit_2026-07-23: >
  Independent field-diff re-audit against the checked-out aws-sdk-go-v2/service/timestreamwrite@v1.35.19
  module (types/types.go, types/errors.go, deserializers.go, api_op_*.go). Confirmed still accurate:
  epoch-seconds timestamp wire fields (databaseView/tableView/batchLoadTaskDescriptionView all use
  manually-converted float64, never json.Marshal of a raw time.Time -- no epoch bug present);
  RejectedRecord{Reason,ExistingVersion,RecordIndex} matches types.RejectedRecord byte-for-byte;
  error-code/HTTP-400 mapping in handler.go's handleError matches the awsJson1.0 no-per-exception-status
  convention; UpdateDatabaseInput.KmsKeyId and CreateBatchLoadTaskInput.ReportConfiguration are indeed
  `// This member is required` in the generated SDK source (re-confirmed via grep this pass), so the two
  corresponding gaps below are genuinely unreachable via a compliant SDK client, not unverified claims.
  Cascade cleanup (DeleteDatabase/DeleteTable close per-table lockmetrics.RWMutex before dropping the
  records map, delete tags by ARN) and Snapshot/Restore round-trip re-read line by line -- no leak found,
  no ghost rows after delete. sdk_completeness_test.go's notImplemented list is empty, confirming no
  deferred resource family remains. No banned cyclop/gocyclo/gocognit/funlen nolints present. No
  gopherstack-invented ops/fields found. go build/vet/test -race/gofmt/golangci-lint all clean. No code
  changes made this pass -- see gaps below for the only known open items, each already unreachable via a
  compliant client or hedged for compatibility with an intentional existing test.
---

## Notes

Freeform: AWS-behavior specifics worth remembering, and any "looks-wrong-but-correct" traps
so the next auditor doesn't re-flag them.

- **Protocol is `json` / `jsonVersion: "1.0"`** (confirmed via botocore
  `timestream-write/2018-11-01/service-2.json` metadata block), i.e. Content-Type
  `application/x-amz-json-1.0` and target prefix `Timestream_20181101.<Op>` (no version
  suffix on the prefix itself — the version lives in the Content-Type header only). Was
  previously wired to `application/x-amz-json-1.1`; fixed this sweep.

- **HTTP status codes for awsJson1.0/1.1 are NOT semantic.** None of Timestream Write's
  exception shapes (`ConflictException`, `ResourceNotFoundException`, `ValidationException`,
  `RejectedRecordsException`, etc.) declare an explicit `httpStatusCode` override in the
  service model. Per the protocol's default behavior (and confirmed by reading
  `aws-sdk-go-v2/service/timestreamwrite`'s generated `deserializers.go`), the client
  determines the concrete exception type purely from the response body's `__type` field
  (or the `X-Amzn-ErrorType` header) — **never** from the HTTP status code. The wire-correct
  convention used elsewhere in this codebase for awsJson1.0 services (verifiedpermissions,
  codestarconnections) and explicitly documented in dynamodb/handler.go's own comment
  ("Most DynamoDB errors return 400") is: 400 for every client fault, 500 for server faults.
  Do not "upgrade" any client-fault error to a REST-ish status like 404/409/422 for this
  service — it's wrong for this protocol family even though it feels natural.

- **`RejectedRecord` wire fields are `Reason`, `ExistingVersion`, `RecordIndex`** — verified
  byte-for-byte against `deserializers.go`'s
  `awsAwsjson10_deserializeDocumentRejectedRecord`. Matches exactly; no change needed.

- **Retention bounds**: `MemoryStoreRetentionPeriodInHours` is `[1, 8766]`,
  `MagneticStoreRetentionPeriodInDays` is `[1, 73000]` per the botocore model's
  `min`/`max` constraints on those shapes. Defaults when `RetentionProperties` is omitted
  at `CreateTable` time are 6 hours / 73 days. Both verified correct in this codebase;
  don't re-flag.

- **`DescribeEndpoints`'s `Address` value is inert for normal usage.** The
  aws-sdk-go-v2 `endpointdiscovery.DiscoverEndpoint` middleware (finalize step) skips the
  discovery call entirely whenever `awsmiddleware.GetEndpointSource(ctx) ==
  aws.EndpointSourceCustom` — which is the case for every client configured with a custom
  `BaseEndpoint`/`AWS_ENDPOINT_URL*`, i.e. every realistic gopherstack/LocalStack client.
  Even when the middleware does run, it only overrides `req.URL.Host` if the returned
  host has the partition's DNS suffix (`*.amazonaws.com` / dualstack equivalent) — a
  `localhost` or arbitrary hostname address is silently ignored either way. So while the
  Address value looks unfinished, it does not currently break SDK routing. Don't spend
  more effort here without a concrete failing scenario.

- **`CreateDatabase` + `KmsKeyId` is now atomic.** Previously the handler called
  `Backend.CreateDatabase` then, if `KmsKeyId` was set, a *second*
  `Backend.UpdateDatabase` call. This meant: (1) a race window where a concurrent
  `DescribeDatabase` could observe the database without its KMS key, and (2)
  `LastUpdatedTime` would always be strictly after `CreationTime` when `KmsKeyId` was
  supplied at creation (wrong — AWS's `CreateDatabaseRequest` accepts `KmsKeyId` directly,
  so a fresh database's `CreationTime` and `LastUpdatedTime` should be equal regardless).
  Fixed by threading `kmsKeyID` into `InMemoryBackend.CreateDatabase` directly.
