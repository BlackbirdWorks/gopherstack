---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: glacier
sdk_module: aws-sdk-go-v2/service/glacier@v1.35.4
last_audit_commit: f8ae77eb7c84189d9fca29cce357a9cfaf72fd9c
last_audit_date: 2026-08-10
overall: A            # both deferred resource families (Select jobs, range inventory retrieval) now implemented for real + field-diffed; 1 pre-existing leak fixed; select jobs now write real S3 OutputLocation output
ops:
  CreateVault:            {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeVault:          {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVault:            {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade-deletes jobs/uploads/lock; blocks on non-empty vault; this pass fixed a leak where cascade-deleting a vault's multipart uploads dropped the store.Table row but orphaned the raw multipartParts map entry (see Notes). gopherstack-ygfk (THIS PASS): now consults the vault's lock policy (checkVaultLockDelete) before deleting -- see families: vault_lock_enforcement"}
  ListVaults:             {wire: ok, errors: ok, state: ok, persist: ok, note: "marker/limit pagination verified vs SDK Marker/VaultList shape"}
  UploadArchive:          {wire: ok, errors: ok, state: ok, persist: ok, note: "ArchiveId/Checksum/Location are header-only on real wire (confirmed via awsRestjson1_deserializeOpHttpBindingsUploadArchiveOutput); gopherstack sets all three headers correctly, body is a harmless bonus"}
  DeleteArchive:          {wire: ok, errors: ok, state: ok, persist: ok, note: "gopherstack-ygfk (THIS PASS): now consults the vault's lock policy (checkVaultLockDelete) before deleting -- see families: vault_lock_enforcement"}
  InitiateJob:            {wire: ok, errors: ok, state: ok, persist: ok, note: "response is header-only (X-Amz-Job-Id/x-amz-job-output-path/Location) on real wire; verified. This pass added real support for JobParameters.Type=select (SelectParameters/OutputLocation, full field validation, MissingParameterValueException vs InvalidParameterValueException distinguished) and JobParameters.InventoryRetrievalParameters (range inventory retrieval: StartDate/EndDate/Limit/Marker, validated) -- see Notes"}
  DescribeJob:            {wire: ok, errors: ok, state: ok, persist: ok, note: "GlacierJobDescription now also carries JobOutputPath/OutputLocation/SelectParameters (select jobs) and a proper nested InventoryRetrievalParameters object (range inventory retrieval jobs) -- see Notes for the invented top-level Format field this replaced"}
  ListJobs:               {wire: ok, errors: ok, state: ok, persist: ok, note: "same describeJobResponse DTO as DescribeJob, same coverage applies"}
  GetJobOutput:           {wire: ok, errors: ok, state: ok, persist: ok, note: "archive-retrieval/inventory-retrieval unchanged; select jobs execute their SQL Expression for real against the stored archive and serve it directly (see select_jobs family note) -- a documented gopherstack convenience, not real AWS behavior (GetJobOutput's own docs cover only archive/inventory output, never Select)"}
  SetVaultNotifications:      {wire: ok, errors: ok, state: ok, persist: ok}
  GetVaultNotifications:      {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVaultNotifications:   {wire: ok, errors: ok, state: ok, persist: ok}
  SetVaultAccessPolicy:       {wire: ok, errors: ok, state: ok, persist: ok, note: "gopherstack-ygfk: stored and echoed, DELIBERATELY still not enforced -- unlike vault lock policy (see families: vault_lock_enforcement), a vault access policy's documented purpose is granting/restricting access by Principal (cross-account/-role access control), which this emulator cannot evaluate without per-request caller identity (tracked separately, gopherstack-cu4g). Disclosed, not approximated."}
  GetVaultAccessPolicy:       {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVaultAccessPolicy:    {wire: ok, errors: ok, state: ok, persist: ok}
  AddTagsToVault:         {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForVault:       {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTagsFromVault:    {wire: ok, errors: ok, state: ok, persist: ok}
  InitiateVaultLock:      {wire: ok, errors: ok, state: ok, persist: ok, note: "gopherstack-ygfk (THIS PASS): fixed two bugs found while wiring enforcement. (1) LockId was JSON-body-only; real AWS returns it via the x-amz-lock-id response header only (confirmed via awsRestjson1_deserializeOpHttpBindingsInitiateVaultLockOutput, which never touches the body) -- a real SDK client got a nil LockId and could never call CompleteVaultLock. Header now set; JSON body kept as a harmless bonus, same pattern as UploadArchive. (2) the request body's top-level JSON unmarshal error was silently discarded (_ = json.Unmarshal(...)), so a malformed request body was accepted with an empty Policy rather than rejected -- see families: vault_lock_enforcement for the policy-content validation fix alongside it."}
  AbortVaultLock:         {wire: ok, errors: ok, state: ok, persist: ok}
  CompleteVaultLock:      {wire: ok, errors: ok, state: ok, persist: ok}
  GetVaultLock:           {wire: ok, errors: ok, state: ok, persist: ok, note: "24h InProgress expiry verified"}
  GetDataRetrievalPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FreeTier default matches AWS"}
  SetDataRetrievalPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  InitiateMultipartUpload:   {wire: ok, errors: ok, state: ok, persist: ok, note: "response header-only (Location/x-amz-multipart-upload-id) confirmed"}
  UploadMultipartPart:       {wire: ok, errors: ok, state: ok, persist: ok}
  CompleteMultipartUpload:   {wire: ok, errors: ok, state: ok, persist: ok, note: "response header-only (ArchiveId/Checksum/Location) confirmed, same as UploadArchive"}
  AbortMultipartUpload:      {wire: ok, errors: ok, state: ok, persist: ok}
  ListMultipartUploads:      {wire: ok, errors: ok, state: ok, persist: ok}
  ListParts:                 {wire: ok, errors: ok, state: ok, persist: ok}
  ListProvisionedCapacity:      {wire: ok, errors: ok, state: ok, persist: ok}
  PurchaseProvisionedCapacity:  {wire: ok, errors: ok, state: ok, persist: ok, note: "2-unit cap + monthly expiry verified"}
families:
  route_matching: {status: ok, note: "RouteMatcher + parseGlacierPath path/method table cross-checked against every literal opPath in serializers.go (SplitURI calls) -- all 32 ops match prefix+method; no unreachable-op bug found"}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to InMemoryBackend.Snapshot/Restore (persistence.go); registered snapshot version-guarded (glacierSnapshotVersion); cli.go wiring not touched/verified this pass (out of scope), but Handler exposes the exact Snapshot(ctx)[]byte / Restore(ctx,[]byte)error signature setupPersistence expects. This pass verified the new Job fields (SelectParameters/OutputLocation/JobOutputPath, InventoryRetrieval* range fields) round-trip through Snapshot/Restore (TestPersistenceRoundTrip_SelectAndRangeInventoryJobs) -- additive fields on an already-JSON-round-trippable struct, no snapshot version bump needed"}
  select_jobs: {status: ok, note: "IMPLEMENTED (2026-07-24 pass; S3 write-back added 2026-08-10). InitiateJob Type=select is fully validated (ArchiveId existence, SelectParameters.Expression/ExpressionType=SQL/InputSerialization.Csv/OutputSerialization.Csv all required with MissingParameterValueException vs InvalidParameterValueException distinguished per-field, OutputLocation.S3.BucketName required, Expression syntax-checked) and the SQL query is REALLY executed against the stored archive bytes (select.go/select_sql.go). RESOLVED (2026-08-10): the earlier 'no cross-service S3 write-back' framing was stale -- gopherstack has an S3 backend and this codebase wires cross-service S3 integrations routinely (DynamoDB/MGN/SageMaker precedent). A completed select job now writes its real S3 OutputLocation output when an S3 backend is wired (cli.go's wireGlacierS3): <prefix>/<jobID>/job.txt, results/1, result_manifest.txt (or errors/1 + error_manifest.txt on query failure), matching the exact key layout documented in glacier-select.md's 'S3 Glacier Select Output' section (awsdocs/amazon-glacier-developer-guide, doc_source/glacier-select.md:49-65). GetJobOutput continues to ALSO serve the same real (not stubbed) computed bytes directly as a documented gopherstack convenience -- confirmed via aws-sdk-go-v2/service/glacier@v1.35.4's GetJobOutput doc and api-job-output-get.md that real AWS's GetJobOutput contract covers only archive-retrieval and inventory-retrieval output, never Select, so there is no real behavior to cite for rejecting it there instead. See select_output.go and select.go's package doc."}
  range_inventory_retrieval: {status: ok, note: "IMPLEMENTED this pass (was deferred). InventoryRetrievalParameters (StartDate/EndDate/Limit/Marker) on InitiateJob is validated (ISO-8601 dates, positive-integer Limit) and echoed back correctly nested under InventoryRetrievalParameters on DescribeJob/ListJobs (inventory_retrieval.go). GetJobOutput's inventory listing is actually filtered by the stored parameters: StartDate inclusive / EndDate exclusive bound on Archive.CreationDate, Marker resumes strictly after the named ArchiveId, Limit caps the count -- filterArchivesForInventory, covered by TestGetJobOutput_InventoryRetrieval_{DateRangeFilters,Limit,Marker}."}
  vault_lock_enforcement: {status: ok, note: "FIXED this pass (gopherstack-ygfk, the mirror of gopherstack-cqy3's cloudformation stack-policy fix): InitiateVaultLock/SetVaultLock stored a policy on VaultLock.Policy and GetVaultLock echoed it, but neither DeleteArchive nor DeleteVault ever read it -- a Vault Lock policy denying deletion did nothing, the exact write-only-state class this issue tracks. Fixed via vault_lock_policy_eval.go (new): parses Statement[].{Effect,Action,Resource,Condition}, evaluated from checkVaultLockDelete (vault_lock.go) called by both DeleteArchive and DeleteVault before mutating any state, while the lock is InProgress OR Locked (AWS's documented 'test your policy before locking it down' workflow evaluates requests against an InProgress lock too, per vault-lock.html). Implemented: Effect=Deny only (Action glacier:DeleteArchive/glacier:DeleteVault with '*' wildcards, Resource the vault ARN with '*' wildcards per glacier-api-permissions-ref.html's vaults/example*, vaults/* patterns), plus the canonical glacier:ArchiveAgeInDays numeric condition (NumericLessThan/LessThanEquals/GreaterThan/GreaterThanEquals/Equals) against Archive.CreationDate -- the documented WORM/retention use case (vault-lock-policy.html Example 1: 'Deny Deletion Permissions for Archives Less Than 365 Days Old'). DISCLOSED, not approximated: Effect=Allow is parsed but grants nothing (no IAM baseline in this emulator for a resource policy to combine with, and AWS documents no CloudFormation-style default-deny-once-a-policy-exists rule for Glacier the way it does for stack policies -- fabricating one would risk blocking permitted deletes); Principal is parsed but not evaluated (no per-request caller identity, gopherstack-cu4g -- every AWS-documented Vault Lock example uses Principal '*' since the feature's whole point is 'prevent anyone, including the account owner'); the ResourceTag condition key (Example 2's legal-hold pattern) is not implemented (Glacier archives carry no tags in this emulator); only DeleteArchive/DeleteVault consult the policy, not UploadArchive/InitiateJob/other Vault-Lock-governable actions (out of scope for a deletion-protection pass). Evaluation semantics are TRANSCRIBED FROM AWS'S DOCUMENTATION (vault-lock.html, vault-lock-policy.html, glacier-api-permissions-ref.html), not the SDK -- the policy body is an opaque string with no wire type in aws-sdk-go-v2, same disclosure shape as cloudformation's stack policy. Also fixed: SetVaultLock now rejects malformed policy JSON at write time (previously accepted, would never have enforced anything even after this fix, same bug class as cloudformation's SetStackPolicy); InitiateVaultLock's request-body top-level JSON unmarshal error was silently discarded, now returns 400 (see ops: InitiateVaultLock for the two wire bugs -- missing x-amz-lock-id header, swallowed unmarshal error -- found while adding this and fixed alongside it). Verified via TestVaultLockPolicy_DeleteEnforcement (sdk_vault_lock_enforcement_test.go), driven through the real aws-sdk-go-v2 client: a blanket Deny blocks both DeleteArchive and DeleteVault and the resource is provably unchanged afterward (DescribeVault NumberOfArchives / a follow-up DescribeVault succeeding), a Deny scoped to a different vault or a different action does not block, the ArchiveAgeInDays condition both blocks and (once its threshold isn't met) permits, enforcement holds during the InProgress test window not only once Locked, no lock ever initiated allows deletion, and malformed policy JSON is rejected at InitiateVaultLock. Hand-reverted checkVaultLockDelete to a no-op and confirmed exactly the 4 refusal-asserting subtests fail while the 5 permitted-path subtests still pass, then restored."}
gaps:
  - select_sql_subset: "VERIFIED 2026-08-10 against awsdocs/amazon-glacier-developer-guide's doc_source/s3-glacier-select-sql-reference*.md (the real SQL reference, shared verbatim with S3 Select except where a page says '(Amazon S3 Select only)'). Correct-as-is: JOINs/subqueries are genuinely unsupported by real Glacier Select too ('Amazon S3 Select and S3 Glacier Select queries currently do not support subqueries or joins' -- s3-glacier-select-sql-reference-select.md), so gopherstack's lack of joins is not a gap. Real gaps (real Glacier Select supports these, gopherstack does not): CAST (s3-glacier-select-sql-reference-conversion.md: 'Amazon S3 Select and S3 Glacier Select support the following conversion functions: CAST' -- no '(S3 Select only)' qualifier), NOT/BETWEEN/IN/LIKE operators and arithmetic (+ - * %) (s3-glacier-select-sql-reference-operators.md's Logical/Comparison/Pattern-Matching/Math Operators sections), and COALESCE/NULLIF (s3-glacier-select-sql-reference-conditional.md). Closing these is moderate: BETWEEN/IN/LIKE/NOT extend select_sql.go's existing predicate grammar (parsePredicate/selectPredicateMatches) without new architecture; arithmetic and CAST need a real scalar-expression evaluator (select_sql.go's WHERE/SELECT-list values are currently bare column refs or literals, not expressions) -- a bigger, structural addition. Parenthesized/nested-boolean grouping has NO citable evidence either way: the real SQL reference's exhaustive 'Scalar Expressions' grammar list (literal | column_reference | unary_op expr | expr binary_op expr | func_name | BETWEEN | LIKE) never includes a generic '( expression )' grouping form, unlike CAST/IN/COALESCE's function-call parens, so gopherstack's flat OR-of-AND WHERE clause (no parenthesized override) is left as-is rather than extended speculatively -- do not add parenthesized grouping without a citable source. NOT extending speculatively per this pass's instructions; not implemented this pass."
  - "Vault Lock policy enforcement (gopherstack-ygfk) only evaluates Effect=Deny (Allow is a no-op -- no IAM baseline to grant against), ignores Principal (no per-request caller identity, gopherstack-cu4g), does not support the ResourceTag condition key (Glacier archives carry no tags here), and only gates DeleteArchive/DeleteVault (not UploadArchive/InitiateJob/other Vault-Lock-governable actions) -- see families: vault_lock_enforcement for the full disclosure. Vault ACCESS policies (SetVaultAccessPolicy) remain entirely unenforced -- their purpose is Principal-based access control, which needs the same caller-identity infrastructure gopherstack-cu4g is deciding, and is a different, larger gap than deletion protection."
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; retrievalDelay promotion is read-triggered (promoteJobIfReady), not a background timer. FIXED this pass: DeleteVault's multipart-upload cascade deleted the store.Table row but never the corresponding raw-map multipartParts[uploadKey] row (AbortMultipartUpload/CompleteMultipartUpload already did this correctly; DeleteVault's cascade loop did not) -- every vault deleted with an in-progress multipart upload left an orphaned parts row forever. Fixed in vaults.go's DeleteVault; regression test TestDeleteVault_CascadeCleansMultipartParts (leak_test.go)."}
---

## Notes

Protocol: **restjson1** (AWS restJson1, not query-XML). Response bodies are JSON;
request/response IDs and checksums are carried in **headers**, not JSON body, for
UploadArchive / CompleteMultipartUpload / InitiateJob / InitiateMultipartUpload
(confirmed via `awsRestjson1_deserializeOpHttpBindings*Output` functions in the
real SDK's `deserializers.go` — these ops use header-only output shapes). Timestamps
are ISO-8601 strings (`2006-01-02T15:04:05.000Z`), never epoch numbers — confirmed
correct throughout (`formatDate` in models.go).

### Bugs fixed this pass

1. **`DescribeJob`/`ListJobs` missing `ArchiveSHA256TreeHash` wire field.**
   The real Glacier `GlacierJobDescription` shape has **two distinct** checksum
   fields: `ArchiveSHA256TreeHash` (checksum of the *entire archive*, archive
   metadata available as soon as the job exists) and `SHA256TreeHash` (checksum
   of the *retrieved range*, null while the job is `InProgress`, confirmed via
   the real deserializer's `case "ArchiveSHA256TreeHash":` / `case
   "SHA256TreeHash":` switch arms in `deserializers.go`). gopherstack's
   `describeJobResponse` only had `SHA256TreeHash` and set it eagerly at
   `InitiateJob` time regardless of completion state — so every real SDK client
   calling `DescribeJob`/`ListJobs` for a completed `ArchiveRetrieval` job got a
   **nil `ArchiveSHA256TreeHash`**, permanently losing the documented way to
   verify the full-archive checksum via `DescribeJob` (see the SDK's own
   `GetJobOutput` doc comment, which tells callers to cross-check downloaded
   chunks against `DescribeJob`'s archive checksum). Fixed: `Job` now carries
   `ArchiveSHA256TreeHash` (set immediately at `InitiateJob`, from archive
   metadata) separately from `SHA256TreeHash` (set only once
   `promoteJobIfReady` transitions the job to `Succeeded`), and
   `describeJobResponse` serializes both under their correct AWS field names.

2. **`GetJobOutput` missing `X-Amz-Archive-Description` response header.**
   For archive-retrieval jobs, real AWS returns the archive's description via
   the `x-amz-archive-description` response header (confirmed via
   `awsRestjson1_deserializeOpHttpBindingsGetJobOutputOutput`, which populates
   `GetJobOutputOutput.ArchiveDescription` purely from that header — there is
   no JSON-body equivalent). `handleArchiveJobOutput` never set this header, so
   `output.ArchiveDescription` was always nil for every archive download.
   Fixed: `Job` now carries `ArchiveDescription` (copied from the `Archive` at
   `InitiateJob` time — internal field, not part of the `DescribeJob` DTO,
   since AWS has no such field there), and `handleArchiveJobOutput` sets the
   header when non-empty.

### Bugs/gaps fixed this pass (2026-07-24)

3. **Select jobs (`Type=select`) were entirely unimplemented** — `InitiateJob`
   only recognized `archive-retrieval`/`inventory-retrieval`, so any real SDK
   client requesting a select job got a generic `InvalidParameterValueException`
   for an unrecognized `Type` instead of a working job. Implemented for real:
   full request-shape validation (`SelectParameters`/`OutputLocation` field-by-
   field against the real `JobParameters`/`SelectParameters`/`OutputLocation`/
   `S3Location`/`CSVInput`/`CSVOutput` types), a real SQL query engine
   (`select.go`, `select_sql.go`) that actually executes the `Expression`
   against the archive's CSV bytes, and correct `GlacierJobDescription` echo of
   `JobOutputPath`/`OutputLocation`/`SelectParameters`. See the `select_jobs`
   family note above for the one documented AWS-behavior deviation (GetJobOutput
   delivery in lieu of cross-service S3 write-back).

4. **Range inventory retrieval (`InventoryRetrievalParameters`) was entirely
   unimplemented** — the request field was silently dropped, so inventory jobs
   always returned the full vault inventory regardless of any
   `StartDate`/`EndDate`/`Limit`/`Marker` the caller specified, with no
   validation error to warn them. Implemented for real: validated parsing,
   correct nested-object echo on `DescribeJob`/`ListJobs` (see bug 5 below),
   and actual `CreationDate`-range/marker/limit filtering of the inventory
   returned by `GetJobOutput` (`inventory_retrieval.go`).

5. **`describeJobResponse.InventoryFormat` (`json:"Format"`) was a
   gopherstack-invented top-level field** — the real `GlacierJobDescription`
   type has **no top-level `Format` field** at all; `Format` only ever exists
   nested under `InventoryRetrievalParameters`. Per this campaign's "delete
   gopherstack-invented fields" rule, the top-level field is now gone,
   replaced by a real `InventoryRetrievalParameters` nested object (which also
   now carries `StartDate`/`EndDate`/`Limit`/`Marker`, previously entirely
   absent — see bug 4). (Previously this was logged as a "harmless, do not
   fix" trap because removing it without also implementing the real nested
   object would have been a net regression; it is safe now that the real
   field exists.)

6. **`DeleteVault` leaked `multipartParts` rows** (leak, not a wire bug) — see
   the `leaks` field above for detail; fixed in `vaults.go`.

### Bugs/gaps fixed this pass (2026-08-10)

7. **Select job SQL grammar accepted `LIMIT`, which real S3 Glacier Select
   explicitly does not support.** The prior pass's framing ("SQL subset
   mirrors real Glacier Select's own subset") was inherited rather than
   verified — checking `awsdocs/amazon-glacier-developer-guide`'s
   `doc_source/s3-glacier-select-sql-reference-select.md` shows `LIMIT` is
   documented as `(Amazon S3 Select only)`: "**S3 Glacier Select does not
   support the `LIMIT` clause**." A real Glacier Select client sending
   `SELECT * FROM archive LIMIT 5` gets rejected; gopherstack silently
   accepted and honored it — an over-permissive superset bug, not a
   documented-subset omission. Fixed: `select_sql.go`'s parser now rejects any
   `LIMIT` clause with the same `ErrSelectExpression`/`InvalidParameterValueException`
   path used for other malformed expressions, at `InitiateJob`-time syntax
   validation (matching real AWS's synchronous rejection). See the
   `select_sql_subset` gap entry above for the fuller SQL-grammar audit this
   also prompted (CAST/BETWEEN/IN/LIKE/NOT/arithmetic are real gaps in the
   other direction — Glacier Select supports them, gopherstack does not).

8. **Select job results were never written to the real S3 `OutputLocation`.**
   Resolved by wiring a real S3 backend (`cli.go`'s `wireGlacierS3`,
   `select_output.go`'s `materializeSelectOutput`) — see the `select_jobs`
   family note above for the full account of what changed and why the prior
   "no cross-service S3 write-back" framing was stale.

### Bugs fixed this pass (2026-08-14, gopherstack-ygfk)

9. **Vault Lock policy was stored, echoed by `GetVaultLock`, and never
   consulted by `DeleteArchive`/`DeleteVault`** — the security variant of the
   write-only-state class this issue tracks (mirrors `gopherstack-cqy3`'s
   CloudFormation stack-policy fix). A policy denying deletion of an archive
   or vault did nothing; the write succeeded, the read confirmed it, and the
   protection it names was cosmetic. Fixed: see the `vault_lock_enforcement`
   family entry above for the full implemented/disclosed breakdown, sourcing,
   and test evidence.

10. **`InitiateVaultLock`'s `LockId` was JSON-body-only.** Real AWS returns it
    exclusively via the `x-amz-lock-id` response header (confirmed via
    `awsRestjson1_deserializeOpHttpBindingsInitiateVaultLockOutput`, which
    never touches the body) — a real `aws-sdk-go-v2` client always got a nil
    `LockId` and could never call `CompleteVaultLock`. Found while writing an
    SDK-driven test for bug 9 above (the completion step failed with
    `"input member lockId must not be empty"`), not something a raw-HTTP test
    could have caught. Fixed: `handleVaultLock` now also sets the header;
    the JSON body is kept as a harmless bonus, same pattern already used for
    `UploadArchive`'s `x-amz-archive-id`.

11. **`InitiateVaultLock`'s request-body JSON unmarshal error was silently
    discarded** (`_ = json.Unmarshal(body, &req)`), so a malformed request
    body was accepted as an empty `Policy` instead of rejected. Fixed
    alongside `SetVaultLock` now also rejecting a malformed *inner* policy
    document at write time (previously accepted, would never have enforced
    anything even after bug 9's fix) — same bug class as `cloudformation`'s
    `SetStackPolicy` fix. Three existing tests used a non-JSON placeholder
    policy string (`"p"`) that only "worked" because the error was swallowed;
    updated to `"{}"`, and one test's inner-policy JSON was embedded
    unescaped (also only working by accident of the swallowed error) and is
    now properly escaped.

### Traps for the next auditor

- `UploadArchive` / `CompleteMultipartUpload` / `InitiateJob` /
  `InitiateMultipartUpload` responses carry a JSON body in gopherstack
  (`uploadArchiveResponse`, `completeMultipartUploadResponse`,
  `initiateJobResponse`, `initiateMultipartUploadResponse`) even though real
  AWS returns an **empty body** for these ops (all data is in headers). This is
  intentional and harmless: the real SDK's `awsRestjson1_deserializeOp*`
  handlers for these ops never call the JSON-body document deserializer, only
  the HTTP-bindings (header) one, so the body is simply never parsed by a real
  client. Do not flag the body-in-a-header-only-op pattern as a bug.
- `ErrResourceInUse` → `ResourceInUseException` and `ErrVaultNotEmpty` /
  `ErrLockConflict` / `ErrLockAlreadyLocked` → `ConflictException` /
  `InvalidParameterValueException`, and (added this pass) `ErrVaultLockDenied`
  → `AccessDeniedException`, are **not** modeled exception types in
  `aws-sdk-go-v2/service/glacier/types/errors.go` (the SDK only models
  `InsufficientCapacityException`, `InvalidParameterValueException`,
  `LimitExceededException`, `MissingParameterValueException`,
  `NoLongerSupportedException`, `PolicyEnforcedException`,
  `RequestTimeoutException`, `ResourceNotFoundException`,
  `ServiceUnavailableException`). `AccessDeniedException` is documented (the
  real error-responses table: "Returned if there was an attempt to access a
  resource not allowed by an IAM policy", 403) even though the SDK doesn't
  model it as a typed error. `PolicyEnforcedException` IS SDK-typed but is a
  different feature entirely -- it covers data-retrieval-rate-limit denials
  (`GetDataRetrievalPolicy`/`SetDataRetrievalPolicy`), not Vault Lock policy
  denials; do not repurpose it for `checkVaultLockDelete`. Real clients still
  get a working `smithy.GenericAPIError` with the correct `Code`/`Message`/HTTP
  status for any unmodeled code (falls through to the generic-error `default:`
  branch in every
  `awsRestjson1_deserializeOpError*` function) — this is NOT a bug, just an
  SDK modeling gap on AWS's side that gopherstack correctly works around.
- Route matching (`RouteMatcher` + `parseGlacierPath`) was cross-checked
  against every literal `httpbinding.SplitURI(...)` path string in the real
  SDK's `serializers.go` (32 matches, one per op) plus HTTP verb per branch —
  no unreachable-op bug found this pass, unlike several other services hit by
  that bug class.
- The Select job SQL engine (`select_sql.go`) supports a **subset** of SQL,
  not full ANSI SQL — but do not assume the subset boundary without checking
  the real SQL reference first (a prior pass's "mirrors real Glacier Select's
  own subset" claim turned out partly wrong — see the `select_sql_subset` gap
  entry above, and bug 7). Verified as of 2026-08-10: joins/subqueries are a
  correct omission (real Glacier Select doesn't support them either); `LIMIT`
  is correctly rejected (real Glacier Select doesn't support it, unlike S3
  Select); `CAST`, `NOT`/`BETWEEN`/`IN`/`LIKE`, arithmetic operators, and
  `COALESCE`/`NULLIF` are real, uncited-as-fixed gaps (real Glacier Select
  supports all of them); parenthesized/nested-boolean grouping has no
  evidence either way and is deliberately left unextended. See `select.go`'s
  package doc comment for the exact grammar and citations.
- Select job results are served via `GetJobOutput` in gopherstack IN ADDITION
  TO the real S3 `OutputLocation` write-back (`select_output.go`,
  `materializeSelectOutput`, wired via `cli.go`'s `wireGlacierS3`) added
  2026-08-10. GetJobOutput serving Select output directly is NOT modeled by
  real AWS (its own docs cover only archive/inventory output), but there is
  also no citable real error behavior to reject it with — so this remains a
  harmless, documented convenience layered on top of the now-real S3 delivery
  path, not a replacement for it. Do not remove the S3 write-back thinking
  GetJobOutput alone is sufficient; do not "fix" GetJobOutput by rejecting
  Select jobs without a cited real error code to match.
