---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: textract
sdk_module: aws-sdk-go-v2/service/textract@v1.41.0   # version audited against
last_audit_commit: 7d7a3363                          # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: B            # already-accurate; op-by-op audit found 2 genuine gaps, both fixed this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  DetectDocumentText: {wire: ok, errors: ok, state: ok, persist: n/a, note: synchronous, deterministic mock Blocks}
  AnalyzeDocument: {wire: ok, errors: ok, state: ok, persist: n/a, note: FeatureTypes + QueriesConfig validated}
  AnalyzeExpense: {wire: ok, errors: ok, state: ok, persist: n/a}
  AnalyzeID: {wire: ok, errors: ok, state: ok, persist: n/a}
  StartDocumentTextDetection: {wire: ok, errors: ok, state: ok, persist: ok, note: ClientRequestToken idempotency wired}
  GetDocumentTextDetection: {wire: ok, errors: ok, state: ok, persist: ok, note: NextToken pagination via paginateBlocks}
  StartDocumentAnalysis: {wire: ok, errors: ok, state: ok, persist: ok, note: ClientRequestToken idempotency wired}
  GetDocumentAnalysis: {wire: ok, errors: ok, state: ok, persist: ok}
  StartExpenseAnalysis: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — ClientRequestToken/OutputConfig/NotificationChannel/JobTag were parsed but silently dropped; now wired via StartExpenseAnalysisWithOptions"}
  GetExpenseAnalysis: {wire: ok, errors: ok, state: ok, persist: ok}
  StartLendingAnalysis: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — same ClientRequestToken/OutputConfig/NotificationChannel/JobTag gap as StartExpenseAnalysis; now wired via StartLendingAnalysisWithOptions"}
  GetLendingAnalysis: {wire: ok, errors: ok, state: ok, persist: ok}
  GetLendingAnalysisSummary: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — response was missing the Warnings field present on every sibling Get* response and in the real SDK's GetLendingAnalysisSummaryOutput"}
  CreateAdapter: {wire: ok, errors: ok, state: ok, persist: ok, note: ClientRequestToken dedup; FeatureTypes restricted to FORMS/QUERIES}
  GetAdapter: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAdapter: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAdapters: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAdapter: {wire: ok, errors: ok, state: ok, persist: ok, note: cascades to delete all adapter versions}
  CreateAdapterVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: CREATION_IN_PROGRESS -> ACTIVE lifecycle}
  GetAdapterVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAdapterVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAdapterVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: resolves adapter or adapter-version ARN}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  async-job-lifecycle: {status: ok, note: "IN_PROGRESS -> SUCCEEDED transition via runDelayed goroutine tracked by sync.WaitGroup, cancelled/awaited on Handler.Shutdown; verified for DocumentAnalysis, TextDetection, ExpenseAnalysis, LendingAnalysis, AdapterVersion creation"}
  adapter-arn-resolution: {status: ok, note: "TagResource/UntagResource/ListTagsForResource resolve either a bare adapter ID, an adapter ARN, or an adapter-version ARN (checks /version/ suffix first)"}
  wire-shapes: {status: ok, note: "Block, ExpenseDocument/ExpenseField/ExpenseGroupProperty, IdentityDocument/IdentityDocumentField, LendingResult/LendingDocument/LendingField/Extraction, DocumentMetadata field names cross-checked field-by-field against aws-sdk-go-v2/service/textract@v1.41.0/types"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Extraction (used inside LendingResult.Extractions) models LendingDocument and ExpenseDocument variants but not the real SDK's third variant, IdentityDocument. Currently harmless: syntheticLendingResults() never populates it, so no wire divergence is observable today. File a bd issue if lending mock data is ever extended to represent an identity-document extraction inside a loan package."
  - "CreateAdapterInput doc comment in the SDK source says only QUERIES is a supported adapter FeatureType, but gopherstack (pre-existing, not touched this pass) accepts FORMS and QUERIES. Low confidence this is a real divergence (SDK doc comments lag); flagging for a future auditor with time to verify against current AWS docs rather than changing behavior speculatively."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Full byte-for-byte Block/Geometry/Point field audit beyond the ones cross-checked in wire-shapes above (Block has ~15 optional fields across text-detection/analysis/layout modes) — spot-checked, not exhaustively diffed field-by-field against every BlockType variant's real usage."
leaks: {status: clean, note: "runDelayed goroutines are tracked by InMemoryBackend.wg and bound to svcCtx; Handler.Shutdown cancels + waits. Verified via existing TestInMemoryBackend_Shutdown table (all 4 job families x zero-delay/shutdown-cancels cases)."}
---

## Notes

Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: Textract.<Op>` dispatch. All 25
ops in the real SDK are routed and reachable (`GetSupportedOperations()` / `buildOps()` match
1:1). `RouteMatcher` is a simple header-prefix check; nothing to trap there.

### Bugs fixed this pass

1. **StartExpenseAnalysis / StartLendingAnalysis silently dropped
   ClientRequestToken idempotency** (`handler.go` handleStartExpenseAnalysis /
   handleStartLendingAnalysis, `backend.go` StartExpenseAnalysis /
   StartLendingAnalysis). The handler parsed `ClientRequestToken`,
   `OutputConfig`, `NotificationChannel`, and `JobTag` from the request body,
   and `ExpenseJob`/`LendingJob` already carried fields for all four — but the
   backend methods never accepted them, so every value was discarded. Real
   AWS Textract guarantees "the same token ... returns the same JobId"
   (see `StartExpenseAnalysisInput.ClientRequestToken` /
   `StartLendingAnalysisInput.ClientRequestToken` doc comments in the SDK).
   Retrying a Start call after a network blip would silently create a
   duplicate job in gopherstack where real AWS would return the original.
   Fixed by adding `StartExpenseAnalysisWithOptions` /
   `StartLendingAnalysisWithOptions` on `InMemoryBackend` (mirroring the
   existing `StartDocumentAnalysisWithOptions` / `StartDocumentTextDetectionWithOptions`
   / `CreateAdapterWithToken` pattern already used by every other async
   Start op), each backed by its own region-nested
   `clientToken -> jobID` map (`expenseClientTokenToJobID`,
   `lendingClientTokenToJobID`), persisted in `backendSnapshot` and reset in
   `Reset()`/on version-mismatch Restore.

2. **GetLendingAnalysisSummary response missing `Warnings`**
   (`handler.go` handleGetLendingAnalysisSummary /
   getLendingAnalysisSummaryResponse). Every sibling Get* op
   (GetDocumentAnalysis, GetDocumentTextDetection, GetExpenseAnalysis,
   GetLendingAnalysis) plumbs `job.Warnings` into its response, and the real
   SDK's `GetLendingAnalysisSummaryOutput` has a `Warnings []types.Warning`
   field — this was the one Get* handler that forgot to wire it. Fixed by
   adding the field to the response struct and populating it from
   `job.Warnings`.

Both fixes are covered by new table-driven tests in `backend_test.go` (direct
backend-level idempotency + field propagation) and `handler_test.go`
(HTTP-wire-level idempotency + Warnings JSON key presence).

### Traps for the next auditor

- `LendingJob.Warnings` / `ExpenseJob.Warnings` / `DocumentJob.Warnings` are
  modeled, cloned, and persisted, but nothing in this backend currently
  populates them with real content (always nil in practice, `omitempty`
  hides them). This is intentional latent extensibility, not a bug in
  itself — but if a future chaos-injection or "unsupported document format"
  path starts setting them, double-check every Get* response wires the field
  through (see bug #2 above for the pattern that was missed).
- `regionFromARN`/`resolveARNToAdapter`/`resolveARNToAdapterVersion` do a
  manual ARN parse rather than using `pkgs/arn`'s parser for the *read* path
  (write path — `buildAdapterARN`/`buildAdapterVersionARN` — does use
  `pkgs/arn.Build`). This looked suspicious at first read but is correct:
  `pkgs/arn` has no ARN-parsing helper as of this audit, only building, so
  the hand-rolled `lastIndex`/`contains` parse is not a reuse violation.
- `cloneAdapter`/`cloneExpenseJob`/`cloneLendingJob`/`cloneJob` are shallow
  `cp := *j` plus explicit deep-copies of slice/map fields only
  (`Blocks`, `Tags`, `FeatureTypes`, `Warnings`, etc.); pointer fields like
  `OutputConfig`/`NotificationChannel`/`DatasetConfig` are intentionally left
  aliased since nothing mutates them in place after Start — don't "fix" this
  into a full deep clone without checking for actual mutation-after-share
  bugs first.
