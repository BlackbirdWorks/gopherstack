---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: support
sdk_module: aws-sdk-go-v2/service/support@v1.31.23   # version audited against
last_audit_commit: 139000b9                          # HEAD when this manifest was written
last_audit_date: 2026-07-13
overall: A                # ~2 genuine wire/logic bugs found and fixed this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateCase: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCases: {wire: ok, errors: ok, state: ok, persist: ok}
  ResolveCase: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: fabricated CaseAlreadyResolved error removed, now idempotent per real AWS contract"}
  AddCommunicationToCase: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCommunications: {wire: ok, errors: ok, state: ok, persist: ok}
  AddAttachmentsToSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAttachment: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCreateCaseOptions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static data, no persistable state"}
  DescribeServices: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static data"}
  DescribeSeverityLevels: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static data"}
  DescribeSupportedLanguages: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "request field was severityLevel; real wire field is categoryCode — fixed"}
  DescribeTrustedAdvisorChecks: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static list"}
  DescribeTrustedAdvisorCheckRefreshStatuses: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTrustedAdvisorCheckResult: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTrustedAdvisorCheckSummaries: {wire: ok, errors: ok, state: ok, persist: ok}
  RefreshTrustedAdvisorCheck: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  case_lifecycle: {status: ok, note: "CreateCase/DescribeCases/ResolveCase/AddCommunicationToCase/DescribeCommunications verified against deserializers.go field-by-field; timeCreated confirmed ISO-8601 string (types.CaseDetails.TimeCreated *string, doc: \"in ISO-8601 format\"), not epoch"}
  attachments: {status: ok, note: "AddAttachmentsToSet/DescribeAttachment/AttachmentRef(attachmentId,fileName) match types.AttachmentDetails; 1hr expiry, 3-per-set cap, 5MB size cap all match AWS docs"}
  trusted_advisor: {status: ok, note: "static check catalogue + refresh-status poll progression (enqueued->processing->success) is a reasonable emulation; no wire mismatches found"}
gaps: []
deferred:
  - integration test suite (test/integration/*_parity_test.go) was not run this pass — only unit tests; see parity-principles.md note 3 that unit tests are not full proof. No SDK client round-trip was exercised.
leaks: {status: clean, note: "janitor.go RunJanitor/sweepExpiredResources reviewed; ticker-based, stops cleanly on ctx cancellation via worker.Group; StartWorker/Shutdown wire it into service.BackgroundWorker/Shutdowner correctly"}
---

## Notes

Two real bugs found and fixed this pass (unit-test audit only; no wire capture/integration
run was performed — see `deferred` above):

1. **Fabricated `CaseAlreadyResolved` error on `ResolveCase`** (accuracy.go
   `ResolveCaseWithStatus`, backend.go legacy `ResolveCase`). Verified against
   `aws-sdk-go-v2/service/support`'s `deserializers.go` —
   `awsAwsjson11_deserializeOpErrorResolveCase` only recognizes `CaseIdNotFound` and
   `InternalServerError`; there is no "already resolved" exception modeled anywhere in
   `types/errors.go`. Real AWS `ResolveCase` is idempotent: resolving an already-resolved
   case succeeds and reports `resolved` for both `initialCaseStatus` and
   `finalCaseStatus`. gopherstack was returning a fabricated `CaseAlreadyResolved`
   ValidationError (400) — a disguised-stub-adjacent bug where a plausible-sounding but
   non-existent AWS error was invented. Fixed to be a no-op that preserves the original
   `ResolvedTime` and returns `resolved`/`resolved`. `ErrAlreadyResolved` var deleted (no
   remaining references); `TestRefinement1_AlreadyResolved` updated to assert the correct
   (idempotent, 200 OK) behavior instead of 400.

2. **`DescribeSupportedLanguages` request field mismatch**: gopherstack's
   `describeSupportedLanguagesInput` decoded `severityLevel` (validated against the
   severity-code enum) instead of the real wire field `categoryCode`. Confirmed via
   `serializers.go`'s `awsAwsjson11_serializeOpDocumentDescribeSupportedLanguagesInput`,
   which emits only `categoryCode`, `issueType`, `serviceCode` — there is no
   `severityLevel` member on `DescribeSupportedLanguagesInput` in the real SDK at all.
   Every real SDK-driven call to this op would have 400'd against gopherstack
   (validation required a field the real client never sends). Fixed: struct field,
   validation, `StorageBackend.DescribeSupportedLanguages` signature, and handler wiring
   all renamed/rewired to `categoryCode`. `handleDescribeSupportedLanguagesInput`
   backend impl already ignored positional params 2/3 (`issueType, _, _`), so no
   behavioral change there beyond accepting the right wire field.

### Verified correct (no bugs, but worth recording so next audit doesn't re-flag)

- **timeCreated / TrustedAdvisorCheckResult.timestamp are ISO-8601 strings**, not epoch
  numbers — confirmed against `types.CaseDetails.TimeCreated *string` and
  `types.Communication.TimeCreated *string` in the real SDK; gopherstack's
  `time.RFC3339` formatting is correct. Do not "fix" this to epoch-seconds; support is one
  of the few JSON-protocol services that keeps timestamps as formatted strings on the
  wire.
- **All other request/response field names cross-checked against
  `serializers.go`/`deserializers.go`** for CreateCase, DescribeCases, ResolveCase,
  AddCommunicationToCase, DescribeCommunications, AddAttachmentsToSet,
  DescribeAttachment: exact match, no casing/naming drift found.
- **Required-field validation** (subject/communicationBody for CreateCase; caseId for
  ResolveCase/AddCommunicationToCase/DescribeCommunications/DescribeAttachment;
  language for DescribeTrustedAdvisorChecks; issueType/serviceCode/categoryCode/language
  for DescribeCreateCaseOptions) all matches the `// This member is required` annotations
  in the real SDK's input structs.
- **Error code set per op** cross-checked against each op's
  `awsAwsjson11_deserializeOpError<Op>` switch in `deserializers.go` (e.g.
  AddAttachmentsToSet: AttachmentLimitExceeded/AttachmentSetExpired/
  AttachmentSetIdNotFound/AttachmentSetSizeLimitExceeded/InternalServerError). No missing
  `errCodeLookup`-equivalent entries found — `handleError` in handler.go maps
  ErrNotFound/ErrAttachmentNotFound/ErrAttachmentSetNotFound to 404, ErrValidation/
  ErrAttachmentSetExpired/errUnknownAction/JSON syntax-or-type errors to 400, else 500.
- **`GetSupportedOperations()` is complete**: `TestSDKCompleteness`
  (sdk_completeness_test.go) passes against `aws-sdk-go-v2/service/support@v1.31.23`
  with zero `notImplemented` — all 15 ops the real SDK client exposes are routed.
- **The two "legacy" non-`WithOptions` backend methods** (`CreateCase`, `DescribeCases`,
  `ResolveCase`, `AddCommunicationToCase`, `DescribeCommunications`,
  `AddAttachmentsToSet(attachmentSetID string)` without an attachments param) are dead
  code from the routed-handler's perspective — handler.go dispatches exclusively through
  the `*WithOptions`/`*WithStatus`/`*WithAttachments` variants. They remain on
  `StorageBackend` only because tests (persistence_test.go, handler_refinement1_test.go)
  exercise them directly. Not a wire-shape risk since they're never reached from
  `Handler()`, but the `ResolveCase` legacy method had the *same* fabricated
  already-resolved bug and was fixed for consistency (see bug 1 above).
- **Persistence**: `Handler.Snapshot`/`Restore` delegate to `InMemoryBackend`, which
  round-trips all four "clean" `store.Table`s plus the "dirty" `attachmentSets` table
  (via DTO) plus the order-sensitive `communications` map plus `nextDisplayID`. Version
  gate (`supportSnapshotVersion`) discards incompatible snapshots cleanly. No gaps found.
- **Route matching**: `X-Amz-Target: AWSSupport_20130415.<Op>` prefix matches the real
  service's target prefix (`ServiceID: "Support"`, API version `2013-04-15` per
  `generated.json`/`doc.go` in the SDK module).

### Not audited this pass (deferred)

- No SDK-client round-trip / integration test was run (`test/integration/*_parity_test.go`
  either doesn't cover support yet or wasn't exercised here) — only `go test` unit tests
  and static comparison against `serializers.go`/`deserializers.go` source. Per
  parity-principles.md note 3, this is a real gap in proof strength even though the
  wire-shape comparison here was done directly against the generated (de)serializer code
  rather than against gopherstack's own output, which is stronger than typical unit-test-only
  audits.
