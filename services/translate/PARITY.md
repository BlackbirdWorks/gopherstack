---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: translate
sdk_module: aws-sdk-go-v2/service/translate@v1.34.2
last_audit_commit: 6eeaefc
last_audit_date: 2026-07-13
overall: A            # genuine fixes found: base64 blob bugs, stuck-job lifecycle, disguised no-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  ImportTerminology: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - TerminologyData.File is base64 on the wire (Base64EncodeBytes in serializers.go) but was stored/parsed as literal text; also Directionality input was silently discarded (always forced to UNI)"}
  GetTerminology: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTerminology: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTerminologies: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateParallelData: {wire: ok, errors: ok, state: partial, persist: ok, note: "resource is ACTIVE immediately; real AWS goes CREATING -> ACTIVE async. Not a stuck-forever bug (opposite direction), left as gap - see gaps below"}
  GetParallelData: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateParallelData: {wire: partial, errors: ok, state: ok, persist: ok, note: "LatestUpdateAttemptStatus is hardcoded ACTIVE rather than tracked per-attempt state; happens to always be correct since there is no update-failure path, see gaps below"}
  DeleteParallelData: {wire: ok, errors: ok, state: ok, persist: ok}
  ListParallelData: {wire: ok, errors: ok, state: ok, persist: ok}
  StartTextTranslationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - jobs were created directly at IN_PROGRESS; real StartTextTranslationJobOutput.JobStatus starts at SUBMITTED (types.JobStatusSubmitted)"}
  StopTextTranslationJob: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTextTranslationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - jobs never advanced past their initial status (IN_PROGRESS/STOP_REQUESTED forever); now advances one lifecycle step per Describe call, matching services/comprehend's DescribeJob/advanceJob pattern (SUBMITTED->IN_PROGRESS->COMPLETED/FAILED, STOP_REQUESTED->STOPPED)"}
  ListTextTranslationJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "intentionally does not advance job status (List is a pure read in real AWS too); only Describe advances"}
  TranslateText: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "fixed - AppliedTerminologies[].Terms was always a fabricated empty array regardless of what actually matched (the existing code comment described the correct behavior without implementing it); also fixed applyCSVTerminology treating the CSV header row (source/target language codes) as a literal term substitution pair"}
  TranslateDocument: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "fixed - Document.Content / TranslatedDocument.Content are base64-encoded blobs on the wire (Base64EncodeBytes / base64-decode in the real SDK's serializers/deserializers) but were treated as literal text in both directions; also missing Document validation and the same AppliedTerminologies.Terms fix as TranslateText"}
  ListLanguages: {wire: ok, errors: ok, state: ok, persist: n/a}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  terminology: {status: ok, note: "ImportTerminology/GetTerminology/DeleteTerminology/ListTerminologies verified against TerminologyProperties/TerminologyDataLocation shapes; base64 + Directionality bugs fixed"}
  parallel_data: {status: ok, note: "Create/Get/Update/Delete/List verified against ParallelDataProperties/ParallelDataDataLocation shapes and CreateParallelDataOutput/UpdateParallelDataOutput/DeleteParallelDataOutput; async CREATING/UPDATING lifecycle left as gap (see below)"}
  translation_jobs: {status: ok, note: "Start/Stop/Describe/List verified against TextTranslationJobProperties/JobDetails; stuck-job lifecycle bug fixed"}
  translation: {status: ok, note: "TranslateText/TranslateDocument verified against TranslateTextOutput/TranslateDocumentOutput/AppliedTerminology/Term shapes; base64 blob bug and disguised-no-op AppliedTerminologies.Terms bug fixed"}
  tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource verified against Tag{Key,Value} shape; arnExists gates both terminology and parallel-data ARNs correctly"}
gaps:
  - "CreateParallelData/UpdateParallelData skip the CREATING/UPDATING async states real AWS goes through (resource is ACTIVE the instant it's created/updated, and UpdateParallelDataOutput.LatestUpdateAttemptStatus is hardcoded ACTIVE rather than a tracked per-attempt value). Not a client-facing bug today (there is no failure path so the hardcoded value is always correct, and being immediately ACTIVE doesn't break polling clients), but a real divergence from AWS's async lifecycle. Mirrors the TranslationJob stuck-status fix in scope/shape if picked up later. (bd: file on next session)"
  - "TranslateText/TranslateDocument echo SourceLanguageCode literally as 'auto' when omitted, instead of resolving it to a detected language code the way real AWS does (via an internal Comprehend call). Left as a mock limitation per parity principles (translation itself is inherently mocked); flagging in case a future pass wants a lightweight heuristic detector."
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; job lifecycle advances synchronously inside DescribeTextTranslationJob under the existing backend mutex, no new background state"}
---

## Notes

Protocol: **awsjson1.1** (single POST endpoint, `X-Amz-Target: AWSShineFrontendService_20170701.<Op>`,
`application/x-amz-json-1.1`) — confirmed against `translateTargetPrefix` in handler.go and the real
SDK's `httpBindingEncoder.SetHeader("Content-Type").String("application/x-amz-json-1.1")` in
serializers.go. Route matcher is a simple header-prefix check; unit tests call `Handler()` directly so
they exercise it for real (not bypassed).

### Real bugs found and fixed this sweep

1. **Blob fields treated as literal text instead of base64** (the headline finding). Two fields are
   `[]byte` in the SDK and therefore base64-encoded/decoded by the real client at the JSON boundary:
   - `TerminologyData.File` (ImportTerminology request) — confirmed via
     `awsAwsjson11_serializeDocumentTerminologyData`'s `ok.Base64EncodeBytes(v.File)` in
     aws-sdk-go-v2/service/translate@v1.34.2/serializers.go.
   - `Document.Content` / `TranslatedDocument.Content` (TranslateDocument request/response) — confirmed
     via the same file's `Base64EncodeBytes` call and `deserializers.go`'s
     `awsAwsjson11_deserializeDocumentTranslatedDocument`, which base64-decodes the response.

   Before this fix, a real SDK client's terminology CSV or document text would be stored/parsed as
   base64 garbage, and the response's `TranslatedDocument.Content` would fail the client's own
   base64-decode (or silently misdecode). Existing unit tests didn't catch this because they call
   `Handler()` directly with hand-built JSON bodies that happened to send plain text — exactly the kind
   of bug integration/SDK-driven tests catch that unit tests miss (parity-principles.md #3). Fixed by
   decoding on the way in and encoding on the way out in `importTerminology`/`translateDocument`
   (handler.go). All test bodies constructing `File`/`Content` now go through a `b64()` test helper
   (handler_test.go) instead of literal strings.

2. **TranslationJob stuck in a transient status forever** (disguised no-op / the exact pattern this
   audit was asked to hunt for). `StartTextTranslationJob` created jobs directly at `IN_PROGRESS` and
   nothing ever advanced them — `DescribeTextTranslationJob` (the documented way SDK callers poll job
   progress) would return `IN_PROGRESS` on every call, forever, and a job that received
   `StopTextTranslationJob` would sit at `STOP_REQUESTED` forever too. Real
   `StartTextTranslationJobOutput.JobStatus` starts at `SUBMITTED`
   (`types.JobStatusSubmitted`). Fixed by starting jobs at `SUBMITTED` and adding an `advanceJob` step
   inside `DescribeTextTranslationJob` that moves the job one step per poll
   (`SUBMITTED -> IN_PROGRESS -> COMPLETED`/`FAILED`, `STOP_REQUESTED -> STOPPED`), mirroring
   `services/comprehend`'s `DescribeJob`/`advanceJob` convention exactly (including a `[fail]`
   job-name marker to deterministically drive the FAILED path in tests). `ListTextTranslationJobs`
   intentionally does **not** advance state — matches comprehend's `ListJobs`, and real AWS's List
   operation is a pure read.

3. **`AppliedTerminologies[].Terms` always fabricated empty** — a real disguised no-op: the existing
   code comment literally said "the Terms slice lists matched pairs (empty if none matched)" but the
   implementation always returned `[]any{}` regardless of what actually matched. Fixed by having
   `applyCSVTerminology` return the source/target pairs it actually substituted, threaded through
   `applyTranslation` and into `buildAppliedTerminologies`.

4. **CSV header row treated as a term pair** — found while fixing #3.
   `applyCSVTerminology` looped over every line of the terminology CSV including line 0, which
   `parseCSVLanguages` (backend.go) correctly treats as the header (source/target language codes, e.g.
   `en,es`), not term data. This meant importing a terminology with header `en,es` would silently
   replace every literal `en` substring in translated output with `es` (e.g. "again", "Listen",
   "different" all contain "en"). No existing test exercised real CSV term substitution through
   TranslateText/TranslateDocument, so this went unnoticed. Fixed by skipping `lines[0]` in
   `applyCSVTerminology`, matching `parseCSVLanguages`'s treatment of the same file.

5. **`TerminologyData.Directionality` silently discarded** — the request field was ignored entirely;
   every terminology was hardcoded to `Directionality: "UNI"` regardless of what was requested. Fixed
   by passing it through (defaulting to `UNI` when absent, validating `UNI`/`MULTI` otherwise).

### Traps for the next auditor (looks-wrong-but-correct)

- `DescribeTextTranslationJob` now takes `b.mu.Lock()` (write lock), not `RLock()` — it mutates job
  state via `advanceJob` on every call. This is intentional, not a leftover.
- `persistence_test.go`'s `assertJobRestored` reads the restored job via `ListTextTranslationJobs`
  rather than `DescribeTextTranslationJob` specifically to avoid the assertion itself advancing the
  job's status as a side effect — don't "simplify" this back to Describe.
- Tests sending `TerminologyData.File`/`Document.Content` use the `b64()` helper
  (handler_test.go) instead of literal strings; a `File`/`Content` value that isn't valid base64 is
  now correctly rejected as `InvalidRequestException`, so any new test must encode it.
