---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: translate
sdk_module: aws-sdk-go-v2/service/translate@v1.36.4
last_audit_commit: 2d47b51d4
last_audit_date: 2026-07-29
overall: A            # genuine fixes: invented error code, wrong error-per-op, missing validation, stuck CREATING/UPDATING
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  ImportTerminology: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - wrong error type (InvalidRequestException, not modeled for this op) for Name/TerminologyData/MergeStrategy/Directionality validation, now InvalidParameterValueException; TerminologyData was silently defaulted instead of validated required; added TerminologyData.Format enum + 10MB file size limit (LimitExceededException) + 50-tag limit (TooManyTagsException)"}
  GetTerminology: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - missing Name now InvalidParameterValueException (this op has no InvalidRequestException in its modeled error list)"}
  DeleteTerminology: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - same InvalidParameterValueException correction as GetTerminology"}
  ListTerminologies: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateParallelData: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - resource now starts CREATING and advances to ACTIVE on GetParallelData poll (previously ACTIVE immediately, skipping the async state real AWS goes through); added ParallelDataConfig.Format enum + 50-tag limit; name-conflict error corrected from invented ResourceInUseException to real ConflictException"}
  GetParallelData: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - missing Name now InvalidParameterValueException (was InvalidRequestException, not modeled for this op); now advances CREATING/UPDATING -> ACTIVE one step per call (DescribeTextTranslationJob's advance-on-poll convention)"}
  UpdateParallelData: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - LatestUpdateAttemptStatus/LatestUpdateAttemptAt are now real tracked per-attempt state (UPDATING -> ACTIVE via GetParallelData poll) instead of a hardcoded ACTIVE constant; added ParallelDataConfig.Format enum validation"}
  DeleteParallelData: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - missing Name now ResourceNotFoundException (this op models no validation exception at all, not even InvalidParameterValueException)"}
  ListParallelData: {wire: ok, errors: ok, state: ok, persist: ok, note: "confirmed pure read: does not advance CREATING/UPDATING state, matching ListTextTranslationJobs precedent"}
  StartTextTranslationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - request had zero required-field validation (DataAccessRoleArn/InputDataConfig/OutputDataConfig/SourceLanguageCode/TargetLanguageCodes could all be omitted and a job would still be created); added InvalidRequestException for missing required fields, UnsupportedLanguagePairException for unrecognized language codes, ResourceNotFoundException when TerminologyNames/ParallelDataNames reference a resource that doesn't exist, and Settings enum validation (Brevity not supported for batch jobs per the API reference, unlike TranslateText/TranslateDocument)"}
  StopTextTranslationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - missing JobId now ResourceNotFoundException (was InvalidRequestException, not modeled for this op)"}
  DescribeTextTranslationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - same ResourceNotFoundException correction as StopTextTranslationJob"}
  ListTextTranslationJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - Filter.JobStatus accepted any string silently matching zero jobs instead of rejecting unrecognized values; added InvalidFilterException validation against the JobStatus enum"}
  TranslateText: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "fixed - TerminologyNames referencing a nonexistent terminology was silently ignored instead of erroring (real AWS models ResourceNotFoundException for exactly this, the operation's only named-resource reference); added TextSizeLimitExceededException (10,000-byte sync quota), UnsupportedLanguagePairException (language code not in the supported list), and Settings.Formality/Profanity/Brevity enum validation"}
  TranslateDocument: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "fixed - Document.ContentType (a required member of Document) was read from the wire nowhere at all and never validated; added ContentType required check, LimitExceededException (100,000-byte document size quota -- this op models LimitExceededException, not TextSizeLimitExceededException, for size overflow), UnsupportedLanguagePairException, the same TerminologyNames ResourceNotFoundException fix as TranslateText, and Settings enum validation"}
  ListLanguages: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed - DisplayLanguageCode accepted any string; real Translate models a fixed 10-value enum (de/en/es/fr/it/ja/ko/pt/zh/zh-TW) distinct from the ~75 translation-target language codes this op itself returns; added UnsupportedDisplayLanguageCodeException"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - missing ResourceArn now InvalidParameterValueException (was InvalidRequestException, not modeled for this op); added 50-tag limit (existing+new union) -> TooManyTagsException"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - same InvalidParameterValueException correction as TagResource"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - same InvalidParameterValueException correction as TagResource"}
# Families audited as a group (when per-op is impractical):
families:
  terminology: {status: ok, note: "ImportTerminology/GetTerminology/DeleteTerminology/ListTerminologies verified against TerminologyProperties/TerminologyDataLocation shapes and api-2.json's per-op error lists; error-code-per-op, Format enum, file size limit, and tag limit fixed"}
  parallel_data: {status: ok, note: "Create/Get/Update/Delete/List verified against ParallelDataProperties/ParallelDataDataLocation shapes and CreateParallelDataOutput/UpdateParallelDataOutput/DeleteParallelDataOutput; async CREATING/UPDATING lifecycle gap from the previous audit is now fixed (advance-on-GetParallelData-poll, mirroring advanceJob)"}
  translation_jobs: {status: ok, note: "Start/Stop/Describe/List verified against TextTranslationJobProperties/JobDetails and api-2.json's per-op error lists; StartTextTranslationJob's missing required-field/language-pair/resource-reference validation fixed, error-code-per-op fixed for Stop/Describe"}
  translation: {status: ok, note: "TranslateText/TranslateDocument verified against TranslateTextOutput/TranslateDocumentOutput/AppliedTerminology/TranslationSettings shapes and Amazon Translate's guidelines/quotas page; missing terminology-reference validation, size limits, language-pair validation, ContentType, and Settings enum validation all fixed"}
  tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource verified against Tag{Key,Value} shape; error-code-per-op and 50-tag limit fixed"}
gaps:
  - "IMPOSSIBLE (re-confirmed gopherstack-llun): TranslateText/TranslateDocument echo SourceLanguageCode literally as 'auto' when omitted, instead of resolving it to a detected language code the way real AWS does (via an internal Comprehend call). Real language detection would require fabricating a plausible-looking detected language for arbitrary input text with no ground truth to check it against -- that is worse than an honest 'auto' echo, not better. Left as a mock limitation per parity principles (translation itself is inherently mocked)."
  - "ALREADY COVERED BY CHAOS (verified gopherstack-llun): DetectedLanguageLowConfidenceException, ConcurrentModificationException, TooManyRequestsException, InternalServerException, and ServiceUnavailableException are real modeled errors for several ops but have no deterministic backend-state trigger in this synchronous, single-lock, unbounded in-memory emulator (no rate limiting, no enforced per-account resource quotas, no real concurrent-write races, no real Comprehend-backed language detection). Concretely verified this pass: translate.Handler implements ChaosServiceName() -> \"translate\" and ChaosOperations() -> h.GetSupportedOperations() (handler.go), and pkgs/chaos.Middleware is wired globally via registry.Use(chaos.Middleware(faultStore)) in cli.go -- it matches purely on the request's SigV4 service name + X-Amz-Target operation + region and injects an arbitrary caller-specified FaultError{Code, StatusCode}, never touching backend state. A fault rule such as {\"service\":\"translate\",\"error\":{\"code\":\"DetectedLanguageLowConfidenceException\",\"statusCode\":400}} deterministically returns that exact typed error to a real aws-sdk-go-v2 client on any operation, with zero backend code changes. Matches services/comprehend's documented precedent for the same class of unmodeled-but-real exceptions; proven end-to-end against a real containerized client in test/integration/chaos_test.go."
  - "IMPOSSIBLE (re-confirmed gopherstack-llun): EncryptionKey.Type (KMS-only enum) and EncryptionKey.Id are accepted without validation across ImportTerminology/CreateParallelData/UpdateParallelData's OutputDataConfig.EncryptionKey. Encryption is inert in this mock (nothing is ever actually encrypted, no KMS cross-service key-existence check exists elsewhere in this pass's scope either), so the field has no real behavior to validate against -- adding an enum check here would be validation theater, not a wire-accuracy fix. Low-value/low-risk gap, left as-is."
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; job lifecycle advances synchronously inside DescribeTextTranslationJob and parallel-data lifecycle advances synchronously inside GetParallelData, both under the existing backend mutex, no new background state"}
---

## Notes

- 2026-08-22, gopherstack-r80d batch 31 (required-output-member audit):
  translate (6 required output fields / 19 ops, 2 ops-with-required per a
  fresh `cmd/requiredoutputfields` run, cross-checked against an independent
  standalone `go/ast` walk of `translate@v1.36.4`'s `api_op_*.go` files --
  both agreed exactly at 6, lowest op-count of this batch's six-way tie).
  Module resolved directly: directory `translate` == SDK module
  `aws-sdk-go-v2/service/translate@v1.36.4` per `go.mod`, no sibling-module
  ambiguity for this name.

  `TranslateText` (`SourceLanguageCode`/`TargetLanguageCode`/`TranslatedText`,
  all `*string`) and `TranslateDocument` (same two language codes plus
  `*types.TranslatedDocument`, itself requiring `Content []byte`,
  types.go:512-520) are both built as a `map[string]any` literal
  (`translateText`/`translateDocument`, handler_translation.go:50-102,
  109-172) passed straight to `json.Marshal` by the shared op dispatcher
  (`json.Marshal(output)`, handler.go:154) -- not a tagged struct, matching
  batch 30's ssoadmin/mediatailor/shield finding for the same reason: there
  is no struct tag for an `omitempty` mistake to hide behind, so shape 1 of
  this campaign's bug class cannot occur syntactically here. Checked shape 2:
  `TargetLanguageCode` is validated non-empty before either function runs
  (`"TargetLanguageCode is required"`, handler_translation.go:59,113);
  `SourceLanguageCode` defaults to `"auto"` rather than being left empty
  (handler_translation.go:65-67,116-118) if the request omits it (gopherstack
  is intentionally more lenient here than the real SDK's own client-side
  validator, which requires it non-nil -- a pre-existing, separately-scoped
  input-validation gap, not this cut's target); `TranslatedText` is derived
  from `Text`, itself required non-empty (handler_translation.go:52-54), so
  it can only gain a language prefix or terminology substitutions, never
  become empty; `TranslatedDocument.Content` mirrors `Document.Content`
  byte-for-byte through a base64 round trip (handler_translation.go:138-147) --
  an explicitly empty (but present) `Content` is honest given a real client
  can legally send one (the real SDK's `validateDocument`, translate's
  validators.go, only null-checks `Content`/`ContentType`, not length), and
  the map-literal `"Content"` key is written unconditionally regardless.
  Result: 0 bugs. No code changes.
Protocol: **awsjson1.1** (single POST endpoint, `X-Amz-Target: AWSShineFrontendService_20170701.<Op>`,
`application/x-amz-json-1.1`) — confirmed against `translateTargetPrefix` in handler.go and the real
SDK's `httpBindingEncoder.SetHeader("Content-Type").String("application/x-amz-json-1.1")` in
serializers.go. Route matcher is a simple header-prefix check; unit tests call `Handler()` directly so
they exercise it for real (not bypassed).

### Real bugs found and fixed this sweep

This sweep's headline finding is that error **codes** were wrong far more often than error **triggers**
were missing: the previous audit (6eeaefc, 2026-07-13) got wire shapes and state-machine bugs right but
never field-diffed each operation's `errors: [...]` array in
`aws-sdk-go@v1.55.5/models/apis/translate/2017-07-01/api-2.json` against what the handler actually threw.
Every op's error set is enforced by a **per-operation** switch in the real SDK's generated
`deserializers.go` (`awsAwsjson11_deserializeOpError<Op>`), not a shared service-wide error table — an
error whose wire code isn't in that specific operation's switch falls through to a generic/untyped API
error client-side instead of the typed exception a caller's `errors.As` would expect.

1. **Invented error code: `ResourceInUseException`**. `ErrConflict` (used by `CreateParallelData`'s
   name-conflict check) mapped to the wire type `"ResourceInUseException"` — a real exception in
   *Comprehend's* API (this codebase's `services/comprehend` legitimately uses it) but **absent entirely**
   from Amazon Translate's exception set (confirmed against
   `aws-sdk-go-v2/service/translate/types/errors.go`, which has no such type, and the full
   `errors.EXCEPTION` list in `translate`'s `api-2.json`). Real Translate models `ConflictException` for
   exactly this case (`CreateParallelData`/`UpdateParallelData`'s error lists). Fixed by renaming the
   sentinel's wire string; this looks like the comprehend-convention sentinel name (`ErrConflict`) was
   copied into translate along with its Comprehend-specific wire string without checking whether Translate
   actually models the same exception.

2. **Wrong error type used for 8 operations** (`GetParallelData`, `DeleteParallelData`,
   `StopTextTranslationJob`, `DescribeTextTranslationJob`, `TagResource`, `UntagResource`,
   `ListTagsForResource`, and `ImportTerminology`/`GetTerminology`/`DeleteTerminology`). The handler used
   one shared `ErrValidation` ("InvalidRequestException") for every "required field missing" case
   service-wide, but several operations' modeled error lists don't include `InvalidRequestException` at
   all:
   - `ImportTerminology`/`GetTerminology`/`DeleteTerminology`/`TagResource`/`UntagResource`/
     `ListTagsForResource`/`GetParallelData` model `InvalidParameterValueException` but never
     `InvalidRequestException` → added `ErrInvalidParameter` and rewired these six operations to use it.
   - `DeleteParallelData`/`StopTextTranslationJob`/`DescribeTextTranslationJob` model **neither**
     `InvalidRequestException` nor `InvalidParameterValueException` — only `ResourceNotFoundException` (plus
     `ConcurrentModificationException`/`TooManyRequestsException`/`InternalServerException`, none of which
     have a deterministic trigger here). A missing key on these three ops now surfaces as
     `ResourceNotFoundException`, matching the only client-error type the real operation ever returns.

3. **`CreateParallelData`/`UpdateParallelData` skipped the async `CREATING`/`UPDATING` lifecycle**
   (carried over from the previous audit's documented gap, now fixed). `CreateParallelData` set
   `Status: "ACTIVE"` immediately; real `CreateParallelDataOutput`'s API reference documents "When the
   resource is ready for you to use, the status is `ACTIVE`" — implying it is *not* immediately ACTIVE.
   Fixed with the same "advance on poll" convention `DescribeTextTranslationJob`'s `advanceJob` already
   established for translation jobs: the resource now starts `CREATING` and `GetParallelData` advances it
   to `ACTIVE` one call later (`advanceParallelData` in parallel_data.go, `GetParallelData` now takes
   `b.mu.Lock()` instead of `RLock()` for the same reason `DescribeTextTranslationJob` does).
   `UpdateParallelData`'s `LatestUpdateAttemptStatus` was hardcoded to the literal string `"ACTIVE"` in the
   handler regardless of actual state — a disguised no-op that happened to always be correct only because
   there was no failure path and the resource never left ACTIVE in the first place. Fixed by adding real
   `LatestUpdateAttemptStatus`/`LatestUpdateAttemptAt` fields to the `ParallelData` struct: `Update` now
   sets `UPDATING`, and the same `GetParallelData` poll advances it to `ACTIVE`.

4. **`StartTextTranslationJob` had zero required-field validation**. `DataAccessRoleArn`, `InputDataConfig`
   (with its own required `S3Uri`+`ContentType`), `OutputDataConfig.S3Uri`, `SourceLanguageCode`, and
   `TargetLanguageCodes` are all required members of `StartTextTranslationJobRequest` (api-2.json), but the
   handler read every one of them with a blind `.(string)`/`.( map[string]any)` type assertion and silently
   proceeded with zero-value defaults on a miss — a job would be created with an empty
   `DataAccessRoleArn`/`InputDataConfig`/etc. and no client-visible error. Fixed with explicit required-field
   checks (`InvalidRequestException`, which this op does model).

5. **Referenced `TerminologyNames`/`ParallelDataNames` were never validated to exist** across
   `TranslateText`, `TranslateDocument`, and `StartTextTranslationJob`. All three model
   `ResourceNotFoundException`, and `TerminologyNames`/`ParallelDataNames` are the *only* named-resource
   references any of them make — `LookupTerminologies` silently skipped missing names instead of erroring,
   and `StartTextTranslationJob` didn't check `ParallelDataNames` at all. A previous-sweep unit test
   (`TestTranslateTextIncludesAppliedTerminologies`'s `unknown_terminology_name_omitted_from_applied` case)
   had actually encoded this bug as expected behavior; corrected to expect `ResourceNotFoundException`
   (see handler_translation_test.go's `TestTranslateText_UnknownTerminologyRejected`).

6. **No language-pair validation anywhere**. `TranslateText`, `TranslateDocument`, and
   `StartTextTranslationJob` all model `UnsupportedLanguagePairException`, but any string was accepted as a
   language code. Fixed by validating non-`auto` source and all target codes against
   `knownLanguageCodesTable` (the same ~75-language list `ListLanguages` serves).

7. **No synchronous size-quota enforcement**. Amazon Translate's guidelines/quotas page documents a
   10,000-byte limit on `TranslateText`'s `Text` (`TextSizeLimitExceededException`, confirmed against
   `BoundedLengthString`'s `max` in the smithy model) and a 100,000-byte limit on `TranslateDocument`'s
   `Document.Content` (`LimitExceededException` — this op does *not* model
   `TextSizeLimitExceededException`, unlike `TranslateText`) and a 10 MB limit on `ImportTerminology`'s
   `TerminologyData.File` (also `LimitExceededException`, confirmed against `TerminologyFile`'s `max`).
   None were enforced; all three now are.

8. **`Document.ContentType` was never read, validated, or required** despite being a required member of
   `Document` (api-2.json) — `TranslateDocument` only ever looked at `Document.Content`. Fixed with a
   required-field check.

9. **`Settings.Formality`/`Profanity`/`Brevity` were echoed back verbatim with no enum validation** (real
   enums: `FORMAL|INFORMAL`, `MASK`, `ON` respectively) across all three translation-settings-accepting
   operations, and `StartTextTranslationJob`'s API reference specifically documents `Brevity` as
   "not supported" for batch jobs (unlike `TranslateText`/`TranslateDocument`, which both support it) — a
   distinction the previous, settings-blind code couldn't have honored. Fixed with `validSettingsEnums`.

10. **`ListTextTranslationJobs`'s `Filter.JobStatus` and `ListLanguages`'s `DisplayLanguageCode` accepted
    any string.** `Filter.JobStatus` models `InvalidFilterException` for an unrecognized value (previously
    just silently matched zero jobs); `DisplayLanguageCode` models `UnsupportedDisplayLanguageCodeException`
    against a **fixed 10-value enum** (`de/en/es/fr/it/ja/ko/pt/zh/zh-TW`) that is deliberately much smaller
    than the ~75 translation-target language codes `ListLanguages` itself returns — a distinction easy to
    miss without reading the smithy model directly. Both now validate.

11. **`TerminologyData.Format`/`ParallelDataConfig.Format` accepted any string** instead of the modeled
    `CSV|TMX|TSV` enum (both shapes share the identical three-value enum). `ImportTerminology` additionally
    silently defaulted an entirely-omitted `TerminologyData` to an empty CSV terminology instead of
    triggering the backend's own (previously unreachable) "TerminologyData is required" check.

12. **`TooManyTagsException` (the real 50-tag-per-resource limit) was never enforced** on
    `ImportTerminology`, `CreateParallelData`, or `TagResource`, despite all three modeling it.

### Traps for the next auditor (looks-wrong-but-correct)

- `DescribeTextTranslationJob` and `GetParallelData` both take `b.mu.Lock()` (write lock), not `RLock()` —
  they mutate job/parallel-data state via `advanceJob`/`advanceParallelData` on every call. This is
  intentional, not a leftover. `ListTextTranslationJobs`/`ListParallelData` deliberately do NOT advance
  state (real List operations are pure reads).
- `persistence_test.go`'s `assertJobRestored` reads the restored job via `ListTextTranslationJobs` rather
  than `DescribeTextTranslationJob` specifically to avoid the assertion itself advancing the job's status
  as a side effect — don't "simplify" this back to Describe.
- Tests sending `TerminologyData.File`/`Document.Content` use the `b64()` helper (handler_test.go) instead
  of literal strings; a `File`/`Content` value that isn't valid base64 is correctly rejected as
  `InvalidRequestException`, so any new test must encode it.
- `ErrValidation` ("InvalidRequestException") is intentionally still used by `CreateParallelData`,
  `UpdateParallelData`, `StartTextTranslationJob`, `ListTextTranslationJobs`, `TranslateText`, and
  `TranslateDocument` — these six DO model `InvalidRequestException`. Don't blanket-replace it with
  `ErrInvalidParameter` service-wide; the split is per-operation and field-diffed against api-2.json's
  per-op `errors: [...]` arrays, not a stylistic preference.
- `LookupTerminologies`'s signature changed from `(names []string) []*Terminology` to
  `(names []string) ([]*Terminology, error)` — it now errors on any name that doesn't exist rather than
  silently skipping it. Both call sites (`translateText`/`translateDocument`) were updated; if a new op
  ever needs terminology lookup, don't revert to the old skip-missing behavior without re-checking whether
  that op also models `ResourceNotFoundException`.
