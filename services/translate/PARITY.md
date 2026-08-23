---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: translate
sdk_module: aws-sdk-go-v2/service/translate@v1.36.4
last_audit_commit: 1efb1a758
last_audit_date: 2026-08-20
# PROVENANCE NOTE (2026-08-20): the 2026-08-20 sweep initially reported this
# manifest's previous stamp (2d47b51d4 / 2026-07-29) as failed provenance,
# because 2d47b51d4 is an ec2 commit that never touched services/translate/.
# That verdict was WRONG and is retracted here. The schema defines
# last_audit_commit as HEAD when the manifest was written, not as a commit
# touching this service, and 2d47b51d4 is dated 2026-07-29 -- exactly the
# recorded audit date. Three sibling manifests (shield, applicationautoscaling,
# apigatewaymanagementapi) cite the same sha with the same date, the signature
# of one legitimate batch audit that day. The stamp was correct.
# Third over-application of the provenance heuristic in this campaign; see
# gopherstack-z31a for the only test that actually discriminates.
overall: A            # genuine fixes: invented error code, wrong error-per-op, missing validation, stuck CREATING/UPDATING, dropped ParallelDataProperties.EncryptionKey
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  ImportTerminology: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - wrong error type (InvalidRequestException, not modeled for this op) for Name/TerminologyData/MergeStrategy/Directionality validation, now InvalidParameterValueException; TerminologyData was silently defaulted instead of validated required; added TerminologyData.Format enum + 10MB file size limit (LimitExceededException) + 50-tag limit (TooManyTagsException). 2026-08-20: wrapper-key sweep re-verified TerminologyProperties field-by-field against types.TerminologyProperties; AuxiliaryDataLocation (import-error-file S3 location) correctly omitted since this emulator never produces import errors/warnings, confirmed against botocore's ImportTerminologyResponse doc (no member marked required). FIXED 2026-08-23 (gopherstack-v71s): MergeStrategy accepted an empty value (`mergeStrategy != \"\" && mergeStrategy != \"OVERWRITE\"`) even though ImportTerminologyInput marks it \"This member is required\" (api_op_ImportTerminology.go) -- gopherstack was looser than AWS, not stricter, so this was the opposite direction from the InvalidRequestException-vs-InvalidParameterValueException fixes above. Now rejects an empty/absent MergeStrategy with InvalidParameterValueException, same error class this op already uses for every other required-field violation (Name/TerminologyData). Checked sibling translate ops (`handler_translation.go`'s Profanity gate) for the same empty-string-passes shape: Profanity is a genuinely optional TranslationSettings member (no \"required\" doc comment), so it correctly stays permissive -- not the same bug."}
  GetTerminology: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - missing Name now InvalidParameterValueException (this op has no InvalidRequestException in its modeled error list). 2026-08-20: TerminologyDataLocation{RepositoryType,Location} both present (both required per that shape's own \"required\" list); AuxiliaryDataLocation correctly omitted for the same no-errors-modeled reason as ImportTerminology."}
  DeleteTerminology: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - same InvalidParameterValueException correction as GetTerminology"}
  ListTerminologies: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateParallelData: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - resource now starts CREATING and advances to ACTIVE on GetParallelData poll (previously ACTIVE immediately, skipping the async state real AWS goes through); added ParallelDataConfig.Format enum + 50-tag limit; name-conflict error corrected from invented ResourceInUseException to real ConflictException"}
  GetParallelData: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - missing Name now InvalidParameterValueException (was InvalidRequestException, not modeled for this op); now advances CREATING/UPDATING -> ACTIVE one step per call (DescribeTextTranslationJob's advance-on-poll convention). 2026-08-20 wrapper-key sweep: fixed - parallelDataToMap (handler_parallel_data.go) never emitted ParallelDataProperties.EncryptionKey even though CreateParallelData accepts+persists it and the sibling terminologyToMap emits the analogous field for GetTerminology; proven with a real-SDK round-trip test (wire_sdk_roundtrip_test.go). AuxiliaryDataLocation/LatestUpdateAttemptAuxiliaryDataLocation correctly omitted (import/update-error S3 locations; this emulator never produces import/update errors, and neither member is required per GetParallelDataResponse)."}
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
  parallel_data: {status: ok, note: "Create/Get/Update/Delete/List verified against ParallelDataProperties/ParallelDataDataLocation shapes and CreateParallelDataOutput/UpdateParallelDataOutput/DeleteParallelDataOutput; async CREATING/UPDATING lifecycle gap from the previous audit is now fixed (advance-on-GetParallelData-poll, mirroring advanceJob). 2026-08-20: fixed a dropped ParallelDataProperties.EncryptionKey member in parallelDataToMap; see GetParallelData note."}
  translation_jobs: {status: ok, note: "Start/Stop/Describe/List verified against TextTranslationJobProperties/JobDetails and api-2.json's per-op error lists; StartTextTranslationJob's missing required-field/language-pair/resource-reference validation fixed, error-code-per-op fixed for Stop/Describe"}
  translation: {status: ok, note: "TranslateText/TranslateDocument verified against TranslateTextOutput/TranslateDocumentOutput/AppliedTerminology/TranslationSettings shapes and Amazon Translate's guidelines/quotas page; missing terminology-reference validation, size limits, language-pair validation, ContentType, and Settings enum validation all fixed"}
  tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource verified against Tag{Key,Value} shape; error-code-per-op and 50-tag limit fixed"}
gaps:
  - "IMPOSSIBLE (re-confirmed gopherstack-llun): TranslateText/TranslateDocument echo SourceLanguageCode literally as 'auto' when omitted, instead of resolving it to a detected language code the way real AWS does (via an internal Comprehend call). Real language detection would require fabricating a plausible-looking detected language for arbitrary input text with no ground truth to check it against -- that is worse than an honest 'auto' echo, not better. Left as a mock limitation per parity principles (translation itself is inherently mocked)."
  - "ALREADY COVERED BY CHAOS (verified gopherstack-llun): DetectedLanguageLowConfidenceException, ConcurrentModificationException, TooManyRequestsException, InternalServerException, and ServiceUnavailableException are real modeled errors for several ops but have no deterministic backend-state trigger in this synchronous, single-lock, unbounded in-memory emulator (no rate limiting, no enforced per-account resource quotas, no real concurrent-write races, no real Comprehend-backed language detection). Concretely verified this pass: translate.Handler implements ChaosServiceName() -> \"translate\" and ChaosOperations() -> h.GetSupportedOperations() (handler.go), and pkgs/chaos.Middleware is wired globally via registry.Use(chaos.Middleware(faultStore)) in cli.go -- it matches purely on the request's SigV4 service name + X-Amz-Target operation + region and injects an arbitrary caller-specified FaultError{Code, StatusCode}, never touching backend state. A fault rule such as {\"service\":\"translate\",\"error\":{\"code\":\"DetectedLanguageLowConfidenceException\",\"statusCode\":400}} deterministically returns that exact typed error to a real aws-sdk-go-v2 client on any operation, with zero backend code changes. Matches services/comprehend's documented precedent for the same class of unmodeled-but-real exceptions; proven end-to-end against a real containerized client in test/integration/chaos_test.go."
  - "IMPOSSIBLE (re-confirmed gopherstack-llun): EncryptionKey.Type (KMS-only enum) and EncryptionKey.Id are accepted without validation across ImportTerminology/CreateParallelData/UpdateParallelData's OutputDataConfig.EncryptionKey. Encryption is inert in this mock (nothing is ever actually encrypted, no KMS cross-service key-existence check exists elsewhere in this pass's scope either), so the field has no real behavior to validate against -- adding an enum check here would be validation theater, not a wire-accuracy fix. Low-value/low-risk gap, left as-is."
  - "VALUE-CORRECTNESS, DISCLOSED NOT FIXED (2026-08-20 wrapper-key sweep): DeleteParallelData returns pd.Status as it stood immediately before deletion (e.g. ACTIVE), never the DELETING value real AWS documents for 'the status of the parallel data deletion' (DeleteParallelDataResponse.Status, botocore service-2.json). This is a right-key/right-type/questionable-VALUE issue, not a shape break -- ACTIVE is still a valid ParallelDataStatus enum member, so no client-side deserialization failure results -- and fixing it properly would need a transient DELETING state in the lifecycle model (delete marks DELETING, a later poll/janitor actually removes the row), which is lifecycle-state-machine work out of scope for a wrapper-key/nesting sweep. Left as-is; flagging for a future targeted pass."
  - "MISSING NON-REQUIRED MEMBERS, DISCLOSED NOT FIXED (2026-08-20 wrapper-key sweep): TerminologyProperties.SkippedTermCount and .Message, and ParallelDataProperties.FailedRecordCount/ImportedDataSize/ImportedRecordCount/SkippedRecordCount/.Message are real optional response members this emulator never populates (terminologyToMap/parallelDataToMap omit them entirely rather than emitting a zero value). None are marked required in types.TerminologyProperties/types.ParallelDataProperties, and populating them honestly would require modeling per-record import/skip counters the backend doesn't track today -- Layer-3-scope, left as a disclosed gap rather than fabricated."
  - "SEMANTIC, DISCLOSED NOT FIXED (2026-08-20 wrapper-key sweep): TextTranslationJobProperties.JobDetails is always {TranslatedDocumentsCount:0, DocumentsWithErrorsCount:0, InputDocumentsCount:0} regardless of job size (jobToMap, handler_text_translation_jobs.go) -- the wrapper key and nested field names are correct (verified against types.JobDetails), but the values are a hardcoded stub since this emulator never actually reads/counts documents in the InputDataConfig S3 location. Semantic gap, not a wire-shape bug; left as-is."
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

## 2026-08-20 — wrapper-key / nested-shape wire-parity sweep

Scope: verify every emitted top-level response key, nesting level, JSON type, and enum value for all 19
ops against `aws-sdk-go-v2/service/translate@v1.36.4` directly (not the previous audit's own notes), plus
the `This member is required` grep across every type this service emits and the three
request/response-split shapes (`Document`/`TranslatedDocument`, `TerminologyData`/`TerminologyProperties`/
`TerminologyDataLocation`, `ParallelDataConfig`/`ParallelDataProperties`/`ParallelDataDataLocation`).

**Protocol reconfirmed**: `awsjson1.1` (JSON-RPC). `grep -c awsAwsjson11_deserializeOpDocument<Op>Output
deserializers.go` returns 2 (defined + called) for 18 of 19 ops; `DeleteTerminologyOutput` has no members at
all (empty struct) so the OpDocument helper doesn't exist for it — expected, not a gap. The restjson
flat-body false-positive trap this session's brief warned about does not apply: translate is JSON-RPC, and
`awsAwsjson11_*` always routes through the live OpDocument helper for every non-empty output.

**`This member is required` grep result**: only `TranslateTextOutput` (`TranslatedText`,
`SourceLanguageCode`, `TargetLanguageCode`) and `TranslateDocumentOutput` (`TranslatedDocument`,
`SourceLanguageCode`, `TargetLanguageCode`) mark any Output-struct member required — every other op's
Output struct has zero required top-level members. All six are modeled and populated
(`handler_translation.go`'s `translateText`/`translateDocument`). Required members on nested types
(`EncryptionKey.Id`/`.Type`, `InputDataConfig.ContentType`/`.S3Uri`, `OutputDataConfig.S3Uri`,
`Language.LanguageCode`/`.LanguageName`, `Tag.Key`/`.Value`, `TerminologyDataLocation`/
`ParallelDataDataLocation`'s `RepositoryType`/`Location`, `TranslatedDocument.Content`,
`Document.Content`/`.ContentType` (request-only), `TerminologyData.File`/`.Format` (request-only)) were all
confirmed modeled and populated everywhere they're emitted.

**One real bug found and fixed**: `ParallelDataProperties.EncryptionKey` was silently dropped from
`GetParallelData`/`ListParallelData` responses. `CreateParallelData` accepts and persists `EncryptionKey`
onto the `ParallelData` backend struct (`parallel_data.go:73`), and the sibling `terminologyToMap`
(`handler_terminologies.go:182-187`) already surfaces the analogous field for `GetTerminology`/
`ListTerminologies` — but `parallelDataToMap` (`handler_parallel_data.go`) never emitted an `EncryptionKey`
key at all, so a real client's `GetParallelDataOutput.ParallelDataProperties.EncryptionKey` deserialized as
`nil` regardless of what was set at creation. This is the sweep's dominant bug class (a): a member modeled
on one type and correctly emitted for a wider/sibling shape, silently missing from the narrower one. Fixed
in `handler_parallel_data.go`'s `parallelDataToMap`; proven with a real-SDK-client round-trip test,
`TestGetParallelData_SDKRoundTrip_EncryptionKey` (`wire_sdk_roundtrip_test.go`), which creates a parallel
data resource with an `EncryptionKey`, calls the real `translatesdk` client's `GetParallelData`, and asserts
`out.ParallelDataProperties.EncryptionKey.Id`/`.Type` are non-nil and correct. Hand-reverted the fix: the
test failed with `Expected value not to be nil` at the `EncryptionKey` assertion, exactly the predicted
symptom; restored, confirmed the diff returned to the intended one-hunk addition.

**Verified clean (no wire bug)**: `GetTerminology`/`ImportTerminology` correctly omit
`AuxiliaryDataLocation`, and `GetParallelData` correctly omits `AuxiliaryDataLocation`/
`LatestUpdateAttemptAuxiliaryDataLocation` — all three are import/update-error-file S3 locations that only
populate when the real service encounters errors/warnings in the input file, which this emulator (no error
path in import/update) never does; confirmed via botocore's `translate/2017-07-01/service-2.json` doc
strings and that none of these members are marked required in their respective `*Response` shapes. The
three request/response splits named in this session's brief were all reconfirmed clean: `Document`
(request: `Content`+`ContentType`) never leaks into `TranslatedDocument` (response: `Content` only, and
that's all `handler_translation.go`'s `translateDocument` emits); `TerminologyData` (request-only: `File`+
`Format`+`Directionality`) never appears in any response, only its already-correct `TerminologyProperties`/
`TerminologyDataLocation` counterparts do; `ParallelDataConfig` correctly nests *inside*
`ParallelDataProperties` (both request- and response-side per the real shape) with no confusion against the
separate `ParallelDataDataLocation` S3-location shape. `TranslationSettings` (used identically on both the
request `Settings` and response `AppliedSettings`) showed no field leakage in either direction since it's
the literal same type both ways in the real SDK too.

**`last_audit_commit` provenance verdict: FAILED, then corrected.** The prior manifest cited
`last_audit_commit: 2d47b51d4` / `last_audit_date: 2026-07-29`. `git show -s --format=%ad 2d47b51d4` does
date to 2026-07-29, but `git show --stat 2d47b51d4` shows its actual content is
`fix(ec2): RestoreImageFromRecycleBin no longer reports success for a no-op` — a wholly unrelated EC2 fix,
never touching `services/translate/`. The real translate audit commit matching this manifest's prose
(per-op error taxonomy, parallel-data CREATING/UPDATING lifecycle, terminology/language-pair validation) is
`afe5bb500` (`fix(translate): per-op error taxonomy, parallel-data lifecycle, validation, terminology
checks`), dated **2026-07-24 — five days before** the claimed `last_audit_date`, exactly the
days-to-weeks-before tell this session's brief warned about. `last_audit_commit`/`last_audit_date` above are
now corrected to the real commit and today's date. SDK version check: `sdk_module` header
(`aws-sdk-go-v2/service/translate@v1.36.4`) matches `go.mod` exactly; the manifest's prose citations of
`aws-sdk-go@v1.55.5/models/apis/translate/2017-07-01/api-2.json` are a different (v1, model-only) package
used solely to read the smithy `api-2.json` source and don't restate the pinned v2 client version, so this
is not the header/prose mismatch pattern flagged elsewhere this session. Every "fixed" error-taxonomy claim
spot-checked this pass (all `InvalidParameterValueException`-vs-`InvalidRequestException`-vs-neither splits,
and the `ResourceInUseException`→`ConflictException` correction) re-derived correctly against
`translate/2017-07-01/service-2.json`'s per-operation `errors: [...]` arrays.

**Gates**: `go build ./services/translate/...`, `go vet ./services/translate/...`, `go fix -diff
./services/translate/...`, `gofmt -l services/translate/` all clean/empty; `go test -race
./services/translate/...` passes (2.5s); `golangci-lint run ./services/translate/...` reports `0 issues`.
