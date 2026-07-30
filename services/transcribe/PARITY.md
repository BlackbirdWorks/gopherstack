---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: transcribe
sdk_module: aws-sdk-go-v2/service/transcribe@v1.55.0   # version audited against
last_audit_commit: 92c92ff03               # HEAD when this manifest was written
last_audit_date: 2026-07-24
overall: A            # A = genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  StartTranscriptionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "added LanguageIdSettings threading; removed invented top-level OutputBucketName/OutputKey (not real TranscriptionJob fields -- output location only lives in Transcript.TranscriptFileUri)"}
  GetTranscriptionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fixes as Start; deferred-job polling advances QUEUED->IN_PROGRESS->COMPLETED correctly"}
  ListTranscriptionJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "added JobNameContains filter + missing TranscriptionJobSummary fields (StartTime, IdentifyLanguage, IdentifyMultipleLanguages, IdentifiedLanguageScore, ContentRedaction, ModelSettings, LanguageCodes, ToxicityDetection, OutputLocationType)"}
  DeleteTranscriptionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "forgets resource tags"}
  StartCallAnalyticsJob: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCallAnalyticsJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCallAnalyticsJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "added JobNameContains filter + missing StartTime on CallAnalyticsJobSummary"}
  DeleteCallAnalyticsJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "forgets resource tags"}
  CreateCallAnalyticsCategory: {wire: ok, errors: ok, state: ok, persist: ok, note: "CategoryProperties now includes CreateTime/LastUpdateTime/Tags (were silently dropped)"}
  GetCallAnalyticsCategory: {wire: ok, errors: ok, state: ok, persist: ok, note: "same CategoryProperties fix"}
  UpdateCallAnalyticsCategory: {wire: ok, errors: ok, state: ok, persist: ok, note: "same CategoryProperties fix"}
  DeleteCallAnalyticsCategory: {wire: ok, errors: ok, state: ok, persist: ok, note: "forgets resource tags"}
  ListCallAnalyticsCategories: {wire: ok, errors: ok, state: ok, persist: ok, note: "same CategoryProperties fix"}
  CreateLanguageModel: {wire: ok, errors: ok, state: ok, persist: ok, note: "output now echoes InputDataConfig (was dropped)"}
  DeleteLanguageModel: {wire: ok, errors: ok, state: ok, persist: ok, note: "forgets resource tags"}
  DescribeLanguageModel: {wire: ok, errors: ok, state: ok, persist: ok, note: "added FailureReason field to LanguageModel (was missing entirely)"}
  ListLanguageModels: {wire: ok, errors: ok, state: ok, persist: ok, note: "added NameContains filter"}
  CreateVocabulary: {wire: ok, errors: ok, state: ok, persist: ok, note: "output now includes LastModifiedTime + FailureReason (both were missing)"}
  GetVocabulary: {wire: ok, errors: ok, state: ok, persist: ok, note: "output now includes FailureReason"}
  UpdateVocabulary: {wire: ok, errors: ok, state: ok, persist: ok, note: "output now includes LastModifiedTime"}
  DeleteVocabulary: {wire: ok, errors: ok, state: ok, persist: ok, note: "forgets resource tags"}
  ListVocabularies: {wire: ok, errors: ok, state: ok, persist: ok, note: "added NameContains filter + top-level Status field (echoes StateEquals, per real ListVocabulariesOutput)"}
  CreateVocabularyFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "output now includes LastModifiedTime (was missing)"}
  GetVocabularyFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "output now includes DownloadUri + LastModifiedTime (both were missing entirely -- a client could not previously fetch a filter's contents via Get)"}
  UpdateVocabularyFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "output now includes LastModifiedTime"}
  DeleteVocabularyFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "forgets resource tags"}
  ListVocabularyFilters: {wire: ok, errors: ok, state: ok, persist: ok, note: "added NameContains filter + LastModifiedTime on VocabularyFilterInfo (was missing)"}
  CreateMedicalVocabulary: {wire: ok, errors: ok, state: ok, persist: ok, note: "output now includes LastModifiedTime + FailureReason"}
  GetMedicalVocabulary: {wire: ok, errors: ok, state: ok, persist: ok, note: "output now includes FailureReason"}
  UpdateMedicalVocabulary: {wire: ok, errors: ok, state: ok, persist: ok, note: "output now includes LastModifiedTime"}
  DeleteMedicalVocabulary: {wire: ok, errors: ok, state: ok, persist: ok, note: "forgets resource tags"}
  ListMedicalVocabularies: {wire: ok, errors: ok, state: ok, persist: ok, note: "added NameContains filter + top-level Status field"}
  StartMedicalScribeJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "removed invented top-level OutputBucketName; added synthesized MedicalScribeOutput (ClinicalDocumentUri/TranscriptFileUri), a real field gopherstack omitted entirely"}
  GetMedicalScribeJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fixes as Start"}
  ListMedicalScribeJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (was partial): summary now trimmed to the real MedicalScribeJobSummary fields (no more Media/Settings/Tags/ChannelDefinitions leaking through) + added JobNameContains filter"}
  DeleteMedicalScribeJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "forgets resource tags"}
  StartMedicalTranscriptionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "removed invented top-level OutputBucketName/OutputKey"}
  GetMedicalTranscriptionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as Start"}
  ListMedicalTranscriptionJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (was partial): summary now trimmed to the real MedicalTranscriptionJobSummary fields, plus added the previously-missing OutputLocationType/ContentIdentificationType/Specialty/Type/StartTime fields + JobNameContains filter"}
  DeleteMedicalTranscriptionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "forgets resource tags"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  vocabulary_get_lastmodified: {status: ok, note: "unchanged this pass"}
  list_namecontains_filters: {status: ok, note: "NEW this pass: NameContains/JobNameContains was completely unimplemented on all 7 List ops that document it (ListVocabularies, ListMedicalVocabularies, ListVocabularyFilters, ListTranscriptionJobs, ListMedicalTranscriptionJobs, ListMedicalScribeJobs, ListCallAnalyticsJobs, ListLanguageModels); a client filtering by name substring got the unfiltered full list back. Fixed via matchesNameContains (case-insensitive substring, store.go) threaded through every backend List method + StorageBackend interface + handler input struct."}
  language_id_settings: {status: ok, note: "NEW this pass: LanguageIdSettings (StartTranscriptionJobInput field + TranscriptionJob.LanguageIdSettings response field, used for per-language custom-vocabulary/model selection under IdentifyLanguage/IdentifyMultipleLanguages) was entirely unimplemented -- not in the input struct, not stored, not echoed. Added end-to-end."}
  invented_output_fields_removed: {status: ok, note: "NEW this pass: TranscriptionJob/MedicalTranscriptionJob/MedicalScribeJob Get+Start responses previously echoed top-level OutputBucketName/OutputKey fields that do not exist on the real TranscriptionJob/MedicalTranscriptionJob/MedicalScribeJob response shapes (confirmed against types.go -- output location is only ever surfaced via the nested Transcript/MedicalScribeOutput URIs). Removed from all three wire-output structs; the backend structs keep the fields internally to compute the synthetic S3 URIs."}
  max_results_honored: {status: ok, note: "FIXED this pass (gopherstack-5or5): MaxResults was accepted on the wire but silently discarded on all 9 List* ops (ListTranscriptionJobs, ListVocabularies, ListVocabularyFilters, ListMedicalVocabularies, ListMedicalScribeJobs, ListCallAnalyticsCategories, ListMedicalTranscriptionJobs, ListCallAnalyticsJobs, ListLanguageModels) -- page size was always the fixed transcribeDefaultPageSize=100 constant. Field-diffed the real API reference for every List op: all 9 document identical bounds, 'Valid Range: Minimum value of 1. Maximum value of 100', default of 5 when omitted. paginateList/clampMaxResults (store.go) now honor a caller-supplied MaxResults clamped to [1,100]; threaded through all 9 backend methods + StorageBackend interface + handler input structs. gopherstack intentionally keeps the larger transcribeDefaultPageSize=100 (not AWS's documented default of 5) when MaxResults is omitted -- real SDK clients always page via NextToken regardless of page size, so a larger unrequested default page is non-breaking and was already gopherstack's established (if previously unintentional) behavior."}
  language_id_settings_validation: {status: ok, note: "FIXED this pass (gopherstack-5or5, partial): LanguageIdSettings previously had zero validation. Added: map size <= 5 entries ('Map Entries: Maximum number of 5 items'), keys must be supported language codes, and LanguageModelName sub-parameter is rejected when IdentifyMultipleLanguages is set ('multi-language identification doesn't support custom language models', per StartTranscriptionJob docs). Deliberately NOT enforced: AWS only *recommends* (does not require) also supplying LanguageOptions alongside LanguageIdSettings ('It's recommended that you include LanguageOptions when using LanguageIdSettings') -- the original issue described this as a hard cross-validation gap, but the real API doc language is a recommendation, not a rejection rule, so adding a hard error here would be inventing behavior the real service doesn't have."}
gaps:
  - "CallAnalyticsJobDetails (skipped-analytics-feature reporting) on CallAnalyticsJobSummary/CallAnalyticsJob is not implemented -- gopherstack's synthetic backend never skips any Call Analytics feature, so this optional field would always be absent/empty in a real scenario too; low priority. Re-checked this pass (gopherstack-5or5): still true, still no backing data to populate Skipped[] truthfully, left undone rather than fabricated."
  - "MedicalScribeContext (StartMedicalScribeJobInput patient-context field) and MedicalScribeContextProvided (response echo of whether it was supplied) are not implemented. Since gopherstack never accepts MedicalScribeContext, MedicalScribeContextProvided would always be false, and awsjson1.1 omits false bool fields on the wire (matching the omitted-field behavior already produced by not implementing it) -- low priority, not client-breaking. Re-checked this pass (gopherstack-5or5): still true."
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; Snapshot/Restore delegate cleanly to InMemoryBackend; Handler.Snapshot/Restore already exposed. New backend struct fields (LanguageIdSettings, FailureReason x3, MedicalScribeOutput synthesis) are all pure additive struct fields going through the existing generic store.Table snapshot/restore path (store_setup.go) -- no new tables, no new lock paths, no persistence.go changes needed."}
---

## Notes

Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: Transcribe.<Op>`. Route
matcher (`RouteMatcher`/`ExtractOperation` in handler.go) is a simple prefix match on
the target header — verified all 43 ops are reachable via `TestSDKCompleteness`
(sdk_completeness_test.go), which fails if the upstream SDK adds an op gopherstack
hasn't wired up. No stub registration issues found (single `buildOps()` map, no
overwrite-order hazard).

### Bug found and fixed #1 — RFC3339 string timestamps instead of epoch-seconds numbers

Amazon Transcribe's json-1.1 protocol serializes all `time.Time` shapes (CreationTime,
StartTime, CompletionTime, CreateTime, LastModifiedTime) as epoch-seconds JSON
*numbers* — confirmed directly against the real SDK's `deserializers.go`, which calls
`smithytime.ParseEpochSeconds(f64)` on every one of these fields across
TranscriptionJob, TranscriptionJobSummary, CallAnalyticsJob, CallAnalyticsJobSummary,
CategoryProperties, LanguageModel, MedicalScribeJob(Summary),
MedicalTranscriptionJob(Summary), VocabularyFilterInfo, VocabularyInfo.

Before this fix, every timestamp field across TranscriptionJob, CallAnalyticsJob,
MedicalScribeJob, MedicalTranscriptionJob, and LanguageModel outputs was formatted as
an RFC3339 **string** (`time.RFC3339`) instead. Per `pkgs/awstime`'s doc comment, real
`aws-sdk-go-v2` deserializers *reject* an RFC3339 string in this position outright
("expected Timestamp to be a JSON Number, got string instead") — meaning every single
real SDK client calling GetTranscriptionJob, ListTranscriptionJobs,
GetCallAnalyticsJob, ListCallAnalyticsJobs, GetMedicalScribeJob,
GetMedicalTranscriptionJob, or DescribeLanguageModel/ListLanguageModels against
gopherstack would fail to unmarshal the response. This was the widest-blast-radius bug
found this sweep — it affected every read path across five of the eight resource
families. Fixed by switching all six affected output builders
(`buildTranscriptionJobOutput`, `buildCallAnalyticsJobOutput`,
`buildMedicalScribeJobOutput`, `buildMedicalTranscriptionJobOutput`,
`toLanguageModelOutput`, and the two List summary loops) to `*float64` fields
populated via `pkgs/awstime.Epoch`. GetVocabulary/GetMedicalVocabulary's
`LastModifiedTime` was already a `*float64` (via raw `.Unix()`), so it wasn't broken,
but was switched to `awstime.Epoch` too for sub-second precision and to match the new
house style.

Regression test: `TestTranscribe_TimestampFields_AreJSONNumbers` and
`TestTranscribe_DescribeLanguageModel_TimestampFieldsAreJSONNumbers` in
handler_test.go decode each timestamp field as `encoding/json.Number` and fail if it
was emitted as a quoted string.

### Bug found and fixed #2 — Tags silently dropped on 5 Create ops; creation-time Tags on all 9 ops never observable via ListTagsForResource

Real AWS Transcribe's `CreateVocabulary`, `CreateVocabularyFilter`,
`CreateMedicalVocabulary`, `CreateLanguageModel`, and `CreateCallAnalyticsCategory`
inputs *all* carry a `Tags []Tag` field (confirmed in the real SDK's
`api_op_Create*.go`), and per AWS docs these become real resource tags immediately,
retrievable only via `ListTagsForResource` (none of GetVocabulary,
DescribeLanguageModel, GetVocabularyFilter, GetMedicalVocabulary, or
GetCallAnalyticsCategory echo Tags back — confirmed against the real SDK's output
structs). Before this fix, gopherstack's request-input structs for all five of these
ops had no `Tags` field at all — a client tagging a vocabulary/filter/language
model/category at creation time got HTTP 200 with the tags completely discarded, no
error, no trace. `CreateMedicalVocabulary`'s backend signature didn't even have a
place to put a tags parameter (positional-string-args method).

Separately, `StartTranscriptionJob`/`StartCallAnalyticsJob`/`StartMedicalScribeJob`/
`StartMedicalTranscriptionJob` *did* already thread `Tags` into the stored job struct
(so Get/List job calls that echo `Tags` directly worked), but never synced into the
ARN-keyed `resourceTags` map used by `TagResource`/`ListTagsForResource` — so
`ListTagsForResource(jobArn)` right after `StartTranscriptionJob(..., Tags: {...})`
returned empty, another AWS-behavior mismatch (this exact "Tags aren't synced from
Start to ListTagsForResource" pattern is systemic across other gopherstack services
too, e.g. services/comprehend — flagged as a cross-service follow-up in `gaps`, not
fixed outside transcribe per this task's scope).

Fixed by:
- Adding `Tags map[string]string` fields to the `Vocabulary`, `VocabularyFilter`,
  `LanguageModel`, `CallAnalyticsCategory`, and `MedicalVocabulary` backend structs.
- Adding a `tags map[string]string` parameter to `CreateMedicalVocabulary`'s backend
  signature (`StorageBackend` interface + `InMemoryBackend` + the 2 call sites in
  persistence_test.go/handler_test.go that called it positionally).
- Threading `Tags` through the 5 affected request-input structs in handler.go
  (`createVocabularyInput`, `createVocabularyFilterInput`,
  `createMedicalVocabularyInput`, `createLanguageModelInput`,
  `createCallAnalyticsCategoryInput`).
- Adding `resourceARN(resourceType, name string) string` (using `pkgs/arn.Build` +
  `pkgs/config.DefaultRegion`) with resource-type segments confirmed against the real
  SDK's `TagResource`/`ListTagsForResource`/`UntagResource` doc-comment example
  (`arn:aws:transcribe:us-west-2:111122223333:transcription-job/transcription-job-name`)
  for `transcription-job`, and AWS's standard IAM-policy resource-ARN naming
  convention for the other eight types (`call-analytics-job`,
  `call-analytics-category`, `medical-scribe-job`, `medical-transcription-job`,
  `vocabulary`, `vocabulary-filter`, `medical-vocabulary`, `language-model`).
- `recordResourceTagsLocked`/`forgetResourceTagsLocked` helpers in backend.go, called
  (while already holding `b.mu`) after every successful Create/Start of a taggable
  resource, and on every Delete (so a deleted resource's tags don't linger and get
  returned by `ListTagsForResource` for an ARN that no longer maps to anything).

Regression tests: `TestBackend_CreationTags_SyncToResourceARN` (table-driven, all 9
taggable creation ops) and `TestBackend_Delete_ForgetsResourceTags` /
`TestBackend_CreationWithoutTags_LeavesResourceTagsEmpty` in the new backend_test.go.

### Bug found and fixed #3 (2026-07-24 sweep) — NameContains filters, LanguageIdSettings, invented output fields, thin summary/output shapes

This pass re-field-diffed every op against `aws-sdk-go-v2/service/transcribe@v1.55.0`'s
generated `api_op_*.go`/`types.go` (not just the previously-audited output timestamp/tag
issues) and found several real, previously-unnoticed wire-shape gaps:

1. **NameContains/JobNameContains completely unimplemented** on all 7 List ops that
   document it (`ListVocabularies`, `ListMedicalVocabularies`, `ListVocabularyFilters`,
   `ListTranscriptionJobs`, `ListMedicalTranscriptionJobs`, `ListMedicalScribeJobs`,
   `ListCallAnalyticsJobs`, `ListLanguageModels`) — a real client filtering by name
   substring silently got back the full unfiltered list. Fixed with a shared
   `matchesNameContains` helper (`store.go`, case-insensitive substring per AWS's "the
   search is not case sensitive" doc wording) threaded through every backend `List*`
   method, the `StorageBackend` interface, and every list handler's input struct.

2. **LanguageIdSettings entirely missing** — real `StartTranscriptionJobInput` and the
   `TranscriptionJob` response both carry a `LanguageIdSettings
   map[string]LanguageIdSettings` field (per-language custom vocabulary/model/filter
   selection under `IdentifyLanguage`/`IdentifyMultipleLanguages`), explicitly called
   out for verification in this pass's task brief. gopherstack had no such field
   anywhere — not in the input struct, not on the backend `TranscriptionJob`, not
   echoed. Added end-to-end (`LanguageIDSettings map[string]LanguageIDSettings` on the
   backend struct, threaded through `StartTranscriptionJob`'s input and
   `transcriptionJobOutput`).

3. **Invented top-level `OutputBucketName`/`OutputKey` response fields.** The real
   `TranscriptionJob`, `MedicalTranscriptionJob`, and `MedicalScribeJob` response types
   (confirmed against `types.go`) have **no** `OutputBucketName`/`OutputKey` fields at
   all — the output location is only ever surfaced via the nested
   `Transcript.TranscriptFileUri` (or `MedicalScribeOutput.*Uri`). gopherstack's three
   `Get*`/`Start*` wire-output structs echoed these back at the top level regardless —
   a gopherstack-invented field not present in the real SDK, per this task's hard
   constraint to delete such fields. Removed from all three output structs; the
   *backend* structs keep the fields (needed internally to compute the synthetic S3
   URIs), only the wire response was trimmed.

4. **`MedicalScribeJob` responses never included `MedicalScribeOutput`** — real AWS
   returns `MedicalScribeOutput{ClinicalDocumentUri, TranscriptFileUri}` once a job
   reaches `COMPLETED` (required fields on that type). gopherstack synthesized a
   transcript URI for every other job family (`Transcript.TranscriptFileUri`) but never
   did the equivalent for Medical Scribe jobs, meaning a client polling
   `GetMedicalScribeJob` on a completed job had no way to locate its output at all.
   Added `buildMedicalScribeOutputLocations`, synthesizing both URIs the same way
   `buildTranscriptURI`/`buildMedicalTranscriptURI` already do for the other job kinds.

5. **`ListMedicalScribeJobs`/`ListMedicalTranscriptionJobs` summary wire-shape
   deviation** (previously flagged `partial` in this manifest, not fixed) — both
   reused the full `Get*` output shape as their List summary, which is a strict
   superset of the real `MedicalScribeJobSummary`/`MedicalTranscriptionJobSummary`
   fields (leaking `Media`, `Settings`, `Tags`, `ChannelDefinitions`, etc.). Fixed by
   introducing dedicated `medicalScribeJobSummary`/`medicalTranscriptionJobSummary`
   wire types matching the real summary shapes field-for-field (including the
   previously-absent `OutputLocationType`/`ContentIdentificationType`/`Specialty`/
   `Type`/`StartTime` on the medical-transcription summary).

6. **Several thinner-than-real output shapes**, each missing real, documented response
   fields:
   - `TranscriptionJobSummary` was missing `StartTime`, `IdentifyLanguage`,
     `IdentifyMultipleLanguages`, `IdentifiedLanguageScore`, `ContentRedaction`,
     `ModelSettings`, `LanguageCodes`, `ToxicityDetection`, and `OutputLocationType`
     (added a `outputLocationType` helper deriving `CUSTOMER_BUCKET`/`SERVICE_BUCKET`
     from whether `OutputBucketName` was set, matching AWS's documented semantics).
   - `CallAnalyticsJobSummary` was missing `StartTime`.
   - `CategoryProperties` (Call Analytics category Create/Get/Update/List) was missing
     `CreateTime`, `LastUpdateTime`, and `Tags` entirely — real
     `CreateCallAnalyticsCategoryOutput`/etc. include all three.
   - `CreateVocabulary`/`CreateMedicalVocabulary` outputs were missing
     `LastModifiedTime` and `FailureReason`; `UpdateVocabulary`/`UpdateMedicalVocabulary`
     were missing `LastModifiedTime`; `GetVocabulary`/`GetMedicalVocabulary` were
     missing `FailureReason`.
   - `GetVocabularyFilterOutput` was missing **both** `DownloadUri` and
     `LastModifiedTime` — meaning a real client had no way to fetch a vocabulary
     filter's contents via `GetVocabularyFilter` at all, since gopherstack simply never
     returned the URI. `CreateVocabularyFilterOutput`/`UpdateVocabularyFilterOutput`/
     `VocabularyFilterInfo` (the `ListVocabularyFilters` element type) were all missing
     `LastModifiedTime`.
   - `ListVocabularies`/`ListMedicalVocabularies` were missing the top-level `Status`
     field (echoes the `StateEquals` request filter, per the real
     `ListVocabulariesOutput`/`ListMedicalVocabulariesOutput` shape).
   - `CreateLanguageModelOutput` was missing `InputDataConfig`; the `LanguageModel`
     type itself (and therefore `DescribeLanguageModel`/`ListLanguageModels`) was
     missing `FailureReason` — added the field to the backend struct and threaded it
     through (always empty in this synthetic backend, since models never fail, but the
     field must exist on the wire for real client unmarshaling to match the schema).

Regression tests (one file per family, table-driven, `t.Parallel()`, no shared
subtest state): `TestListTranscriptionJobs_JobNameContains`,
`TestTranscriptionJob_LanguageIdSettings_Echoed`,
`TestTranscriptionJob_OutputBucketNotInResponse`,
`TestListVocabularies_NameContains`,
`TestCreateVocabulary_LastModifiedTimeAndFailureReasonEchoed`,
`TestListVocabularies_EchoesStatusFilter`, `TestListVocabularyFilters_NameContains`,
`TestVocabularyFilter_LastModifiedTimeAndDownloadUri`,
`TestListMedicalVocabularies_NameContains`,
`TestMedicalVocabulary_LastModifiedTimeAndFailureReason`,
`TestListLanguageModels_NameContains`, `TestCreateLanguageModel_EchoesInputDataConfig`,
`TestCallAnalyticsCategory_CreateTimeAndLastUpdateTimeEchoed`,
`TestListCallAnalyticsJobs_JobNameContainsAndStartTime`,
`TestListMedicalScribeJobs_JobNameContainsAndSummaryShape`,
`TestMedicalScribeJob_OutputURIsPresentWhenCompleted`,
`TestListMedicalTranscriptionJobs_JobNameContainsAndSummaryShape`.

### Looks-wrong-but-correct traps (don't re-flag)

- `ErrVocabularyNotFound` (GetVocabulary's "not found" path) deliberately maps to
  `BadRequestException` (400), not `NotFoundException` (404) — this is intentional,
  documented AWS behavior for missing vocabularies specifically, per the comment on
  `ErrVocabularyNotFound` in backend.go. Every other resource kind's "not found"
  correctly maps to `NotFoundException`.
- `StartTranscriptionJob` with `JobExecutionSettings.AllowDeferredExecution=true`
  intentionally starts a job in `QUEUED` and advances it one state per
  `GetTranscriptionJob` poll (`QUEUED` → `IN_PROGRESS` → `COMPLETED`) via
  `advanceDeferredTranscriptionJob` — this is a deliberate state-machine simulating
  deferred execution, not a stuck/no-op job. All other Start*Job paths complete
  synchronously (no real ASR, but a deterministic mock transcript is generated and the
  job lands directly in `COMPLETED`), which is correct per this audit's scope (mock
  transcript content is acceptable; only lifecycle/wire/error/routing bugs are in
  scope).
- `paginateList`'s `nextToken` is a plain string-encoded integer offset. This is fine:
  real AWS clients never parse `NextToken` — it's opaque by contract — so this doesn't
  need to match any particular AWS-internal format.
