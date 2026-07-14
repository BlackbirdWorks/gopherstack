---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: transcribe
sdk_module: aws-sdk-go-v2/service/transcribe@v1.55.0   # version audited against
last_audit_commit: 0e2e9a93               # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: A            # A = genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  StartTranscriptionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed epoch timestamps + tag sync"}
  GetTranscriptionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed epoch timestamps; deferred-job polling advances QUEUED->IN_PROGRESS->COMPLETED correctly"}
  ListTranscriptionJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed epoch timestamps"}
  DeleteTranscriptionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "now forgets resource tags"}
  StartCallAnalyticsJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed epoch timestamps + tag sync"}
  GetCallAnalyticsJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCallAnalyticsJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed epoch timestamps"}
  DeleteCallAnalyticsJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "now forgets resource tags"}
  CreateCallAnalyticsCategory: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags input was silently dropped; now threaded through and synced"}
  GetCallAnalyticsCategory: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCallAnalyticsCategory: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCallAnalyticsCategory: {wire: ok, errors: ok, state: ok, persist: ok, note: "now forgets resource tags"}
  ListCallAnalyticsCategories: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLanguageModel: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags input was silently dropped; now threaded through and synced"}
  DeleteLanguageModel: {wire: ok, errors: ok, state: ok, persist: ok, note: "now forgets resource tags"}
  DescribeLanguageModel: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed epoch timestamps"}
  ListLanguageModels: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed epoch timestamps"}
  CreateVocabulary: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags input was silently dropped; now threaded through and synced"}
  GetVocabulary: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateVocabulary: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVocabulary: {wire: ok, errors: ok, state: ok, persist: ok, note: "now forgets resource tags"}
  ListVocabularies: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateVocabularyFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags input was silently dropped; now threaded through and synced"}
  GetVocabularyFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateVocabularyFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVocabularyFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "now forgets resource tags"}
  ListVocabularyFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMedicalVocabulary: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags param was entirely absent from the backend signature; added"}
  GetMedicalVocabulary: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMedicalVocabulary: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMedicalVocabulary: {wire: ok, errors: ok, state: ok, persist: ok, note: "now forgets resource tags"}
  ListMedicalVocabularies: {wire: ok, errors: ok, state: ok, persist: ok}
  StartMedicalScribeJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed epoch timestamps + tag sync"}
  GetMedicalScribeJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed epoch timestamps"}
  ListMedicalScribeJobs: {wire: partial, errors: ok, state: ok, persist: ok, note: "summary reuses the full get-shape (extra fields Media/Settings/Tags/etc. beyond real MedicalScribeJobSummary); harmless (unknown JSON fields ignored by SDK deserializer), not fixed this pass"}
  DeleteMedicalScribeJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "now forgets resource tags"}
  StartMedicalTranscriptionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed epoch timestamps + tag sync"}
  GetMedicalTranscriptionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed epoch timestamps"}
  ListMedicalTranscriptionJobs: {wire: partial, errors: ok, state: ok, persist: ok, note: "summary reuses the full get-shape (extra fields beyond real MedicalTranscriptionJobSummary); harmless, not fixed this pass"}
  DeleteMedicalTranscriptionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "now forgets resource tags"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "now actually observes tags supplied at resource-creation time, not just tags added later via TagResource"}
families:
  vocabulary_get_lastmodified: {status: ok, note: "GetVocabulary/GetMedicalVocabulary already used epoch float64 (Unix()); standardized on pkgs/awstime.Epoch for sub-second precision consistency with the rest of the fix"}
gaps:
  - "ListMedicalScribeJobs/ListMedicalTranscriptionJobs summaries include extra fields (Media, Settings, Tags, OutputBucketName, etc.) not present on the real MedicalScribeJobSummary/MedicalTranscriptionJobSummary shapes. Harmless (unknown JSON keys are ignored by the SDK's json-1.1 deserializer) but a wire-shape deviation worth trimming in a future pass. Not fixed this sweep to keep scope on client-breaking bugs."
  - "Systemic cross-service pattern (NOT transcribe-specific, seen in services/comprehend too): before this fix, Tags supplied at resource-creation time were never synced into the ARN-keyed tag store, so ListTagsForResource never reflected them. Transcribe now fixes this locally; other services likely still have the same gap and were left untouched per this task's scope (services/transcribe/ only) — worth a dedicated cross-service sweep, no bd issue filed for this run's scope."
deferred:
  - MedicalScribeJobSummary/MedicalTranscriptionJobSummary field-trimming (see gaps)
leaks: {status: clean, note: "no goroutines/janitors in this service; Snapshot/Restore delegate cleanly to InMemoryBackend; Handler.Snapshot/Restore already exposed pre-audit"}
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
