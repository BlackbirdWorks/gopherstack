---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: comprehend
sdk_module: aws-sdk-go-v2/service/comprehend@v1.41.0
last_audit_commit: 0e933737
last_audit_date: 2026-07-24
overall: A            # fabricated op family deleted, wire-shape/error-code bugs fixed, prior gaps closed
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  DetectSentiment: {wire: ok, errors: ok, state: ok, persist: n/a, note: "synchronous, deterministic word-list mock is acceptable; LanguageCode now required+validated (12-lang enum), Text now enforces the real 5KB limit -> TextSizeLimitExceededException"}
  DetectEntities: {wire: ok, errors: ok, state: ok, persist: n/a, note: "LanguageCode correctly optional (EndpointArn alternative per real API) but format-validated when supplied; Text enforces 100KB limit"}
  DetectKeyPhrases: {wire: ok, errors: ok, state: ok, persist: n/a, note: "LanguageCode required+validated; Text enforces 100KB limit"}
  DetectPiiEntities: {wire: ok, errors: ok, state: ok, persist: n/a, note: "LanguageCode required+validated; Text enforces 100KB limit"}
  DetectSyntax: {wire: ok, errors: ok, state: ok, persist: n/a, note: "LanguageCode validated against the narrower 6-value SyntaxLanguageCode enum (types.LanguageCode's 12 values do NOT all apply here); Text enforces 5KB limit"}
  DetectDominantLanguage: {wire: ok, errors: ok, state: ok, persist: n/a, note: "correctly has no LanguageCode field; Text enforces 100KB limit"}
  DetectToxicContent: {wire: ok, errors: ok, state: ok, persist: n/a, note: "ResultList/Labels/Toxicity field names verified against types.ToxicLabels; LanguageCode required+English-only per real doc comment despite the general enum type; TextSegments now enforces 1KB-per-segment/10KB-total"}
  DetectTargetedSentiment: {wire: ok, errors: ok, state: ok, persist: n/a, note: "LanguageCode required+English-only per real doc comment; Text enforces 5KB limit"}
  ClassifyDocument: {wire: ok, errors: ok, state: ok, persist: n/a, note: "correctly has no LanguageCode field; Text enforces 100KB limit"}
  ContainsPiiEntities: {wire: ok, errors: ok, state: ok, persist: n/a, note: "LanguageCode required+validated; Text enforces 100KB limit"}
  BatchDetect*: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED: TextList>25 items now rejected whole-request with BatchSizeLimitExceededException (was silently accepted); per-item >5KB now becomes a BatchItemError entry (ErrorCode/ErrorMessage/Index) in ErrorList instead of being ignored, matching every Batch*Output doc comment's 'if there are no errors in the batch, the ErrorList is empty' partial-failure semantics; shared LanguageCode validated once per request against the correct per-op allowed set (BatchDetectSyntax: 6-lang, BatchDetectTargetedSentiment: English-only, others: 12-lang)"}
  Start*DetectionJob (9 families): {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags correctly seed b.tags[JobArn] (prior fix, re-verified); NEW this pass: TooManyTagsException (>50 initial tags) and KmsKeyValidationException (malformed VolumeKmsKeyId) enforced before job creation"}
  Describe*DetectionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED wire-shape bug: per-family *Properties field sets now field-diffed individually (see jobSpec/jobMap) -- e.g. DocumentClassificationJobProperties carries FlywheelArn+VolumeKmsKeyId+VpcConfig but NO LanguageCode, PiiEntitiesDetectionJobProperties carries Mode+RedactionConfig but NO VolumeKmsKeyId/VpcConfig, TopicsDetectionJobProperties carries NumberOfTopics but NO LanguageCode; previously every family emitted the SAME field set regardless of its real shape. FIXED error-code bug: job-not-found now returns JobNotFoundException, not ResourceNotFoundException (confirmed against every awsAwsjson11_deserializeOpErrorDescribe*Job case in the SDK's deserializers.go). FIXED field-name bug: failure description field is 'Message' on every real *Properties shape, not 'FailureReason' (no such field exists on any of them -- a failed job's description was previously always lost on the wire). NEW: Filter (JobName/JobStatus/SubmitTimeBefore/SubmitTimeAfter) now supported on List*Jobs, previously ignored entirely."}
  List*DetectionJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "see Describe*DetectionJob for the per-family field-set fix and new Filter support"}
  Stop*DetectionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly rejects stop on terminal states with InvalidRequestException; not-found now JobNotFoundException (see Describe*DetectionJob)"}
  CreateDocumentClassifier/CreateEntityRecognizer: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: deleted the fabricated CreateDocumentClassifierVersion/CreateEntityRecognizerVersion op family -- no such operations exist in the real SDK (confirmed: no matching api_op_*.go files); a new version is created by calling these SAME ops again with the same name and a new VersionName, which they already supported generically. NEW: TooManyTagsException/KmsKeyValidationException (ModelKmsKeyId) enforced; DocumentClassifierProperties/EntityRecognizerProperties now populate TrainingStartTime/TrainingEndTime/ClassifierMetadata/RecognizerMetadata (deterministic synthetic values, only once status=TRAINED, matching real semantics) -- closes last pass's documented gap"}
  DescribeDocumentClassifier/DescribeEntityRecognizer: {wire: ok, errors: ok, state: ok, persist: ok, note: "SubmitTime/EndTime field names correct; see CreateDocumentClassifier/CreateEntityRecognizer for the removed fabricated Version ops and new metadata fields"}
  ListDocumentClassifiers/ListEntityRecognizers: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW: Filter (Name/Status/SubmitTimeBefore/SubmitTimeAfter) now supported, previously ignored entirely"}
  DeleteDocumentClassifier/DeleteEntityRecognizer: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEndpoint/DescribeEndpoint/ListEndpoints/UpdateEndpoint/DeleteEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime/LastModifiedTime correct (prior fix, re-verified); NEW: ListEndpoints Filter (ModelArn/Status/CreationTimeBefore/CreationTimeAfter) now supported"}
  CreateFlywheel/DescribeFlywheel/ListFlywheels/UpdateFlywheel/DeleteFlywheel: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime/LastModifiedTime + FlywheelSummaryList list-wrapper correct (prior fixes, re-verified); NEW: ListFlywheels Filter (Status/CreationTimeBefore/CreationTimeAfter) now supported"}
  CreateDataset/DescribeDataset/ListDatasets: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime/EndTime correct (prior fix, re-verified); NEW: ListDatasets Filter (DatasetType/Status/CreationTimeBefore/CreationTimeAfter) now supported"}
  StartFlywheelIteration/GetFlywheelIteration/DescribeFlywheelIteration/ListFlywheelIterationHistory: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource/UntagResource/ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "covers job ARNs too (prior fix); NEW: TagResource now enforces TooManyTagsException when the merged (existing+new) tag count would exceed 50"}
  ImportModel: {wire: ok, errors: ok, state: ok, persist: ok, note: "resourceType correctly derived from SourceModelArn (prior fix, re-verified)"}
  ListDocumentClassifierSummaries/ListEntityRecognizerSummaries: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED: now groups resources by Name into one summary row per distinct name with an aggregated NumberOfVersions and the most-recently-created resource as the 'latest version' -- previously emitted one row per stored resource with NumberOfVersions hardcoded to 1, which became visibly wrong once real multi-version classifiers/recognizers were reachable (see the fabricated-Version-op removal above)"}
  StopTrainingDocumentClassifier/StopTrainingEntityRecognizer: {wire: ok, errors: ok, state: ok, persist: ok}
  Put/Describe/DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "revision-conflict checked via ResourceInUseException, matches AWS optimistic-concurrency semantics"}
# Families audited as a group (when per-op is impractical):
families:
  routing: {status: ok, note: "RouteMatcher/ExtractOperation verified against X-Amz-Target: Comprehend_20171127.<Op> prefix; sdk_completeness_test.go confirms every SDK op is routed (no notImplemented entries needed) -- also re-confirms the deleted fabricated Version ops were never part of the real SDK surface this test checks against, so removing them didn't regress completeness"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "None carried over from last pass: both documented gaps (BatchDetect* ErrorList, ClassifierMetadata/RecognizerMetadata/TrainingStartTime/TrainingEndTime) are fixed this pass -- see ops above."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "ResourceLimitExceededException/ResourceUnavailableException/TooManyRequestsException/ConcurrentModificationException are real modeled errors for several ops here (confirmed against deserializers.go's per-op error-case switches) but have no non-fabricated deterministic trigger in this emulator: no rate limiting is implemented anywhere in gopherstack per-service (generic throttling/5xx injection exists instead via the chaos fault-injection system -- ChaosOperations/ChaosServiceName), no fixed per-account resource quota is documented precisely enough to emulate without risking false failures on legitimate high-volume test/integration usage, and ConcurrentModificationException describes a real-AWS eventual-consistency race that cannot occur under this backend's single coarse lock. Error-code wiring in errors.go/handler.go intentionally does not include sentinels for these four; add them if a concrete, non-arbitrary trigger condition is ever identified."
  - "KmsKeyValidationException is enforced for the two top-level KMS key fields (ModelKmsKeyId on Create*/ImportModel, VolumeKmsKeyId on Start*Job) but not for KMS key fields potentially nested inside DataSecurityConfig (CreateFlywheel/CreateDataset) -- narrower shape, not reached by any current test or known client code path."
  - "Nested VpcConfig/RedactionConfig/DataSecurityConfig object shapes are passed through opaquely (whatever the caller sent) rather than field-diffed sub-field-by-sub-field against their real types.VpcConfig/types.RedactionConfig/types.DataSecurityConfig shapes -- the top-level presence/absence per job-family and resource-family is now correct (see Describe*DetectionJob/CreateFlywheel notes above), but the internals of those nested objects are unverified this pass."
leaks: {status: clean, note: "no goroutines/timers spawned by this service; job/resource lifecycle advances synchronously on each Describe/List poll (advanceJob/advanceTrainingResource), no background janitor to leak. Confirmed unchanged this pass -- no new goroutines/timers were introduced by any of this pass's fixes."}
---

## Notes

Freeform: AWS-behavior specifics worth remembering (exact algorithms, wire quirks,
error-message text, protocol = query-XML / REST-XML / REST-JSON / json-1.0), and any
"looks-wrong-but-correct" traps so the next auditor doesn't re-flag them.

- Protocol is awsjson1.1 (`X-Amz-Target: Comprehend_20171127.<Op>`,
  `application/x-amz-json-1.1`). All error bodies use `{"__type": "<Code>", "message": "..."}`
  via `service.JSONErrorResponse`; HTTP status is 400 for every client-fault exception used
  here (all of `ResourceNotFoundException`/`ResourceInUseException`/`InvalidRequestException`/
  `JobNotFoundException`/`TooManyTagsException`/`BatchSizeLimitExceededException`/
  `TextSizeLimitExceededException`/`UnsupportedLanguageException`/`KmsKeyValidationException`
  have `smithy.ErrorFault() == FaultClient`; only `InternalServerException` is `FaultServer`
  and stays the unmapped 500 default) -- none of these carry an `@httpError` override to
  404/409/etc., so 400-for-everything-but-InternalServerException is correct for this
  protocol, not a bug.

- **A prior audit pass invented an entire fabricated resource-op family: "DocumentClassifierVersion"/
  "EntityRecognizerVersion".** `resourceSpecs()` had dedicated entries for these, generating 8
  operation names (`CreateDocumentClassifierVersion`, `DescribeDocumentClassifierVersion`,
  `ListDocumentClassifierVersions`, `DeleteDocumentClassifierVersion`, and the EntityRecognizer
  equivalents) that **do not exist in the real AWS SDK** -- confirmed by the complete absence of
  matching `api_op_*.go` files in `aws-sdk-go-v2/service/comprehend`. The real API creates a new
  version of an existing classifier/recognizer by calling `CreateDocumentClassifier`/
  `CreateEntityRecognizer` again with the SAME name and a new `VersionName`
  (`CreateDocumentClassifierInput.VersionName`/`CreateEntityRecognizerInput.VersionName` are both
  real, optional fields) -- there is no separate operation. `createResource()`'s generic handling
  already threaded `VersionName` through for every spec, so the base `"DocumentClassifier"`/
  `"EntityRecognizer"` entries in `resourceSpecs()` handle versioning correctly without a
  separate resource type; the fabricated entries and their now-orphaned `resourceTypeDocClassifierVersion`/
  `resourceTypeEntityRecognizerVer` constants have been deleted. **If you ever see a
  resourceSpecs()/asyncJobSpecs() entry whose op-name prefix has no matching real operation
  in the SDK's operation list, treat it as suspect and cross-check `api_op_*.go` before trusting it.**
  `sdkcheck.CheckCompleteness` (used by `sdk_completeness_test.go`) only checks for MISSING
  coverage of real ops, not for EXTRA fabricated ones -- it would never have caught this.

- **Async job `*Properties` shapes are NOT uniform across the 9 job families**, the same bug
  class as the resource-Properties timestamp-field split documented below, but affecting more
  fields: field-diffed against every real `*JobProperties` struct in
  `aws-sdk-go-v2/service/comprehend/types`, the split is:
  - `DocumentClassificationJobProperties`: `DocumentClassifierArn` + `FlywheelArn` +
    `VolumeKmsKeyId` + `VpcConfig`, but **no `LanguageCode`**.
  - `EntitiesDetectionJobProperties`: `EntityRecognizerArn` + `FlywheelArn` + `LanguageCode` +
    `VolumeKmsKeyId` + `VpcConfig`.
  - `KeyPhrasesDetectionJobProperties`/`SentimentDetectionJobProperties`/
    `TargetedSentimentDetectionJobProperties`: `LanguageCode` + `VolumeKmsKeyId` + `VpcConfig`,
    no classifier/recognizer/flywheel ARN fields.
  - `PiiEntitiesDetectionJobProperties`: `LanguageCode` + `Mode` + `RedactionConfig`, but
    **no `VolumeKmsKeyId`/`VpcConfig` at all**.
  - `TopicsDetectionJobProperties`: `NumberOfTopics` + `VolumeKmsKeyId` + `VpcConfig`, but
    **no `LanguageCode`**.
  - `DominantLanguageDetectionJobProperties`: `VolumeKmsKeyId` + `VpcConfig` only, **no
    `LanguageCode`** (it detects the language).
  - `EventsDetectionJobProperties`: `LanguageCode` + `TargetEventTypes`, no KMS/VPC fields.
  `jobSpec` (handler.go) now carries one bool per optional field
  (`hasLanguageCode`/`hasDocumentClassifierArn`/`hasEntityRecognizerArn`/`hasFlywheelArn`/
  `hasVolumeKmsKeyID`/`hasVpcConfig`/`hasTargetEventTypes`/`hasPiiMode`/`hasNumberOfTopics`),
  set per-family in `asyncJobSpecs()` (handler_jobs.go), and `jobMap()` gates each field on its
  flag. Previously every family emitted the SAME fixed field set (including e.g.
  `EntityRecognizerArn` and `LanguageCode` on `DocumentClassificationJobProperties`, which the
  real shape doesn't have) -- harmless-looking extra JSON keys the real SDK's client just
  ignores, but genuinely missing keys (like `FlywheelArn` on `EntitiesDetectionJobProperties`)
  left real fields permanently nil for any caller.

- **The failure-description field on every one of those 9 `*Properties` shapes is `Message`,
  not `FailureReason`.** No `FailureReason` field exists on any real `*JobProperties` struct.
  `jobMap()` previously emitted `"FailureReason": job.FailureReason`; a client unmarshalling a
  FAILED job's Describe/List response into the real SDK's generated struct would see this key
  simply dropped (no matching field), so `Message` was always nil -- the failure reason text was
  entirely unreachable from client code despite being computed correctly server-side. Fixed by
  changing the wire key only (the internal Go field `Job.FailureReason` is unchanged, it is
  purely a wire-key rename in `jobMap()`).

- **`Describe*DetectionJob`/`Stop*DetectionJob` return `JobNotFoundException` for an unknown
  job ID, not `ResourceNotFoundException`.** Confirmed against every
  `awsAwsjson11_deserializeOpErrorDescribe*Job`/`awsAwsjson11_deserializeOpErrorStop*Job` case
  in the SDK's generated `deserializers.go` -- `JobNotFoundException` is a distinct modeled
  exception used only by job Describe/Stop, while every resource family's Describe/Delete still
  correctly uses `ResourceNotFoundException`. `InMemoryBackend.DescribeJob`/`StopJob` now wrap
  `ErrJobNotFound` instead of `ErrNotFound`.

- **`List*Jobs`/`List<Resource>s` now support the `Filter` request field**, previously parsed
  and silently ignored entirely (only `NextToken`/`MaxResults` were read). Every job family's
  real `Filter` type (`JobFilter`/`SentimentDetectionJobFilter`/...) shares the same
  `JobName`/`JobStatus`/`SubmitTimeBefore`/`SubmitTimeAfter` shape (`matchesJobFilter` in
  handler_jobs.go). Resource family `Filter` types are NOT uniform (`matchesResourceFilter` in
  handler_resources.go): `DocumentClassifierFilter`/`EntityRecognizerFilter` key on
  name+`SubmitTime*`, `EndpointFilter` keys on `ModelArn`+`CreationTime*`,
  `FlywheelFilter`/`DatasetFilter` key on `CreationTime*` only (no name field), and
  `DatasetFilter` additionally has `DatasetType`. `SubmitTimeBefore`/`SubmitTimeAfter`/
  `CreationTimeBefore`/`CreationTimeAfter` arrive as epoch-seconds JSON numbers (same
  awsjson1.1 timestamp encoding as every other timestamp field here) -- `filterTime()` decodes
  them the same way `awstime.Epoch` encodes them on the way out.

- **`BatchDetect*` now enforces both real batch limits** (field-diffed from every
  `Batch*Input`/`Batch*Output` doc comment): `TextList` over 25 items is a whole-request
  `BatchSizeLimitExceededException` (nothing processed), while a single oversized (>5KB) item
  becomes a `BatchItemError` entry in `ErrorList` (`ErrorCode: "TEXT_SIZE_LIMIT_EXCEEDED"`,
  matching `Index`) while every other well-formed item still succeeds into `ResultList` --
  "If there are no errors in the batch, the ErrorList is empty" (every `Batch*Output` doc
  comment) implies per-item failures are an ordinary, expected batch outcome, not something
  that should abort the whole call. The exact `ErrorCode` string values Comprehend uses on the
  wire for `BatchItemError` aren't published in the SDK's Go types (`ErrorCode *string` is
  opaque) -- `"TEXT_SIZE_LIMIT_EXCEEDED"`/`"UNSUPPORTED_LANGUAGE"`/`"INVALID_REQUEST"` are this
  emulator's best-effort synthetic values matching the wire *shape* (Index/ErrorCode/
  ErrorMessage all populated, sorted ascending by Index) rather than confirmed exact strings.

- **LanguageCode is required (and validated against the correct allowed set) on every
  Detect\*/BatchDetect\* op here except two**: `DetectEntities` (an `EndpointArn` alternative
  makes it optional, though still format-validated if supplied) and `DetectDominantLanguage`
  (no `LanguageCode` field exists at all -- it infers the language). Three different allowed
  sets exist depending on the op (`generalLanguageCodes`/`syntaxLanguageCodes`/
  `englishOnlyLanguageCodes` in handler_detection.go), field-diffed against
  `types.LanguageCode`'s 12 enum values, `DetectSyntaxInput`'s narrower `types.SyntaxLanguageCode`
  (6 values: de/en/es/fr/it/pt), and `DetectToxicContent`/`DetectTargetedSentiment`'s doc
  comments ("Currently, English is the only supported language" despite typing `LanguageCode`
  as the general 12-value enum). An unsupported (but otherwise valid-shaped) code returns
  `UnsupportedLanguageException`; a missing required code returns `InvalidRequestException`.

- **Text size limits are enforced per operation's documented byte cap**, field-diffed from each
  op's own doc comment rather than assumed uniform: 5KB for `DetectSentiment`/`DetectSyntax`/
  `DetectTargetedSentiment`, 100KB for `DetectEntities`/`DetectKeyPhrases`/`DetectPiiEntities`/
  `ContainsPiiEntities`/`ClassifyDocument`/`DetectDominantLanguage`, and `DetectToxicContent`'s
  distinct per-segment (1KB)/total (10KB) `TextSegments` caps. Exceeding the limit returns
  `TextSizeLimitExceededException`.

- **`TooManyTagsException`** (50-tag-per-resource limit, both existing and newly-requested tags
  counted) is now enforced on `Create*`/`ImportModel`/`Start*Job` (checked before the resource/job
  is created -- a rejected request leaves no partial state) and on `TagResource` (checked against
  the merged existing+incoming key set before mutating, so a rejected call leaves existing tags
  untouched).

- **`KmsKeyValidationException`** is now enforced for `ModelKmsKeyId` (`Create*`/`ImportModel`)
  and `VolumeKmsKeyId` (`Start*Job`): the value must match either a bare KMS key ID (UUID form)
  or a `key`/`alias` ARN shape (both documented formats for every KMS key ID field in this
  service's doc comments). Empty is valid (the field is optional everywhere it appears). See
  `deferred` above for the narrower gap (nested `DataSecurityConfig` KMS fields not covered).

- **Timestamp field names are NOT uniform across resource Properties shapes** (prior pass's
  finding, unchanged and still correct). Real AWS shapes split three ways:
  - `DocumentClassifierProperties` / `EntityRecognizerProperties`: `SubmitTime` + `EndTime`
    (plus, once `Status == TRAINED`, `TrainingStartTime` + `TrainingEndTime` -- NEW this pass,
    see below).
  - `EndpointProperties` / `FlywheelProperties`: `CreationTime` + `LastModifiedTime`.
  - `DatasetProperties`: `CreationTime` + `EndTime` (no `LastModifiedTime` field exists here).
  `resourceMap()` in handler_resources.go switches on `resource.Type` to emit the right pair.

- **`ClassifierMetadata`/`RecognizerMetadata` and `TrainingStartTime`/`TrainingEndTime` now
  populate once a classifier/recognizer reaches `TRAINED`** (previously always absent -- last
  pass's documented gap). Real AWS only carries these once training has actually completed, so
  `resourceMap()` gates them on `Status == statusTrained`. The emulator fast-forwards training
  straight to `TRAINED` on create (`initialResourceStatus`, unchanged from prior passes, still
  intentional -- see below), so `CreateResource` sets `TrainingStartTime`/`TrainingEndTime` to
  the creation instant in that fast-forwarded path; `advanceTrainingResource` sets them properly
  on the SUBMITTED->IN_PROGRESS->TRAINED transitions for any resource that does start at
  SUBMITTED. `ClassifierMetadata`/`RecognizerMetadata`'s accuracy/precision/recall figures are
  deterministic synthetic constants (`classifierMetadata()`/`recognizerMetadata()` in
  handler_resources.go) -- no real training happens, matching the same
  deterministic-synthetic-result approach `detectSentiment`/`detectEntities` already use for
  word-list-based mock detection (explicitly acceptable per this service's parity bar).
  `RecognizerMetadata.EntityTypes` is derived from the `InputDataConfig.EntityTypes` the caller
  actually supplied at creation, not a hardcoded placeholder list.

- **`ListDocumentClassifierSummaries`/`ListEntityRecognizerSummaries` now group by name.**
  Fixed alongside the fabricated-Version-op removal above: since a "version" is now correctly
  just another resource sharing its base classifier/recognizer's `Name`, the summary view groups
  same-`Name` resources into one row with an aggregated `NumberOfVersions` and the most-recently-
  created resource as `LatestVersion*`, rather than the previous one-row-per-stored-resource
  (`NumberOfVersions` hardcoded to `1`) behavior, which was silently wrong the moment a second
  version of the same classifier/recognizer existed.

- **`ListFlywheelsOutput` is the one List response whose wrapper name does NOT match its
  Describe counterpart's object field.** Every other resource family here reuses the same
  name for both (e.g. `EndpointProperties` / `EndpointPropertiesList`), but Flywheel's list
  response is `FlywheelSummaryList` (of `FlywheelSummary`, a slimmer shape than
  `FlywheelProperties` -- no `DataAccessRoleArn`/`DataSecurityConfig`/`TaskConfig`). Describe/
  Update still return `FlywheelProperties`. `resourceSpec.objectField` and `.listField` are
  intentionally different strings for the Flywheel entry in `resourceSpecs()` -- do not
  "simplify" them back to matching values.

- **Start\*DetectionJob accepts an optional `Tags` field** (all 9 job families) and the job's
  ARN is taggable via `TagResource`/`ListTagsForResource`/`UntagResource` just like a Create*
  resource's ARN. `InMemoryBackend.StartJob` takes a `tags []Tag` param and always seeds
  `b.tags[job.JobArn]` (even to an empty map when no tags given) so the ARN is never a 404 for
  tag operations (prior pass's fix, re-verified, now also gated by the new
  `TooManyTagsException`/`KmsKeyValidationException` checks -- see above).

- **`ImportModel`'s resource type must be derived from `SourceModelArn`** (a required input),
  not hardcoded to DocumentClassifier: the imported model mirrors whichever kind of model
  `SourceModelArn` points at. `modelNameFromArn()` also supplies a fallback name (the ARN's
  name segment) when the optional `ModelName` is omitted, matching how AWS names an imported
  model after its source when no override is given.

- Classifier/recognizer training lifecycle is deliberately fast-forwarded: `CreateResource`
  sets `resourceTypeDocClassifier`/`resourceTypeEntityRecognizer` straight to `TRAINED` (see
  `initialResourceStatus`) rather than starting at `SUBMITTED`, because the real API can take
  minutes to train and CI can't wait that long. `advanceTrainingResource` still exists and is
  exercised (SUBMITTED -> IN_PROGRESS -> TRAINED/FAILED) for any resource that *does* start at
  SUBMITTED, so the state machine itself is real, just fast-started for the two long-training
  types. This is intentional, not a disguised no-op -- don't "fix" it back to SUBMITTED without
  also fixing the CI timeout implications.

- `comprehendPaginate` uses an integer-offset string as `NextToken` (not an opaque token via
  `pkgs/page`). This works correctly for the synchronous request/response cycle Comprehend
  clients actually use it in, but is a plaintext offset rather than opaque -- functionally
  fine, flagged here only so a future auditor doesn't mistake the plain integer for a stub.
