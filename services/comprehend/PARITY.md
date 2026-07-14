---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: comprehend
sdk_module: aws-sdk-go-v2/service/comprehend@v1.41.0
last_audit_commit: 0e933737
last_audit_date: 2026-07-13
overall: A            # genuine wire-shape + tag-sync bugs found and fixed this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  DetectSentiment: {wire: ok, errors: ok, state: ok, persist: n/a, note: synchronous, deterministic word-list mock is acceptable}
  DetectEntities: {wire: ok, errors: ok, state: ok, persist: n/a}
  DetectKeyPhrases: {wire: ok, errors: ok, state: ok, persist: n/a}
  DetectPiiEntities: {wire: ok, errors: ok, state: ok, persist: n/a}
  DetectSyntax: {wire: ok, errors: ok, state: ok, persist: n/a}
  DetectDominantLanguage: {wire: ok, errors: ok, state: ok, persist: n/a}
  DetectToxicContent: {wire: ok, errors: ok, state: ok, persist: n/a, note: "ResultList/Labels/Toxicity field names verified against types.ToxicLabels"}
  DetectTargetedSentiment: {wire: ok, errors: ok, state: ok, persist: n/a}
  ClassifyDocument: {wire: ok, errors: ok, state: ok, persist: n/a}
  ContainsPiiEntities: {wire: ok, errors: ok, state: ok, persist: n/a}
  BatchDetect*: {wire: ok, errors: ok, state: ok, persist: n/a, note: "6 sync ops + BatchDetectTargetedSentiment wrapped via h.batch(); ResultList/Index/ErrorList shape verified"}
  Start*DetectionJob (9 families): {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: Tags were silently dropped -- StartJob had no tags param and never seeded b.tags[JobArn], so TagResource/ListTagsForResource against a job ARN always 404'd even for an existing job. See backend.go StartJob signature + handler.go startJob."}
  Describe*DetectionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "SubmitTime/EndTime via awstime.Epoch; advanceJob() steps SUBMITTED->IN_PROGRESS->COMPLETED/FAILED on each Describe poll -- real lifecycle, not a disguised no-op"}
  List*DetectionJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  Stop*DetectionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly rejects stop on terminal states with InvalidRequestException"}
  CreateDocumentClassifier(Version): {wire: ok, errors: ok, state: ok, persist: ok, note: "SubmitTime/EndTime field names correct for this family (verified against types.DocumentClassifierProperties)"}
  CreateEntityRecognizer(Version): {wire: ok, errors: ok, state: ok, persist: ok, note: "SubmitTime/EndTime correct (types.EntityRecognizerProperties)"}
  CreateEndpoint/DescribeEndpoint/ListEndpoints/UpdateEndpoint/DeleteEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: was emitting SubmitTime/EndTime; real types.EndpointProperties uses CreationTime/LastModifiedTime -- client always saw nil timestamps before this fix"}
  CreateFlywheel/DescribeFlywheel/ListFlywheels/UpdateFlywheel/DeleteFlywheel: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED two bugs: (1) timestamp fields same class as Endpoint above (types.FlywheelProperties uses CreationTime/LastModifiedTime); (2) ListFlywheelsOutput wraps items as FlywheelSummaryList (FlywheelSummary shape), not FlywheelPropertiesList like every other List* op here -- client always saw an empty list before this fix"}
  CreateDataset/DescribeDataset/ListDatasets: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: types.DatasetProperties uses CreationTime/EndTime, not SubmitTime/EndTime"}
  StartFlywheelIteration/GetFlywheelIteration/DescribeFlywheelIteration/ListFlywheelIterationHistory: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource/UntagResource/ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "now correctly covers job ARNs too, see Start*DetectionJob fix above"}
  ImportModel: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: always created a DocumentClassifier regardless of SourceModelArn; now derives resourceType (document-classifier vs entity-recognizer) from SourceModelArn's resource-type segment, and falls back to the ARN's name segment when ModelName is omitted (both required real-AWS semantics)"}
  ListDocumentClassifierSummaries/ListEntityRecognizerSummaries: {wire: ok, errors: ok, state: ok, persist: n/a, note: derived view over resources table, not separately persisted}
  StopTrainingDocumentClassifier/StopTrainingEntityRecognizer: {wire: ok, errors: ok, state: ok, persist: ok}
  Put/Describe/DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "revision-conflict checked via ResourceInUseException, matches AWS optimistic-concurrency semantics"}
# Families audited as a group (when per-op is impractical):
families:
  routing: {status: ok, note: "RouteMatcher/ExtractOperation verified against X-Amz-Target: Comprehend_20171127.<Op> prefix; sdk_completeness_test.go confirms every SDK op is routed (no notImplemented entries needed)"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "BatchDetect* never populates ErrorList even for oversized/invalid TextList entries (AWS enforces a 25-item/5KB-per-item limit and reports per-item BatchItemError); acceptable for now since input is never rejected, but a client relying on partial-failure semantics won't see it (no bd issue filed yet)"
  - "DocumentClassifierProperties/EntityRecognizerProperties never populate TrainingStartTime/TrainingEndTime/ClassifierMetadata/RecognizerMetadata (fields exist on the real shape, we return zero-ish mock values via Configuration passthrough only if the caller set them) -- low priority, no client code path in the wild depends on these for basic lifecycle polling (no bd issue filed yet)"
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "DetectDominantLanguage/DetectEntities/etc. input validation (LanguageCode required per real API for all Detect* ops except DetectDominantLanguage) -- emulator is more permissive than AWS, not flagged as a parity bug per the no-stub focus on state/wire/tags"
  - "EventsDetectionJob family (Start/Describe/List) -- routed and shaped consistently with the other 8 async job families via the same jobSpec table, not individually re-verified beyond that consistency check"
leaks: {status: clean, note: "no goroutines/timers spawned by this service; job/resource lifecycle advances synchronously on each Describe/List poll (advanceJob/advanceTrainingResource), no background janitor to leak"}
---

## Notes

Freeform: AWS-behavior specifics worth remembering (exact algorithms, wire quirks,
error-message text, protocol = query-XML / REST-XML / REST-JSON / json-1.0), and any
"looks-wrong-but-correct" traps so the next auditor doesn't re-flag them.

- Protocol is awsjson1.1 (`X-Amz-Target: Comprehend_20171127.<Op>`,
  `application/x-amz-json-1.1`). All error bodies use `{"__type": "<Code>", "message": "..."}`
  via `service.JSONErrorResponse`; HTTP status is 400 for all three modeled exceptions used
  here (`ResourceNotFoundException`, `ResourceInUseException`, `InvalidRequestException`) --
  this matches the SDK's client error types, none of which carry an `@httpError` override to
  404/409/etc., so 400-for-everything is correct for this protocol, not a bug.

- **Timestamp field names are NOT uniform across resource Properties shapes** — this was
  the main wire-shape bug this pass. Real AWS shapes split three ways:
  - `DocumentClassifierProperties` / `EntityRecognizerProperties` (and their `*Version`
    siblings): `SubmitTime` + `EndTime`.
  - `EndpointProperties` / `FlywheelProperties`: `CreationTime` + `LastModifiedTime`.
  - `DatasetProperties`: `CreationTime` + `EndTime` (no `LastModifiedTime` field exists here).
  `resourceMap()` in handler.go now switches on `resource.Type` to emit the right pair.
  If a new resource family is ever added to `resourceSpecs()`, check its real Properties
  shape in `aws-sdk-go-v2/service/comprehend/types` before assuming `SubmitTime`/`EndTime`.

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
  resource's ARN. `InMemoryBackend.StartJob` now takes a `tags []Tag` param and always seeds
  `b.tags[job.JobArn]` (even to an empty map when no tags given) so the ARN is never a 404 for
  tag operations. This is the same bug class already fixed in `services/transcribe` per the
  prior parity sweep -- check `pkgs/tags`-adjacent Start*/Create* ops elsewhere for the same
  pattern.

- **`ImportModel`'s resource type must be derived from `SourceModelArn`** (a required input),
  not hardcoded to DocumentClassifier: the imported model mirrors whichever kind of model
  `SourceModelArn` points at. `modelNameFromArn()` also supplies a fallback name (the ARN's
  name segment) when the optional `ModelName` is omitted, matching how AWS names an imported
  model after its source when no override is given.

- Classifier/recognizer training lifecycle is deliberately fast-forwarded: `CreateResource`
  sets `resourceTypeDocClassifier`/`resourceTypeEntityRecognizer(Version)` straight to
  `TRAINED` (see `initialResourceStatus`) rather than starting at `SUBMITTED`, because the
  real API can take minutes to train and CI can't wait that long. `advanceTrainingResource`
  still exists and is exercised (SUBMITTED -> IN_PROGRESS -> TRAINED/FAILED) for any resource
  that *does* start at SUBMITTED, so the state machine itself is real, just fast-started for
  the two long-training types. This is intentional, not a disguised no-op -- don't "fix" it
  back to SUBMITTED without also fixing the CI timeout implications.

- `comprehendPaginate` uses an integer-offset string as `NextToken` (not an opaque token via
  `pkgs/page`). This works correctly for the synchronous request/response cycle Comprehend
  clients actually use it in, but is a plaintext offset rather than opaque -- functionally
  fine, flagged here only so a future auditor doesn't mistake the plain integer for a stub.
