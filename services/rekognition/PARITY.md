---
service: rekognition
sdk_module: aws-sdk-go-v2/service/rekognition@v1.54.4   # version audited against (was stale at v1.51.26; go.mod pins v1.54.4 -- corrected this sweep)
last_audit_commit: 903d74b67                       # HEAD when this manifest was written
last_audit_date: 2026-08-10
overall: A            # field-completeness follow-up sweep (see Notes #6): shallow CreateProjectVersion/StartProjectVersion/CopyProjectVersion fields and async-video Get* JobTag/Video/SelectedSegmentTypes/GetRequestMetadata now modeled; deep Custom Labels manifests and post-training fields stay deliberately deferred
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateCollection: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCollection: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades: deletes all faces in the collection + its tags"}
  DescribeCollection: {wire: ok, errors: ok, state: ok, persist: ok, note: "UserCount field omitted from response (optional, client-side nil-safe — not a bug)"}
  ListCollections: {wire: ok, errors: ok, state: ok, persist: ok}
  IndexFaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "real face storage; deterministic per-identity Confidence (not canned) — see backend.go faceConfidence. FaceDetail/BoundingBox/IndexFacesModelVersion/UserId fields on Face are omitted (optional pointer fields on the real SDK type, zero-value-safe on decode)"}
  DeleteFaces: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "real pagination via facesByCollection index"}
  SearchFaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "deterministic per-identity similarity (same ExternalImageId => 100.0), not canned — see faceSimilarity"}
  SearchFacesByImage: {wire: ok, errors: ok, state: ok, persist: ok, note: "similarity varies per imageKey (S3 path or byte length) via FNV-1a seed, not canned"}
  CreateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: duplicate UserId now returns ConflictException (was ResourceAlreadyExistsException) — see Notes #2"}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUsers: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateFaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "real FaceId membership check against the collection; unknown faces reported in UnsuccessfulFaceAssociations with FACE_NOT_FOUND"}
  DisassociateFaces: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchUsers: {wire: ok, errors: fixed, state: ok, persist: ok, note: "gopherstack-2wvq (2026-08-21): handler unconditionally required UserId, but SearchUsersInput marks only CollectionId required (rekognition@v1.54.4 api_op_SearchUsers.go) -- 'The request must be provided with either FaceId or UserId... If a FaceId is provided, UserId isn't required to be present in the Collection.' Added SearchUsersByFace, reusing the existing facesByCollection index SearchFaces already uses (faces.go) rather than a new one, so a FaceId-only request now resolves (and errors ResourceNotFoundException if the face itself doesn't exist); UserId-absent-and-FaceId-absent still rejects. Response now emits SearchedFace (not SearchedUser) when searched by FaceId, matching the real SearchUsersOutput having both as distinct optional members."}
  SearchUsersByImage: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStreamProcessor: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep (2026-07-23): Input/Output/Settings/RegionsOfInterest/NotificationChannel/KmsKeyId/DataSharingPreference are now parsed from the request and stored (see Notes #5). Also FIXED prior sweep: duplicate Name now returns ResourceInUseException (was ResourceAlreadyExistsException) — see Notes #2"}
  DeleteStreamProcessor: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStreamProcessor: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep (2026-07-23): now returns Input/Output/Settings/RegionsOfInterest/NotificationChannel/KmsKeyId/DataSharingPreference/LastUpdateTimestamp/StatusMessage, all routed through epochSeconds() for the two timestamp fields — see Notes #5"}
  ListStreamProcessors: {wire: ok, errors: ok, state: ok, persist: ok}
  StartStreamProcessor: {wire: ok, errors: ok, state: ok, persist: ok}
  StopStreamProcessor: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStreamProcessor: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep (2026-07-23): DataSharingPreferenceForUpdate/ParametersToDelete/RegionsOfInterestForUpdate/SettingsForUpdate.ConnectedHomeForUpdate now actually mutate the stored stream processor (was a pure existence-check no-op) — see Notes #5"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: resourceExists() now also recognizes ProjectVersion ARNs (the 'Custom Labels model' AWS's TagResource doc says is taggable, alongside collections/stream processors) — was previously always ResourceNotFoundException for a real, existing ProjectVersion — see Notes #3"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateProject: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: duplicate name now returns ResourceInUseException (was ResourceAlreadyExistsException) — see Notes #2"}
  DeleteProject: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeProjects: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: CreationTimestamp was an ISO8601 string ('2006-01-02T15:04:05.000Z' Format()) — real awsjson1.1 wire shape is an epoch-seconds JSON number; SDK deserializer errors with 'expected DateTime to be a JSON Number, got string instead'. Now epochSeconds() — see Notes #1"}
  CreateProjectVersion: {wire: partial, errors: ok, state: ok, persist: ok, note: "FIXED this sweep (2026-08-10, Notes #6): OutputConfig is now enforced as required (was silently optional -- more permissive than the real validator); FeatureConfig.ContentModeration.ConfidenceThreshold now parsed/stored/echoed (shallow, 2 levels, no unions); TrainingData/TestingData now cross-validated (both-or-neither) though their contents stay opaque -- see gaps. Prior sweep (2026-07-23): Tags/OutputConfig/KmsKeyId/VersionDescription parsed, stored, echoed — see Notes #5. Duplicate (ProjectArn,VersionName) returns ResourceInUseException — see Notes #2"}
  DeleteProjectVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeProjectVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep (Notes #6): now also echoes FeatureConfig/MaxInferenceUnits/MinInferenceUnits/SourceProjectVersionArn (previously stored by Start/CopyProjectVersion but never serialized here). Prior sweep: CreationTimestamp string->epoch-seconds — see Notes #1"}
  CopyProjectVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep (Notes #6): now stores SourceProjectVersionArn on the destination version (echoed by DescribeProjectVersions)"}
  StartProjectVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep (Notes #6): now accepts and stores the optional MaxInferenceUnits (StartProjectVersionInput member; was parsed nowhere, so MinInferenceUnits was the only value ever recorded)"}
  StopProjectVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProjectPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: CreationTimestamp + LastUpdatedTimestamp string->epoch-seconds — see Notes #1"}
  PutProjectPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteProjectPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDataset: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep (2026-07-23): now rejects a duplicate (ProjectArn,DatasetType) pair with ResourceAlreadyExistsException (via an explicit b.datasets.Range scan, since datasetARN is still always uuid-suffixed so the table key itself never collides) — see Notes #5"}
  DeleteDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDataset: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: CreationTimestamp + LastUpdatedTimestamp string->epoch-seconds — see Notes #1"}
  ListDatasetEntries: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasetLabels: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDatasetEntries: {wire: ok, errors: ok, state: ok, persist: ok}
  DistributeDatasetEntries: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateFaceLivenessSession: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFaceLivenessSessionResults: {wire: ok, errors: ok, state: ok, persist: ok}
  StartMediaAnalysisJob: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMediaAnalysisJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: CreationTimestamp string->epoch-seconds — see Notes #1"}
  ListMediaAnalysisJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: CreationTimestamp string->epoch-seconds — see Notes #1"}
families:
  detect_and_recognize: {status: ok, note: "CompareFaces/DetectFaces/DetectLabels/DetectText/DetectCustomLabels/DetectModerationLabels/DetectProtectiveEquipment/RecognizeCelebrities/GetCelebrityInfo — inherently-ML ops, correctly deterministic mocks per parity-principles.md rule 4 (not flagged as bugs); DetectLabels' plausibleLabels() genuinely varies with MinConfidence/MaxLabels, CompareFaces/DetectFaces/RecognizeCelebrities always return an empty/fixed-shape result regardless of input — acceptable, these are stateless single-shot image ops with no backing resource to fake statefulness against"
  async_video_jobs: {status: ok, note: "Start*/Get* (CelebrityRecognition, ContentModeration, FaceDetection, FaceSearch, LabelDetection, PersonTracking, SegmentDetection, TextDetection) — real StartAsyncJob/GetAsyncJob state machine (IN_PROGRESS -> SUCCEEDED on 2nd poll, PollCount persisted). FIXED this sweep (Notes #6): JobTag and Video (S3 reference) were parsed from every Start* request and then discarded -- both are real GetXxxOutput members, now stored and echoed back. GetSegmentDetection.SelectedSegmentTypes now echoes the Type values from StartSegmentDetection's SegmentTypes (ModelVersion omitted, no legitimate source). GetLabelDetection/GetContentModeration now return GetRequestMetadata (SortBy/AggregateBy echo). Detection-result arrays (Celebrities/ModerationLabels/Faces/Labels/Persons/Segments/TextDetections) remain synthesized-empty — acceptable mock, ML-inherent-op exemption, see gaps/deferred"}
routing: {status: ok, note: "single X-Amz-Target: RekognitionService.<Op> POST endpoint (awsjson1.1), verified every op in the dispatch map (buildOps + appendixAOps) against a real op name in aws-sdk-go-v2/service/rekognition; no name mismatches found"}
gaps:
  - CreateProjectVersion still drops TrainingData/TestingData contents (Custom Labels external-manifest structures: TrainingData/TestingData -> []Asset -> GroundTruthManifest -> S3Object, 3-4 levels, no unions, structurally simple but pointless to store -- the only place they'd resurface is TrainingDataResult/TestingDataResult, which requires a training-completion lifecycle this backend never reaches; both-or-neither presence is still cross-validated) — see Notes #6
deferred:
  - ProjectVersionDescription's BaseModelVersion (needs data this emulator cannot have: an AWS-internal base-model-catalog string, not derivable or user-supplied) and BillableTrainingTimeInSeconds/TrainingEndTimestamp/EvaluationResult/ManifestSummary/TestingDataResult/TrainingDataResult (needs a lifecycle that does not exist: all are documented as populated only once training completes, and this backend's Status never advances past TRAINING_IN_PROGRESS; EvaluationResult additionally requires a fabricated F1 score, which the no-fabrication rule forbids outright) — see Notes #6
  - ProjectVersionDescription.Feature / DescribeProjects' Feature (large mechanical surface deferred for size: Feature is set at CreateProject time, which does not currently accept or store it at all; modeling ProjectVersionDescription.Feature honestly requires a CreateProject signature change cascading through DescribeProjects too, a separate op family from this sweep's CreateProjectVersion/StartProjectVersion/CopyProjectVersion scope) — see Notes #6
  - SegmentTypeInfo.ModelVersion (needs data this emulator cannot have: AWS-internal segment-detection model build string) — Type is modeled, ModelVersion is not, see Notes #6
  - Detection-result arrays (Celebrities/ModerationLabels/Faces/Labels/Persons/Segments/TextDetections) stay synthesized-empty; acceptable per the ML-mock exemption, not individually wire-diffed field-by-field this sweep (this sweep's scope was CreateProjectVersion/ProjectVersionDescription/async-video envelope fields, not the ML detection payloads themselves)
leaks: {status: clean, note: "no goroutines/janitors in this service; lockmetrics.RWMutex coarse lock verified around every backend mutation; Snapshot/Restore delegation (Handler->Backend) verified wired (persistence.go)"}
---

## Notes

1. **Timestamp wire shape (the main bug class this sweep).** awsjson1.1 (which
   Rekognition uses) always serializes `time.Time` fields as epoch-seconds JSON
   *numbers*, never ISO8601 strings — confirmed by reading
   `aws-sdk-go-v2/service/rekognition@v1.54.4/deserializers.go`'s
   `case "CreationTimestamp": ... case json.Number: ... default: return
   fmt.Errorf("expected DateTime to be a JSON Number, got %T instead", value)`.
   8 fields across 5 response types in `handler_appendixa.go` were rendering
   `time.Time.Format("2006-01-02T15:04:05.000Z")` (a string) instead: those
   responses would fail to decode in a real SDK client with "expected DateTime
   to be a JSON Number, got string instead". Fixed by switching every
   affected field from `string` to `float64` and rendering via the existing
   `epochSeconds()` helper (already used correctly by `handleDescribeCollection`
   in `handler.go`): `projectDescription.CreationTimestamp`,
   `projectVersionDescription.CreationTimestamp`,
   `projectPolicyEntry.{CreationTimestamp,LastUpdatedTimestamp}`,
   `datasetDescription.{CreationTimestamp,LastUpdatedTimestamp}`,
   `mediaAnalysisJobDescription.CreationTimestamp` /
   `getMediaAnalysisJobResp.CreationTimestamp`. `DescribeStreamProcessor`'s
   `CreationTimestamp` was already a number (`float64(t.Unix())`) — correct,
   just not routed through `epochSeconds()`; left alone (not a bug, no
   fractional-second loss matters here since `time.Now()` sub-second precision
   isn't asserted anywhere).

2. **"Already exists" exception type varies per operation — do not generically
   dispatch on the AlreadyExists sentinel.** `aws-sdk-go-v2/service/rekognition`'s
   generated `deserializers.go` gives each `Create*` op its OWN switch of
   recognized exception-type strings; an exception type not in that op's
   switch deserializes as an untyped `smithy.GenericAPIError` instead of the
   typed exception, breaking SDK-side `errors.As(&typedErr)` matching. Verified
   per-op via each op's `awsAwsjson11_deserializeOpError<Op>` switch:
   - `CreateCollection`, `CreateDataset` → `ResourceAlreadyExistsException`
   - `CreateStreamProcessor`, `CreateProject`, `CreateProjectVersion` →
     `ResourceInUseException`
   - `CreateUser` → `ConflictException`
   Before this sweep, `handleError` (`handler.go`) had a single generic
   `errors.Is(err, awserr.ErrAlreadyExists)` case hardcoded to
   `ResourceAlreadyExistsException`, so only `CreateCollection`/`CreateDataset`
   were actually correct — the other 3 create ops silently emitted the wrong
   `__type`. Fixed by introducing two new local sentinels in `backend.go`
   (`ErrNameInUse` → `ResourceInUseException`, `ErrUserConflict` →
   `ConflictException`), routing `CreateStreamProcessor`/`CreateProject`/
   `CreateProjectVersion`/`CreateUser`'s duplicate-checks through them, and
   adding matching cases to `handleError` ahead of the generic
   `ErrAlreadyExists` case. `handler_audit1_test.go`'s
   "CreateStreamProcessor duplicate returns error" test asserted the old
   (wrong) `ResourceAlreadyExistsException` value — updated to
   `ResourceInUseException`.

3. **`resourceExists()` (backend.go, gates TagResource/UntagResource/
   ListTagsForResource) only recognized collection and stream-processor
   ARNs.** Per `aws-sdk-go-v2/service/rekognition@v1.54.4/api_op_TagResource.go`'s
   doc comment, tagging also applies to "an Amazon Rekognition ... Custom
   Labels model" — i.e. a ProjectVersion ARN. Before this sweep, tagging a
   real, just-created ProjectVersion always failed with
   `ResourceNotFoundException`. Fixed by adding a `b.projectVersions.All()`
   scan to `resourceExists()`. (Project ARNs themselves are deliberately NOT
   included — AWS's TagResource doc explicitly scopes to
   collection/stream-processor/model, not project.)

4. **False leads ruled out** (documented so the next audit doesn't re-flag):
   - `CompareFaces`/`DetectFaces`/`RecognizeCelebrities` always return an
     empty/fixed result regardless of input image — this is the accepted
     ML-mock exemption (parity-principles.md rule 4), not a disguised no-op:
     there's no backing *resource* (collection/face/user) being silently
     dropped, just a stateless single-shot detection call with no real vision
     model behind it.
   - `IndexFaces`/`SearchFaces`/`SearchFacesByImage`/`SearchUsers`/
     `SearchUsersByImage` confidence/similarity scores are deterministic
     hashes of stored identity (FaceID/ExternalImageId/UserID), not canned
     constants — genuinely varies per input and is stable across repeated
     calls, matching how a real client test would expect determinism.
   - `AsyncJob`/`MediaAnalysisJob` Start/Get lifecycle (`PollCount`-driven
     IN_PROGRESS → SUCCEEDED state transition) is real, persisted state, not a
     stub — verified `GetAsyncJob` mutates and returns based on
     `storedAsyncJob.PollCount`.

5. **2026-07-23 sweep: closed every remaining `gaps:` item from the prior
   audit except the CreateProjectVersion TrainingData/TestingData/
   FeatureConfig one (kept as a gap, see above — deliberately deferred, not
   an oversight).**
   - **Stream processor config fields.** `CreateStreamProcessor` previously
     accepted `Input`/`Output`/`Settings`/`RegionsOfInterest`/
     `NotificationChannel`/`KmsKeyId`/`DataSharingPreference` but discarded
     them; `DescribeStreamProcessor` always returned them absent. Added
     `StreamProcessorInput`/`StreamProcessorOutput`/`StreamProcessorSettings`/
     `RegionOfInterest`/`BoundingBox`/`Point`/
     `StreamProcessorNotificationChannel`/`StreamProcessorDataSharingPreference`
     domain types (`interfaces.go`) mirroring the real SDK's nested
     `types.*` shapes field-for-field (verified against
     `aws-sdk-go-v2/service/rekognition@v1.54.4/types/types.go` and the
     `awsAwsjson11_serialize/deserializeDocument*` functions for exact JSON
     key names/nesting), threaded through a `CreateStreamProcessorParams`
     struct (avoids an unbounded positional-parameter CreateStreamProcessor
     signature), stored on `storedStreamProcessor`, and echoed back by
     `DescribeStreamProcessor` (`handler_stream_processors.go`'s
     `*Wire` request/response types + `*FromDomain`/`.toDomain()`
     converters). Optional pointer wire fields use `omitempty` so an unset
     field is *absent* from the JSON (matching the real serializer's
     `if v.X != nil { ... }` guards), not present-as-`null`.
   - **`UpdateStreamProcessor` was a pure existence-check no-op.** Now
     applies `DataSharingPreferenceForUpdate`,
     `SettingsForUpdate.ConnectedHomeForUpdate.{Labels,MinConfidence}`, and
     `RegionsOfInterestForUpdate` (wholesale replace, not merge), with
     `ParametersToDelete` (`RegionsOfInterest` / `ConnectedHomeMinConfidence`)
     applied last so a delete always wins over a same-request set — matches
     AWS's documented apply-then-delete order. Presence/absence of each
     update field is signaled the same way the AWS wire shape does: Go's
     `encoding/json` leaves an absent key's pointer/slice field `nil` and a
     present-but-empty JSON array as a non-nil empty slice, so no extra
     `*Set bool` sidecar fields were needed.
   - **`CreateDataset` never rejected a duplicate `(ProjectArn,DatasetType)`
     pair.** `datasetARN` is still always uuid-suffixed (so the table key
     itself never collides — left as-is, this is how dataset identity is
     modeled here), so the check is now explicit: a `b.datasets.Range` scan
     for an existing dataset with the same `(ProjectARN, DatasetType)`
     before insert, returning the new `ErrDatasetAlreadyExists` sentinel
     (→ `ResourceAlreadyExistsException`, verified against
     `CreateDataset`'s own error-deserializer switch — same exception type
     as `CreateCollection`, not `ResourceInUseException`).
   - **`CreateProjectVersion` dropped `Tags`/`OutputConfig`/`KmsKeyId`/
     `VersionDescription`.** These four are now parsed, stored on
     `storedProjectVersion`, and (for `OutputConfig`/`KmsKeyId`/
     `VersionDescription`) echoed back by `DescribeProjectVersions`; initial
     `Tags` are applied to the ProjectVersion ARN's tag-store entry the same
     way `CreateStreamProcessor` applies its initial tags (ProjectVersion
     ARNs are already confirmed taggable — see Notes #3). `TrainingData`/
     `TestingData`/`FeatureConfig` remain a deliberate gap (see `gaps:`):
     each describes a nested Custom Labels training-manifest structure
     (`GroundTruthManifest`/`Asset`/feature-variant unions) with no backing
     resource this in-memory backend can meaningfully simulate, and are
     lower-traffic than the four fields fixed this sweep.
   - Added `fieldalignment`-optimal struct field ordering (via
     `fieldalignment -fix`) to every struct touched this sweep
     (`storedStreamProcessor`, `StreamProcessor`, `StreamProcessorSettings`,
     `CreateStreamProcessorParams`) to keep `golangci-lint`'s `govet`
     fieldalignment check at 0 issues; field order in those structs carries
     no semantic meaning beyond that.

6. **2026-08-10 field-completeness follow-up (gopherstack-3tzd).** SDK pin
   was stale (`v1.51.26` in this file vs `v1.54.4` pinned in `go.mod`) —
   corrected here and in every inline citation above. Depth measurement of
   the recorded gaps, read directly from
   `aws-sdk-go-v2/service/rekognition@v1.54.4/types/types.go`:
   - `TrainingData`/`TestingData` -> `[]Asset` -> `Asset.GroundTruthManifest`
     -> `GroundTruthManifest.S3Object` -> `S3Object{Bucket,Name,Version}`: 4
     struct levels, no unions, every level 1-3 fields. Structurally shallow,
     but each level's only content is an S3 pointer to a manifest this
     backend never trains against — there is no training-completion
     lifecycle here for `TrainingDataResult`/`TestingDataResult` (the only
     place a stored copy would resurface) to ever populate. Left opaque; only
     the documented "both or neither" cross-field requirement
     (`api_op_CreateProjectVersion.go` doc comment) is enforced, since that's
     cheaply checkable without modeling the contents — this closes a
     more-permissive-than-real gap (gopherstack previously accepted either
     field alone).
   - `FeatureConfig` -> `CustomizationFeatureConfig.ContentModeration` ->
     `CustomizationFeatureContentModerationConfig.ConfidenceThreshold`: 2
     levels, single member each, no unions — the prior audit's "genuinely
     complex... feature-variant unions" characterization (see the Notes #5
     entry above) was wrong; verified by reading `types.go:486,495` directly.
     Modeled and echoed verbatim (no fabrication: it's the client's own
     training-job config, not an inference result).
   - **`CreateProjectVersion` was more permissive than the real service**:
     `OutputConfig` is a required `CreateProjectVersionInput` member
     (`validateOpCreateProjectVersionInput`, `validators.go:2107`) but
     gopherstack never checked for it. Fixed with a failing-first test
     (`TestProjectVersions/CreateProjectVersion_missing_OutputConfig_returns_error`);
     three existing tests asserted the old (wrong) permissive behavior by
     omitting `OutputConfig` and expecting 200 — fixed to send it.
   - **`StartProjectVersion`/`CopyProjectVersion`/`DescribeProjectVersions`
     dropped fields the backend already had, or could trivially have,
     but never serialized**: `MaxInferenceUnits` (a real
     `StartProjectVersionInput` member, parsed nowhere before this sweep);
     `SourceProjectVersionArn` (never stored by `CopyProjectVersion`); and
     `MinInferenceUnits` itself (stored since a prior sweep via
     `StartProjectVersion`, but never echoed by `DescribeProjectVersions` —
     a pure serialization gap). All three fixed.
   - **Async-video `Get*` responses: `JobTag` and `Video` are real
     `GetXxxOutput` members** (verified against every `api_op_GetXxx.go` in
     this family) that every `Start*` handler already parsed into its
     request struct and then discarded (`_ *startXxxReq`). Now threaded
     through `StartAsyncJobParams` -> `storedAsyncJob` -> the shared
     `getJobBase` helper, so all seven `Get*` responses
     (LabelDetection/ContentModeration/CelebrityRecognition/FaceDetection/
     FaceSearch/TextDetection/PersonTracking) echo them. `getJobBase`'s
     signature grew a second return value (the raw `*AsyncJob`) so
     `GetSegmentDetection` doesn't have to call `GetAsyncJob` a second time
     to read `SegmentTypes` — doing so would have double-advanced the
     `PollCount` IN_PROGRESS->SUCCEEDED state machine per client-visible
     call, a real bug caught before it shipped.
   - **`GetSegmentDetection.SelectedSegmentTypes`** now echoes the `Type` of
     each `SegmentTypes` entry from the matching `StartSegmentDetection`
     call (previously always `[]`, despite the value being sitting right
     there in the discarded request). `ModelVersion` is deliberately left
     off `segmentTypeInfoWire` — it names the internal Rekognition model
     build and there is no legitimate source for that string.
   - **`GetLabelDetection`/`GetContentModeration.GetRequestMetadata`** now
     echo the current call's `SortBy`/`AggregateBy`, applying the documented
     default when omitted (`LabelDetectionSortBy`/`ContentModerationSortBy`
     default to `TIMESTAMP`, `ContentModerationAggregateBy` defaults to
     `TIMESTAMPS` — all three defaults are stated in their respective
     `api_op_GetXxx.go` doc comments). `LabelDetectionAggregateBy` has no
     documented default, so it is only reported when the caller supplies
     one — inventing a default here would be the same class of mistake as a
     fabricated confidence score.
   - **Deliberately not modeled, with reasons** (see `deferred:`):
     `BaseModelVersion` (needs data this emulator cannot have — AWS-internal
     base-model catalog); `BillableTrainingTimeInSeconds`,
     `TrainingEndTimestamp`, `EvaluationResult`, `ManifestSummary`,
     `TestingDataResult`, `TrainingDataResult` (needs a lifecycle that does
     not exist — this backend's `Status` never advances past
     `TRAINING_IN_PROGRESS`, and `EvaluationResult.F1Score` additionally
     can't be computed without a real model); `Feature` on
     `ProjectVersionDescription`/`DescribeProjects` (large mechanical
     surface deferred for size — `CreateProject` doesn't accept or store
     `Feature` at all yet, so modeling it honestly is a separate
     `CreateProject`+`DescribeProjects` change, not a `CreateProjectVersion`
     one); `SegmentTypeInfo.ModelVersion` (needs data this emulator cannot
     have, see above).
   - All new struct fields use `omitempty` and are additive — no
     `rekognitionSnapshotVersion` bump; round-trip verified by
     `TestSnapshotRestore_ProjectVersionAndAsyncJobNewFields`
     (`persistence_test.go`).
