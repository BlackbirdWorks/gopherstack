---
service: rekognition
sdk_module: aws-sdk-go-v2/service/rekognition@v1.51.26   # version audited against
last_audit_commit: 6642a73c                       # HEAD when this manifest was written
last_audit_date: 2026-07-23
overall: A            # this sweep closed every remaining gaps-list item from the prior audit (see Notes #5)
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
  SearchUsers: {wire: ok, errors: ok, state: ok, persist: ok}
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
  CreateProjectVersion: {wire: partial, errors: ok, state: ok, persist: ok, note: "FIXED this sweep (2026-07-23): Tags/OutputConfig/KmsKeyId/VersionDescription now parsed, stored, and echoed back by DescribeProjectVersions; initial Tags applied to the ProjectVersion ARN the same way CreateStreamProcessor applies its tags — see Notes #5. Also FIXED prior sweep: duplicate (ProjectArn,VersionName) now returns ResourceInUseException — see Notes #2. Still gap (not fixed, see gaps): TrainingData/TestingData/FeatureConfig — genuinely complex nested Custom Labels training-manifest structures with no backing resource this mock can meaningfully echo, out of budget this sweep too"}
  DeleteProjectVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeProjectVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: CreationTimestamp string->epoch-seconds — see Notes #1"}
  CopyProjectVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  StartProjectVersion: {wire: ok, errors: ok, state: ok, persist: ok}
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
  async_video_jobs: {status: ok, note: "Start*/Get* (CelebrityRecognition, ContentModeration, FaceDetection, FaceSearch, LabelDetection, PersonTracking, SegmentDetection, TextDetection) — real StartAsyncJob/GetAsyncJob state machine (IN_PROGRESS -> SUCCEEDED on 2nd poll, PollCount persisted); Get* response bodies are synthesized empty/placeholder result arrays (acceptable mock, same ML-inherent-op exemption)"}
routing: {status: ok, note: "single X-Amz-Target: RekognitionService.<Op> POST endpoint (awsjson1.1), verified every op in the dispatch map (buildOps + appendixAOps) against a real op name in aws-sdk-go-v2/service/rekognition; no name mismatches found"}
gaps:
  - CreateProjectVersion still drops TrainingData/TestingData/FeatureConfig from the request (Custom Labels training-manifest config: nested GroundTruthManifest/Asset structures with no backing resource this in-memory mock can meaningfully echo back) — low-traffic Custom Labels training flow, file a bd issue if this is needed
deferred:
  - Full field-level completeness audit of the async-video Get* response bodies (Celebrities/ModerationLabels/Faces/Labels/Persons/Segments/TextDetections arrays) — always empty; acceptable per the ML-mock exemption but not individually wire-diffed field-by-field this sweep
  - Full field-level completeness audit of ProjectVersionDescription (BaseModelVersion/BillableTrainingTimeInSeconds/EvaluationResult/Feature/ManifestSummary/MaxInferenceUnits/SourceProjectVersionArn/TestingDataResult/TrainingDataResult/TrainingEndTimestamp) beyond the OutputConfig/KmsKeyId/VersionDescription fields added this sweep — DescribeProjectVersions was already marked wire:ok by a prior audit and expanding its full field set was out of this sweep's assigned gap list (CreateProjectVersion's dropped-fields gap only)
leaks: {status: clean, note: "no goroutines/janitors in this service; lockmetrics.RWMutex coarse lock verified around every backend mutation; Snapshot/Restore delegation (Handler->Backend) verified wired (persistence.go)"}
---

## Notes

1. **Timestamp wire shape (the main bug class this sweep).** awsjson1.1 (which
   Rekognition uses) always serializes `time.Time` fields as epoch-seconds JSON
   *numbers*, never ISO8601 strings — confirmed by reading
   `aws-sdk-go-v2/service/rekognition@v1.51.26/deserializers.go`'s
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
   ARNs.** Per `aws-sdk-go-v2/service/rekognition@v1.51.26/api_op_TagResource.go`'s
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
     `aws-sdk-go-v2/service/rekognition@v1.51.26/types/types.go` and the
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
