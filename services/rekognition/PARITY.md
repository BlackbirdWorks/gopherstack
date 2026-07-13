---
service: rekognition
sdk_module: aws-sdk-go-v2/service/rekognition@v1.51.26   # version audited against
last_audit_commit: 6642a73c                       # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: A            # fresh audit — 4 real classes of bugs found and fixed (see families below)
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
  CreateStreamProcessor: {wire: partial, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: duplicate Name now returns ResourceInUseException (was ResourceAlreadyExistsException) — see Notes #2. Gap (pre-existing, not fixed): Input/Output/Settings/RegionsOfInterest/NotificationChannel/KmsKeyId/DataSharingPreference are parsed from neither the request nor stored/returned by Describe — see gaps"}
  DeleteStreamProcessor: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStreamProcessor: {wire: partial, errors: ok, state: ok, persist: ok, note: "returns Name/RoleArn/Status/StreamProcessorArn/CreationTimestamp only; Input/Output/Settings/LastUpdateTimestamp/StatusMessage/etc always absent — see gaps. CreationTimestamp already epoch-seconds (float64(t.Unix())), left as-is (correct, just not routed through the epochSeconds() helper used elsewhere)"}
  ListStreamProcessors: {wire: ok, errors: ok, state: ok, persist: ok}
  StartStreamProcessor: {wire: ok, errors: ok, state: ok, persist: ok}
  StopStreamProcessor: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStreamProcessor: {wire: partial, errors: ok, state: ok, persist: ok, note: "existence-check only, no field actually updated — pre-existing, out of audit budget"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: resourceExists() now also recognizes ProjectVersion ARNs (the 'Custom Labels model' AWS's TagResource doc says is taggable, alongside collections/stream processors) — was previously always ResourceNotFoundException for a real, existing ProjectVersion — see Notes #3"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateProject: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: duplicate name now returns ResourceInUseException (was ResourceAlreadyExistsException) — see Notes #2"}
  DeleteProject: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeProjects: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: CreationTimestamp was an ISO8601 string ('2006-01-02T15:04:05.000Z' Format()) — real awsjson1.1 wire shape is an epoch-seconds JSON number; SDK deserializer errors with 'expected DateTime to be a JSON Number, got string instead'. Now epochSeconds() — see Notes #1"}
  CreateProjectVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: duplicate (ProjectArn,VersionName) now returns ResourceInUseException — see Notes #2. Gap (not fixed): Tags/OutputConfig/TrainingData/TestingData/FeatureConfig accepted by the real API but not parsed here — low-traffic Custom Labels training flow, out of audit budget"}
  DeleteProjectVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeProjectVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: CreationTimestamp string->epoch-seconds — see Notes #1"}
  CopyProjectVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  StartProjectVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  StopProjectVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProjectPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: CreationTimestamp + LastUpdatedTimestamp string->epoch-seconds — see Notes #1"}
  PutProjectPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteProjectPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDataset: {wire: ok, errors: ok, state: ok, persist: ok, note: "datasetARN always uuid-suffixed so duplicate-dataset-type-per-project never collides; real AWS rejects a second dataset of the same type for a project with ResourceAlreadyExistsException — missing validation, not a wire bug, deferred"}
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
  - CreateStreamProcessor / DescribeStreamProcessor: Input/Output/Settings/RegionsOfInterest/NotificationChannel/KmsKeyId/DataSharingPreference are accepted by the real API but neither parsed from the request nor stored/returned — file a bd issue if stream-processor-heavy workloads need this
  - UpdateStreamProcessor is an existence-check no-op; none of UpdateStreamProcessorInput's fields (DataSharingPreferenceForUpdate, ParametersToDelete, RegionsOfInterestForUpdate, SettingsForUpdate) are applied
  - CreateProjectVersion drops Tags/OutputConfig/TrainingData/TestingData/FeatureConfig from the request (Custom Labels training config, low-traffic)
  - CreateDataset never rejects a duplicate (ProjectArn, DatasetType) pair (real AWS: ResourceAlreadyExistsException) because datasetARN is always uuid-suffixed
deferred:
  - Full field-level completeness audit of the async-video Get* response bodies (Celebrities/ModerationLabels/Faces/Labels/Persons/Segments/TextDetections arrays) — always empty; acceptable per the ML-mock exemption but not individually wire-diffed field-by-field this sweep
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
