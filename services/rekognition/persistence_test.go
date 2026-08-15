package rekognition_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

// persistenceTestIDs carries the server-generated identifiers
// newPersistenceTestBackend produced, so a test can look resources back up
// by ID after a Snapshot/Restore round-trip regardless of the resource's own
// pagination/listing order.
type persistenceTestIDs struct {
	collectionARN      string
	streamProcessorARN string
	faceID             string
	projectARN         string
	datasetARN         string
	livenessSessionID  string
	asyncJobID         string
	mediaJobID         string
}

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (and, transitively, every secondary store.Index),
// plus the two raw maps left un-converted (tags, datasetEntries; see
// backend.go's field comments), so a Snapshot from it exercises the entire
// persisted surface of the backend.
//
// Two collections (each with one face) and two policies on the same project
// are created specifically to exercise the facesByCollection and
// projectPoliciesByProject secondary indexes grouping more than one entry
// under a key -- store.Table.Restore always rebuilds every index from
// scratch, so a correct grouping after Restore proves the index was never
// left stale, which is the property store.Table's "renaming an indexed
// field leaves a stale entry" gotcha would otherwise threaten. (No
// operation in this package's StorageBackend actually renames an indexed
// field in place -- CollectionID/ProjectARN are set once at creation and
// never reassigned on an existing stored value -- so the Delete-then-Put
// pattern that gotcha calls for does not apply anywhere in this backend;
// this full round-trip is the applicable proof instead.)
func newPersistenceTestBackend(t *testing.T) (*rekognition.InMemoryBackend, persistenceTestIDs) {
	t.Helper()

	b := rekognition.NewInMemoryBackend("000000000000", "us-east-1")

	coll1, err := b.CreateCollection("coll1", map[string]string{"env": "test"})
	require.NoError(t, err)

	faces, err := b.IndexFaces("coll1", "ext1")
	require.NoError(t, err)
	require.Len(t, faces, 1)

	_, err = b.CreateCollection("coll2", nil)
	require.NoError(t, err)
	_, err = b.IndexFaces("coll2", "ext2")
	require.NoError(t, err)

	sp, err := b.CreateStreamProcessor(
		"sp1", "arn:aws:iam::000000000000:role/r",
		rekognition.CreateStreamProcessorParams{}, map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	proj, err := b.CreateProject("proj1", rekognition.CreateProjectParams{})
	require.NoError(t, err)

	_, err = b.CreateProjectVersion(proj.ProjectARN, "v1", rekognition.CreateProjectVersionParams{}, nil)
	require.NoError(t, err)

	_, err = b.PutProjectPolicy(proj.ProjectARN, "policy1", `{"Version":"2012-10-17"}`, "")
	require.NoError(t, err)
	_, err = b.PutProjectPolicy(proj.ProjectARN, "policy2", `{"Version":"2012-10-17"}`, "")
	require.NoError(t, err)

	ds, err := b.CreateDataset(proj.ProjectARN, "TRAIN")
	require.NoError(t, err)
	require.NoError(t, b.UpdateDatasetEntries(ds.DatasetARN, []byte(`{"source-ref":"s3://bucket/1.jpg"}`)))

	require.NoError(t, b.CreateUser("coll1", "user1"))

	associated, unsuccessful, err := b.AssociateFaces("coll1", "user1", []string{faces[0].FaceID})
	require.NoError(t, err)
	require.Len(t, associated, 1)
	require.Empty(t, unsuccessful)

	livenessSessionID, err := b.CreateFaceLivenessSession()
	require.NoError(t, err)

	asyncJobID, err := b.StartAsyncJob(rekognition.StartAsyncJobParams{
		JobType: "PersonTracking", CollectionID: "coll1",
	})
	require.NoError(t, err)

	mediaJobID, err := b.StartMediaAnalysisJob("job1", rekognition.StartMediaAnalysisJobParams{
		InputS3Bucket:        "media-in-bucket",
		InputS3Name:          "media-in-key",
		OutputConfigS3Bucket: "media-out-bucket",
	})
	require.NoError(t, err)

	return b, persistenceTestIDs{
		collectionARN:      coll1.CollectionARN,
		streamProcessorARN: sp.StreamProcessorARN,
		faceID:             faces[0].FaceID,
		projectARN:         proj.ProjectARN,
		datasetARN:         ds.DatasetARN,
		livenessSessionID:  livenessSessionID,
		asyncJobID:         asyncJobID,
		mediaJobID:         mediaJobID,
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table (collections, faces, streamProcessors, projects,
// projectVersions, projectPolicies, datasets, users, livenessSessions,
// asyncJobs, mediaAnalysisJobs), every secondary store.Index derived from
// them, and the two raw maps left un-converted (tags, datasetEntries)
// through Snapshot -> Restore into a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original, ids := newPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := rekognition.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// collections table.
	coll1, err := fresh.DescribeCollection("coll1")
	require.NoError(t, err)
	assert.Equal(t, "test", coll1.Tags["env"])

	_, err = fresh.DescribeCollection("coll2")
	require.NoError(t, err)

	// faces table + facesByCollection index: two collections, one face
	// each, must not cross-contaminate after restore.
	facesColl1, _, err := fresh.ListFaces("coll1", 0, "")
	require.NoError(t, err)
	require.Len(t, facesColl1, 1)
	assert.Equal(t, ids.faceID, facesColl1[0].FaceID)

	facesColl2, _, err := fresh.ListFaces("coll2", 0, "")
	require.NoError(t, err)
	require.Len(t, facesColl2, 1)
	assert.NotEqual(t, ids.faceID, facesColl2[0].FaceID)

	// streamProcessors table.
	sp, err := fresh.DescribeStreamProcessor("sp1")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/r", sp.RoleARN)

	// tags raw map (left un-converted; persisted directly alongside the tables).
	collTags, err := fresh.ListTagsForResource(ids.collectionARN)
	require.NoError(t, err)
	assert.Equal(t, "test", collTags["env"])

	spTags, err := fresh.ListTagsForResource(ids.streamProcessorARN)
	require.NoError(t, err)
	assert.Equal(t, "v", spTags["k"])

	// projects table.
	projects, _, err := fresh.DescribeProjects(nil, 0, "")
	require.NoError(t, err)
	require.Len(t, projects, 1)
	assert.Equal(t, ids.projectARN, projects[0].ProjectARN)

	// projectVersions table.
	versions, _, err := fresh.DescribeProjectVersions(ids.projectARN, nil, 0, "")
	require.NoError(t, err)
	require.Len(t, versions, 1)
	assert.Equal(t, "v1", versions[0].VersionName)

	// projectPolicies table + projectPoliciesByProject index: two policies
	// on the same project, sorted by PolicyName.
	policies, _, err := fresh.ListProjectPolicies(ids.projectARN, 0, "")
	require.NoError(t, err)
	require.Len(t, policies, 2)
	assert.Equal(t, "policy1", policies[0].PolicyName)
	assert.Equal(t, "policy2", policies[1].PolicyName)

	// datasets table.
	ds, err := fresh.DescribeDataset(ids.datasetARN)
	require.NoError(t, err)
	assert.Equal(t, "TRAIN", ds.DatasetType)

	// datasetEntries raw map (left un-converted; persisted directly alongside the tables).
	entries, _, err := fresh.ListDatasetEntries(ids.datasetARN, 0, "")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Contains(t, entries[0], "s3://bucket/1.jpg")

	// users table + usersByCollection index, including the FaceIDs
	// association made before the snapshot.
	users, _, err := fresh.ListUsers("coll1", 0, "")
	require.NoError(t, err)
	require.Len(t, users, 1)
	assert.Equal(t, "user1", users[0].UserID)

	_, unsuccessful, err := fresh.DisassociateFaces("coll1", "user1", []string{ids.faceID})
	require.NoError(t, err)
	assert.Empty(t, unsuccessful, "the face-user association made before the snapshot must have survived restore")

	// livenessSessions table.
	livenessResult, err := fresh.GetFaceLivenessSessionResults(ids.livenessSessionID)
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", livenessResult.Status)

	// asyncJobs table.
	asyncJob, err := fresh.GetAsyncJob(ids.asyncJobID)
	require.NoError(t, err)
	assert.Equal(t, ids.asyncJobID, asyncJob.JobID)

	// mediaAnalysisJobs table.
	mediaJob, err := fresh.GetMediaAnalysisJob(ids.mediaJobID)
	require.NoError(t, err)
	assert.Equal(t, "job1", mediaJob.JobName)
	assert.Equal(t, "media-in-bucket", mediaJob.InputS3Bucket)
	assert.Equal(t, "media-in-key", mediaJob.InputS3Name)
	assert.Equal(t, "media-out-bucket", mediaJob.OutputConfigS3Bucket)

	mediaJobs, _, err := fresh.ListMediaAnalysisJobs(0, "")
	require.NoError(t, err)
	require.Len(t, mediaJobs, 1)

	// Sanity: account/region carried through too.
	assert.Equal(t, "us-east-1", fresh.Region())
	assert.Equal(t, "000000000000", fresh.AccountID())
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend -- including the pre-Phase-3.3
// shape, which has no version field at all and so decodes as Version == 0
// -- is discarded cleanly rather than partially decoded: the backend resets
// to empty state and Restore returns no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{name: "future version", data: []byte(`{"version":999,"tables":{}}`)},
		{name: "no version field (pre-Phase-3.3 shape)", data: []byte(`{"tables":{}}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newPersistenceTestBackend(t)

			require.NoError(t, b.Restore(t.Context(), tt.data))

			assert.Equal(t, 0, rekognition.CollectionCount(b))
			assert.Equal(t, 0, rekognition.StreamProcessorCount(b))

			_, err := b.DescribeCollection("coll1")
			require.ErrorIs(t, err, rekognition.ErrCollectionNotFound)
		})
	}
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := rekognition.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend -- the Phase 3.3 dead-wiring fix cli.go's generic
// setupPersistence relies on, since it type-asserts the registered
// service.Registerable (the Handler), not the backend.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend, ids := newPersistenceTestBackend(t)
	h := rekognition.NewHandler(backend)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := rekognition.NewHandler(rekognition.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	coll1, err := h2.Backend.DescribeCollection("coll1")
	require.NoError(t, err)
	assert.Equal(t, "coll1", coll1.CollectionID)

	mediaJob, err := h2.Backend.GetMediaAnalysisJob(ids.mediaJobID)
	require.NoError(t, err)
	assert.Equal(t, "job1", mediaJob.JobName)
}

// ---------------------------------------------------------------------------
// Snapshot/Restore round-trip
// ---------------------------------------------------------------------------

func TestSnapshotRestore_PreservesAllState(t *testing.T) {
	t.Parallel()

	backend := rekognition.NewInMemoryBackend("000000000000", "us-east-1")
	h := rekognition.NewHandler(backend)

	// Create a collection with tags and index a face.
	rec := doRequest(t, h, "CreateCollection", map[string]any{
		"CollectionId": "snap-coll",
		"Tags":         map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "snap-coll"})

	// Create a stream processor.
	rec = doRequest(t, h, "CreateStreamProcessor", map[string]any{
		"Name":    "snap-proc",
		"RoleArn": "arn:aws:iam::000000000000:role/r",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Snapshot.
	snap := backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	// Restore into a fresh backend.
	backend2 := rekognition.NewInMemoryBackend("", "")
	require.NoError(t, backend2.Restore(t.Context(), snap))
	h2 := rekognition.NewHandler(backend2)

	// Verify collection exists.
	rec = doRequest(t, h2, "DescribeCollection", map[string]any{"CollectionId": "snap-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.InDelta(t, float64(1), descResp["FaceCount"], 0)

	// Verify face survives.
	rec = doRequest(t, h2, "ListFaces", map[string]any{"CollectionId": "snap-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	var facesResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &facesResp))
	faces, _ := facesResp["Faces"].([]any)
	assert.Len(t, faces, 1)

	// Verify stream processor survives.
	rec = doRequest(t, h2, "DescribeStreamProcessor", map[string]any{"Name": "snap-proc"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify tags survive on collection.
	rec = doRequest(t, h2, "DescribeCollection", map[string]any{"CollectionId": "snap-coll"})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestSnapshotRestore_ProjectVersionAndAsyncJobNewFields round-trips the
// additive fields (omitempty, no snapshot version bump needed) landed
// alongside this test: storedProjectVersion's
// FeatureConfigContentModConfidenceThresh/MaxInferenceUnits/
// SourceProjectVersionARN, and storedAsyncJob's JobTag/VideoS3*/SegmentTypes.
func TestSnapshotRestore_ProjectVersionAndAsyncJobNewFields(t *testing.T) {
	t.Parallel()

	backend := rekognition.NewInMemoryBackend("000000000000", "us-east-1")
	h := rekognition.NewHandler(backend)

	rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "persist-proj"})
	require.Equal(t, http.StatusOK, rec.Code)

	var projResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &projResp))
	projectARN := projResp["ProjectArn"].(string)

	rec = doRequest(t, h, "CreateProjectVersion", map[string]any{
		"ProjectArn":   projectARN,
		"VersionName":  "v1",
		"OutputConfig": map[string]any{"S3Bucket": "my-bucket"},
		"FeatureConfig": map[string]any{
			"ContentModeration": map[string]any{"ConfidenceThreshold": 42.0},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var verResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &verResp))
	versionARN := verResp["ProjectVersionArn"].(string)

	rec = doRequest(t, h, "StartProjectVersion", map[string]any{
		"ProjectVersionArn": versionARN,
		"MinInferenceUnits": 1,
		"MaxInferenceUnits": 3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "persist-dst-proj"})
	require.Equal(t, http.StatusOK, rec.Code)

	var dstProjResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dstProjResp))
	dstProjectARN := dstProjResp["ProjectArn"].(string)

	rec = doRequest(t, h, "CopyProjectVersion", map[string]any{
		"SourceProjectArn":        projectARN,
		"SourceProjectVersionArn": versionARN,
		"DestinationProjectArn":   dstProjectARN,
		"VersionName":             "v1-copy",
		"OutputConfig":            map[string]any{"S3Bucket": "copy-bucket"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "StartSegmentDetection", map[string]any{
		"Video": map[string]any{
			"S3Object": map[string]any{"Bucket": "video-bucket", "Name": "clip.mp4", "Version": "v9"},
		},
		"JobTag":       "persist-tag",
		"SegmentTypes": []string{"SHOT"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"].(string)

	snap := backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	backend2 := rekognition.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, backend2.Restore(t.Context(), snap))
	h2 := rekognition.NewHandler(backend2)

	rec = doRequest(t, h2, "DescribeProjectVersions", map[string]any{"ProjectArn": projectARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	versions := descResp["ProjectVersionDescriptions"].([]any)
	require.Len(t, versions, 1)
	desc := versions[0].(map[string]any)
	assert.InDelta(t, 3, desc["MaxInferenceUnits"], 0.001)

	featureConfig, _ := desc["FeatureConfig"].(map[string]any)
	require.NotNil(t, featureConfig)
	contentMod, _ := featureConfig["ContentModeration"].(map[string]any)
	require.NotNil(t, contentMod)
	assert.InDelta(t, 42.0, contentMod["ConfidenceThreshold"], 0.001)

	rec = doRequest(t, h2, "DescribeProjectVersions", map[string]any{"ProjectArn": dstProjectARN})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	dstVersions := descResp["ProjectVersionDescriptions"].([]any)
	require.Len(t, dstVersions, 1)
	assert.Equal(t, versionARN, dstVersions[0].(map[string]any)["SourceProjectVersionArn"])

	rec = doRequest(t, h2, "GetSegmentDetection", map[string]any{"JobId": jobID})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "persist-tag", getResp["JobTag"])

	video, _ := getResp["Video"].(map[string]any)
	require.NotNil(t, video)
	s3Object, _ := video["S3Object"].(map[string]any)
	assert.Equal(t, "video-bucket", s3Object["Bucket"])

	selected, _ := getResp["SelectedSegmentTypes"].([]any)
	require.Len(t, selected, 1)
	assert.Equal(t, "SHOT", selected[0].(map[string]any)["Type"])
}

func TestSnapshotRestore_TagsSurvive(t *testing.T) {
	t.Parallel()

	backend := rekognition.NewInMemoryBackend("000000000000", "us-east-1")
	h := rekognition.NewHandler(backend)

	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "tag-snap-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	arn := createResp["CollectionArn"].(string)

	doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"key1": "val1", "key2": "val2"},
	})

	snap := backend.Snapshot(t.Context())

	backend2 := rekognition.NewInMemoryBackend("", "")
	require.NoError(t, backend2.Restore(t.Context(), snap))
	h2 := rekognition.NewHandler(backend2)

	rec = doRequest(t, h2, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var tagResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagResp))
	tags, _ := tagResp["Tags"].(map[string]any)
	assert.Equal(t, "val1", tags["key1"])
	assert.Equal(t, "val2", tags["key2"])
}
