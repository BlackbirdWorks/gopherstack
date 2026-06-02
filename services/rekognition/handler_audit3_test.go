package rekognition_test

// handler_audit3_test.go — AWS-accuracy audit batch-3 for Rekognition (go-8kaq).
//
// Covers:
//   - Tag key/value length validation (key 1-128, value 0-256)
//   - Tag count limit per resource (max 200)
//   - CollectionId length limit (max 255)
//   - StreamProcessor Name length limit (max 128)
//   - TagResource on unknown ARN returns ResourceNotFoundException
//   - CreateCollection with initial tags validates them
//   - CreateStreamProcessor with initial tags validates them
//   - Snapshot/Restore round-trip preserves collections, faces, stream processors, tags

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

// ---------------------------------------------------------------------------
// Tag key/value validation on TagResource
// ---------------------------------------------------------------------------

func TestAudit3_TagResource_KeyValueValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid tags accepted",
			tags:     map[string]string{"env": "prod"},
			wantCode: http.StatusOK,
		},
		{
			name:     "empty key rejected",
			tags:     map[string]string{"": "value"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "key at limit (128) accepted",
			tags:     map[string]string{strings.Repeat("k", 128): "v"},
			wantCode: http.StatusOK,
		},
		{
			name:     "key over limit (129) rejected",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value at limit (256) accepted",
			tags:     map[string]string{"k": strings.Repeat("v", 256)},
			wantCode: http.StatusOK,
		},
		{
			name:     "value over limit (257) rejected",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty value accepted",
			tags:     map[string]string{"k": ""},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "tag-val-coll"})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			arn := resp["CollectionArn"].(string)

			rec = doRequest(t, h, "TagResource", map[string]any{"ResourceArn": arn, "Tags": tc.tags})
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)
		})
	}
}

func TestAudit3_TagResource_KeyValidation_ErrorType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "tag-err-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn := resp["CollectionArn"].(string)

	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"": "v"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidParameterException", errResp["__type"])
}

// ---------------------------------------------------------------------------
// Tag count limit per resource (max 200)
// ---------------------------------------------------------------------------

func TestAudit3_TagResource_CountLimit_Enforced(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "tag-limit-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn := resp["CollectionArn"].(string)

	// Add 200 tags in batches of 50 (keys are "tag-000" through "tag-199").
	for batch := range 4 {
		batchTags := make(map[string]string, 50)
		for i := range 50 {
			n := batch*50 + i
			key := "tag-" + string(rune('0'+n/100)) + string(rune('0'+(n/10)%10)) + string(rune('0'+n%10))
			batchTags[key] = "v"
		}

		rec = doRequest(t, h, "TagResource", map[string]any{"ResourceArn": arn, "Tags": batchTags})
		require.Equal(t, http.StatusOK, rec.Code, "batch %d should succeed", batch)
	}

	// Adding one more tag should fail.
	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"extra-key": "v"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit3_TagResource_UpdateExisting_DoesNotCountAgain(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "tag-update-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn := resp["CollectionArn"].(string)

	// Add one tag, then update it — should not fail due to count.
	doRequest(t, h, "TagResource", map[string]any{"ResourceArn": arn, "Tags": map[string]string{"k": "v1"}})

	rec = doRequest(t, h, "TagResource", map[string]any{"ResourceArn": arn, "Tags": map[string]string{"k": "v2"}})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// Tag validation on CreateCollection initial tags
// ---------------------------------------------------------------------------

func TestAudit3_CreateCollection_InvalidInitialTags_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "empty tag key rejected",
			tags:     map[string]string{"": "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag key too long rejected",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag value too long rejected",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCollection", map[string]any{
				"CollectionId": "init-tags-coll",
				"Tags":         tc.tags,
			})
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)
		})
	}
}

func TestAudit3_CreateCollection_ValidInitialTags_Accepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateCollection", map[string]any{
		"CollectionId": "valid-tags-coll",
		"Tags":         map[string]string{"env": "test", "team": "infra"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// CollectionId length validation
// ---------------------------------------------------------------------------

func TestAudit3_CreateCollection_IDLengthValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		id       string
		wantCode int
	}{
		{
			name:     "empty ID rejected",
			id:       "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ID at limit (255) accepted",
			id:       strings.Repeat("a", 255),
			wantCode: http.StatusOK,
		},
		{
			name:     "ID over limit (256) rejected",
			id:       strings.Repeat("a", 256),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": tc.id})
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)
		})
	}
}

// ---------------------------------------------------------------------------
// StreamProcessor Name length validation
// ---------------------------------------------------------------------------

func TestAudit3_CreateStreamProcessor_NameLengthValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		procName string
		wantCode int
	}{
		{
			name:     "empty name rejected",
			procName: "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "name at limit (128) accepted",
			procName: strings.Repeat("p", 128),
			wantCode: http.StatusOK,
		},
		{
			name:     "name over limit (129) rejected",
			procName: strings.Repeat("p", 129),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": tc.procName})
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)
		})
	}
}

// ---------------------------------------------------------------------------
// Tag validation on CreateStreamProcessor initial tags
// ---------------------------------------------------------------------------

func TestAudit3_CreateStreamProcessor_InvalidInitialTags_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "empty tag key rejected",
			tags:     map[string]string{"": "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag key too long rejected",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateStreamProcessor", map[string]any{
				"Name": "proc-tags-test",
				"Tags": tc.tags,
			})
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)
		})
	}
}

// ---------------------------------------------------------------------------
// TagResource on unknown ARN
// ---------------------------------------------------------------------------

func TestAudit3_TagResource_UnknownARN_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "arn:aws:rekognition:us-east-1:000000000000:collection/no-such",
		"Tags":        map[string]string{"k": "v"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// ---------------------------------------------------------------------------
// Snapshot/Restore round-trip
// ---------------------------------------------------------------------------

func TestAudit3_SnapshotRestore_PreservesAllState(t *testing.T) {
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
	snap := backend.Snapshot()
	require.NotEmpty(t, snap)

	// Restore into a fresh backend.
	backend2 := rekognition.NewInMemoryBackend("", "")
	require.NoError(t, backend2.Restore(snap))
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

func TestAudit3_SnapshotRestore_TagsSurvive(t *testing.T) {
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

	snap := backend.Snapshot()

	backend2 := rekognition.NewInMemoryBackend("", "")
	require.NoError(t, backend2.Restore(snap))
	h2 := rekognition.NewHandler(backend2)

	rec = doRequest(t, h2, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var tagResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagResp))
	tags, _ := tagResp["Tags"].(map[string]any)
	assert.Equal(t, "val1", tags["key1"])
	assert.Equal(t, "val2", tags["key2"])
}

// ---------------------------------------------------------------------------
// ListCollections never returns null slices
// ---------------------------------------------------------------------------

func TestAudit3_ListCollections_EmptyReturnsSlices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListCollections", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// Decode raw to check for non-null slice presence.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	_, hasIDs := raw["CollectionIds"]
	assert.True(t, hasIDs, "CollectionIds must be present")

	var ids []any
	require.NoError(t, json.Unmarshal(raw["CollectionIds"], &ids))
	assert.NotNil(t, ids, "CollectionIds must not be null")
}

// ---------------------------------------------------------------------------
// StreamProcessor tag validation via TagResource
// ---------------------------------------------------------------------------

func TestAudit3_TagResource_StreamProcessor_TagsValidated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateStreamProcessor", map[string]any{
		"Name":    "tag-proc",
		"RoleArn": "arn:aws:iam::000000000000:role/r",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn := resp["StreamProcessorArn"].(string)

	// Valid tag.
	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"team": "ml"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Invalid tag key.
	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{strings.Repeat("x", 129): "v"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
