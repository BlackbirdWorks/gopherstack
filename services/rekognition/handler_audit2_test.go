package rekognition_test

// handler_audit2_test.go — AWS-accuracy audit batch-2 for Rekognition (go-cn1y).
//
// Covers:
//   - Pagination: ListCollections MaxResults + NextToken
//   - Pagination: ListFaces MaxResults + NextToken
//   - Pagination: ListStreamProcessors MaxResults + NextToken
//   - DeleteCollection cascades to faces
//   - SearchFaces with real FaceId returns matches
//   - DeleteFaces by specific IDs removes only those faces
//   - UntagResource on unknown ARN returns ResourceNotFoundException
//   - ListFaces unknown collection returns ResourceNotFoundException
//   - ListFaces empty collection returns non-null Faces slice
//   - ListStreamProcessors empty returns non-null StreamProcessors slice
//   - DescribeCollection not-found returns ResourceNotFoundException
//   - IndexFaces ExternalImageId round-trips via ListFaces
//   - StopStreamProcessor on already-stopped is idempotent
//   - StartStreamProcessor on unknown returns ResourceNotFoundException
//   - UpdateStreamProcessor on unknown returns ResourceNotFoundException
//   - ListTagsForResource on stream processor ARN returns tags

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Pagination: ListCollections
// ---------------------------------------------------------------------------

func TestAudit2_ListCollections_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 3 collections: alpha, beta, gamma (sorted order).
	for _, id := range []string{"alpha", "beta", "gamma"} {
		rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": id})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: MaxResults=2 → alpha, beta, NextToken=gamma.
	rec := doRequest(t, h, "ListCollections", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))

	ids1, _ := page1["CollectionIds"].([]any)
	require.Len(t, ids1, 2)
	assert.Equal(t, "alpha", ids1[0])
	assert.Equal(t, "beta", ids1[1])

	nextToken1, _ := page1["NextToken"].(string)
	require.NotEmpty(t, nextToken1, "NextToken must be set when there are more results")

	// Page 2: NextToken from page 1 → gamma, no NextToken.
	rec = doRequest(t, h, "ListCollections", map[string]any{"NextToken": nextToken1})
	require.Equal(t, http.StatusOK, rec.Code)

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))

	ids2, _ := page2["CollectionIds"].([]any)
	require.Len(t, ids2, 1)
	assert.Equal(t, "gamma", ids2[0])
	assert.Empty(t, page2["NextToken"], "NextToken must be absent on last page")
}

// ---------------------------------------------------------------------------
// Pagination: ListFaces
// ---------------------------------------------------------------------------

func TestAudit2_ListFaces_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "page-faces-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Index 3 faces.
	for range 3 {
		doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "page-faces-coll"})
	}

	// Page 1: MaxResults=2.
	rec = doRequest(t, h, "ListFaces", map[string]any{
		"CollectionId": "page-faces-coll",
		"MaxResults":   2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))

	faces1, _ := page1["Faces"].([]any)
	require.Len(t, faces1, 2)

	nextToken1, _ := page1["NextToken"].(string)
	require.NotEmpty(t, nextToken1)

	// Page 2: remaining face.
	rec = doRequest(t, h, "ListFaces", map[string]any{
		"CollectionId": "page-faces-coll",
		"NextToken":    nextToken1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))

	faces2, _ := page2["Faces"].([]any)
	require.Len(t, faces2, 1)
	assert.Empty(t, page2["NextToken"])
}

// ---------------------------------------------------------------------------
// Pagination: ListStreamProcessors
// ---------------------------------------------------------------------------

func TestAudit2_ListStreamProcessors_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 3 stream processors: proc-a, proc-b, proc-c.
	for _, name := range []string{"proc-a", "proc-b", "proc-c"} {
		rec := doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": name})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: MaxResults=2.
	rec := doRequest(t, h, "ListStreamProcessors", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))

	procs1, _ := page1["StreamProcessors"].([]any)
	require.Len(t, procs1, 2)

	nextToken1, _ := page1["NextToken"].(string)
	require.NotEmpty(t, nextToken1)

	// Page 2: remaining processor.
	rec = doRequest(t, h, "ListStreamProcessors", map[string]any{"NextToken": nextToken1})
	require.Equal(t, http.StatusOK, rec.Code)

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))

	procs2, _ := page2["StreamProcessors"].([]any)
	require.Len(t, procs2, 1)
	assert.Empty(t, page2["NextToken"])
}

// ---------------------------------------------------------------------------
// DeleteCollection cascades to faces
// ---------------------------------------------------------------------------

func TestAudit2_DeleteCollection_CascadesToFaces(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "cascade-coll"})
	doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "cascade-coll"})
	doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "cascade-coll"})

	// Verify faces exist before delete.
	rec := doRequest(t, h, "ListFaces", map[string]any{"CollectionId": "cascade-coll"})
	require.Equal(t, http.StatusOK, rec.Code)
	var lf map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lf))
	faces, _ := lf["Faces"].([]any)
	require.Len(t, faces, 2)

	// Delete collection.
	rec = doRequest(t, h, "DeleteCollection", map[string]any{"CollectionId": "cascade-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListFaces should now return ResourceNotFoundException.
	rec = doRequest(t, h, "ListFaces", map[string]any{"CollectionId": "cascade-coll"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp["__type"])
}

// ---------------------------------------------------------------------------
// SearchFaces with real FaceId returns matches
// ---------------------------------------------------------------------------

func TestAudit2_SearchFaces_RealFaceId_ReturnsMatches(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "search-real-coll"})

	// Index 3 faces, capture the first face ID.
	rec := doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "search-real-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	var idx1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &idx1))
	records, _ := idx1["FaceRecords"].([]any)
	require.Len(t, records, 1)
	rec1, _ := records[0].(map[string]any)
	face1, _ := rec1["Face"].(map[string]any)
	faceID := face1["FaceId"].(string)

	// Index 2 more faces to match against.
	doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "search-real-coll"})
	doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "search-real-coll"})

	// SearchFaces returns the other 2 faces.
	rec = doRequest(t, h, "SearchFaces", map[string]any{
		"CollectionId": "search-real-coll",
		"FaceId":       faceID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var sf map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &sf))
	matches, _ := sf["FaceMatches"].([]any)
	assert.Len(t, matches, 2)
	assert.Equal(t, faceID, sf["SearchedFaceId"])
}

// ---------------------------------------------------------------------------
// SearchFaces with FaceId not in collection returns ResourceNotFoundException
// ---------------------------------------------------------------------------

func TestAudit2_SearchFaces_UnknownFaceId_Returns404(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "sf-notfound-coll"})

	rec := doRequest(t, h, "SearchFaces", map[string]any{
		"CollectionId": "sf-notfound-coll",
		"FaceId":       "00000000-0000-0000-0000-000000000000",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// ---------------------------------------------------------------------------
// DeleteFaces by specific IDs removes only those faces
// ---------------------------------------------------------------------------

func TestAudit2_DeleteFaces_BySpecificIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "delf-coll"})

	// Index 3 faces, capture all face IDs.
	faceIDs := make([]string, 3)

	for i := range 3 {
		rec := doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "delf-coll"})
		require.Equal(t, http.StatusOK, rec.Code)

		var idx map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &idx))
		records, _ := idx["FaceRecords"].([]any)
		rec0, _ := records[0].(map[string]any)
		face, _ := rec0["Face"].(map[string]any)
		faceIDs[i] = face["FaceId"].(string)
	}

	// Delete only the first two faces.
	rec := doRequest(t, h, "DeleteFaces", map[string]any{
		"CollectionId": "delf-coll",
		"FaceIds":      faceIDs[:2],
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var delResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &delResp))
	deleted, _ := delResp["DeletedFaces"].([]any)
	assert.Len(t, deleted, 2)

	// Verify only third face remains.
	rec = doRequest(t, h, "ListFaces", map[string]any{"CollectionId": "delf-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	var lf map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lf))
	remaining, _ := lf["Faces"].([]any)
	require.Len(t, remaining, 1)
	remainFace, _ := remaining[0].(map[string]any)
	assert.Equal(t, faceIDs[2], remainFace["FaceId"])
}

// ---------------------------------------------------------------------------
// UntagResource on unknown ARN returns ResourceNotFoundException
// ---------------------------------------------------------------------------

func TestAudit2_UntagResource_UnknownARN_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": "arn:aws:rekognition:us-east-1:000000000000:collection/no-such",
		"TagKeys":     []string{"k"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// ---------------------------------------------------------------------------
// ListFaces unknown collection returns ResourceNotFoundException
// ---------------------------------------------------------------------------

func TestAudit2_ListFaces_UnknownCollection_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListFaces", map[string]any{"CollectionId": "no-such-coll"})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// ---------------------------------------------------------------------------
// ListFaces empty collection returns non-null Faces slice
// ---------------------------------------------------------------------------

func TestAudit2_ListFaces_EmptyCollection_ReturnsNonNullSlice(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "empty-faces-coll"})

	rec := doRequest(t, h, "ListFaces", map[string]any{"CollectionId": "empty-faces-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	_, hasFaces := raw["Faces"]
	assert.True(t, hasFaces, "Faces field must be present")

	var faces []any
	require.NoError(t, json.Unmarshal(raw["Faces"], &faces))
	assert.NotNil(t, faces, "Faces must not be null")
}

// ---------------------------------------------------------------------------
// ListStreamProcessors empty returns non-null StreamProcessors slice
// ---------------------------------------------------------------------------

func TestAudit2_ListStreamProcessors_EmptyReturnsNonNullSlice(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListStreamProcessors", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	_, hasProcs := raw["StreamProcessors"]
	assert.True(t, hasProcs, "StreamProcessors field must be present")

	var procs []any
	require.NoError(t, json.Unmarshal(raw["StreamProcessors"], &procs))
	assert.NotNil(t, procs, "StreamProcessors must not be null")
}

// ---------------------------------------------------------------------------
// DescribeCollection not-found returns ResourceNotFoundException
// ---------------------------------------------------------------------------

func TestAudit2_DescribeCollection_NotFound_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeCollection", map[string]any{"CollectionId": "no-coll"})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// ---------------------------------------------------------------------------
// IndexFaces ExternalImageId round-trips via ListFaces
// ---------------------------------------------------------------------------

func TestAudit2_IndexFaces_ExternalImageId_RoundTrips(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "extid-coll"})

	rec := doRequest(t, h, "IndexFaces", map[string]any{
		"CollectionId":    "extid-coll",
		"ExternalImageId": "my-image-123",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var idx map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &idx))
	records, _ := idx["FaceRecords"].([]any)
	require.Len(t, records, 1)
	rec0, _ := records[0].(map[string]any)
	face0, _ := rec0["Face"].(map[string]any)
	assert.Equal(t, "my-image-123", face0["ExternalImageId"])

	// Verify via ListFaces.
	rec2 := doRequest(t, h, "ListFaces", map[string]any{"CollectionId": "extid-coll"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var lf map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &lf))
	faces, _ := lf["Faces"].([]any)
	require.Len(t, faces, 1)
	f0, _ := faces[0].(map[string]any)
	assert.Equal(t, "my-image-123", f0["ExternalImageId"])
}

// ---------------------------------------------------------------------------
// StopStreamProcessor on already-stopped is idempotent
// ---------------------------------------------------------------------------

func TestAudit2_StopStreamProcessor_AlreadyStopped_IsIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": "idem-proc"})

	// Stop a processor that was never started — should succeed without error.
	rec := doRequest(t, h, "StopStreamProcessor", map[string]any{"Name": "idem-proc"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Status should remain STOPPED.
	rec = doRequest(t, h, "DescribeStreamProcessor", map[string]any{"Name": "idem-proc"})
	require.Equal(t, http.StatusOK, rec.Code)

	var desc map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
	assert.Equal(t, "STOPPED", desc["Status"])
}

// ---------------------------------------------------------------------------
// StartStreamProcessor on unknown returns ResourceNotFoundException
// ---------------------------------------------------------------------------

func TestAudit2_StartStreamProcessor_Unknown_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StartStreamProcessor", map[string]any{"Name": "no-such-proc"})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// ---------------------------------------------------------------------------
// UpdateStreamProcessor on unknown returns ResourceNotFoundException
// ---------------------------------------------------------------------------

func TestAudit2_UpdateStreamProcessor_Unknown_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UpdateStreamProcessor", map[string]any{"Name": "no-such-proc"})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// ---------------------------------------------------------------------------
// ListTagsForResource on stream processor ARN returns tags
// ---------------------------------------------------------------------------

func TestAudit2_ListTagsForResource_StreamProcessor_ReturnsTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateStreamProcessor", map[string]any{
		"Name":    "tagged-proc",
		"RoleArn": "arn:aws:iam::000000000000:role/r",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	arn := createResp["StreamProcessorArn"].(string)

	// Tag the stream processor.
	doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"env": "staging", "team": "ml"},
	})

	// List tags via ARN.
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var tagResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagResp))
	tags, _ := tagResp["Tags"].(map[string]any)
	assert.Equal(t, "staging", tags["env"])
	assert.Equal(t, "ml", tags["team"])
}

// ---------------------------------------------------------------------------
// DescribeStreamProcessor reflects start/stop status transitions
// ---------------------------------------------------------------------------

func TestAudit2_StreamProcessor_StatusTransitions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": "status-proc"})

	getStatus := func() string {
		rec := doRequest(t, h, "DescribeStreamProcessor", map[string]any{"Name": "status-proc"})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		return resp["Status"].(string)
	}

	assert.Equal(t, "STOPPED", getStatus())

	doRequest(t, h, "StartStreamProcessor", map[string]any{"Name": "status-proc"})
	assert.Equal(t, "RUNNING", getStatus())

	doRequest(t, h, "StopStreamProcessor", map[string]any{"Name": "status-proc"})
	assert.Equal(t, "STOPPED", getStatus())
}
