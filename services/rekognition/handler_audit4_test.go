package rekognition_test

// handler_audit4_test.go — AWS-accuracy audit batch-4 for Rekognition (parity §A/§B).
//
// Proves similarity/confidence are derived deterministically from stored face
// state rather than canned constants:
//   - IndexFaces confidence is in a plausible range, stable, and varies per face
//   - SearchFaces similarity is deterministic and reproducible across calls
//   - SearchFaces returns exactly 100.0 for faces sharing an ExternalImageId
//   - SearchFacesByImage similarity depends on the supplied image
//   - Full round-trip: index → search → list → delete → search-not-found

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decodeFaceIDs pulls FaceId values out of an IndexFaces response.
func decodeFaceIDs(t *testing.T, body []byte) []string {
	t.Helper()

	var resp struct {
		FaceRecords []struct {
			Face struct {
				FaceID string `json:"FaceId"`
			} `json:"Face"`
		} `json:"FaceRecords"`
	}
	require.NoError(t, json.Unmarshal(body, &resp))

	ids := make([]string, 0, len(resp.FaceRecords))
	for _, r := range resp.FaceRecords {
		ids = append(ids, r.Face.FaceID)
	}

	return ids
}

func TestAudit4_IndexFaces_ConfidenceDeterministicAndPlausible(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "conf-coll"}).Code)

	confidences := make([]float64, 0, 5)

	for i := range 5 {
		rec := doRequest(t, h, "IndexFaces", map[string]any{
			"CollectionId":    "conf-coll",
			"ExternalImageId": map[bool]string{true: "", false: "subject"}[i%2 == 0],
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			FaceRecords []struct {
				Face struct {
					Confidence float64 `json:"Confidence"`
				} `json:"Face"`
			} `json:"FaceRecords"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.FaceRecords, 1)

		conf := resp.FaceRecords[0].Face.Confidence
		assert.GreaterOrEqual(t, conf, 90.0, "confidence must be a plausible detection value")
		assert.Less(t, conf, 100.0, "confidence must stay below 100")
		confidences = append(confidences, conf)
	}

	// Distinct faces should not all collapse to a single canned constant.
	distinct := map[float64]struct{}{}
	for _, c := range confidences {
		distinct[c] = struct{}{}
	}

	assert.Greater(t, len(distinct), 1, "confidence must vary per face, not be a canned constant")
}

func TestAudit4_SearchFaces_SimilarityDeterministic(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "sim-coll"}).Code)

	// Two faces with distinct external image IDs (different subjects).
	rec1 := doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "sim-coll", "ExternalImageId": "alice"})
	rec2 := doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "sim-coll", "ExternalImageId": "bob"})
	queryID := decodeFaceIDs(t, rec1.Body.Bytes())[0]
	require.NotEmpty(t, queryID)
	require.NotEmpty(t, decodeFaceIDs(t, rec2.Body.Bytes())[0])

	getSim := func() float64 {
		rec := doRequest(t, h, "SearchFaces", map[string]any{"CollectionId": "sim-coll", "FaceId": queryID})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			FaceMatches []struct {
				Similarity float64 `json:"Similarity"`
			} `json:"FaceMatches"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.FaceMatches, 1)

		return resp.FaceMatches[0].Similarity
	}

	first := getSim()
	second := getSim()

	assert.InDelta(t, first, second, 1e-9, "similarity must be deterministic across calls")
	assert.GreaterOrEqual(t, first, 75.0)
	assert.Less(t, first, 100.0, "different subjects must score below an exact match")
}

func TestAudit4_SearchFaces_SameExternalImageId_ScoresExactMatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "exact-coll"}).Code)

	// Two faces sharing the same ExternalImageId model the same subject.
	same := map[string]any{"CollectionId": "exact-coll", "ExternalImageId": "same-person"}
	rec1 := doRequest(t, h, "IndexFaces", same)
	doRequest(t, h, "IndexFaces", same)
	queryID := decodeFaceIDs(t, rec1.Body.Bytes())[0]

	rec := doRequest(t, h, "SearchFaces", map[string]any{"CollectionId": "exact-coll", "FaceId": queryID})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		FaceMatches []struct {
			Similarity float64 `json:"Similarity"`
		} `json:"FaceMatches"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.FaceMatches, 1)
	assert.InDelta(t, 100.0, resp.FaceMatches[0].Similarity, 1e-9,
		"faces sharing an ExternalImageId must score an exact match")
}

func TestAudit4_SearchFacesByImage_SimilarityDependsOnImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "img-coll"}).Code)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "img-coll", "ExternalImageId": "x"}).Code)

	search := func(bucket, key string) float64 {
		rec := doRequest(t, h, "SearchFacesByImage", map[string]any{
			"CollectionId": "img-coll",
			"Image":        map[string]any{"S3Object": map[string]any{"Bucket": bucket, "Name": key}},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			FaceMatches []struct {
				Similarity float64 `json:"Similarity"`
			} `json:"FaceMatches"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.FaceMatches, 1)

		return resp.FaceMatches[0].Similarity
	}

	a := search("b1", "img-a.jpg")
	aAgain := search("b1", "img-a.jpg")
	b := search("b1", "img-b.jpg")

	assert.InDelta(t, a, aAgain, 1e-9, "same image must yield the same similarity")
	assert.NotEqual(t, a, b, "different images must yield different similarities (not canned)")
}

func TestAudit4_FaceRoundTrip_IndexSearchListDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.Equal(t, http.StatusOK,
		doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "rt-coll"}).Code)

	// Index two faces.
	rec1 := doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "rt-coll", "ExternalImageId": "a"})
	rec2 := doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "rt-coll", "ExternalImageId": "b"})
	id1 := decodeFaceIDs(t, rec1.Body.Bytes())[0]
	id2 := decodeFaceIDs(t, rec2.Body.Bytes())[0]
	require.NotEmpty(t, id1)
	require.NotEmpty(t, id2)

	// ListFaces reflects both.
	listRec := doRequest(t, h, "ListFaces", map[string]any{"CollectionId": "rt-coll"})
	require.Equal(t, http.StatusOK, listRec.Code)

	var list struct {
		Faces []struct {
			FaceID string `json:"FaceId"`
		} `json:"Faces"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &list))
	require.Len(t, list.Faces, 2)

	// SearchFaces from id1 finds id2.
	searchRec := doRequest(t, h, "SearchFaces", map[string]any{"CollectionId": "rt-coll", "FaceId": id1})
	require.Equal(t, http.StatusOK, searchRec.Code)

	var search struct {
		FaceMatches []struct {
			Face struct {
				FaceID string `json:"FaceId"`
			} `json:"Face"`
		} `json:"FaceMatches"`
	}
	require.NoError(t, json.Unmarshal(searchRec.Body.Bytes(), &search))
	require.Len(t, search.FaceMatches, 1)
	assert.Equal(t, id2, search.FaceMatches[0].Face.FaceID)

	// Delete id2.
	delRec := doRequest(t, h, "DeleteFaces", map[string]any{"CollectionId": "rt-coll", "FaceIds": []string{id2}})
	require.Equal(t, http.StatusOK, delRec.Code)

	// SearchFaces from id1 now finds nothing.
	searchRec2 := doRequest(t, h, "SearchFaces", map[string]any{"CollectionId": "rt-coll", "FaceId": id1})
	require.Equal(t, http.StatusOK, searchRec2.Code)

	var search2 struct {
		FaceMatches []json.RawMessage `json:"FaceMatches"`
	}
	require.NoError(t, json.Unmarshal(searchRec2.Body.Bytes(), &search2))
	assert.Empty(t, search2.FaceMatches)

	// SearchFaces for the deleted id returns ResourceNotFoundException.
	nfRec := doRequest(t, h, "SearchFaces", map[string]any{"CollectionId": "rt-coll", "FaceId": id2})
	require.Equal(t, http.StatusBadRequest, nfRec.Code)
	assert.Contains(t, nfRec.Body.String(), "ResourceNotFoundException")
}
