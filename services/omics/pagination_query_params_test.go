package omics_test

// Real aws-sdk-go-v2 List* operations for HealthOmics's ReferenceStore,
// SequenceStore, ReadSet/import-job, AnnotationStore, VariantStore and Share
// families send maxResults/nextToken as query-string parameters — only the
// optional "filter" travels in the JSON body (confirmed against
// awsRestjson1_serializeOpHttpBindings*Input / awsRestjson1_serializeOpDocument*Input
// in aws-sdk-go-v2/service/omics's generated serializers.go). Previously this
// handler read maxResults/nextToken from the JSON body instead, so a real SDK
// client's pagination silently did nothing: nextToken was ignored (always
// restarting from page one) and maxResults was ignored (always defaulting to
// the 100-item cap). These tests drive pagination purely via the query string,
// the way a real SDK client does, and confirm a JSON-body-only maxResults is
// ignored (proving the fix actually moved the read site rather than just
// accepting both).
//
// The RunBatch family (ListBatch / ListRunsInBatch) uses "maxItems" instead of
// "maxResults" as the query key; that is covered separately below.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ListReferenceStores_PaginatesViaQueryString(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, http.MethodPost, "/referencestore", map[string]any{
			"name": "store-" + string(rune('a'+i)),
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	// maxResults belongs in the query string on real AWS; a value placed only
	// in the JSON body must be ignored (so it doesn't cap the page instead).
	rec := doRequest(t, h, http.MethodPost, "/referencestores?maxResults=2", map[string]any{
		"maxResults": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stores := resp["referenceStores"].([]any)
	assert.Len(t, stores, 2, "maxResults=2 from the query string must be honored")
	require.NotEmpty(t, resp["nextToken"], "a nextToken must be returned when more pages remain")

	nextRec := doRequest(
		t, h, http.MethodPost,
		"/referencestores?maxResults=2&nextToken="+resp["nextToken"].(string),
		nil,
	)
	require.Equal(t, http.StatusOK, nextRec.Code)

	var nextResp map[string]any
	require.NoError(t, json.Unmarshal(nextRec.Body.Bytes(), &nextResp))
	remaining := nextResp["referenceStores"].([]any)
	assert.Len(t, remaining, 1, "nextToken from the query string must resume from page two")
}

func Test_ListAnnotationStores_PaginatesViaQueryString(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"store-a", "store-b", "store-c"} {
		rec := doRequest(t, h, http.MethodPost, "/annotationStore", map[string]any{
			"name":        name,
			"storeFormat": "VCF",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	rec := doRequest(t, h, http.MethodPost, "/annotationStores?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	stores := resp["annotationStores"].([]any)
	assert.Len(t, stores, 2)
	require.NotEmpty(t, resp["nextToken"])
}

func Test_ListRunBatches_UsesMaxItemsNotMaxResultsQueryParam(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, http.MethodPost, "/runBatch", map[string]any{
			"workflowId": "wf123",
			"roleArn":    "arn:aws:iam::000000000000:role/role",
			"name":       "batch-" + string(rune('a'+i)),
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	// A "maxResults" query param must be ignored: real AWS ListBatch keys its
	// page size off "maxItems".
	ignoredRec := doRequest(t, h, http.MethodGet, "/runBatch?maxResults=1", nil)
	require.Equal(t, http.StatusOK, ignoredRec.Code)

	var ignoredResp map[string]any
	require.NoError(t, json.Unmarshal(ignoredRec.Body.Bytes(), &ignoredResp))
	assert.Len(t, ignoredResp["runBatches"].([]any), 3, "maxResults must not cap ListBatch")

	limitedRec := doRequest(t, h, http.MethodGet, "/runBatch?maxItems=1", nil)
	require.Equal(t, http.StatusOK, limitedRec.Code)

	var limitedResp map[string]any
	require.NoError(t, json.Unmarshal(limitedRec.Body.Bytes(), &limitedResp))
	assert.Len(t, limitedResp["runBatches"].([]any), 1, "maxItems must cap ListBatch")
}
