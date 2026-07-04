package textract_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ListAdapters_SummaryShape verifies that ListAdapters returns
// FeatureTypes in each adapter summary and omits Tags. Real AWS ListAdapters
// returns AdapterOverview items with FeatureTypes but no Tags; Tags are only
// accessible via GetAdapter or ListTagsForResource.
func TestParity_ListAdapters_SummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create an adapter with tags.
	createRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "tagged-adapter",
		"FeatureTypes": []string{"FORMS", "QUERIES"},
		"Tags":         map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	listRec := doTextractRequest(t, h, "ListAdapters", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	adapters, ok := resp["Adapters"].([]any)
	require.True(t, ok)
	require.Len(t, adapters, 1)

	summary, ok := adapters[0].(map[string]any)
	require.True(t, ok)

	// FeatureTypes must be present in the summary.
	fts, hasFT := summary["FeatureTypes"].([]any)
	assert.True(t, hasFT, "ListAdapters summary must include FeatureTypes")
	assert.Len(t, fts, 2, "FeatureTypes must reflect adapter's feature types")

	// Tags must NOT appear in ListAdapters summary.
	_, hasTags := summary["Tags"]
	assert.False(t, hasTags, "ListAdapters summary must not include Tags")
}

// TestParity_ListAdapterVersions_SummaryShape verifies that ListAdapterVersions
// returns FeatureTypes in each version summary and omits Tags. Real AWS returns
// AdapterVersionOverview items with FeatureTypes/Status but without Tags.
func TestParity_ListAdapterVersions_SummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create adapter then a version.
	createRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "version-shape-adapter",
		"FeatureTypes": []string{"QUERIES"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	adapterID := createResp["AdapterId"]

	verRec := doTextractRequest(t, h, "CreateAdapterVersion", map[string]any{
		"AdapterId": adapterID,
		"Tags":      map[string]string{"phase": "alpha"},
	})
	require.Equal(t, http.StatusOK, verRec.Code)

	listRec := doTextractRequest(t, h, "ListAdapterVersions", map[string]any{
		"AdapterId": adapterID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	versions, ok := resp["AdapterVersions"].([]any)
	require.True(t, ok)
	require.Len(t, versions, 1)

	vSummary, ok := versions[0].(map[string]any)
	require.True(t, ok)

	// FeatureTypes must be present in the version summary.
	fts, hasFT := vSummary["FeatureTypes"].([]any)
	assert.True(t, hasFT, "ListAdapterVersions summary must include FeatureTypes")
	assert.Len(t, fts, 1)

	// Tags must NOT appear in ListAdapterVersions summary.
	_, hasTags := vSummary["Tags"]
	assert.False(t, hasTags, "ListAdapterVersions summary must not include Tags")

	// Status must be present.
	status, hasStatus := vSummary["Status"].(string)
	assert.True(t, hasStatus, "ListAdapterVersions summary must include Status")
	assert.NotEmpty(t, status)
}

// TestParity_AnalyzeDocument_QUERIES_RequiresQueriesConfig verifies that
// AnalyzeDocument with FeatureTypes=["QUERIES"] but no QueriesConfig returns
// HTTP 400. Real AWS enforces this as a ValidationException.
func TestParity_AnalyzeDocument_QUERIES_RequiresQueriesConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "queries_without_queriesconfig_rejected",
			body: map[string]any{
				"Document":     map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "f.pdf"}},
				"FeatureTypes": []string{"QUERIES"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "queries_with_empty_queries_rejected",
			body: map[string]any{
				"Document":      map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "f.pdf"}},
				"FeatureTypes":  []string{"QUERIES"},
				"QueriesConfig": map[string]any{"Queries": []any{}},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "queries_with_queriesconfig_accepted",
			body: map[string]any{
				"Document":     map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "f.pdf"}},
				"FeatureTypes": []string{"QUERIES"},
				"QueriesConfig": map[string]any{
					"Queries": []any{map[string]any{"Text": "What is the total?"}},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "other_features_without_queriesconfig_accepted",
			body: map[string]any{
				"Document":     map[string]any{"S3Object": map[string]any{"Bucket": "b", "Name": "f.pdf"}},
				"FeatureTypes": []string{"FORMS", "TABLES"},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "AnalyzeDocument", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestParity_StartDocumentAnalysis_QUERIES_RequiresQueriesConfig verifies the
// same QueriesConfig requirement applies to the async StartDocumentAnalysis path.
func TestParity_StartDocumentAnalysis_QUERIES_RequiresQueriesConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "f.pdf"},
		},
		"FeatureTypes": []string{"QUERIES"},
		// QueriesConfig omitted — must return 400.
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"StartDocumentAnalysis with QUERIES but no QueriesConfig must return 400")
}

// TestParity_GetAdapter_IncludesTags verifies that GetAdapter (single adapter
// fetch) does include Tags. Tags appear in the per-resource GetAdapter response
// but not in the ListAdapters summary.
func TestParity_GetAdapter_IncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "tags-adapter",
		"FeatureTypes": []string{"FORMS"},
		"Tags":         map[string]string{"key": "value"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	getRec := doTextractRequest(t, h, "GetAdapter", map[string]any{
		"AdapterId": createResp["AdapterId"],
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	tags, hasTags := getResp["Tags"].(map[string]any)
	assert.True(t, hasTags, "GetAdapter must include Tags")
	assert.Equal(t, "value", tags["key"])
}
