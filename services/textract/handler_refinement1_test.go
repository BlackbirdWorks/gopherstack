package textract_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/textract"
)

// ----- helpers -----

func newTestBackend(t *testing.T) *textract.InMemoryBackend {
	t.Helper()

	return textract.NewInMemoryBackendSync("123456789012", "us-east-1")
}

func newTestHandlerR1(t *testing.T) *textract.Handler {
	t.Helper()

	return textract.NewHandler(newTestBackend(t))
}

// ----- TestRefinement1_* tests -----

// TestRefinement1_OpsLenMatchesSupportedOperations verifies that the cached ops
// map has exactly the same number of entries as GetSupportedOperations.
func TestRefinement1_OpsLenMatchesSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)
	assert.Equal(t, len(h.GetSupportedOperations()), textract.HandlerOpsLen(h))
}

// TestRefinement1_BackendAccountIDAndRegion ensures the backend stores
// account-ID and region supplied at construction.
func TestRefinement1_BackendAccountIDAndRegion(t *testing.T) {
	t.Parallel()

	b := textract.NewInMemoryBackend("000111222333", "eu-west-1")
	assert.Equal(t, "000111222333", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

// TestRefinement1_ProviderNilAppContext verifies Init returns ErrNilAppContext
// when passed a nil context.
func TestRefinement1_ProviderNilAppContext(t *testing.T) {
	t.Parallel()

	p := &textract.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, textract.ErrNilAppContext)
}

// TestRefinement1_BackendReset ensures Reset clears all stored state.
func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	_, err := b.StartDocumentAnalysis(context.Background(), "s3://bucket/doc.pdf")
	require.NoError(t, err)

	_, err = b.CreateAdapter(context.Background(), "myAdapter", "desc", "DISABLED", []string{"FORMS"}, nil)
	require.NoError(t, err)

	require.Equal(t, 1, textract.JobCount(b))
	require.Equal(t, 1, textract.AdapterCount(b))

	b.Reset()

	assert.Equal(t, 0, textract.JobCount(b))
	assert.Equal(t, 0, textract.AdapterCount(b))
}

// TestRefinement1_HandlerReset delegates to the backend Reset.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)

	doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "k"},
		},
		"FeatureTypes": []string{"TABLES"},
	})

	h.Reset()

	b := h.Backend.(*textract.InMemoryBackend)
	assert.Equal(t, 0, textract.JobCount(b))
}

// TestRefinement1_StartExpenseAnalysis_HappyPath tests the full async
// StartExpenseAnalysis → GetExpenseAnalysis flow.
func TestRefinement1_StartExpenseAnalysis_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)

	startRec := doTextractRequest(t, h, "StartExpenseAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "invoices", "Name": "q4.pdf"},
		},
	})

	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]
	require.NotEmpty(t, jobID)

	getRec := doTextractRequest(t, h, "GetExpenseAnalysis", map[string]string{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.Equal(t, "SUCCEEDED", getResp["JobStatus"])
	assert.Equal(t, "1.0", getResp["AnalyzeExpenseModelVersion"])
	docs, ok := getResp["ExpenseDocuments"].([]any)
	assert.True(t, ok)
	assert.NotEmpty(t, docs)
}

// TestRefinement1_StartExpenseAnalysis_MissingBucket validates required fields.
func TestRefinement1_StartExpenseAnalysis_MissingBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)
	rec := doTextractRequest(t, h, "StartExpenseAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "", "Name": ""},
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_StartLendingAnalysis_HappyPath tests the full async
// StartLendingAnalysis → GetLendingAnalysis flow.
func TestRefinement1_StartLendingAnalysis_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)

	startRec := doTextractRequest(t, h, "StartLendingAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "loans", "Name": "app.pdf"},
		},
	})

	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]
	require.NotEmpty(t, jobID)

	getRec := doTextractRequest(t, h, "GetLendingAnalysis", map[string]string{"JobId": jobID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.Equal(t, "SUCCEEDED", getResp["JobStatus"])
}

// TestRefinement1_StartLendingAnalysis_MissingBucket validates required fields.
func TestRefinement1_StartLendingAnalysis_MissingBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)
	rec := doTextractRequest(t, h, "StartLendingAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "", "Name": ""},
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_GetLendingAnalysisSummary tests the summary endpoint for a
// lending analysis job.
func TestRefinement1_GetLendingAnalysisSummary_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)

	startRec := doTextractRequest(t, h, "StartLendingAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "loans", "Name": "app.pdf"},
		},
	})

	var startResp map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"]

	rec := doTextractRequest(t, h, "GetLendingAnalysisSummary", map[string]string{"JobId": jobID})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "SUCCEEDED", resp["JobStatus"])
	assert.Equal(t, "1.0", resp["AnalyzeLendingModelVersion"])
}

// TestRefinement1_GetLendingAnalysisSummary_NotFound ensures not-found returns 400.
func TestRefinement1_GetLendingAnalysisSummary_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)
	rec := doTextractRequest(t, h, "GetLendingAnalysisSummary", map[string]string{"JobId": "no-such-job"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidJobIdException", errResp["__type"])
}

// TestRefinement1_CreateAdapter_FeatureTypeValidation ensures at least one valid
// feature type is required.
func TestRefinement1_CreateAdapter_FeatureTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		featureTypes []string
		wantStatus   int
	}{
		{
			// TABLES is not allowed for adapters (only FORMS and QUERIES per AWS spec).
			name:         "TABLES returns 400 for adapter",
			featureTypes: []string{"TABLES"},
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "valid FORMS",
			featureTypes: []string{"FORMS"},
			wantStatus:   http.StatusOK,
		},
		{
			name:         "valid QUERIES",
			featureTypes: []string{"QUERIES"},
			wantStatus:   http.StatusOK,
		},
		{
			name:         "multiple valid types FORMS+QUERIES",
			featureTypes: []string{"FORMS", "QUERIES"},
			wantStatus:   http.StatusOK,
		},
		{
			name:         "empty slice returns 400",
			featureTypes: []string{},
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "invalid type returns 400",
			featureTypes: []string{"INVALID_TYPE"},
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerR1(t)
			rec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
				"AdapterName":  "test-adapter",
				"FeatureTypes": tt.featureTypes,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_CreateAdapter_AutoUpdateValidation validates the AutoUpdate field.
func TestRefinement1_CreateAdapter_AutoUpdateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		autoUpdate string
		wantStatus int
	}{
		{name: "ENABLED", autoUpdate: "ENABLED", wantStatus: http.StatusOK},
		{name: "DISABLED", autoUpdate: "DISABLED", wantStatus: http.StatusOK},
		{name: "empty defaults to DISABLED", autoUpdate: "", wantStatus: http.StatusOK},
		{name: "invalid returns 400", autoUpdate: "INVALID", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerR1(t)
			rec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
				"AdapterName":  "adapter",
				"FeatureTypes": []string{"FORMS"},
				"AutoUpdate":   tt.autoUpdate,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_UpdateAdapter_HappyPath tests updating adapter description and AutoUpdate.
func TestRefinement1_UpdateAdapter_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)

	createRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "my-adapter",
		"FeatureTypes": []string{"FORMS"},
		"Description":  "original",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	adapterID := createResp["AdapterId"]

	updateRec := doTextractRequest(t, h, "UpdateAdapter", map[string]any{
		"AdapterId":   adapterID,
		"Description": "updated description",
		"AutoUpdate":  "ENABLED",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	assert.Equal(t, "updated description", updateResp["Description"])
	assert.Equal(t, "ENABLED", updateResp["AutoUpdate"])
}

// TestRefinement1_UpdateAdapter_NotFound ensures not-found returns 400.
func TestRefinement1_UpdateAdapter_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)
	rec := doTextractRequest(t, h, "UpdateAdapter", map[string]any{
		"AdapterId":  "nonexistent",
		"AutoUpdate": "DISABLED",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidParameterException", errResp["__type"])
}

// TestRefinement1_ListAdapters tests ListAdapters with multiple entries.
func TestRefinement1_ListAdapters_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		rec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
			"AdapterName":  name,
			"FeatureTypes": []string{"FORMS"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doTextractRequest(t, h, "ListAdapters", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	adapters, ok := resp["Adapters"].([]any)
	assert.True(t, ok)
	assert.Len(t, adapters, 3)
}

// TestRefinement1_ListAdapterVersions tests listing versions for an adapter.
func TestRefinement1_ListAdapterVersions_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)

	createRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "versioned-adapter",
		"FeatureTypes": []string{"FORMS"},
	})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	adapterID := createResp["AdapterId"]

	for range 3 {
		doTextractRequest(t, h, "CreateAdapterVersion", map[string]any{
			"AdapterId": adapterID,
		})
	}

	listRec := doTextractRequest(t, h, "ListAdapterVersions", map[string]any{
		"AdapterId": adapterID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
	versions, ok := resp["AdapterVersions"].([]any)
	assert.True(t, ok)
	assert.Len(t, versions, 3)
	assert.Equal(t, adapterID, resp["AdapterId"])
}

// TestRefinement1_ListAdapterVersions_NotFound returns 400 for unknown adapter.
func TestRefinement1_ListAdapterVersions_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)
	rec := doTextractRequest(t, h, "ListAdapterVersions", map[string]any{
		"AdapterId": "nonexistent",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_TagResource tests tagging and listing tags on an adapter.
func TestRefinement1_TagResource_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)

	createRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "taggable-adapter",
		"FeatureTypes": []string{"FORMS"},
	})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	adapterID := createResp["AdapterId"]

	tagRec := doTextractRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": adapterID,
		"Tags":        map[string]string{"env": "prod", "team": "ml"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	listRec := doTextractRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": adapterID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "ml", tags["team"])
}

// TestRefinement1_UntagResource tests removing tags from an adapter.
func TestRefinement1_UntagResource_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)

	createRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "tagged-adapter",
		"FeatureTypes": []string{"FORMS"},
		"Tags":         map[string]string{"env": "dev", "team": "ml"},
	})
	var createResp map[string]string
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	adapterID := createResp["AdapterId"]

	untagRec := doTextractRequest(t, h, "UntagResource", map[string]any{
		"ResourceARN": adapterID,
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec := doTextractRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceARN": adapterID,
	})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, _ := listResp["Tags"].(map[string]any)
	_, hasEnv := tags["env"]
	assert.False(t, hasEnv)
	assert.Equal(t, "ml", tags["team"])
}

// TestRefinement1_TagResource_NotFound returns 400 for unknown resource.
func TestRefinement1_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)
	rec := doTextractRequest(t, h, "TagResource", map[string]any{
		"ResourceARN": "nonexistent-arn",
		"Tags":        map[string]string{"k": "v"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_HandleError_AdapterNotFound ensures adapter not-found returns
// InvalidParameterException (not InvalidJobIdException).
func TestRefinement1_HandleError_AdapterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)
	rec := doTextractRequest(t, h, "GetAdapter", map[string]any{
		"AdapterId": "no-such-adapter",
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidParameterException", errResp["__type"])
}

// TestRefinement1_HandleError_JobNotFound ensures job not-found returns
// InvalidJobIdException (not InvalidParameterException).
func TestRefinement1_HandleError_JobNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)
	rec := doTextractRequest(t, h, "GetDocumentAnalysis", map[string]any{
		"JobId": "no-such-job",
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidJobIdException", errResp["__type"])
}

// TestRefinement1_DeleteAdapter_CascadesVersions ensures that deleting an adapter
// also removes all its versions.
func TestRefinement1_DeleteAdapter_CascadesVersions(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	adapter, err := b.CreateAdapter(context.Background(), "cascade-test", "", "DISABLED", []string{"FORMS"}, nil)
	require.NoError(t, err)

	_, err = b.CreateAdapterVersion(context.Background(), adapter.AdapterID, nil)
	require.NoError(t, err)

	_, err = b.CreateAdapterVersion(context.Background(), adapter.AdapterID, nil)
	require.NoError(t, err)

	require.Equal(t, 2, textract.AdapterVersionCount(b))

	err = b.DeleteAdapter(context.Background(), adapter.AdapterID)
	require.NoError(t, err)

	assert.Equal(t, 0, textract.AdapterCount(b))
	assert.Equal(t, 0, textract.AdapterVersionCount(b))
}

// TestRefinement1_PersistenceWithExpenseAndLendingJobs tests Snapshot/Restore with
// expense and lending jobs present.
func TestRefinement1_PersistenceWithExpenseAndLendingJobs(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	expJob, err := b.StartExpenseAnalysis(context.Background(), "s3://bucket/invoice.pdf")
	require.NoError(t, err)

	lendJob, err := b.StartLendingAnalysis(context.Background(), "s3://bucket/loan.pdf")
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := textract.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 1, textract.ExpenseJobCount(b2))
	assert.Equal(t, 1, textract.LendingJobCount(b2))

	fetched, err := b2.GetExpenseAnalysis(context.Background(), expJob.JobID)
	require.NoError(t, err)
	assert.Equal(t, expJob.JobID, fetched.JobID)
	assert.Equal(t, "SUCCEEDED", fetched.JobStatus)
	assert.NotEmpty(t, fetched.ExpenseDocuments)

	fetchedL, err := b2.GetLendingAnalysis(context.Background(), lendJob.JobID)
	require.NoError(t, err)
	assert.Equal(t, lendJob.JobID, fetchedL.JobID)
}

// TestRefinement1_PersistenceWithAdapters tests Snapshot/Restore with adapters
// and adapter versions.
func TestRefinement1_PersistenceWithAdapters(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	adapter, err := b.CreateAdapter(
		context.Background(),
		"persist-adapter",
		"desc",
		"ENABLED",
		[]string{"FORMS"},
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	av, err := b.CreateAdapterVersion(context.Background(), adapter.AdapterID, nil)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := textract.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 1, textract.AdapterCount(b2))
	assert.Equal(t, 1, textract.AdapterVersionCount(b2))

	fetchedAdapter, err := b2.GetAdapter(context.Background(), adapter.AdapterID)
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", fetchedAdapter.AutoUpdate)
	assert.Equal(t, "v", fetchedAdapter.Tags["k"])

	fetchedAV, err := b2.GetAdapterVersion(context.Background(), adapter.AdapterID, av.AdapterVersion)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", fetchedAV.Status)
}

// TestRefinement1_SeedHelpersAdapterInternal tests the AddAdapterInternal seed helper.
func TestRefinement1_SeedHelpersAdapterInternal(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	a := &textract.Adapter{
		AdapterID:    "seeded-id",
		AdapterName:  "seeded-adapter",
		AutoUpdate:   "DISABLED",
		CreationTime: time.Now(),
		FeatureTypes: []string{"TABLES"},
		Tags:         map[string]string{},
	}

	textract.AddAdapterInternal(b, a)

	assert.Equal(t, 1, textract.AdapterCount(b))

	fetched, err := b.GetAdapter(context.Background(), "seeded-id")
	require.NoError(t, err)
	assert.Equal(t, "seeded-adapter", fetched.AdapterName)
}

// TestRefinement1_SeedHelpersExpenseLendingInternal tests the expense/lending
// seed helpers for the happy path of GetExpenseAnalysis / GetLendingAnalysis.
func TestRefinement1_SeedHelpersExpenseLendingInternal(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	expJob := &textract.ExpenseJob{
		JobID:        "expense-seed-job",
		JobStatus:    "SUCCEEDED",
		CreationTime: time.Now(),
		ExpenseDocuments: []textract.ExpenseDocument{
			{ExpenseIndex: 1, Blocks: []textract.Block{{BlockType: "PAGE", ID: "blk-1"}}},
		},
	}
	textract.AddExpenseJobInternal(b, expJob)
	assert.Equal(t, 1, textract.ExpenseJobCount(b))

	lendJob := &textract.LendingJob{
		JobID:        "lending-seed-job",
		JobStatus:    "SUCCEEDED",
		CreationTime: time.Now(),
		Results:      []textract.LendingResult{{Page: 1}},
	}
	textract.AddLendingJobInternal(b, lendJob)
	assert.Equal(t, 1, textract.LendingJobCount(b))

	fetchedExp, err := b.GetExpenseAnalysis(context.Background(), "expense-seed-job")
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", fetchedExp.JobStatus)
	assert.Len(t, fetchedExp.ExpenseDocuments, 1)
	// Deep copy check: blocks inside expense documents must be independent.
	fetchedExp.ExpenseDocuments[0].Blocks[0].BlockType = "MUTATED"

	fetchedExp2, err := b.GetExpenseAnalysis(context.Background(), "expense-seed-job")
	require.NoError(t, err)
	assert.Equal(t, "PAGE", fetchedExp2.ExpenseDocuments[0].Blocks[0].BlockType)
}

// TestRefinement1_CloneTags_NilInput tests that cloneAdapter returns non-nil tags
// even when the input adapter has nil tags.
func TestRefinement1_CloneTags_NilInput(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	// Create adapter without tags.
	adapter, err := b.CreateAdapter(context.Background(), "no-tags", "", "DISABLED", []string{"FORMS"}, nil)
	require.NoError(t, err)
	assert.NotNil(t, adapter.Tags)
}

// TestRefinement1_ErrValidationSentinel verifies ErrValidation wraps
// awserr.ErrInvalidParameter.
func TestRefinement1_ErrValidationSentinel(t *testing.T) {
	t.Parallel()

	assert.ErrorIs(t, textract.ErrValidation, awserr.ErrInvalidParameter)
}

// TestRefinement1_UnknownActionReturnsValidationException tests that an unknown
// action returns a ValidationException.
func TestRefinement1_UnknownActionReturnsValidationException(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)
	rec := doTextractRequest(t, h, "UndefinedOperation", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ValidationException", errResp["__type"])
}

// TestRefinement1_GetExpenseAnalysis_MissingJobId validates required JobId.
func TestRefinement1_GetExpenseAnalysis_MissingJobId(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)
	rec := doTextractRequest(t, h, "GetExpenseAnalysis", map[string]any{"JobId": ""})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_GetLendingAnalysis_MissingJobId validates required JobId.
func TestRefinement1_GetLendingAnalysis_MissingJobId(t *testing.T) {
	t.Parallel()

	h := newTestHandlerR1(t)
	rec := doTextractRequest(t, h, "GetLendingAnalysis", map[string]any{"JobId": ""})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
