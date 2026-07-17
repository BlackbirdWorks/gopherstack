package textract_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/textract"
)

func TestHandler_AdapterVersionLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "full adapter version lifecycle", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// First create an adapter
			createAdapterBody := map[string]any{
				"AdapterName":  "version-test-adapter",
				"FeatureTypes": []string{"QUERIES"},
			}
			createAdapterRec := doTextractRequest(t, h, "CreateAdapter", createAdapterBody)
			require.Equal(t, tt.wantStatus, createAdapterRec.Code)

			var createAdapterResp map[string]string
			require.NoError(t, json.Unmarshal(createAdapterRec.Body.Bytes(), &createAdapterResp))
			adapterID := createAdapterResp["AdapterId"]

			// CreateAdapterVersion
			createVersionBody := map[string]any{
				"AdapterId": adapterID,
				"Tags":      map[string]string{"env": "test"},
			}
			createVersionRec := doTextractRequest(t, h, "CreateAdapterVersion", createVersionBody)
			assert.Equal(t, http.StatusOK, createVersionRec.Code)

			var createVersionResp map[string]string
			require.NoError(t, json.Unmarshal(createVersionRec.Body.Bytes(), &createVersionResp))
			adapterVersion := createVersionResp["AdapterVersion"]
			assert.NotEmpty(t, adapterVersion)
			assert.Equal(t, adapterID, createVersionResp["AdapterId"])

			// GetAdapterVersion
			getVersionBody := map[string]string{
				"AdapterId":      adapterID,
				"AdapterVersion": adapterVersion,
			}
			getVersionRec := doTextractRequest(t, h, "GetAdapterVersion", getVersionBody)
			assert.Equal(t, http.StatusOK, getVersionRec.Code)

			var getVersionResp map[string]any
			require.NoError(t, json.Unmarshal(getVersionRec.Body.Bytes(), &getVersionResp))
			assert.Equal(t, adapterID, getVersionResp["AdapterId"])
			assert.Equal(t, adapterVersion, getVersionResp["AdapterVersion"])
			assert.Equal(t, "ACTIVE", getVersionResp["Status"])

			// DeleteAdapterVersion
			deleteVersionRec := doTextractRequest(t, h, "DeleteAdapterVersion", getVersionBody)
			assert.Equal(t, http.StatusOK, deleteVersionRec.Code)

			// GetAdapterVersion after delete returns error
			getVersionRec2 := doTextractRequest(t, h, "GetAdapterVersion", getVersionBody)
			assert.Equal(t, http.StatusBadRequest, getVersionRec2.Code)
		})
	}
}

func TestHandler_CreateAdapterVersion_MissingAdapterId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{"AdapterId": ""}
	rec := doTextractRequest(t, h, "CreateAdapterVersion", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateAdapterVersion_AdapterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{"AdapterId": "nonexistent-adapter"}
	rec := doTextractRequest(t, h, "CreateAdapterVersion", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetAdapterVersion_MissingFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]string
		name string
	}{
		{
			name: "missing adapter id",
			body: map[string]string{"AdapterId": "", "AdapterVersion": "v1"},
		},
		{
			name: "missing adapter version",
			body: map[string]string{"AdapterId": "adapter-id", "AdapterVersion": ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "GetAdapterVersion", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestHandler_CreateAdapterVersion_DatasetConfig verifies GetAdapterVersion
// includes the DatasetConfig and KMSKeyId supplied at creation.
func TestHandler_CreateAdapterVersion_DatasetConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createAdapterRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "dataset-adapter",
		"FeatureTypes": []string{"FORMS"},
	})
	require.Equal(t, http.StatusOK, createAdapterRec.Code)

	var createAdapterResp map[string]string
	require.NoError(t, json.Unmarshal(createAdapterRec.Body.Bytes(), &createAdapterResp))
	adapterID := createAdapterResp["AdapterId"]

	createVersionRec := doTextractRequest(t, h, "CreateAdapterVersion", map[string]any{
		"AdapterId": adapterID,
		"DatasetConfig": map[string]any{
			"ManifestS3Object": map[string]any{
				"Bucket": "my-dataset-bucket",
				"Name":   "manifest.json",
			},
		},
		"KMSKeyId": "arn:aws:kms:us-east-1:123456789012:key/abcd1234",
	})
	require.Equal(t, http.StatusOK, createVersionRec.Code)

	var createVersionResp map[string]string
	require.NoError(t, json.Unmarshal(createVersionRec.Body.Bytes(), &createVersionResp))
	adapterVersion := createVersionResp["AdapterVersion"]

	getVersionRec := doTextractRequest(t, h, "GetAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": adapterVersion,
	})
	require.Equal(t, http.StatusOK, getVersionRec.Code)

	var getVersionResp map[string]any
	require.NoError(t, json.Unmarshal(getVersionRec.Body.Bytes(), &getVersionResp))

	datasetConfig, ok := getVersionResp["DatasetConfig"].(map[string]any)
	require.True(t, ok, "GetAdapterVersion should include DatasetConfig")
	manifest, ok2 := datasetConfig["ManifestS3Object"].(map[string]any)
	require.True(t, ok2)
	assert.Equal(t, "my-dataset-bucket", manifest["Bucket"])

	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/abcd1234", getVersionResp["KMSKeyId"])
}

// TestHandler_AdapterVersion_InProgressThenActive verifies an adapter version
// starts CREATION_IN_PROGRESS then becomes ACTIVE after the async delay.
func TestHandler_AdapterVersion_InProgressThenActive(t *testing.T) {
	t.Parallel()

	// Use a backend with a short async delay.
	b := textract.NewInMemoryBackendSync("123456789012", "us-east-1")
	textract.SetBackendAsyncDelay(b, 50*time.Millisecond)
	h := textract.NewHandler(b)

	createAdapterRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "lifecycle-adapter",
		"FeatureTypes": []string{"FORMS"},
	})
	require.Equal(t, http.StatusOK, createAdapterRec.Code)

	var createAdapterResp map[string]string
	require.NoError(t, json.Unmarshal(createAdapterRec.Body.Bytes(), &createAdapterResp))
	adapterID := createAdapterResp["AdapterId"]

	createVersionRec := doTextractRequest(t, h, "CreateAdapterVersion", map[string]any{
		"AdapterId": adapterID,
	})
	require.Equal(t, http.StatusOK, createVersionRec.Code)

	var createVersionResp map[string]string
	require.NoError(t, json.Unmarshal(createVersionRec.Body.Bytes(), &createVersionResp))
	adapterVersion := createVersionResp["AdapterVersion"]

	// Immediately check: should be CREATION_IN_PROGRESS.
	getRec1 := doTextractRequest(t, h, "GetAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": adapterVersion,
	})
	require.Equal(t, http.StatusOK, getRec1.Code)

	var getResp1 map[string]any
	require.NoError(t, json.Unmarshal(getRec1.Body.Bytes(), &getResp1))
	assert.Equal(t, "CREATION_IN_PROGRESS", getResp1["Status"])

	// After delay, should be ACTIVE.
	time.Sleep(200 * time.Millisecond)

	getRec2 := doTextractRequest(t, h, "GetAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": adapterVersion,
	})
	require.Equal(t, http.StatusOK, getRec2.Code)

	var getResp2 map[string]any
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getResp2))
	assert.Equal(t, "ACTIVE", getResp2["Status"])
}

// TestHandler_AdapterVersion_EvaluationMetrics verifies GetAdapterVersion
// returns the deterministic EvaluationMetrics.
func TestHandler_AdapterVersion_EvaluationMetrics(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createAdapterRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "metrics-adapter",
		"FeatureTypes": []string{"QUERIES"},
	})
	require.Equal(t, http.StatusOK, createAdapterRec.Code)

	var createAdapterResp map[string]string
	require.NoError(t, json.Unmarshal(createAdapterRec.Body.Bytes(), &createAdapterResp))
	adapterID := createAdapterResp["AdapterId"]

	createVersionRec := doTextractRequest(t, h, "CreateAdapterVersion", map[string]any{
		"AdapterId": adapterID,
	})
	require.Equal(t, http.StatusOK, createVersionRec.Code)

	var createVersionResp map[string]string
	require.NoError(t, json.Unmarshal(createVersionRec.Body.Bytes(), &createVersionResp))
	adapterVersion := createVersionResp["AdapterVersion"]

	getVersionRec := doTextractRequest(t, h, "GetAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": adapterVersion,
	})
	require.Equal(t, http.StatusOK, getVersionRec.Code)

	var getVersionResp map[string]any
	require.NoError(t, json.Unmarshal(getVersionRec.Body.Bytes(), &getVersionResp))

	evalMetrics, ok := getVersionResp["EvaluationMetrics"].(map[string]any)
	require.True(t, ok, "GetAdapterVersion should include EvaluationMetrics")
	assert.InDelta(t, 0.85, evalMetrics["F1Score"], 0.001)
	assert.InDelta(t, 0.88, evalMetrics["Precision"], 0.001)
	assert.InDelta(t, 0.82, evalMetrics["Recall"], 0.001)
}

// TestHandler_DeleteAdapterVersion_GetReturnsErrorAfterDelete verifies that
// after DeleteAdapterVersion, calling GetAdapterVersion on the deleted version
// returns InvalidParameterException (400).
func TestHandler_DeleteAdapterVersion_GetReturnsErrorAfterDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createAdapterRec := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "versioned",
		"FeatureTypes": []string{"FORMS"},
	})
	require.Equal(t, http.StatusOK, createAdapterRec.Code)
	var adapterResp map[string]string
	require.NoError(t, json.Unmarshal(createAdapterRec.Body.Bytes(), &adapterResp))
	adapterID := adapterResp["AdapterId"]

	createVerRec := doTextractRequest(t, h, "CreateAdapterVersion", map[string]any{
		"AdapterId": adapterID,
	})
	require.Equal(t, http.StatusOK, createVerRec.Code)
	var verResp map[string]string
	require.NoError(t, json.Unmarshal(createVerRec.Body.Bytes(), &verResp))
	version := verResp["AdapterVersion"]
	require.NotEmpty(t, version)

	getBeforeRec := doTextractRequest(t, h, "GetAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": version,
	})
	require.Equal(t, http.StatusOK, getBeforeRec.Code, "version must exist before deletion")

	deleteRec := doTextractRequest(t, h, "DeleteAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": version,
	})
	require.Equal(t, http.StatusOK, deleteRec.Code, "delete must succeed")

	getAfterRec := doTextractRequest(t, h, "GetAdapterVersion", map[string]any{
		"AdapterId":      adapterID,
		"AdapterVersion": version,
	})
	assert.Equal(t, http.StatusBadRequest, getAfterRec.Code)
	var errResp map[string]any
	require.NoError(t, json.Unmarshal(getAfterRec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidParameterException", errResp["__type"],
		"deleted version must return InvalidParameterException")
}

// TestHandler_ListAdapterVersions_HappyPath tests listing versions for an adapter.
func TestHandler_ListAdapterVersions_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

// TestHandler_ListAdapterVersions_NotFound returns 400 for unknown adapter.
func TestHandler_ListAdapterVersions_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "ListAdapterVersions", map[string]any{
		"AdapterId": "nonexistent",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_ListAdapterVersions_SummaryShape verifies that
// ListAdapterVersions returns FeatureTypes in each version summary and omits
// Tags: real AWS returns AdapterVersionOverview items with
// FeatureTypes/Status but without Tags.
func TestHandler_ListAdapterVersions_SummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

	fts, hasFT := vSummary["FeatureTypes"].([]any)
	assert.True(t, hasFT, "ListAdapterVersions summary must include FeatureTypes")
	assert.Len(t, fts, 1)

	_, hasTags := vSummary["Tags"]
	assert.False(t, hasTags, "ListAdapterVersions summary must not include Tags")

	status, hasStatus := vSummary["Status"].(string)
	assert.True(t, hasStatus, "ListAdapterVersions summary must include Status")
	assert.NotEmpty(t, status)
}
