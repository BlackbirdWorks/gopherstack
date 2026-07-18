package sesv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateImportJob tests the CreateImportJob operation.
func TestCreateImportJob(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPost, "/v2/email/import-jobs", map[string]any{
		"ImportDataSource": map[string]any{
			"S3Url":      "s3://bucket/key.csv",
			"DataFormat": "CSV",
		},
		"ImportDestination": map[string]any{
			"SuppressionListDestination": map[string]any{
				"SuppressionListImportAction": "PUT",
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetImportJob tests the GetImportJob operation.
func TestGetImportJob(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := doRequest(t, h, http.MethodPost, "/v2/email/import-jobs", map[string]any{
		"ImportDataSource": map[string]any{
			"S3Url":      "s3://bucket/key.csv",
			"DataFormat": "CSV",
		},
		"ImportDestination": map[string]any{
			"SuppressionListDestination": map[string]any{
				"SuppressionListImportAction": "PUT",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	jobID := createResp["JobId"].(string)

	rec := doRequest(t, h, http.MethodGet, "/v2/email/import-jobs/"+jobID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListImportJobs tests the ListImportJobs operation.
func TestListImportJobs(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/import-jobs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}
