package sesv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestCancelExportJob tests the CancelExportJob operation.
func TestCancelExportJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*sesv2.InMemoryBackend)
		jobID   string
		wantErr bool
	}{
		{
			name: "cancel_existing_job",
			setup: func(b *sesv2.InMemoryBackend) {
				b.AddExportJobInternal("job-1", "IN_PROGRESS")
			},
			jobID: "job-1",
		},
		{
			name:    "cancel_nonexistent_job",
			setup:   func(*sesv2.InMemoryBackend) {},
			jobID:   "no-such-job",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			err := backend.CancelExportJob(tt.jobID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestExportJobCount verifies ExportJobCount helper.
func TestExportJobCount(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	assert.Equal(t, 0, sesv2.ExportJobCount(backend))

	backend.AddExportJobInternal("job-1", "CREATED")
	backend.AddExportJobInternal("job-2", "IN_PROGRESS")
	assert.Equal(t, 2, sesv2.ExportJobCount(backend))
}

// TestCancelExportJobHTTP tests CancelExportJob via HTTP.
func TestCancelExportJobHTTP(t *testing.T) {
	t.Parallel()

	h, backend := newSESv2TestHandler(t)
	backend.AddExportJobInternal("job-123", "IN_PROGRESS")

	rec := doReq(t, h, http.MethodPut, "/v2/email/export-jobs/job-123/cancel", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCancelExportJobHTTPNotFound tests CancelExportJob 404.
func TestCancelExportJobHTTPNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	rec := doReq(t, h, http.MethodPut, "/v2/email/export-jobs/no-such/cancel", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestCreateExportJob tests the CreateExportJob operation.
func TestCreateExportJob(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodPost, "/v2/email/export-jobs", map[string]any{
		"ExportDataSource": map[string]any{
			"MetricsDataSource": map[string]any{
				"Namespace": "VDM",
				"Metrics":   []map[string]any{{"Name": "SEND", "Aggregation": "VOLUME"}},
			},
		},
		"ExportDestination": map[string]any{
			"DataFormat": "CSV",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetExportJob tests the GetExportJob operation.
func TestGetExportJob(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := doRequest(t, h, http.MethodPost, "/v2/email/export-jobs", map[string]any{
		"ExportDataSource": map[string]any{
			"MetricsDataSource": map[string]any{
				"Namespace": "VDM",
				"Metrics":   []map[string]any{{"Name": "SEND", "Aggregation": "VOLUME"}},
			},
		},
		"ExportDestination": map[string]any{
			"DataFormat": "CSV",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	jobID := createResp["JobId"].(string)

	rec := doRequest(t, h, http.MethodGet, "/v2/email/export-jobs/"+jobID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListExportJobs tests the ListExportJobs operation.
func TestListExportJobs(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/export-jobs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCancelExportJobAfterCreate tests CancelExportJob on a job created through the same
// HTTP round-trip (as opposed to TestCancelExportJob above, which seeds the job directly
// via AddExportJobInternal).
func TestCancelExportJobAfterCreate(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := doRequest(t, h, http.MethodPost, "/v2/email/export-jobs", map[string]any{
		"ExportDataSource": map[string]any{
			"MetricsDataSource": map[string]any{
				"Namespace": "VDM",
				"Metrics":   []map[string]any{{"Name": "SEND", "Aggregation": "VOLUME"}},
			},
		},
		"ExportDestination": map[string]any{
			"DataFormat": "CSV",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	jobID := createResp["JobId"].(string)

	rec := doRequest(t, h, http.MethodPut, "/v2/email/export-jobs/"+jobID+"/cancel", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}
