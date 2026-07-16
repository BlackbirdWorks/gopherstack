package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserImportJob_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "import-job-pool")

	// Create
	rec := doCognitoRequest(t, h, "CreateUserImportJob", map[string]any{
		"UserPoolId": poolID,
		"JobName":    "my-import",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		UserImportJob struct {
			JobID   string `json:"JobId,omitempty"`
			JobName string `json:"JobName,omitempty"`
			Status  string `json:"Status,omitempty"`
		} `json:"UserImportJob"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	jobID := createResp.UserImportJob.JobID
	require.NotEmpty(t, jobID)
	assert.Equal(t, "my-import", createResp.UserImportJob.JobName)
	assert.Equal(t, "Created", createResp.UserImportJob.Status)

	// Describe
	rec = doCognitoRequest(t, h, "DescribeUserImportJob", map[string]any{
		"UserPoolId": poolID,
		"JobId":      jobID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doCognitoRequest(t, h, "ListUserImportJobs", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var listResp struct {
		UserImportJobs []struct {
			JobID string `json:"JobId,omitempty"`
		} `json:"UserImportJobs"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.UserImportJobs, 1)

	// GetCSVHeader
	rec = doCognitoRequest(t, h, "GetCSVHeader", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, rec.Code)

	// Start
	rec = doCognitoRequest(t, h, "StartUserImportJob", map[string]any{
		"UserPoolId": poolID,
		"JobId":      jobID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var startResp struct {
		UserImportJob struct {
			Status string `json:"Status,omitempty"`
		} `json:"UserImportJob"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	assert.Equal(t, "InProgress", startResp.UserImportJob.Status)

	// Stop
	rec = doCognitoRequest(t, h, "StopUserImportJob", map[string]any{
		"UserPoolId": poolID,
		"JobId":      jobID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var stopResp struct {
		UserImportJob struct {
			Status string `json:"Status,omitempty"`
		} `json:"UserImportJob"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stopResp))
	assert.Equal(t, "Stopped", stopResp.UserImportJob.Status)

	// Start again on stopped job — error
	rec = doCognitoRequest(t, h, "StartUserImportJob", map[string]any{
		"UserPoolId": poolID,
		"JobId":      jobID,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
