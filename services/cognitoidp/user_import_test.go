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

type userImportJobWire struct {
	JobID                    string  `json:"JobId,omitempty"`
	JobName                  string  `json:"JobName,omitempty"`
	Status                   string  `json:"Status,omitempty"`
	UserPoolID               string  `json:"UserPoolId,omitempty"`
	CloudWatchLogsRoleArn    string  `json:"CloudWatchLogsRoleArn,omitempty"`
	PasswordHashingAlgorithm string  `json:"PasswordHashingAlgorithm,omitempty"`
	PreSignedURL             string  `json:"PreSignedUrl,omitempty"`
	CreationDate             float64 `json:"CreationDate,omitempty"`
	StartDate                float64 `json:"StartDate,omitempty"`
	CompletionDate           float64 `json:"CompletionDate,omitempty"`
	FailedUsers              int64   `json:"FailedUsers"`
	ImportedUsers            int64   `json:"ImportedUsers"`
	SkippedUsers             int64   `json:"SkippedUsers"`
}

func TestUserImportJob_EchoesCloudWatchLogsRoleArnAndPasswordHashingAlgorithm(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "import-job-fields-pool")

	rec := doCognitoRequest(t, h, "CreateUserImportJob", map[string]any{
		"UserPoolId":               poolID,
		"JobName":                  "my-import",
		"CloudWatchLogsRoleArn":    "arn:aws:iam::000000000000:role/CognitoImportRole",
		"PasswordHashingAlgorithm": "SCRYPT",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var createResp struct {
		UserImportJob userImportJobWire `json:"UserImportJob"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	assert.Equal(t,
		"arn:aws:iam::000000000000:role/CognitoImportRole", createResp.UserImportJob.CloudWatchLogsRoleArn,
		"CloudWatchLogsRoleArn is a required Create input and must be echoed back",
	)
	assert.Equal(t, "SCRYPT", createResp.UserImportJob.PasswordHashingAlgorithm)
	assert.NotEmpty(t, createResp.UserImportJob.PreSignedURL)
	assert.NotZero(t, createResp.UserImportJob.CreationDate)
	assert.Zero(t, createResp.UserImportJob.StartDate, "a just-created job has not started yet")
	assert.Zero(t, createResp.UserImportJob.CompletionDate, "a just-created job has not completed yet")

	jobID := createResp.UserImportJob.JobID

	startRec := doCognitoRequest(t, h, "StartUserImportJob", map[string]any{"UserPoolId": poolID, "JobId": jobID})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp struct {
		UserImportJob userImportJobWire `json:"UserImportJob"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	assert.NotZero(t, startResp.UserImportJob.StartDate)
	assert.Zero(t, startResp.UserImportJob.CompletionDate)

	stopRec := doCognitoRequest(t, h, "StopUserImportJob", map[string]any{"UserPoolId": poolID, "JobId": jobID})
	require.Equal(t, http.StatusOK, stopRec.Code)

	var stopResp struct {
		UserImportJob userImportJobWire `json:"UserImportJob"`
	}
	require.NoError(t, json.Unmarshal(stopRec.Body.Bytes(), &stopResp))
	assert.NotZero(t, stopResp.UserImportJob.CompletionDate)
}
