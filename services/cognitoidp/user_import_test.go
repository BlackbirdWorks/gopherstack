package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
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

// TestUserImportJob_SelfCompletesToSucceeded is a regression test for
// gopherstack-muzq: StartUserImportJob correctly stamps InProgress, but until
// now nothing ever advanced a job left alone by the client -- StopUserImportJob
// reaches Stopped, but that path exists only if the client explicitly calls
// Stop. A real import job finishes on its own once its CSV is processed;
// TestUserImportJob_CRUD only ever asserted "InProgress" right after Start
// and moved on to Stop, so it could never have caught a machine that never
// advances on its own. This confirms the janitor's AdvanceUserImportJobStatuses
// reaches the terminal Succeeded status without any client calling Stop.
func TestUserImportJob_SelfCompletesToSucceeded(t *testing.T) {
	t.Parallel()

	backend := newTestBackend()
	h := cognitoidp.NewHandler(backend, "us-east-1")
	poolID, _ := setupHandlerPoolAndClient(t, h, "import-job-self-complete-pool")

	rec := doCognitoRequest(t, h, "CreateUserImportJob", map[string]any{
		"UserPoolId": poolID,
		"JobName":    "self-completing-import",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		UserImportJob struct {
			JobID string `json:"JobId,omitempty"`
		} `json:"UserImportJob"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	jobID := createResp.UserImportJob.JobID
	require.NotEmpty(t, jobID)

	rec = doCognitoRequest(t, h, "StartUserImportJob", map[string]any{
		"UserPoolId": poolID,
		"JobId":      jobID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	janitor := cognitoidp.NewJanitor(backend, time.Minute)

	require.Eventually(t, func() bool {
		janitor.SweepOnce(t.Context())

		descRec := doCognitoRequest(t, h, "DescribeUserImportJob", map[string]any{
			"UserPoolId": poolID,
			"JobId":      jobID,
		})
		if descRec.Code != http.StatusOK {
			return false
		}

		var desc struct {
			UserImportJob struct {
				Status string `json:"Status,omitempty"`
			} `json:"UserImportJob"`
		}
		require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &desc))

		return desc.UserImportJob.Status == "Succeeded"
	}, 2*time.Second, 10*time.Millisecond,
		"InProgress import job must self-complete to Succeeded without a client calling Stop")
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

// TestListUserImportJobs_Pagination proves the op pages through every import
// job exactly once instead of returning them all on a single page with no
// cursor.
func TestListUserImportJobs_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "import-pagination-pool")

	names := []string{"job-a", "job-b", "job-c"}
	for _, n := range names {
		rec := doCognitoRequest(t, h, "CreateUserImportJob", map[string]any{
			"UserPoolId": poolID,
			"JobName":    n,
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body)
	}

	type listOut struct {
		PaginationToken string           `json:"PaginationToken,omitempty"`
		UserImportJobs  []map[string]any `json:"UserImportJobs"`
	}

	rec1 := doCognitoRequest(t, h, "ListUserImportJobs", map[string]any{
		"UserPoolId": poolID,
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec1.Code, "body: %s", rec1.Body)

	var page1 listOut
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &page1))
	require.Len(t, page1.UserImportJobs, 2)
	require.NotEmpty(t, page1.PaginationToken, "first page must return a cursor when more import jobs remain")

	rec2 := doCognitoRequest(t, h, "ListUserImportJobs", map[string]any{
		"UserPoolId":      poolID,
		"MaxResults":      2,
		"PaginationToken": page1.PaginationToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code, "body: %s", rec2.Body)

	var page2 listOut
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))
	require.Len(t, page2.UserImportJobs, 1)
	require.Empty(t, page2.PaginationToken)

	seen := map[string]bool{}
	for _, j := range page1.UserImportJobs {
		seen[j["JobId"].(string)] = true
	}

	for _, j := range page2.UserImportJobs {
		id := j["JobId"].(string)
		require.False(t, seen[id], "job %s returned on both pages", id)
		seen[id] = true
	}

	require.Len(t, seen, len(names))
}
