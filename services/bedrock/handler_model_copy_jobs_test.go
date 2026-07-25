package bedrock_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccuracy_ModelCopyJob_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create.
	rec := doRequest(t, h, http.MethodPost, "/model-copy-jobs",
		map[string]any{"sourceModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1"})

	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN := createOut["jobArn"].(string)
	assert.NotEmpty(t, jobARN)
	assert.Equal(t, "InProgress", createOut["status"])
	assert.NotEmpty(t, createOut["creationTime"])
	assert.NotEmpty(t, createOut["lastModifiedTime"])

	// List.
	recList := doRequest(t, h, http.MethodGet, "/model-copy-jobs", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	mustUnmarshal(t, recList, &listOut)
	summaries, ok := listOut["modelCopyJobSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	// Get.
	recGet := doRequest(t, h, http.MethodGet, "/model-copy-jobs/"+url.PathEscape(jobARN), nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	assert.Equal(t, jobARN, getOut["jobArn"])
	assert.Equal(t, "InProgress", getOut["status"])
}

func TestAccuracy_ModelCopyJob_MissingSourceModelArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-copy-jobs", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_AdvanceCopyImportJobStatuses(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")

	copyJob, err := b.CreateModelCopyJob(
		"arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1", nil,
	)
	require.NoError(t, err)

	importJob, err := b.CreateModelImportJob(
		"test-import", "test-import-model", "arn:aws:iam::000000000000:role/import-role", "", nil,
	)
	require.NoError(t, err)

	assert.Equal(t, "InProgress", copyJob.Status)
	assert.Equal(t, "InProgress", importJob.Status)

	n := b.AdvanceCopyImportJobStatuses(0)
	assert.Equal(t, 2, n)

	advCopy, err := b.GetModelCopyJob(copyJob.JobArn)
	require.NoError(t, err)
	assert.Equal(t, "Completed", advCopy.Status)

	advImport, err := b.GetModelImportJob(importJob.JobArn)
	require.NoError(t, err)
	assert.NotEqual(t, "InProgress", advImport.Status)
}

func TestAccuracy_ModelCopyJob_AdvanceStatusToCompleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sourceARN string
	}{
		{
			name:      "copy titan model",
			sourceARN: "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			job, err := b.CreateModelCopyJob(tt.sourceARN, nil)
			require.NoError(t, err)
			assert.Equal(t, "InProgress", job.Status)

			n := b.AdvanceCopyImportJobStatuses(0)
			assert.GreaterOrEqual(t, n, 1)

			got, err := b.GetModelCopyJob(job.JobArn)
			require.NoError(t, err)
			assert.Equal(t, "Completed", got.Status)
		})
	}
}

// TestParity_ValidModelCopyJob_Returns201 confirms that a well-formed
// CreateModelCopyJob request succeeds — distinguishing it from invalid requests.
func TestParity_ValidModelCopyJob_Returns201(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-copy-jobs", map[string]any{
		"sourceModelArn": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2",
	})

	assert.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "jobArn", "successful response must include jobArn")
}
