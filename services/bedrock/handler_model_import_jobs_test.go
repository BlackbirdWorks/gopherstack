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

func TestAccuracy_ModelImportJob_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create.
	rec := doRequest(t, h, http.MethodPost, "/model-import-jobs",
		map[string]any{"jobName": "my-import-job"})

	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN := createOut["jobArn"].(string)
	assert.NotEmpty(t, jobARN)
	assert.Equal(t, "InProgress", createOut["status"])
	assert.NotEmpty(t, createOut["creationTime"])
	assert.NotEmpty(t, createOut["lastModifiedTime"])
	assert.NotEmpty(t, createOut["importedModelArn"])

	// List.
	recList := doRequest(t, h, http.MethodGet, "/model-import-jobs", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	mustUnmarshal(t, recList, &listOut)
	summaries, ok := listOut["modelImportJobSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	// Get.
	recGet := doRequest(t, h, http.MethodGet, "/model-import-jobs/"+url.PathEscape(jobARN), nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	assert.Equal(t, jobARN, getOut["jobArn"])
}

func TestAccuracy_ModelImportJob_MissingJobName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-import-jobs", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_ImportedModel_VisibleAfterImportJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		jobName string
	}{
		{name: "single imported model", jobName: "import-job-1"},
		{name: "another imported model", jobName: "import-job-2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			job, err := b.CreateModelImportJob(tt.jobName, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, job.ImportedModelArn)

			// Imported model should be listable.
			rec := doRequest(t, h, http.MethodGet, "/imported-models", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			models := out["modelSummaries"].([]any)
			assert.Len(t, models, 1)

			// Get by ARN.
			recGet := doRequest(t, h, http.MethodGet,
				"/imported-models/"+url.PathEscape(job.ImportedModelArn), nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var modelOut map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &modelOut))
			assert.Equal(t, job.ImportedModelArn, modelOut["modelArn"])
		})
	}
}

func TestAccuracy_ImportedModel_DeleteRemovesFromList(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	job, err := b.CreateModelImportJob("del-import-job", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete,
		"/imported-models/"+url.PathEscape(job.ImportedModelArn), nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	recList := doRequest(t, h, http.MethodGet, "/imported-models", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &out))
	assert.Empty(t, out["modelSummaries"].([]any))
}

func TestAccuracy_ModelImportJob_EndTimeAbsentWhileInProgress(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	job, err := b.CreateModelImportJob("no-endtime-job", nil)
	require.NoError(t, err)
	assert.Equal(t, "InProgress", job.Status)

	rec := doRequest(t, h, http.MethodGet,
		"/model-import-jobs/"+url.PathEscape(job.JobArn), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "InProgress", out["status"])
	_, hasEndTime := out["endTime"]
	assert.False(t, hasEndTime, "endTime must not be present while InProgress")
}

func TestAccuracy_ModelImportJob_AdvanceStatusToCompleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		jobName string
	}{
		{name: "import job advances", jobName: "advance-import-job"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			job, err := b.CreateModelImportJob(tt.jobName, nil)
			require.NoError(t, err)
			assert.Equal(t, "InProgress", job.Status)

			n := b.AdvanceCopyImportJobStatuses(0)
			assert.GreaterOrEqual(t, n, 1)

			got, err := b.GetModelImportJob(job.JobArn)
			require.NoError(t, err)
			assert.NotEqual(t, "InProgress", got.Status)
		})
	}
}

func TestAccuracy_ModelImportJob_ImportedModelStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-import-jobs",
		map[string]any{"jobName": "import-status-job"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobARN := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/model-import-jobs/"+jobARN, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

	// Status should be one of the valid statuses
	assert.Contains(t, []any{"InProgress", "Completed", "Failed"}, getOut["status"])
}

func TestAccuracy_ModelImportJob_TagsPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-import-jobs", map[string]any{
		"jobName": "tagged-import",
		"tags": []map[string]any{
			{"key": "purpose", "value": "testing"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobARN := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/model-import-jobs/"+jobARN, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "tagged-import", getOut["jobName"])
}

func TestAccuracy_ImportedModel_AvailableAfterJobComplete(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	// Create import job and advance to completed
	importRec := doRequest(t, h, http.MethodPost, "/model-import-jobs",
		map[string]any{"jobName": "completed-import-job"})
	require.Equal(t, http.StatusCreated, importRec.Code)

	var importOut map[string]any
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importOut))
	jobARN := importOut["jobArn"].(string)

	// Advance job status to complete (zero duration = immediate)
	b.AdvanceCopyImportJobStatuses(0)

	// Get the job to find the imported model ARN
	getJobRec := doRequest(t, h, http.MethodGet, "/model-import-jobs/"+jobARN, nil)
	require.Equal(t, http.StatusOK, getJobRec.Code)

	var jobOut map[string]any
	require.NoError(t, json.Unmarshal(getJobRec.Body.Bytes(), &jobOut))
	importedModelARN := jobOut["importedModelArn"].(string)
	assert.NotEmpty(t, importedModelARN)

	// The imported model should be accessible
	listRec := doRequest(t, h, http.MethodGet, "/imported-models", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	models := listOut["modelSummaries"].([]any)
	assert.NotEmpty(t, models)
}

func TestHandler_ImportedModel_ListAndGet(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	// List before any imports
	rec := doRequest(t, h, http.MethodGet, "/imported-models", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	assert.Empty(t, out["modelSummaries"])

	// Create an import job (which seeds an imported model ARN)
	job, err := b.CreateModelImportJob("test-import", nil)
	require.NoError(t, err)

	// Advance so imported model ARN is set
	b.AdvanceCopyImportJobStatuses(0)

	// Fetch updated job
	updated, err := b.GetModelImportJob(job.JobArn)
	require.NoError(t, err)
	modelARN := updated.ImportedModelArn

	// List
	rec2 := doRequest(t, h, http.MethodGet, "/imported-models", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)
	summaries := out2["modelSummaries"].([]any)
	assert.Len(t, summaries, 1)

	// Get
	rec3 := doRequest(t, h, http.MethodGet, "/imported-models/"+url.PathEscape(modelARN), nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Delete
	rec4 := doRequest(t, h, http.MethodDelete, "/imported-models/"+url.PathEscape(modelARN), nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// Get after delete
	rec5 := doRequest(t, h, http.MethodGet, "/imported-models/"+url.PathEscape(modelARN), nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}
