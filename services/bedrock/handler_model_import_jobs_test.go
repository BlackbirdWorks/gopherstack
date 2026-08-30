package bedrock_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// validModelImportJobBody returns a CreateModelImportJob request body with
// every real-AWS-required field populated (importedModelName, roleArn,
// modelDataSource) alongside jobName.
func validModelImportJobBody(jobName string) map[string]any {
	return map[string]any{
		"jobName":           jobName,
		"importedModelName": jobName + "-model",
		"roleArn":           "arn:aws:iam::000000000000:role/import-role",
		"modelDataSource": map[string]any{
			"s3DataSource": map[string]any{"s3Uri": "s3://my-bucket/model-data/"},
		},
	}
}

func createTestModelImportJob(
	t *testing.T, b *bedrock.InMemoryBackend, jobName string,
) *bedrock.ModelImportJob {
	t.Helper()

	job, err := b.CreateModelImportJob(
		jobName, jobName+"-model", "arn:aws:iam::000000000000:role/import-role",
		"s3://my-bucket/model-data/", nil,
	)
	require.NoError(t, err)

	return job
}

func TestAccuracy_ModelImportJob_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create.
	rec := doRequest(t, h, http.MethodPost, "/model-import-jobs", validModelImportJobBody("my-import-job"))

	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN := createOut["jobArn"].(string)
	assert.NotEmpty(t, jobARN)
	assert.Equal(t, "InProgress", createOut["status"])
	assert.NotEmpty(t, createOut["creationTime"])
	assert.NotEmpty(t, createOut["lastModifiedTime"])
	assert.NotEmpty(t, createOut["importedModelArn"])
	assert.Equal(t, "my-import-job-model", createOut["importedModelName"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/import-role", createOut["roleArn"])

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

// TestAccuracy_ModelImportJob_ListOmitsGetOnlyFields verifies gopherstack-uult:
// ListModelImportJobs must emit only types.ModelImportJobSummary's members
// (creationTime, jobArn, jobName, status, endTime, importedModelArn,
// importedModelName, lastModifiedTime) -- bedrock@v1.66.4
// types/types.go:5479-5514. roleArn, modelDataSource and tags are
// GetModelImportJob/CreateModelImportJob-only and must not leak.
func TestAccuracy_ModelImportJob_ListOmitsGetOnlyFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := validModelImportJobBody("scoped-import")
	body["jobTags"] = []map[string]any{{"key": "k", "value": "v"}}
	rec := doRequest(t, h, http.MethodPost, "/model-import-jobs", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	recList := doRequest(t, h, http.MethodGet, "/model-import-jobs", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut struct {
		Summaries []map[string]any `json:"modelImportJobSummaries"`
	}
	mustUnmarshal(t, recList, &listOut)
	require.Len(t, listOut.Summaries, 1)

	item := listOut.Summaries[0]
	keys := make([]string, 0, len(item))
	for k := range item {
		keys = append(keys, k)
	}
	assert.ElementsMatch(t,
		[]string{
			"creationTime", "jobArn", "jobName", "status",
			"importedModelArn", "importedModelName", "lastModifiedTime",
		},
		keys,
	)
	assert.NotContains(t, item, "roleArn")
	assert.NotContains(t, item, "modelDataSource")
	assert.NotContains(t, item, "tags")
}

func TestAccuracy_ModelImportJob_MissingJobName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-import-jobs", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestAccuracy_ModelImportJob_MissingRequiredFieldsRejected locks in that
// importedModelName and roleArn are required (real AWS: CreateModelImportJobInput
// marks both "This member is required") -- gopherstack previously accepted a
// bare {"jobName": ...} body and silently dropped both.
func TestAccuracy_ModelImportJob_MissingRequiredFieldsRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing importedModelName", body: map[string]any{"jobName": "j1", "roleArn": "arn:role"}},
		{name: "missing roleArn", body: map[string]any{"jobName": "j2", "importedModelName": "m1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model-import-jobs", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
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

			job := createTestModelImportJob(t, b, tt.jobName)
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
			assert.Equal(t, tt.jobName+"-model", modelOut["modelName"])
			assert.Equal(t, job.JobArn, modelOut["jobArn"])
			assert.Equal(t, tt.jobName, modelOut["jobName"])
			assert.NotEmpty(t, modelOut["creationTime"])
			_, hasStatus := modelOut["status"]
			assert.False(t, hasStatus, "ImportedModel has no real 'status' field")
		})
	}
}

func TestAccuracy_ImportedModel_DeleteRemovesFromList(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	job := createTestModelImportJob(t, b, "del-import-job")

	rec := doRequest(t, h, http.MethodDelete,
		"/imported-models/"+url.PathEscape(job.ImportedModelArn), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

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
	job := createTestModelImportJob(t, b, "no-endtime-job")
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
			job := createTestModelImportJob(t, b, tt.jobName)
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
	rec := doRequest(t, h, http.MethodPost, "/model-import-jobs", validModelImportJobBody("import-status-job"))
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
	body := validModelImportJobBody("tagged-import")
	body["tags"] = []map[string]any{
		{"key": "purpose", "value": "testing"},
	}
	rec := doRequest(t, h, http.MethodPost, "/model-import-jobs", body)
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
		validModelImportJobBody("completed-import-job"))
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

// TestAccuracy_ImportedModel_ListFilterByNameContains locks in the
// nameContains query filter and nextToken pagination that ListImportedModels
// previously ignored entirely.
func TestAccuracy_ImportedModel_ListFilterByNameContains(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)

	createTestModelImportJob(t, b, "alpha-import")
	createTestModelImportJob(t, b, "beta-import")

	rec := doRequest(t, h, http.MethodGet, "/imported-models?nameContains=alpha", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	models := out["modelSummaries"].([]any)
	require.Len(t, models, 1)
	assert.Equal(t, "alpha-import-model", models[0].(map[string]any)["modelName"])
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
	job := createTestModelImportJob(t, b, "test-import")

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
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Get after delete
	rec5 := doRequest(t, h, http.MethodGet, "/imported-models/"+url.PathEscape(modelARN), nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}

// TestParity_ListModelImportJobs_NameContainsFilter locks in the
// nameContains query filter (bedrock@v1.66.4
// api_op_ListModelImportJobs.go's NameContains, wire query key
// "nameContains" per serializers.go:7099-7101) -- ListModelImportJobs
// previously took no arguments at all, so no filter, sort, or maxResults
// query parameter reached the backend regardless of what a real client sent.
func TestParity_ListModelImportJobs_NameContainsFilter(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestBedrockClient(t, bedrock.NewHandler(b))

	_, err := b.CreateModelImportJob(
		"other-job", "other-imported-model", "arn:aws:iam::123456789012:role/import-role", "", nil,
	)
	require.NoError(t, err)

	wantJob, err := b.CreateModelImportJob(
		"target-job", "target-imported-model", "arn:aws:iam::123456789012:role/import-role", "", nil,
	)
	require.NoError(t, err)

	out, err := client.ListModelImportJobs(t.Context(), &bedrocksdk.ListModelImportJobsInput{
		NameContains: aws.String("target"),
	})
	require.NoError(t, err)
	require.Len(t, out.ModelImportJobSummaries, 1)
	assert.Equal(t, wantJob.JobArn, aws.ToString(out.ModelImportJobSummaries[0].JobArn))
}
