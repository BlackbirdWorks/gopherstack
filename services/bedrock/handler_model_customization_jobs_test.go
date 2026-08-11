package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccuracy_CreateModelCustomizationJob_MissingJobName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-customization-jobs",
		map[string]any{"baseModelIdentifier": "amazon.titan-text-express-v1"})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_CreateModelCustomizationJob_StatusInProgress(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-customization-jobs",
		map[string]any{
			"jobName":             "my-finetune-job",
			"customModelName":     "my-finetune-model",
			"baseModelIdentifier": "amazon.titan-text-express-v1",
		})

	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	jobARN := out["jobArn"].(string)

	recGet := doRequest(
		t,
		h,
		http.MethodGet,
		"/model-customization-jobs/"+url.PathEscape(jobARN),
		nil,
	)
	require.Equal(t, http.StatusOK, recGet.Code)

	var jobOut map[string]any
	mustUnmarshal(t, recGet, &jobOut)
	assert.Equal(t, "InProgress", jobOut["status"])
	assert.NotEmpty(t, jobOut["creationTime"])
	assert.NotEmpty(t, jobOut["lastModifiedTime"])
}

func TestAccuracy_AdvanceCustomizationJobStatus(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	job, err := b.CreateModelCustomizationJob(
		"advance-test-job",
		"advance-test-model",
		"amazon.titan-text-express-v1",
		"",
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "InProgress", job.Status)

	n := b.AdvanceCustomizationJobStatuses(0)
	assert.Equal(t, 1, n)

	advanced, err := b.GetModelCustomizationJob(job.JobArn)
	require.NoError(t, err)
	assert.Equal(t, "Completed", advanced.Status)
}

func TestAccuracy_CustomizationJob_StopTransitionsStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "stop running job"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			job, err := b.CreateModelCustomizationJob(
				"stop-job",
				"stop-job-model",
				"amazon.titan-text-express-v1",
				"",
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, "InProgress", job.Status)

			rec := doRequest(t, h, http.MethodPost,
				"/model-customization-jobs/"+url.PathEscape(job.JobArn)+"/stop", nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			recGet := doRequest(t, h, http.MethodGet,
				"/model-customization-jobs/"+url.PathEscape(job.JobArn), nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			assert.Equal(t, "Stopped", out["status"])
		})
	}
}

func TestAccuracy_CustomizationJob_ListViaHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jobNames []string
		wantLen  int
	}{
		{name: "empty list", jobNames: nil, wantLen: 0},
		{name: "single job", jobNames: []string{"job-a"}, wantLen: 1},
		{name: "multiple jobs", jobNames: []string{"job-x", "job-y", "job-z"}, wantLen: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			for _, name := range tt.jobNames {
				_, err := b.CreateModelCustomizationJob(
					name,
					name+"-model",
					"amazon.titan-text-express-v1",
					"",
					nil,
				)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodGet, "/model-customization-jobs", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			summaries := out["modelCustomizationJobSummaries"].([]any)
			assert.Len(t, summaries, tt.wantLen)

			for _, raw := range summaries {
				s := raw.(map[string]any)
				assert.NotEmpty(t, s["jobArn"])
				assert.NotEmpty(t, s["jobName"])
				assert.Equal(t, "InProgress", s["status"])
				assert.NotEmpty(t, s["creationTime"])
				assert.NotEmpty(t, s["lastModifiedTime"])
			}
		})
	}
}

// TestAccuracy_CustomizationJob_ListFilters locks in that ListModelCustomizationJobs'
// real query params (nameContains, statusEquals) are parsed and applied, not
// silently ignored (aws-sdk-go-v2 serializers.go:6989-7027).
func TestAccuracy_CustomizationJob_ListFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantNames []string
	}{
		{
			name:      "namecontains matches one",
			query:     "?nameContains=alpha",
			wantNames: []string{"alpha-job"},
		},
		{name: "namecontains matches none", query: "?nameContains=zzz", wantNames: nil},
		{
			name:      "statusequals inprogress matches unstopped",
			query:     "?statusEquals=InProgress",
			wantNames: []string{"alpha-job"},
		},
		{
			name:      "statusequals stopped matches stopped job",
			query:     "?statusEquals=Stopped",
			wantNames: []string{"beta-job"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			_, err := b.CreateModelCustomizationJob(
				"alpha-job",
				"alpha-job-model",
				"amazon.titan-text-express-v1",
				"",
				nil,
			)
			require.NoError(t, err)
			betaJob, err := b.CreateModelCustomizationJob(
				"beta-job",
				"beta-job-model",
				"amazon.titan-text-express-v1",
				"",
				nil,
			)
			require.NoError(t, err)
			require.NoError(t, b.StopModelCustomizationJob(betaJob.JobArn))

			rec := doRequest(t, h, http.MethodGet, "/model-customization-jobs"+tt.query, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			summaries := out["modelCustomizationJobSummaries"].([]any)

			gotNames := make([]string, 0, len(summaries))
			for _, raw := range summaries {
				gotNames = append(gotNames, raw.(map[string]any)["jobName"].(string))
			}

			assert.ElementsMatch(t, tt.wantNames, gotNames)
		})
	}
}

func TestAccuracy_CustomizationJob_DuplicateNameConflict(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateModelCustomizationJob("dup-job", "dup-job-model", "amazon.titan-text-express-v1", "", nil)
	require.NoError(t, err)

	_, err2 := b.CreateModelCustomizationJob("dup-job", "dup-job-model-2", "amazon.titan-text-express-v1", "", nil)
	require.Error(t, err2)
}

func TestAccuracy_CustomizationJob_DuplicateModelNameConflict(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateModelCustomizationJob("job-one", "shared-model-name", "amazon.titan-text-express-v1", "", nil)
	require.NoError(t, err)

	_, err2 := b.CreateModelCustomizationJob("job-two", "shared-model-name", "amazon.titan-text-express-v1", "", nil)
	require.Error(t, err2)
}

func TestAccuracy_CustomizationJob_GetByNameOrARN(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	job, err := b.CreateModelCustomizationJob("lookup-job", "lookup-job-model", "amazon.titan-text-express-v1", "", nil)
	require.NoError(t, err)

	// Get by ARN.
	byARN, err := b.GetModelCustomizationJob(job.JobArn)
	require.NoError(t, err)
	assert.Equal(t, job.JobArn, byARN.JobArn)

	// Get by name.
	byName, err := b.GetModelCustomizationJob("lookup-job")
	require.NoError(t, err)
	assert.Equal(t, job.JobArn, byName.JobArn)
}

func TestAccuracy_CustomizationJob_AdvanceCompletesJob(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	job, err := b.CreateModelCustomizationJob(
		"advance-job",
		"advance-job-model",
		"amazon.titan-text-express-v1",
		"",
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "InProgress", job.Status)

	n := b.AdvanceCustomizationJobStatuses(0)
	assert.Equal(t, 1, n)

	got, err := b.GetModelCustomizationJob(job.JobArn)
	require.NoError(t, err)
	assert.Equal(t, "Completed", got.Status)
}

func TestAccuracy_CustomizationJob_CustomizationTypePreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		customizationType string
	}{
		{name: "fine tuning", customizationType: "FINE_TUNING"},
		{name: "continued pretraining", customizationType: "CONTINUED_PRE_TRAINING"},
		{name: "distillation", customizationType: "DISTILLATION"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			rec := doRequest(
				t, h, http.MethodPost, "/model-customization-jobs",
				map[string]any{
					"jobName":             fmt.Sprintf("job-%s", tt.name),
					"customModelName":     fmt.Sprintf("model-%s", tt.name),
					"baseModelIdentifier": "amazon.titan-text-express-v1",
					"customizationType":   tt.customizationType,
				},
			)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createOut map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
			jobARN := createOut["jobArn"].(string)

			recGet := doRequest(
				t,
				h,
				http.MethodGet,
				"/model-customization-jobs/"+url.PathEscape(jobARN),
				nil,
			)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			assert.Equal(t, tt.customizationType, out["customizationType"])
		})
	}
}

func TestHandler_ModelCustomizationJobLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create customization job.
	rec := doRequest(t, h, http.MethodPost, "/model-customization-jobs", map[string]any{
		"jobName":             "my-customization-job",
		"baseModelIdentifier": "amazon.titan-text-express-v1",
		"customModelName":     "my-fine-tuned-model",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	jobARN := createOut["jobArn"].(string)
	assert.NotEmpty(t, jobARN)

	// List jobs.
	rec2 := doRequest(t, h, http.MethodGet, "/model-customization-jobs", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	assert.NotEmpty(t, listOut["modelCustomizationJobSummaries"])

	// Get job.
	rec3 := doRequest(
		t,
		h,
		http.MethodGet,
		"/model-customization-jobs/"+url.PathEscape(jobARN),
		nil,
	)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Stop job.
	rec4 := doRequest(
		t,
		h,
		http.MethodPost,
		"/model-customization-jobs/"+url.PathEscape(jobARN)+"/stop",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec4.Code)
}
