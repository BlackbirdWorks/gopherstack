package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

func TestAccuracy_CreateModelCustomizationJob_MissingJobName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-customization-jobs",
		map[string]any{"baseModelIdentifier": "amazon.titan-text-express-v1"})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestAccuracy_CreateModelCustomizationJob_RequiredMembersRejected proves
// RoleArn, OutputDataConfig and TrainingDataConfig are enforced as required
// members (api_op_CreateModelCustomizationJob.go:66,75,80), including
// OutputDataConfig's own required S3Uri leaf (types.go:5781). Against
// unfixed code (which reads none of these members) every case here gets
// 201, not 400.
func TestAccuracy_CreateModelCustomizationJob_RequiredMembersRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body func() map[string]any
		name string
	}{
		{
			name: "missing rolearn",
			body: func() map[string]any {
				return map[string]any{
					"jobName":             "job-no-role",
					"customModelName":     "model-no-role",
					"baseModelIdentifier": "amazon.titan-text-express-v1",
					"outputDataConfig":    map[string]any{"s3Uri": "s3://bucket/out/"},
					"trainingDataConfig":  map[string]any{"s3Uri": "s3://bucket/train/"},
				}
			},
		},
		{
			name: "missing outputdataconfig",
			body: func() map[string]any {
				return map[string]any{
					"jobName":             "job-no-output",
					"customModelName":     "model-no-output",
					"baseModelIdentifier": "amazon.titan-text-express-v1",
					"roleArn":             testCustomizationRoleArn,
					"trainingDataConfig":  map[string]any{"s3Uri": "s3://bucket/train/"},
				}
			},
		},
		{
			name: "outputdataconfig missing s3uri",
			body: func() map[string]any {
				return map[string]any{
					"jobName":             "job-empty-output",
					"customModelName":     "model-empty-output",
					"baseModelIdentifier": "amazon.titan-text-express-v1",
					"roleArn":             testCustomizationRoleArn,
					"outputDataConfig":    map[string]any{},
					"trainingDataConfig":  map[string]any{"s3Uri": "s3://bucket/train/"},
				}
			},
		},
		{
			name: "missing trainingdataconfig",
			body: func() map[string]any {
				return map[string]any{
					"jobName":             "job-no-training",
					"customModelName":     "model-no-training",
					"baseModelIdentifier": "amazon.titan-text-express-v1",
					"roleArn":             testCustomizationRoleArn,
					"outputDataConfig":    map[string]any{"s3Uri": "s3://bucket/out/"},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model-customization-jobs", tt.body())
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestAccuracy_CreateModelCustomizationJob_RequiredMembersRoundTrip proves
// RoleArn, OutputDataConfig.S3Uri and TrainingDataConfig.S3Uri survive from
// Create through Get, not just that Create returns 201 (a field parsed and
// discarded looks identical to one that works if only the status is
// checked).
func TestAccuracy_CreateModelCustomizationJob_RequiredMembersRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-customization-jobs", map[string]any{
		"jobName":             "roundtrip-job",
		"customModelName":     "roundtrip-model",
		"baseModelIdentifier": "amazon.titan-text-express-v1",
		"roleArn":             "arn:aws:iam::000000000000:role/customize-role",
		"outputDataConfig":    map[string]any{"s3Uri": "s3://roundtrip-bucket/output/"},
		"trainingDataConfig":  map[string]any{"s3Uri": "s3://roundtrip-bucket/training/"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN, ok := createOut["jobArn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, jobARN)

	recGet := doRequest(t, h, http.MethodGet, "/model-customization-jobs/"+url.PathEscape(jobARN), nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var out map[string]any
	mustUnmarshal(t, recGet, &out)

	assert.Equal(t, "arn:aws:iam::000000000000:role/customize-role", out["roleArn"])

	outputDataConfig, ok := out["outputDataConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "s3://roundtrip-bucket/output/", outputDataConfig["s3Uri"])

	trainingDataConfig, ok := out["trainingDataConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "s3://roundtrip-bucket/training/", trainingDataConfig["s3Uri"])
}

func TestAccuracy_CreateModelCustomizationJob_StatusInProgress(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model-customization-jobs",
		withCustomizationJobRequiredFields(map[string]any{
			"jobName":             "my-finetune-job",
			"customModelName":     "my-finetune-model",
			"baseModelIdentifier": "amazon.titan-text-express-v1",
		}))

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
		testCustomizationRoleArn,
		testOutputDataConfig(),
		testTrainingDataConfig(),
		nil,
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
				testCustomizationRoleArn,
				testOutputDataConfig(),
				testTrainingDataConfig(),
				nil,
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
					testCustomizationRoleArn,
					testOutputDataConfig(),
					testTrainingDataConfig(),
					nil,
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

			_, err := createCustomizationJob(b, "alpha-job", "alpha-job-model")
			require.NoError(t, err)
			betaJob, err := createCustomizationJob(b, "beta-job", "beta-job-model")
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
	_, err := createCustomizationJob(b, "dup-job", "dup-job-model")
	require.NoError(t, err)

	_, err2 := createCustomizationJob(b, "dup-job", "dup-job-model-2")
	require.Error(t, err2)
}

func TestAccuracy_CustomizationJob_DuplicateModelNameConflict(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := createCustomizationJob(b, "job-one", "shared-model-name")
	require.NoError(t, err)

	_, err2 := createCustomizationJob(b, "job-two", "shared-model-name")
	require.Error(t, err2)
}

func TestAccuracy_CustomizationJob_GetByNameOrARN(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	job, err := createCustomizationJob(b, "lookup-job", "lookup-job-model")
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
	job, err := createCustomizationJob(b, "advance-job", "advance-job-model")
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
				withCustomizationJobRequiredFields(map[string]any{
					"jobName":             fmt.Sprintf("job-%s", tt.name),
					"customModelName":     fmt.Sprintf("model-%s", tt.name),
					"baseModelIdentifier": "amazon.titan-text-express-v1",
					"customizationType":   tt.customizationType,
				}),
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
	body := withCustomizationJobRequiredFields(map[string]any{
		"jobName":             "my-customization-job",
		"baseModelIdentifier": "amazon.titan-text-express-v1",
		"customModelName":     "my-fine-tuned-model",
	})
	rec := doRequest(t, h, http.MethodPost, "/model-customization-jobs", body)
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
