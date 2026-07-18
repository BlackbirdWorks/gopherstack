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

// --- New operation tests --- //nolint:godot // existing issue.
func TestHandler_CreateEvaluationJob(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
		wantJobArn bool
	}{
		{
			name:       "valid job",
			input:      map[string]any{"jobName": "my-eval-job"},
			wantStatus: http.StatusCreated,
			wantJobArn: true,
		},
		{
			name:       "missing job name",
			input:      map[string]any{"jobName": ""},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantJobArn {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["jobArn"])
			}
		})
	}
}

func TestHandler_BatchDeleteEvaluationJob(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup      func(*testing.T, *bedrock.Handler) []string
		name       string
		wantStatus int
		wantErrs   int
		wantJobs   int
	}{
		{
			name: "delete existing jobs",
			setup: func(t *testing.T, h *bedrock.Handler) []string {
				t.Helper()

				job, err := h.Backend.CreateEvaluationJob("job-1", nil)
				require.NoError(t, err)
				// AWS only allows batch-delete on terminal-state jobs; stop first.
				require.NoError(t, h.Backend.StopEvaluationJob(job.JobArn))

				return []string{job.JobArn}
			},
			wantStatus: http.StatusOK,
			wantErrs:   0,
			wantJobs:   1,
		},
		{
			name: "delete non-existent job",
			setup: func(_ *testing.T, _ *bedrock.Handler) []string {
				return []string{"arn:aws:bedrock:us-east-1:000000000000:evaluation-job/nonexistent"}
			},
			wantStatus: http.StatusOK,
			wantErrs:   1,
			wantJobs:   0,
		},
		{
			name: "mixed existing and non-existent",
			setup: func(t *testing.T, h *bedrock.Handler) []string {
				t.Helper()

				job, err := h.Backend.CreateEvaluationJob("job-mixed", nil)
				require.NoError(t, err)
				// Stop the job so it's in a terminal state before batch-delete.
				require.NoError(t, h.Backend.StopEvaluationJob(job.JobArn))

				return []string{
					job.JobArn,
					"arn:aws:bedrock:us-east-1:000000000000:evaluation-job/nonexistent",
				}
			},
			wantStatus: http.StatusOK,
			wantErrs:   1,
			wantJobs:   1,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			jobARNs := tt.setup(t, h)

			rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs/batch-delete", map[string]any{
				"jobIdentifiers": jobARNs,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			mustUnmarshal(t, rec, &out)
			assert.Len(t, out["errors"].([]any), tt.wantErrs)
			assert.Len(t, out["evaluationJobs"].([]any), tt.wantJobs)
		})
	}
}

func TestHandler_BatchDeleteEvaluationJobEmptyList(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs/batch-delete", map[string]any{
		"jobIdentifiers": []string{},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateEvaluationJobNameUniqueness(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "unique-job"})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2 := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "unique-job"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestHandler_TagsOnEvaluationJob(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create a job with tags.
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "tagged-job",
		"tags": []map[string]string{
			{"key": "project", "value": "alpha"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN := createOut["jobArn"].(string)

	// List tags via ListTagsForResource.
	rec2 := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
		"resourceARN": jobARN,
	})
	assert.Equal(t, http.StatusOK, rec2.Code)

	var tagsOut map[string]any
	mustUnmarshal(t, rec2, &tagsOut)
	tags := tagsOut["tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "project", tags[0].(map[string]any)["key"])

	// Add more tags.
	rec3 := doRequest(t, h, http.MethodPost, "/tagResource", map[string]any{
		"resourceARN": jobARN,
		"tags": []map[string]string{
			{"key": "env", "value": "prod"},
		},
	})
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Verify new tag added.
	rec4 := doRequest(t, h, http.MethodPost, "/listTagsForResource", map[string]any{
		"resourceARN": jobARN,
	})
	assert.Equal(t, http.StatusOK, rec4.Code)

	var tagsOut2 map[string]any
	mustUnmarshal(t, rec4, &tagsOut2)
	assert.Len(t, tagsOut2["tags"].([]any), 2)
}

func TestBatch1_EvaluationJob_ModelConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	opts := &bedrock.CreateEvaluationJobInput{
		EvaluatorConfig: &bedrock.EvaluationModelConfig{
			ModelIdentifier: "anthropic.claude-3-sonnet-20240229-v1:0",
		},
		InferenceConfig: &bedrock.EvaluationInferenceConfig{
			Models: []bedrock.EvaluationInferenceModelConfig{
				{ModelIdentifier: "amazon.titan-text-express-v1"},
			},
		},
		EvalConfig: []bedrock.EvaluationTaskConfig{
			{
				TaskType: "Summarization",
				Dataset:  &bedrock.EvaluationDataset{Name: "squad-v2"},
				MetricNames: []bedrock.EvaluationMetricConfig{
					{MetricName: "Builtin.Rouge"},
				},
			},
		},
	}

	job, err := b.CreateEvaluationJob("model-eval-job", nil, opts)
	require.NoError(t, err)
	require.NotNil(t, job.EvaluatorConfig)
	assert.Equal(t, "anthropic.claude-3-sonnet-20240229-v1:0", job.EvaluatorConfig.ModelIdentifier)
	require.NotNil(t, job.InferenceConfig)
	require.Len(t, job.InferenceConfig.Models, 1)
	assert.Equal(t, "amazon.titan-text-express-v1", job.InferenceConfig.Models[0].ModelIdentifier)
	require.Len(t, job.EvaluationConfig, 1)
	assert.Equal(t, "Summarization", job.EvaluationConfig[0].TaskType)
	assert.Equal(t, "squad-v2", job.EvaluationConfig[0].Dataset.Name)
	assert.Len(t, job.EvaluationConfig[0].MetricNames, 1)
	assert.Equal(t, "Builtin.Rouge", job.EvaluationConfig[0].MetricNames[0].MetricName)
}

func TestBatch1_EvaluationJob_RAGConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	opts := &bedrock.CreateEvaluationJobInput{
		EvaluatorConfig: &bedrock.EvaluationModelConfig{
			ModelIdentifier: "anthropic.claude-v2",
		},
		InferenceConfig: &bedrock.EvaluationInferenceConfig{
			RAG: &bedrock.EvaluationRAGConfig{
				KnowledgeBaseID: "kb-0001",
			},
		},
	}

	job, err := b.CreateEvaluationJob("rag-eval-job", nil, opts)
	require.NoError(t, err)
	require.NotNil(t, job.InferenceConfig)
	require.NotNil(t, job.InferenceConfig.RAG)
	assert.Equal(t, "kb-0001", job.InferenceConfig.RAG.KnowledgeBaseID)
}

func TestBatch1_EvaluationJob_ModelConfig_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "http-model-eval",
		"evaluatorConfig": map[string]any{
			"modelIdentifier": "anthropic.claude-3-sonnet-20240229-v1:0",
		},
		"inferenceConfig": map[string]any{
			"models": []map[string]any{
				{"modelIdentifier": "amazon.titan-text-express-v1"},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN := createOut["jobArn"].(string)
	assert.NotEmpty(t, jobARN)

	recGet := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobARN, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	assert.Equal(t, jobARN, getOut["jobArn"])
	assert.Equal(t, "InProgress", getOut["status"])

	evaluatorConfig := getOut["evaluatorConfig"].(map[string]any)
	assert.Equal(t, "anthropic.claude-3-sonnet-20240229-v1:0", evaluatorConfig["modelIdentifier"])

	inferenceConfig := getOut["inferenceConfig"].(map[string]any)
	models := inferenceConfig["models"].([]any)
	require.Len(t, models, 1)
	assert.Equal(t, "amazon.titan-text-express-v1", models[0].(map[string]any)["modelIdentifier"])
}

func TestBatch1_EvaluationJob_RAGConfig_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "http-rag-eval",
		"evaluatorConfig": map[string]any{
			"modelIdentifier": "anthropic.claude-v2",
		},
		"inferenceConfig": map[string]any{
			"ragConfig": map[string]any{
				"knowledgeBaseId": "kb-test-001",
			},
		},
		"jobDescription": "RAG evaluation test",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN := createOut["jobArn"].(string)

	recGet := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobARN, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)
	assert.Equal(t, "RAG evaluation test", getOut["jobDescription"])

	inferenceConfig := getOut["inferenceConfig"].(map[string]any)
	ragConfig := inferenceConfig["ragConfig"].(map[string]any)
	assert.Equal(t, "kb-test-001", ragConfig["knowledgeBaseId"])
}

func TestBatch1_EvaluationJob_WithEvalConfig_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "http-full-eval",
		"evaluatorConfig": map[string]any{
			"modelIdentifier": "anthropic.claude-v2",
		},
		"inferenceConfig": map[string]any{
			"models": []map[string]any{
				{"modelIdentifier": "meta.llama3-8b-instruct-v1:0"},
			},
		},
		"evaluationConfig": []map[string]any{
			{
				"taskType": "QuestionAndAnswer",
				"dataset": map[string]any{
					"name": "my-qa-dataset",
					"datasetLocation": map[string]any{
						"s3Uri": "s3://my-bucket/qa-data",
					},
				},
				"metricNames": []map[string]any{
					{"metricName": "Builtin.Accuracy"},
					{"metricName": "Builtin.Robustness"},
				},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createOut map[string]any
	mustUnmarshal(t, rec, &createOut)
	jobARN := createOut["jobArn"].(string)

	recGet := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobARN, nil)
	require.Equal(t, http.StatusOK, recGet.Code)

	var getOut map[string]any
	mustUnmarshal(t, recGet, &getOut)

	evalConfigs := getOut["evaluationConfig"].([]any)
	require.Len(t, evalConfigs, 1)
	ec := evalConfigs[0].(map[string]any)
	assert.Equal(t, "QuestionAndAnswer", ec["taskType"])
	dataset := ec["dataset"].(map[string]any)
	assert.Equal(t, "my-qa-dataset", dataset["name"])
	metrics := ec["metricNames"].([]any)
	assert.Len(t, metrics, 2)
}

func TestBatch1_EvaluationJob_ListEvaluationJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "list-eval-1"})
	doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "list-eval-2"})

	rec := doRequest(t, h, http.MethodGet, "/evaluation-jobs", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	jobs := out["jobSummaries"].([]any)
	assert.Len(t, jobs, 2)

	for _, raw := range jobs {
		j := raw.(map[string]any)
		assert.NotEmpty(t, j["jobArn"])
		assert.NotEmpty(t, j["jobName"])
		assert.NotEmpty(t, j["status"])
		assert.NotEmpty(t, j["creationTime"])
	}
}

func TestAccuracy_EvaluationJob_StopTransitionsStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "stop in-progress job"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			job, err := b.CreateEvaluationJob("stop-eval-job", nil)
			require.NoError(t, err)

			recStop := doRequest(t, h, http.MethodPost,
				"/evaluation-job/"+url.PathEscape(job.JobArn)+"/stop", nil)
			assert.Equal(t, http.StatusOK, recStop.Code)

			recGet := doRequest(t, h, http.MethodGet,
				"/evaluation-jobs/"+url.PathEscape(job.JobArn), nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &out))
			assert.Equal(t, "Stopped", out["status"])
		})
	}
}

func TestAccuracy_EvaluationJob_ListResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jobNames []string
		wantLen  int
	}{
		{name: "empty", jobNames: nil, wantLen: 0},
		{name: "two jobs", jobNames: []string{"eval-a", "eval-b"}, wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			for _, name := range tt.jobNames {
				_, err := b.CreateEvaluationJob(name, nil)
				require.NoError(t, err)
			}

			rec := doRequest(t, h, http.MethodGet, "/evaluation-jobs", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			summaries := out["jobSummaries"].([]any)
			assert.Len(t, summaries, tt.wantLen)

			for _, raw := range summaries {
				s := raw.(map[string]any)
				assert.NotEmpty(t, s["jobArn"])
				assert.NotEmpty(t, s["jobName"])
			}
		})
	}
}

func TestAccuracy_EvaluationJob_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/evaluation-jobs/nonexistent-arn", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_EvaluationJob_EvaluatorConfigPreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		evaluatorModel string
		name           string
	}{
		{name: "claude evaluator", evaluatorModel: "anthropic.claude-3-sonnet-20240229-v1:0"},
		{name: "titan evaluator", evaluatorModel: "amazon.titan-text-express-v1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
				"jobName": tt.name,
				"roleArn": "arn:aws:iam::000000000000:role/eval-role",
				"evaluatorConfig": map[string]any{
					"modelIdentifier": tt.evaluatorModel,
				},
			})
			require.Equal(t, http.StatusCreated, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			jobArn := out["jobArn"].(string)

			getRec := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobArn, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getOut map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

			evaluatorCfg := getOut["evaluatorConfig"].(map[string]any)
			assert.Equal(t, tt.evaluatorModel, evaluatorCfg["modelIdentifier"])
		})
	}
}

func TestAccuracy_EvaluationJob_InferenceModelsPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "inference-config-job",
		"roleArn": "arn:aws:iam::000000000000:role/eval-role",
		"inferenceConfig": map[string]any{
			"models": []map[string]any{
				{"modelIdentifier": "amazon.titan-text-express-v1"},
				{"modelIdentifier": "anthropic.claude-v2"},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobArn := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobArn, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

	inferCfg := getOut["inferenceConfig"].(map[string]any)
	models := inferCfg["models"].([]any)
	assert.Len(t, models, 2)
}

func TestAccuracy_EvaluationJob_RAGConfigPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "rag-eval-job",
		"roleArn": "arn:aws:iam::000000000000:role/eval-role",
		"inferenceConfig": map[string]any{
			"ragConfig": map[string]any{
				"knowledgeBaseId": "kb-abc123",
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobArn := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobArn, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

	inferCfg := getOut["inferenceConfig"].(map[string]any)
	ragCfg := inferCfg["ragConfig"].(map[string]any)
	assert.Equal(t, "kb-abc123", ragCfg["knowledgeBaseId"])
}

func TestAccuracy_EvaluationJob_EvalTaskConfigPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName": "eval-config-job",
		"roleArn": "arn:aws:iam::000000000000:role/eval-role",
		"evaluationConfig": []map[string]any{
			{
				"taskType": "GENERAL",
				"dataset": map[string]any{
					"name": "test-dataset",
					"datasetLocation": map[string]any{
						"s3Uri": "s3://my-bucket/eval-data/",
					},
				},
				"metricNames": []map[string]any{
					{"metricName": "Builtin.Accuracy"},
					{"metricName": "Builtin.Robustness"},
				},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobArn := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobArn, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

	evalCfg := getOut["evaluationConfig"].([]any)
	require.Len(t, evalCfg, 1)
	task := evalCfg[0].(map[string]any)
	assert.Equal(t, "GENERAL", task["taskType"])
	metrics := task["metricNames"].([]any)
	assert.Len(t, metrics, 2)
}

func TestAccuracy_EvaluationJob_JobDescriptionPreserved(t *testing.T) {
	t.Parallel()

	const desc = "evaluating knowledge retention in LLM"

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{
		"jobName":        "described-eval-job",
		"jobDescription": desc,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	jobArn := out["jobArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+jobArn, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, desc, getOut["jobDescription"])
}

func TestHandler_GetEvaluationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "my-eval-job"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	jobARN := created["jobArn"].(string)

	rec2 := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	mustUnmarshal(t, rec2, &out)
	assert.Equal(t, jobARN, out["jobArn"])
	assert.Equal(t, "my-eval-job", out["jobName"])
	assert.NotEmpty(t, out[keyStatus])
}

func TestHandler_GetEvaluationJob_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	path := "/evaluation-jobs/" + url.PathEscape("arn:aws:bedrock:us-east-1::evaluation-job/nope")
	rec := doRequest(t, h, http.MethodGet, path, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListEvaluationJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/evaluation-jobs", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	assert.Empty(t, out["jobSummaries"])

	doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "job-a"})
	doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "job-b"})

	rec2 := doRequest(t, h, http.MethodGet, "/evaluation-jobs", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)
	assert.Len(t, out2["jobSummaries"], 2)
}

func TestHandler_StopEvaluationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "stop-me"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	jobARN := created["jobArn"].(string)

	rec2 := doRequest(t, h, http.MethodPost, "/evaluation-job/"+url.PathEscape(jobARN)+"/stop", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	rec3 := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
	var out map[string]any
	mustUnmarshal(t, rec3, &out)
	assert.Equal(t, "Stopped", out[keyStatus])
}

func TestBatch2Ops_StopEvaluationJob_AlreadyStopped_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "second_stop_fails"},
		{name: "rapid_double_stop"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": tc.name})
			require.Equal(t, http.StatusCreated, rec.Code)

			var created map[string]any
			mustUnmarshal(t, rec, &created)
			jobARN := created["jobArn"].(string)

			// First stop: must succeed.
			rec2 := doRequest(t, h, http.MethodPost, "/evaluation-job/"+url.PathEscape(jobARN)+"/stop", nil)
			assert.Equal(t, http.StatusOK, rec2.Code, "first stop should succeed")

			// Second stop: must fail — job is now Stopped.
			rec3 := doRequest(t, h, http.MethodPost, "/evaluation-job/"+url.PathEscape(jobARN)+"/stop", nil)
			assert.Equal(t, http.StatusBadRequest, rec3.Code,
				"stop of already-stopped job should return 400 ValidationException")

			var errBody map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &errBody))
			assert.Equal(t, "ValidationException", errBody["__type"],
				"error type must be ValidationException")
		})
	}
}

func TestBatch2Ops_StopEvaluationJob_InProgress_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", map[string]any{"jobName": "stop-inprogress"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	jobARN := created["jobArn"].(string)

	// Job starts InProgress — stop must succeed.
	rec2 := doRequest(t, h, http.MethodPost, "/evaluation-job/"+url.PathEscape(jobARN)+"/stop", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// Verify status is now Stopped.
	rec3 := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var out map[string]any
	mustUnmarshal(t, rec3, &out)
	assert.Equal(t, "Stopped", out["status"])
}

func TestBatch2Ops_BatchDeleteEvaluationJob_InProgress_ReportedAsError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "single-inprogress-job"},
		{name: "inprogress-not-deleted"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs",
				map[string]any{"jobName": tc.name})
			require.Equal(t, http.StatusCreated, rec.Code)

			var created map[string]any
			mustUnmarshal(t, rec, &created)
			jobARN := created["jobArn"].(string)

			// BatchDelete an InProgress job — must appear in errors, not in deleted list.
			rec2 := doRequest(t, h, http.MethodPost, "/evaluation-jobs/batch-delete",
				map[string]any{"jobIdentifiers": []string{jobARN}})
			require.Equal(t, http.StatusOK, rec2.Code)

			var out map[string]any
			mustUnmarshal(t, rec2, &out)

			errors, _ := out["errors"].([]any)
			deleted, _ := out["evaluationJobs"].([]any)

			assert.Len(t, errors, 1,
				"InProgress job must appear in errors array")
			assert.Empty(t, deleted,
				"InProgress job must NOT appear in deleted list")

			errEntry, _ := errors[0].(map[string]any)
			assert.Equal(t, "ValidationException", errEntry["code"])
			assert.Equal(t, jobARN, errEntry["jobIdentifier"])

			// Verify the job still exists (was not deleted).
			rec3 := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
			assert.Equal(t, http.StatusOK, rec3.Code, "InProgress job must still exist after rejected batch-delete")
		})
	}
}

func TestBatch2Ops_BatchDeleteEvaluationJob_StoppedJob_Deleted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create then stop (→ terminal state) before batch-delete.
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs",
		map[string]any{"jobName": "batch-del-stopped"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	jobARN := created["jobArn"].(string)

	// Stop it first.
	doRequest(t, h, http.MethodPost, "/evaluation-job/"+url.PathEscape(jobARN)+"/stop", nil)

	// BatchDelete the stopped job — must succeed.
	rec2 := doRequest(t, h, http.MethodPost, "/evaluation-jobs/batch-delete",
		map[string]any{"jobIdentifiers": []string{jobARN}})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	mustUnmarshal(t, rec2, &out)

	errors, _ := out["errors"].([]any)
	deleted, _ := out["evaluationJobs"].([]any)

	assert.Empty(t, errors, "stopped job should have no errors")
	assert.Len(t, deleted, 1, "stopped job should appear in deleted list")

	// Verify it's gone.
	rec3 := doRequest(t, h, http.MethodGet, "/evaluation-jobs/"+url.PathEscape(jobARN), nil)
	assert.Equal(t, http.StatusNotFound, rec3.Code)
}

func TestBatch2Ops_BatchDeleteEvaluationJob_MixedStates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create two jobs: one will be stopped (terminal), one stays InProgress.
	recA := doRequest(t, h, http.MethodPost, "/evaluation-jobs",
		map[string]any{"jobName": "batch-mix-stopped"})
	require.Equal(t, http.StatusCreated, recA.Code)

	recB := doRequest(t, h, http.MethodPost, "/evaluation-jobs",
		map[string]any{"jobName": "batch-mix-inprogress"})
	require.Equal(t, http.StatusCreated, recB.Code)

	var outA, outB map[string]any
	mustUnmarshal(t, recA, &outA)
	mustUnmarshal(t, recB, &outB)

	arnA := outA["jobArn"].(string)
	arnB := outB["jobArn"].(string)

	// Stop job A.
	doRequest(t, h, http.MethodPost, "/evaluation-job/"+url.PathEscape(arnA)+"/stop", nil)

	// BatchDelete both: A (stopped) should succeed, B (InProgress) should error.
	rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs/batch-delete",
		map[string]any{"jobIdentifiers": []string{arnA, arnB}})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)

	errors, _ := out["errors"].([]any)
	deleted, _ := out["evaluationJobs"].([]any)

	assert.Len(t, deleted, 1, "one stopped job should be deleted")
	assert.Len(t, errors, 1, "one InProgress job should appear in errors")

	// Confirm arnB is the error entry.
	errEntry, _ := errors[0].(map[string]any)
	assert.Equal(t, arnB, errEntry["jobIdentifier"])
	assert.Equal(t, "ValidationException", errEntry["code"])
}
