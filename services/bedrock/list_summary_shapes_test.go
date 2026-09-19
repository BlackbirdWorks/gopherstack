package bedrock_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// listItems unmarshals a List response body and returns the array found
// under key.
func listItems(t *testing.T, body []byte, key string) []map[string]any {
	t.Helper()

	var resp map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &resp))

	raw, ok := resp[key]
	require.True(t, ok, "response missing key %q", key)

	var items []map[string]any
	require.NoError(t, json.Unmarshal(raw, &items))

	return items
}

func assertKeysAbsent(t *testing.T, item map[string]any, keys ...string) {
	t.Helper()

	for _, k := range keys {
		assert.NotContains(t, item, k)
	}
}

// TestListSummaries_MemberRoundTrips drives each op's Create then List and
// asserts a member that was missing from the List summary (over-wide
// response class, gopherstack-dv4s) now round-trips.
func TestListSummaries_MemberRoundTrips(t *testing.T) {
	t.Parallel()

	t.Run("custom model owner account id", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		_, err := createCustomizationJob(h.Backend, "cm-owner-job", "cm-owner-model")
		require.NoError(t, err)
		require.Positive(t, h.Backend.AdvanceCustomizationJobStatuses(0))

		rec := doRequest(t, h, http.MethodGet, "/custom-models", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		items := listItems(t, rec.Body.Bytes(), "modelSummaries")
		require.Len(t, items, 1)
		assert.Equal(t, "000000000000", items[0]["ownerAccountId"])
	})

	t.Run("evaluation job type and task types", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		body := map[string]any{
			"jobName": "eval-tasktypes",
			"roleArn": "arn:aws:iam::000000000000:role/test-role",
			"outputDataConfig": map[string]any{
				"s3Uri": "s3://bucket/output/",
			},
			"evaluationConfig": map[string]any{
				"automated": map[string]any{
					"datasetMetricConfigs": []map[string]any{
						{
							"taskType":    "Summarization",
							"dataset":     map[string]any{"name": "ds-1"},
							"metricNames": []string{"Accuracy"},
						},
					},
				},
			},
			"inferenceConfig": map[string]any{
				"models": []map[string]any{
					{"bedrockModel": map[string]any{"modelId": "amazon.titan-text-express-v1"}},
				},
			},
		}

		rec := doRequest(t, h, http.MethodPost, "/evaluation-jobs", body)
		require.Equal(t, http.StatusCreated, rec.Code)

		listRec := doRequest(t, h, http.MethodGet, "/evaluation-jobs", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "jobSummaries")
		require.Len(t, items, 1)
		assert.Equal(t, "Automated", items[0]["jobType"])
		assert.ElementsMatch(t, []any{"Summarization"}, items[0]["evaluationTaskTypes"])
	})

	t.Run("model customization job end time", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		_, err := createCustomizationJob(h.Backend, "mcj-endtime-job", "mcj-endtime-model")
		require.NoError(t, err)
		require.Positive(t, h.Backend.AdvanceCustomizationJobStatuses(0))

		rec := doRequest(t, h, http.MethodGet, "/model-customization-jobs", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		items := listItems(t, rec.Body.Bytes(), "modelCustomizationJobSummaries")
		require.Len(t, items, 1)
		assert.NotEmpty(t, items[0]["endTime"])
	})

	t.Run("model invocation job client token and message", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		createRec := doRequest(t, h, http.MethodPost, "/model-invocation-job", map[string]any{
			"jobName":            "miv-token",
			"modelId":            "amazon.titan-text-express-v1",
			"roleArn":            "arn:aws:iam::000000000000:role/test-role",
			"clientRequestToken": "tok-abc",
			"inputDataConfig": map[string]any{
				"s3InputDataConfig": map[string]any{"s3Uri": "s3://bucket/in/"},
			},
			"outputDataConfig": map[string]any{
				"s3OutputDataConfig": map[string]any{"s3Uri": "s3://bucket/out/"},
			},
		})
		require.Equal(t, http.StatusCreated, createRec.Code)

		listRec := doRequest(t, h, http.MethodGet, "/model-invocation-jobs", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "invocationJobSummaries")
		require.Len(t, items, 1)
		assert.Equal(t, "tok-abc", items[0]["clientRequestToken"])
	})

	t.Run("model copy job target model name", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		_, err := createCustomizationJob(h.Backend, "src-job", "src-model")
		require.NoError(t, err)
		require.Positive(t, h.Backend.AdvanceCustomizationJobStatuses(0))

		srcModel, err := h.Backend.GetCustomModel("src-model")
		require.NoError(t, err)

		createRec := doRequest(t, h, http.MethodPost, "/model-copy-jobs", map[string]any{
			"sourceModelArn":  srcModel.ModelArn,
			"targetModelName": "copied-model",
		})
		require.Equal(t, http.StatusCreated, createRec.Code)

		listRec := doRequest(t, h, http.MethodGet, "/model-copy-jobs", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "modelCopyJobSummaries")
		require.Len(t, items, 1)
		assert.Equal(t, "copied-model", items[0]["targetModelName"])
	})

	t.Run("automated reasoning policy id and version", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		policy, err := h.Backend.CreateAutomatedReasoningPolicy("arp-idver", "a policy", nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodGet, "/automated-reasoning-policies", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		items := listItems(t, rec.Body.Bytes(), "automatedReasoningPolicySummaries")
		require.Len(t, items, 1)
		assert.NotEmpty(t, items[0]["policyId"])
		assert.Equal(t, policy.Version, items[0]["version"])
	})

	t.Run("imported model instructSupported gap is honestly absent, no leak", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		_, err := h.Backend.CreateModelImportJob(
			"imp-job", "imp-model", "arn:aws:iam::000000000000:role/test-role", "s3://bucket/model/", nil,
		)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodGet, "/imported-models", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		items := listItems(t, rec.Body.Bytes(), "modelSummaries")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "jobArn", "jobName")
		assert.NotEmpty(t, items[0]["modelArn"])
		assert.NotEmpty(t, items[0]["modelName"])
	})
}

// TestListSummaries_OmitDescribeOnlyFields proves, per flagged List op, that
// keys only the Get/Describe-shaped struct carries are absent from the real
// List response (over-wide response class, gopherstack-dv4s).
func TestListSummaries_OmitDescribeOnlyFields(t *testing.T) {
	t.Parallel()

	t.Run("advanced prompt optimization jobs omit describe-only fields", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		createRec := doRequest(t, h, http.MethodPost, "/advanced-prompt-optimization-jobs", map[string]any{
			"jobName":          "apo-leak",
			"jobDescription":   "a job",
			"encryptionKeyArn": "arn:aws:kms:us-east-1:000000000000:key/abc",
			"inputConfig":      map[string]any{"s3Uri": "s3://bucket/in/"},
			"outputConfig":     map[string]any{"s3Uri": "s3://bucket/out/"},
			"modelConfigurations": []map[string]any{
				{"modelId": "amazon.titan-text-express-v1"},
			},
		})
		require.Equal(t, http.StatusOK, createRec.Code)

		listRec := doRequest(t, h, http.MethodGet, "/advanced-prompt-optimization-jobs", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "jobSummaries")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0],
			"jobDescription", "encryptionKeyArn", "failureMessage",
			"inputConfig", "outputConfig", "modelConfigurations",
		)
		assert.Equal(t, "apo-leak", items[0]["jobName"])
	})

	t.Run("automated reasoning policies omit status", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		_, err := h.Backend.CreateAutomatedReasoningPolicy("arp-leak", "", nil)
		require.NoError(t, err)

		rec := doRequest(t, h, http.MethodGet, "/automated-reasoning-policies", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		items := listItems(t, rec.Body.Bytes(), "automatedReasoningPolicySummaries")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "status")
	})

	t.Run("custom models omit tags", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		_, err := createCustomizationJob(h.Backend, "cm-tags-job", "cm-tags-model")
		require.NoError(t, err)
		require.Positive(t, h.Backend.AdvanceCustomizationJobStatuses(0))

		rec := doRequest(t, h, http.MethodGet, "/custom-models", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		items := listItems(t, rec.Body.Bytes(), "modelSummaries")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "tags", "jobArn", "jobName")
	})

	t.Run("model copy jobs omit lastModifiedTime and use targetModelTags key", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		_, err := createCustomizationJob(h.Backend, "src-job2", "src-model2")
		require.NoError(t, err)
		require.Positive(t, h.Backend.AdvanceCustomizationJobStatuses(0))

		srcModel, err := h.Backend.GetCustomModel("src-model2")
		require.NoError(t, err)

		createRec := doRequest(t, h, http.MethodPost, "/model-copy-jobs", map[string]any{
			"sourceModelArn":  srcModel.ModelArn,
			"targetModelName": "copied-model2",
			"targetModelTags": []map[string]any{{"key": "k", "value": "v"}},
		})
		require.Equal(t, http.StatusCreated, createRec.Code)

		listRec := doRequest(t, h, http.MethodGet, "/model-copy-jobs", nil)
		require.Equal(t, http.StatusOK, listRec.Code)

		items := listItems(t, listRec.Body.Bytes(), "modelCopyJobSummaries")
		require.Len(t, items, 1)
		assertKeysAbsent(t, items[0], "lastModifiedTime", "tags")
		assert.NotEmpty(t, items[0]["targetModelTags"])
	})
}
