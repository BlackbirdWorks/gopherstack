package bedrock_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

func TestRequestRealism_ErrorMessageHasNoCodePrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "custom model", path: "/custom-models/nope"},
		{name: "guardrail", path: "/guardrails/nope"},
		{name: "customization job", path: "/model-customization-jobs/nope"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusNotFound, rec.Code)

			var out map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, "ResourceNotFoundException", out["__type"])
			assert.NotContains(t, out["message"], "ResourceNotFoundException")
		})
	}
}

func TestRequestRealism_CustomizationJobPatterns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(map[string]any)
		name   string
	}{
		{name: "job name", mutate: func(b map[string]any) { b["jobName"] = "bad name" }},
		{name: "model name", mutate: func(b map[string]any) { b["customModelName"] = "bad_name" }},
		{name: "role arn", mutate: func(b map[string]any) { b["roleArn"] = "role" }},
		{
			name:   "output uri",
			mutate: func(b map[string]any) { b["outputDataConfig"] = map[string]any{"s3Uri": "http://x/y"} },
		},
		{
			name:   "training uri",
			mutate: func(b map[string]any) { b["trainingDataConfig"] = map[string]any{"s3Uri": "bucket/key"} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := withCustomizationJobRequiredFields(map[string]any{
				"jobName": "good-job", "customModelName": "good-model",
				"baseModelIdentifier": "amazon.titan-text-express-v1",
			})
			tt.mutate(body)

			rec := doRequest(t, newTestHandler(t), http.MethodPost, "/model-customization-jobs", body)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "ValidationException")
		})
	}
}

func TestRequestRealism_ListPaging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{name: "max zero", query: "?maxResults=0", want: http.StatusBadRequest},
		{name: "max over", query: "?maxResults=1001", want: http.StatusBadRequest},
		{name: "bad token", query: "?nextToken=zzz!", want: http.StatusBadRequest},
		{name: "valid", query: "?maxResults=1000", want: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), http.MethodGet, "/custom-models"+tt.query, nil)
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}

func TestRequestRealism_InvocationJobProgression(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
		job, err := b.CreateModelInvocationJob("inv", nil)
		require.NoError(t, err)

		const window = 40 * time.Second

		statusAt := func(d time.Duration) string {
			time.Sleep(d)
			b.AdvanceEvaluationAndInvocationJobStatuses(window)

			got, getErr := b.GetModelInvocationJob(job.JobArn)
			require.NoError(t, getErr)

			return got.Status
		}

		assert.Equal(t, "Submitted", statusAt(time.Second))
		assert.Equal(t, "Validating", statusAt(10*time.Second))
		assert.Equal(t, "Scheduled", statusAt(10*time.Second))
		assert.Equal(t, "InProgress", statusAt(10*time.Second))
		assert.Equal(t, "Completed", statusAt(10*time.Second))
	})
}

func TestRequestRealism_StopSettlesThroughStopping(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	eval, err := b.CreateEvaluationJob("ev", nil)
	require.NoError(t, err)
	inv, err := b.CreateModelInvocationJob("inv", nil)
	require.NoError(t, err)

	require.NoError(t, b.StopEvaluationJob(eval.JobArn))
	require.NoError(t, b.StopModelInvocationJob(inv.JobArn))

	gotEval, err := b.GetEvaluationJob(eval.JobArn)
	require.NoError(t, err)
	assert.Equal(t, "Stopping", gotEval.Status)

	assert.Equal(t, 2, b.SettleStoppingJobs())

	gotEval, err = b.GetEvaluationJob(eval.JobArn)
	require.NoError(t, err)
	assert.Equal(t, "Stopped", gotEval.Status)

	gotInv, err := b.GetModelInvocationJob(inv.JobArn)
	require.NoError(t, err)
	assert.Equal(t, "Stopped", gotInv.Status)
	assert.NotNil(t, gotInv.EndTime)
}

func TestRequestRealism_ImportJobCompletesWithSDKStatus(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	job := createTestModelImportJob(t, b, "imp")

	assert.Equal(t, 1, b.AdvanceCopyImportJobStatuses(time.Nanosecond))

	got, err := b.GetModelImportJob(job.JobArn)
	require.NoError(t, err)
	assert.Equal(t, "Completed", got.Status)
}
