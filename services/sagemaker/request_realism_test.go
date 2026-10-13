package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const realismRole = "arn:aws:iam::000000000000:role/r"

func trainingBody(name string, mutate func(map[string]any)) map[string]any {
	b := map[string]any{
		"TrainingJobName":        name,
		"RoleArn":                realismRole,
		"AlgorithmSpecification": map[string]any{"TrainingImage": "img", "TrainingInputMode": "File"},
		"OutputDataConfig":       map[string]any{"S3OutputPath": "s3://b/o"},
		"ResourceConfig": map[string]any{
			"InstanceType":   "ml.m5.large",
			"InstanceCount":  1,
			"VolumeSizeInGB": 10,
		},
		"StoppingCondition": map[string]any{"MaxRuntimeInSeconds": 3600},
	}
	if mutate != nil {
		mutate(b)
	}

	return b
}

func errorOf(t *testing.T, body []byte) (string, string) {
	t.Helper()

	var out map[string]string
	require.NoError(t, json.Unmarshal(body, &out))

	return out["__type"], out["message"]
}

func TestRequestRealism_TypedValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		target string
		want   string
	}{
		{name: "model name", target: "CreateModel", body: map[string]any{"ModelName": "bad name!"}, want: "modelName"},
		{
			name: "model role", target: "CreateModel",
			body: map[string]any{"ModelName": "m", "ExecutionRoleArn": "notarn"}, want: "executionRoleArn",
		},
		{
			name: "training instance", target: "CreateTrainingJob", want: "resourceConfig.InstanceType",
			body: trainingBody("t", func(b map[string]any) {
				b["ResourceConfig"] = map[string]any{"InstanceType": "bogus.large", "InstanceCount": 1}
			}),
		},
		{
			name:   "training s3 path",
			target: "CreateTrainingJob",
			want:   "outputDataConfig.S3OutputPath",
			body: trainingBody(
				"t",
				func(b map[string]any) { b["OutputDataConfig"] = map[string]any{"S3OutputPath": "nope"} },
			),
		},
		{
			name: "training name", target: "CreateTrainingJob", want: "trainingJobName",
			body: trainingBody("under_score", nil),
		},
		{
			name: "endpoint config instance", target: "CreateEndpointConfig", want: "InstanceType",
			body: map[string]any{"EndpointConfigName": "ec", "ProductionVariants": []map[string]any{
				{"VariantName": "v", "ModelName": "m", "InitialInstanceCount": 1, "InstanceType": "foo.large"},
			}},
		},
		{name: "empty body", target: "CreateModel", body: map[string]any{}, want: "ModelName is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, tt.target, tt.body)
			require.Equal(t, http.StatusBadRequest, rec.Code)

			code, msg := errorOf(t, rec.Body.Bytes())
			assert.Equal(t, "ValidationException", code)
			assert.Contains(t, msg, tt.want)
			assert.NotContains(t, msg, "invalid request")
		})
	}
}

func TestRequestRealism_NotFoundMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		body   map[string]any
		want   string
	}{
		{
			name: "endpoint", target: "DescribeEndpoint", body: map[string]any{"EndpointName": "nope"},
			want: `Could not find endpoint "arn:aws:sagemaker:us-east-1:000000000000:endpoint/nope".`,
		},
		{
			name: "model", target: "DescribeModel", body: map[string]any{"ModelName": "nope"},
			want: `Could not find model "arn:aws:sagemaker:us-east-1:000000000000:model/nope".`,
		},
		{
			name:   "endpoint config",
			target: "DescribeEndpointConfig",
			body:   map[string]any{"EndpointConfigName": "nope"},
			want:   `Could not find endpoint configuration "arn:aws:sagemaker:us-east-1:000000000000:endpoint-config/nope".`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, tt.target, tt.body)
			require.Equal(t, http.StatusBadRequest, rec.Code)

			code, msg := errorOf(t, rec.Body.Bytes())
			assert.Equal(t, "ValidationException", code)
			assert.Equal(t, tt.want, msg)
		})
	}
}

func TestRequestRealism_ListPaging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		want int
	}{
		{name: "max zero", body: map[string]any{"MaxResults": 0}, want: http.StatusBadRequest},
		{name: "max over", body: map[string]any{"MaxResults": 101}, want: http.StatusBadRequest},
		{name: "bad token", body: map[string]any{"NextToken": "!!not-base64"}, want: http.StatusBadRequest},
		{name: "valid", body: map[string]any{"MaxResults": 100}, want: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "ListEndpoints", tt.body)
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}

func TestRequestRealism_TrainingSecondaryStatusProgression(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Backend.SetLifecycleDelay(20 * time.Millisecond)

	rec := doSageMakerRequest(t, h, "CreateTrainingJob", trainingBody("progress", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var got struct {
		TrainingJobStatus          string `json:"TrainingJobStatus"`
		SecondaryStatus            string `json:"SecondaryStatus"`
		SecondaryStatusTransitions []struct {
			EndTime *float64 `json:"EndTime"`
			Status  string   `json:"Status"`
		} `json:"SecondaryStatusTransitions"`
	}

	require.Eventually(t, func() bool {
		r := doSageMakerRequest(t, h, "DescribeTrainingJob", map[string]any{"TrainingJobName": "progress"})
		got.SecondaryStatusTransitions = nil

		return json.Unmarshal(r.Body.Bytes(), &got) == nil && got.TrainingJobStatus == "Completed"
	}, 5*time.Second, 5*time.Millisecond)

	statuses := make([]string, 0, len(got.SecondaryStatusTransitions))
	for _, tr := range got.SecondaryStatusTransitions {
		statuses = append(statuses, tr.Status)
		assert.NotNil(t, tr.EndTime, tr.Status)
	}

	assert.Equal(t, []string{"Starting", "Downloading", "Training", "Uploading", "Completed"}, statuses)
	assert.Equal(t, "Completed", got.SecondaryStatus)
}

func TestRequestRealism_StopCompletedTrainingJobIsNoop(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Backend.SetLifecycleDelay(time.Millisecond)

	require.Equal(t, http.StatusOK, doSageMakerRequest(t, h, "CreateTrainingJob", trainingBody("done", nil)).Code)

	require.Eventually(t, func() bool {
		var out struct {
			TrainingJobStatus string `json:"TrainingJobStatus"`
		}
		r := doSageMakerRequest(t, h, "DescribeTrainingJob", map[string]any{"TrainingJobName": "done"})

		return json.Unmarshal(r.Body.Bytes(), &out) == nil && out.TrainingJobStatus == "Completed"
	}, 5*time.Second, 5*time.Millisecond)

	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "StopTrainingJob", map[string]any{"TrainingJobName": "done"}).Code)

	var out struct {
		TrainingJobStatus string `json:"TrainingJobStatus"`
	}
	r := doSageMakerRequest(t, h, "DescribeTrainingJob", map[string]any{"TrainingJobName": "done"})
	require.NoError(t, json.Unmarshal(r.Body.Bytes(), &out))
	assert.Equal(t, "Completed", out.TrainingJobStatus)
}

func TestRequestRealism_StopFinishedProcessingJobKeepsStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Backend.SetLifecycleDelay(time.Millisecond)

	create := map[string]any{
		"ProcessingJobName": "pj",
		"RoleArn":           realismRole,
		"AppSpecification":  map[string]any{"ImageUri": "img"},
		"ProcessingResources": map[string]any{"ClusterConfig": map[string]any{
			"InstanceCount": 1, "InstanceType": "ml.m5.large", "VolumeSizeInGB": 1,
		}},
	}
	require.Equal(t, http.StatusOK, doSageMakerRequest(t, h, "CreateProcessingJob", create).Code)

	status := func() string {
		var out struct {
			ProcessingJobStatus string `json:"ProcessingJobStatus"`
		}
		r := doSageMakerRequest(t, h, "DescribeProcessingJob", map[string]any{"ProcessingJobName": "pj"})
		_ = json.Unmarshal(r.Body.Bytes(), &out)

		return out.ProcessingJobStatus
	}

	require.Eventually(t, func() bool { return status() == "Completed" }, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, http.StatusOK,
		doSageMakerRequest(t, h, "StopProcessingJob", map[string]any{"ProcessingJobName": "pj"}).Code)

	assert.Equal(t, "Completed", status())
}
