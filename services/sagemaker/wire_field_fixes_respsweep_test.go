package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompilationJob_RequiredOutputMembersSurviveInProgress drives
// CreateCompilationJob/DescribeCompilationJob at the raw-request level and
// asserts two required DescribeCompilationJobOutput members
// (api_op_DescribeCompilationJob.go:67-69,84-87, sagemaker@v1.263.2) survive
// the common non-terminal case: FailureReason (legitimately empty for any
// non-failed job) and ModelArtifacts (previously computed only on
// INPROGRESS->COMPLETED transition, so every freshly created job's Describe
// response dropped it). Both bugs shared the same root cause: `omitempty` on
// a required member with a legitimately empty/absent zero value. Checked as
// raw JSON, not the typed SDK client, since the typed client can't
// distinguish "key absent" from "key present with the Go zero value" once
// unmarshaled into a string/pointer field.
func TestCompilationJob_RequiredOutputMembersSurviveInProgress(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doSageMakerRequest(t, h, "CreateCompilationJob", map[string]any{
		"CompilationJobName": "cj-respsweep",
		"RoleArn":            "arn:aws:iam::000000000000:role/x",
		"InputConfig": map[string]any{
			"S3Uri":     "s3://bucket/model.tar.gz",
			"Framework": "TENSORFLOW",
		},
		"OutputConfig": map[string]any{
			"S3OutputLocation": "s3://bucket/output/",
			"TargetDevice":     "ml_c5",
		},
		"StoppingCondition": map[string]any{"MaxRuntimeInSeconds": 3600},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	rr = doSageMakerRequest(t, h, "DescribeCompilationJob", map[string]any{
		"CompilationJobName": "cj-respsweep",
	})
	require.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	require.Equal(
		t, "INPROGRESS", resp["CompilationJobStatus"],
		"test relies on describing before the async completion timer fires",
	)

	failureReason, ok := resp["FailureReason"]
	assert.True(t, ok, "FailureReason key must be present on a non-failed job")
	assert.Empty(t, failureReason)

	artifacts, ok := resp["ModelArtifacts"].(map[string]any)
	require.True(t, ok, "ModelArtifacts key must be present, even while INPROGRESS")
	assert.Equal(t, "s3://bucket/output/model.tar.gz", artifacts["S3ModelArtifacts"])
}

// TestModelCard_ContentSurvivesEmpty drives CreateModelCard/DescribeModelCard
// at the raw-request level. Content is required on CreateModelCardInput but
// only nil-checked client-side (*string, validateOpCreateModelCardInput), so
// a conformant client can send an empty string -- which this backend's
// `omitempty` on ModelCard.Content silently dropped from the response.
func TestModelCard_ContentSurvivesEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName":   "mc-respsweep",
		"Content":         "",
		"ModelCardStatus": "Draft",
	})
	require.Equal(t, http.StatusOK, rr.Code)

	rr = doSageMakerRequest(t, h, "DescribeModelCard", map[string]any{
		"ModelCardName": "mc-respsweep",
	})
	require.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	content, ok := resp["Content"]
	assert.True(t, ok, "Content key must be present even when empty")
	assert.Empty(t, content)
}

// TestCreateOptimizationJob_StoppingConditionRequired proves
// CreateOptimizationJob rejects a request omitting StoppingCondition.
// StoppingCondition is required on both CreateOptimizationJobInput
// (validateOpCreateOptimizationJobInput) and DescribeOptimizationJobOutput
// (api_op_DescribeOptimizationJob.go:111) -- previously entirely
// unvalidated, so an omitted StoppingCondition would have stored an
// OptimizationJob whose Describe response silently dropped it.
func TestCreateOptimizationJob_StoppingConditionRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doSageMakerRequest(t, h, "CreateOptimizationJob", map[string]any{
		"OptimizationJobName":    "oj-respsweep",
		"RoleArn":                "arn:aws:iam::000000000000:role/x",
		"DeploymentInstanceType": "ml.g5.xlarge",
		"ModelSource":            map[string]any{"S3": map[string]any{"S3Uri": "s3://bucket/model"}},
		"OptimizationConfigs":    []any{map[string]any{}},
		"OutputConfig":           map[string]any{"S3OutputLocation": "s3://bucket/out"},
	})
	assert.NotEqual(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "StoppingCondition")
}

// TestCreateComputeQuota_RequiredFieldsValidated proves CreateComputeQuota
// rejects requests omitting ClusterArn/ComputeQuotaConfig/ComputeQuotaTarget
// -- all three "This member is required" on CreateComputeQuotaInput
// (validateOpCreateComputeQuotaInput) but previously entirely unvalidated;
// ComputeQuotaTarget is also required on DescribeComputeQuotaOutput.
func TestCreateComputeQuota_RequiredFieldsValidated(t *testing.T) {
	t.Parallel()

	base := map[string]any{
		"Name":               "cq-respsweep",
		"ClusterArn":         "arn:aws:sagemaker:us-east-1:000000000000:cluster/abc123",
		"ComputeQuotaConfig": map[string]any{},
		"ComputeQuotaTarget": map[string]any{"TeamName": "team1"},
	}

	tests := []struct {
		omit string
		name string
	}{
		{name: "missing cluster arn", omit: "ClusterArn"},
		{name: "missing compute quota config", omit: "ComputeQuotaConfig"},
		{name: "missing compute quota target", omit: "ComputeQuotaTarget"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			req := map[string]any{}
			for k, v := range base {
				if k != tt.omit {
					req[k] = v
				}
			}
			req["Name"] = "cq-respsweep-" + tt.omit

			rr := doSageMakerRequest(t, h, "CreateComputeQuota", req)
			assert.NotEqual(t, http.StatusOK, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.omit)
		})
	}
}
