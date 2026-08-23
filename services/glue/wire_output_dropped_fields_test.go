package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestGetResourcePolicy_ReturnsCreateAndUpdateTime proves GetResourcePolicyOutput
// carries CreateTime/UpdateTime (api_op_GetResourcePolicy.go, confirmed against
// deserializers.go's awsAwsjson11_deserializeOpDocumentGetResourcePolicyOutput
// case list: "CreateTime"/"UpdateTime"/"PolicyHash"/"PolicyInJson") -- previously
// dropped entirely even though this backend already tracks both timestamps per
// policy (resourcePolicyEntry, used correctly by the sibling GetResourcePolicies op).
func TestGetResourcePolicy_ReturnsCreateAndUpdateTime(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.PutResourcePolicy(t.Context(), &gluesdk.PutResourcePolicyInput{
		PolicyInJson: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		ResourceArn:  aws.String("arn:aws:glue:us-east-1:000000000000:catalog"),
	})
	require.NoError(t, err)

	got, err := client.GetResourcePolicy(t.Context(), &gluesdk.GetResourcePolicyInput{
		ResourceArn: aws.String("arn:aws:glue:us-east-1:000000000000:catalog"),
	})
	require.NoError(t, err)

	assert.NotNil(t, got.CreateTime, "CreateTime must decode non-nil")
	assert.NotNil(t, got.UpdateTime, "UpdateTime must decode non-nil")
}

// TestGetMLTaskRun_ReturnsRealTrackedFields proves GetMLTaskRunOutput carries
// StartedOn/CompletedOn/ErrorString/LogGroupName (api_op_GetMLTaskRun.go) --
// previously dropped entirely even though this backend already tracks
// StartedOn (set by StartMLEvaluationTaskRun) and CompletedOn (set by
// CancelMLTaskRun) on MLTaskRun (models.go).
func TestGetMLTaskRun_ReturnsRealTrackedFields(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	transform, err := backend.CreateMLTransformWithOptions(
		"mlt", "", "role", nil, glue.MLTransformParameter{}, nil, glue.MLTransformOptions{},
	)
	require.NoError(t, err)

	run, err := backend.StartMLEvaluationTaskRun(transform.TransformID)
	require.NoError(t, err)
	require.NotZero(t, run.StartedOn)

	require.NoError(t, backend.CancelMLTaskRun(transform.TransformID, run.TaskRunID))

	got, err := client.GetMLTaskRun(t.Context(), &gluesdk.GetMLTaskRunInput{
		TransformId: aws.String(transform.TransformID),
		TaskRunId:   aws.String(run.TaskRunID),
	})
	require.NoError(t, err)

	assert.NotNil(t, got.StartedOn, "StartedOn must decode non-nil")
	assert.NotNil(t, got.CompletedOn, "CompletedOn must decode non-nil")
}

// TestGetDataQualityRuleRecommendationRun_ReturnsStartedOn proves
// GetDataQualityRuleRecommendationRunOutput.StartedOn
// (api_op_GetDataQualityRuleRecommendationRun.go) -- previously dropped
// entirely even though this backend already tracks it on
// DQRuleRecommendationRun (models.go), set by
// StartDataQualityRuleRecommendationRun.
func TestGetDataQualityRuleRecommendationRun_ReturnsStartedOn(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	run, err := backend.StartDataQualityRuleRecommendationRun("s3://bucket/data.csv")
	require.NoError(t, err)
	require.NotZero(t, run.StartedOn)

	got, err := client.GetDataQualityRuleRecommendationRun(
		t.Context(),
		&gluesdk.GetDataQualityRuleRecommendationRunInput{RunId: aws.String(run.RecommendationRunID)},
	)
	require.NoError(t, err)

	assert.NotNil(t, got.StartedOn, "StartedOn must decode non-nil")
}
