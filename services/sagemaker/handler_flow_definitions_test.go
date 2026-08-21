package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createFlowDefinitionRequest(name string) map[string]any {
	return map[string]any{
		"FlowDefinitionName": name,
		"RoleArn":            "arn:aws:iam::000000000000:role/TestRole",
		"OutputConfig":       map[string]any{"S3OutputPath": "s3://bucket/out"},
	}
}

func TestHandler_CreateFlowDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateFlowDefinition", createFlowDefinitionRequest("my-flow"))
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["FlowDefinitionArn"], "my-flow")
}

func TestHandler_CreateFlowDefinition_RequiredFieldsEnforced(t *testing.T) {
	t.Parallel()

	tests := map[string]map[string]any{
		"missing name": {
			"RoleArn":      "arn:aws:iam::000000000000:role/TestRole",
			"OutputConfig": map[string]any{"S3OutputPath": "s3://bucket/out"},
		},
		"missing role arn": {
			"FlowDefinitionName": "flow-req",
			"OutputConfig":       map[string]any{"S3OutputPath": "s3://bucket/out"},
		},
		"missing output config": {
			"FlowDefinitionName": "flow-req",
			"RoleArn":            "arn:aws:iam::000000000000:role/TestRole",
		},
		"missing s3 output path": {
			"FlowDefinitionName": "flow-req",
			"RoleArn":            "arn:aws:iam::000000000000:role/TestRole",
			"OutputConfig":       map[string]any{},
		},
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateFlowDefinition", body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_DescribeFlowDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateFlowDefinition", createFlowDefinitionRequest("flow-1"))
	rec := doSageMakerRequest(t, h, "DescribeFlowDefinition", map[string]any{"FlowDefinitionName": "flow-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "flow-1", resp["FlowDefinitionName"])
}

func TestHandler_DescribeFlowDefinition_HumanLoopConfig_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateFlowDefinition(t.Context(), &sagemakersdk.CreateFlowDefinitionInput{
		FlowDefinitionName: aws.String("flow-hlc"),
		RoleArn:            aws.String("arn:aws:iam::000000000000:role/TestRole"),
		OutputConfig: &smtypes.FlowDefinitionOutputConfig{
			S3OutputPath: aws.String("s3://bucket/out"),
			KmsKeyId:     aws.String("kms-key-1"),
		},
		HumanLoopActivationConfig: &smtypes.HumanLoopActivationConfig{
			HumanLoopActivationConditionsConfig: &smtypes.HumanLoopActivationConditionsConfig{
				HumanLoopActivationConditions: aws.String(`{"Conditions":[]}`),
			},
		},
		HumanLoopConfig: &smtypes.HumanLoopConfig{
			HumanTaskUiArn:  aws.String("arn:aws:sagemaker:us-east-1:000000000000:human-task-ui/my-ui"),
			WorkteamArn:     aws.String("arn:aws:sagemaker:us-east-1:000000000000:workteam/private-crowd/team"),
			TaskTitle:       aws.String("Review"),
			TaskDescription: aws.String("Review the output"),
			TaskCount:       aws.Int32(1),
		},
		HumanLoopRequestSource: &smtypes.HumanLoopRequestSource{
			AwsManagedHumanLoopRequestSource: smtypes.AwsManagedHumanLoopRequestSourceTextractAnalyzeDocumentFormsV1,
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeFlowDefinition(t.Context(), &sagemakersdk.DescribeFlowDefinitionInput{
		FlowDefinitionName: aws.String("flow-hlc"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.OutputConfig)
	assert.Equal(t, "s3://bucket/out", aws.ToString(out.OutputConfig.S3OutputPath))
	assert.Equal(t, "kms-key-1", aws.ToString(out.OutputConfig.KmsKeyId))
	require.NotNil(t, out.HumanLoopActivationConfig)
	assert.JSONEq(t, `{"Conditions":[]}`,
		aws.ToString(out.HumanLoopActivationConfig.HumanLoopActivationConditionsConfig.HumanLoopActivationConditions))
	require.NotNil(t, out.HumanLoopConfig)
	assert.Equal(t, "Review", aws.ToString(out.HumanLoopConfig.TaskTitle))
	assert.Equal(t, int32(1), aws.ToInt32(out.HumanLoopConfig.TaskCount))
	require.NotNil(t, out.HumanLoopRequestSource)
	assert.Equal(t, smtypes.AwsManagedHumanLoopRequestSourceTextractAnalyzeDocumentFormsV1,
		out.HumanLoopRequestSource.AwsManagedHumanLoopRequestSource)
}

func TestHandler_DeleteFlowDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateFlowDefinition", createFlowDefinitionRequest("flow-del"))
	rec := doSageMakerRequest(t, h, "DeleteFlowDefinition", map[string]any{"FlowDefinitionName": "flow-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeFlowDefinition", map[string]any{"FlowDefinitionName": "flow-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListFlowDefinitions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListFlowDefinitions", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["FlowDefinitionSummaries"])

	doSageMakerRequest(t, h, "CreateFlowDefinition", createFlowDefinitionRequest("my-flow"))

	rec = doSageMakerRequest(t, h, "ListFlowDefinitions", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	defs := resp["FlowDefinitionSummaries"].([]any)
	assert.Len(t, defs, 1)
	d := defs[0].(map[string]any)
	assert.Equal(t, "my-flow", d["FlowDefinitionName"])
}

func TestHandler_ListFlowDefinitions_FilterSort_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	for _, name := range []string{"alpha-flow", "beta-flow"} {
		_, err := client.CreateFlowDefinition(t.Context(), &sagemakersdk.CreateFlowDefinitionInput{
			FlowDefinitionName: aws.String(name),
			RoleArn:            aws.String("arn:aws:iam::000000000000:role/TestRole"),
			OutputConfig:       &smtypes.FlowDefinitionOutputConfig{S3OutputPath: aws.String("s3://bucket/out")},
		})
		require.NoError(t, err)
	}

	out, err := client.ListFlowDefinitions(t.Context(), &sagemakersdk.ListFlowDefinitionsInput{
		SortOrder: smtypes.SortOrderDescending,
	})
	require.NoError(t, err)
	require.Len(t, out.FlowDefinitionSummaries, 2)
	assert.Equal(t, "beta-flow", aws.ToString(out.FlowDefinitionSummaries[0].FlowDefinitionName))
	assert.Equal(t, "alpha-flow", aws.ToString(out.FlowDefinitionSummaries[1].FlowDefinitionName))
}
