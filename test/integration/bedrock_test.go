package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksvc "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_Bedrock_Guardrail_CRUD(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createBedrockClient(t)
	ctx := t.Context()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_and_get",
			run: func(t *testing.T) {
				t.Helper()

				createOut, err := client.CreateGuardrail(ctx, &bedrocksvc.CreateGuardrailInput{
					Name:                    aws.String("integration-guardrail"),
					BlockedInputMessaging:   aws.String("Input blocked."),
					BlockedOutputsMessaging: aws.String("Output blocked."),
				})
				require.NoError(t, err, "CreateGuardrail should succeed")
				require.NotNil(t, createOut.GuardrailId)
				guardrailID := aws.ToString(createOut.GuardrailId)
				assert.NotEmpty(t, guardrailID)

				getOut, err := client.GetGuardrail(ctx, &bedrocksvc.GetGuardrailInput{
					GuardrailIdentifier: aws.String(guardrailID),
				})
				require.NoError(t, err, "GetGuardrail should succeed")
				assert.Equal(t, guardrailID, aws.ToString(getOut.GuardrailId))
				assert.Equal(t, "integration-guardrail", aws.ToString(getOut.Name))
			},
		},
		{
			name: "list",
			run: func(t *testing.T) {
				t.Helper()

				_, err := client.CreateGuardrail(ctx, &bedrocksvc.CreateGuardrailInput{
					Name:                    aws.String("list-guardrail"),
					BlockedInputMessaging:   aws.String("Blocked."),
					BlockedOutputsMessaging: aws.String("Blocked."),
				})
				require.NoError(t, err)

				listOut, err := client.ListGuardrails(ctx, &bedrocksvc.ListGuardrailsInput{})
				require.NoError(t, err, "ListGuardrails should succeed")
				assert.NotEmpty(t, listOut.Guardrails)
			},
		},
		{
			name: "delete",
			run: func(t *testing.T) {
				t.Helper()

				createOut, err := client.CreateGuardrail(ctx, &bedrocksvc.CreateGuardrailInput{
					Name:                    aws.String("to-delete-guardrail"),
					BlockedInputMessaging:   aws.String("Blocked."),
					BlockedOutputsMessaging: aws.String("Blocked."),
				})
				require.NoError(t, err)

				guardrailID := aws.ToString(createOut.GuardrailId)

				_, err = client.DeleteGuardrail(ctx, &bedrocksvc.DeleteGuardrailInput{
					GuardrailIdentifier: aws.String(guardrailID),
				})
				require.NoError(t, err, "DeleteGuardrail should succeed")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestIntegration_Bedrock_FoundationModels(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createBedrockClient(t)
	ctx := t.Context()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "list",
			run: func(t *testing.T) {
				t.Helper()

				listOut, err := client.ListFoundationModels(ctx, &bedrocksvc.ListFoundationModelsInput{})
				require.NoError(t, err, "ListFoundationModels should succeed")
				assert.NotEmpty(t, listOut.ModelSummaries)

				var titanFound bool

				for _, m := range listOut.ModelSummaries {
					if aws.ToString(m.ModelId) == "amazon.titan-text-express-v1" {
						titanFound = true

						break
					}
				}

				assert.True(t, titanFound, "amazon.titan-text-express-v1 should be in model list")
			},
		},
		{
			name: "get",
			run: func(t *testing.T) {
				t.Helper()

				getOut, err := client.GetFoundationModel(ctx, &bedrocksvc.GetFoundationModelInput{
					ModelIdentifier: aws.String("amazon.titan-text-express-v1"),
				})
				require.NoError(t, err, "GetFoundationModel should succeed")
				require.NotNil(t, getOut.ModelDetails)
				assert.Equal(t, "amazon.titan-text-express-v1", aws.ToString(getOut.ModelDetails.ModelId))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestIntegration_Bedrock_ProvisionedModelThroughput(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createBedrockClient(t)
	ctx := t.Context()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_and_get",
			run: func(t *testing.T) {
				t.Helper()

				createOut, err := client.CreateProvisionedModelThroughput(
					ctx,
					&bedrocksvc.CreateProvisionedModelThroughputInput{
						ProvisionedModelName: aws.String("integration-pmt"),
						ModelId:              aws.String("amazon.titan-text-express-v1"),
						ModelUnits:           aws.Int32(1),
					},
				)
				require.NoError(t, err, "CreateProvisionedModelThroughput should succeed")
				require.NotNil(t, createOut.ProvisionedModelArn)
				pmtARN := aws.ToString(createOut.ProvisionedModelArn)
				assert.NotEmpty(t, pmtARN)

				getOut, err := client.GetProvisionedModelThroughput(ctx, &bedrocksvc.GetProvisionedModelThroughputInput{
					ProvisionedModelId: aws.String(pmtARN),
				})
				require.NoError(t, err, "GetProvisionedModelThroughput should succeed")
				assert.Equal(t, "integration-pmt", aws.ToString(getOut.ProvisionedModelName))
				assert.Equal(t, "InService", string(getOut.Status))
			},
		},
		{
			name: "list",
			run: func(t *testing.T) {
				t.Helper()

				_, err := client.CreateProvisionedModelThroughput(
					ctx,
					&bedrocksvc.CreateProvisionedModelThroughputInput{
						ProvisionedModelName: aws.String("list-pmt"),
						ModelId:              aws.String("amazon.titan-text-express-v1"),
						ModelUnits:           aws.Int32(1),
					},
				)
				require.NoError(t, err)

				listOut, err := client.ListProvisionedModelThroughputs(
					ctx,
					&bedrocksvc.ListProvisionedModelThroughputsInput{},
				)
				require.NoError(t, err, "ListProvisionedModelThroughputs should succeed")
				assert.NotEmpty(t, listOut.ProvisionedModelSummaries)
			},
		},
		{
			name: "delete",
			run: func(t *testing.T) {
				t.Helper()

				createOut, err := client.CreateProvisionedModelThroughput(
					ctx,
					&bedrocksvc.CreateProvisionedModelThroughputInput{
						ProvisionedModelName: aws.String("delete-pmt"),
						ModelId:              aws.String("amazon.titan-text-express-v1"),
						ModelUnits:           aws.Int32(1),
					},
				)
				require.NoError(t, err)

				pmtARN := aws.ToString(createOut.ProvisionedModelArn)

				_, err = client.DeleteProvisionedModelThroughput(ctx, &bedrocksvc.DeleteProvisionedModelThroughputInput{
					ProvisionedModelId: aws.String(pmtARN),
				})
				require.NoError(t, err, "DeleteProvisionedModelThroughput should succeed")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
