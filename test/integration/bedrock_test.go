package integration_test

import (
	"testing"
	"time"

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

				// PMT starts as "Creating"; janitor advances to "InService" within ~5s.
				require.Eventually(t, func() bool {
					out, getErr := client.GetProvisionedModelThroughput(
						ctx,
						&bedrocksvc.GetProvisionedModelThroughputInput{
							ProvisionedModelId: aws.String(pmtARN),
						},
					)

					return getErr == nil && string(out.Status) == "InService"
				}, 10*time.Second, 500*time.Millisecond, "PMT should reach InService status")

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

// TestIntegration_Bedrock_GetAutomatedReasoningPolicy drives
// CreateAutomatedReasoningPolicy -> GetAutomatedReasoningPolicy via the real
// AWS SDK v2 client. GetAutomatedReasoningPolicyOutput requires
// DefinitionHash, PolicyId, and Version; the SDK leaves each nil/zero-value
// when the server names the field wrong or omits it, so decoded non-zero
// values are the only proof the wire keys round-trip (gopherstack-lx5h).
func TestIntegration_Bedrock_GetAutomatedReasoningPolicy(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createBedrockClient(t)
	ctx := t.Context()

	createOut, err := client.CreateAutomatedReasoningPolicy(ctx, &bedrocksvc.CreateAutomatedReasoningPolicyInput{
		Name:        aws.String("integration-arp"),
		Description: aws.String("integration test policy"),
	})
	require.NoError(t, err, "CreateAutomatedReasoningPolicy should succeed")
	require.NotNil(t, createOut.PolicyArn)
	policyARN := aws.ToString(createOut.PolicyArn)

	getOut, err := client.GetAutomatedReasoningPolicy(ctx, &bedrocksvc.GetAutomatedReasoningPolicyInput{
		PolicyArn: aws.String(policyARN),
	})
	require.NoError(t, err, "GetAutomatedReasoningPolicy should succeed")
	assert.Equal(t, policyARN, aws.ToString(getOut.PolicyArn))
	assert.Equal(t, "integration-arp", aws.ToString(getOut.Name))
	assert.NotEmpty(t, aws.ToString(getOut.DefinitionHash), "definitionHash is a required response field")
	assert.NotEmpty(t, aws.ToString(getOut.Version), "version is a required response field")
	assert.NotEmpty(t, aws.ToString(getOut.PolicyId), "policyId is a required response field")
	assert.Contains(
		t,
		policyARN,
		aws.ToString(getOut.PolicyId),
		"policyId should be the ARN's own embedded resource id, not a fabricated value",
	)
}
