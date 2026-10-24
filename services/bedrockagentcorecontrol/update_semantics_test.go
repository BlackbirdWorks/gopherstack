package bedrockagentcorecontrol_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdatesClearOnlyDocumentedMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "runtime", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			rt, err := f.client.CreateAgentRuntime(ctx, &sdk.CreateAgentRuntimeInput{
				AgentRuntimeName:     aws.String("agent_a"),
				AgentRuntimeArtifact: containerArtifact(testImage),
				RoleArn:              aws.String(testRole),
				NetworkConfiguration: &types.NetworkConfiguration{NetworkMode: types.NetworkModePublic},
				Description:          aws.String("first"),
				EnvironmentVariables: map[string]string{"K": "v"},
			})
			require.NoError(t, err)
			f.clock.advance()

			_, err = f.client.UpdateAgentRuntime(ctx, &sdk.UpdateAgentRuntimeInput{
				AgentRuntimeId: rt.AgentRuntimeId, AgentRuntimeArtifact: containerArtifact(testImage),
				RoleArn: aws.String(testRole),
			})
			require.NoError(t, err)
			f.clock.advance()

			got, err := f.client.GetAgentRuntime(ctx, &sdk.GetAgentRuntimeInput{AgentRuntimeId: rt.AgentRuntimeId})
			require.NoError(t, err)
			assert.Equal(t, "first", aws.ToString(got.Description), "undocumented members merge")
			assert.Equal(t, map[string]string{"K": "v"}, got.EnvironmentVariables)
			require.NotNil(t, got.NetworkConfiguration)
			assert.Equal(t, types.NetworkModePublic, got.NetworkConfiguration.NetworkMode)
		}},
		{name: "gateway", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			gw, err := f.client.CreateGateway(ctx, &sdk.CreateGatewayInput{
				Name: aws.String("gw-a"), RoleArn: aws.String(testRole), AuthorizerType: types.AuthorizerTypeCustomJwt,
				AuthorizerConfiguration: jwtAuthorizer(), Description: aws.String("first"),
				ExceptionLevel: types.ExceptionLevelDebug,
			})
			require.NoError(t, err)
			f.clock.advance()

			_, err = f.client.UpdateGateway(ctx, &sdk.UpdateGatewayInput{
				GatewayIdentifier: gw.GatewayId, Name: aws.String("gw-a"), RoleArn: aws.String(testRole),
				AuthorizerType: types.AuthorizerTypeCustomJwt, AuthorizerConfiguration: jwtAuthorizer(),
			})
			require.NoError(t, err)
			f.clock.advance()

			got, err := f.client.GetGateway(ctx, &sdk.GetGatewayInput{GatewayIdentifier: gw.GatewayId})
			require.NoError(t, err)
			assert.Equal(t, "first", aws.ToString(got.Description), "undocumented members merge")
			assert.Empty(t, got.ExceptionLevel, "documented: omitted exceptionLevel clears")
			assert.Equal(t, types.GatewayProtocolTypeMcp, got.ProtocolType)
			assert.NotNil(t, got.AuthorizerConfiguration)
		}},
		{name: "gateway target", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			gw := createGateway(t, f.client, "gw-b")
			f.clock.advance()

			tgt, err := f.client.CreateGatewayTarget(ctx, &sdk.CreateGatewayTargetInput{
				GatewayIdentifier: gw.GatewayId, Name: aws.String("t1"), TargetConfiguration: lambdaTarget(lambdaARN),
				Description: aws.String("first"),
				CertificateConfigurations: []types.CertificateConfiguration{
					&types.CertificateConfigurationMemberS3{Value: types.S3CertificateConfiguration{
						Uri: aws.String("s3://bucket/ca.pem"),
					}},
				},
				CredentialProviderConfigurations: []types.CredentialProviderConfiguration{
					{CredentialProviderType: types.CredentialProviderTypeGatewayIamRole},
				},
			})
			require.NoError(t, err)
			f.clock.advance()

			before, err := f.client.GetGatewayTarget(
				ctx, &sdk.GetGatewayTargetInput{GatewayIdentifier: gw.GatewayId, TargetId: tgt.TargetId})
			require.NoError(t, err)
			require.Len(t, before.CertificateConfigurations, 1)

			_, err = f.client.UpdateGatewayTarget(ctx, &sdk.UpdateGatewayTargetInput{
				GatewayIdentifier: gw.GatewayId, TargetId: tgt.TargetId, TargetConfiguration: lambdaTarget(lambdaARN),
			})
			require.NoError(t, err)
			f.clock.advance()

			got, err := f.client.GetGatewayTarget(
				ctx, &sdk.GetGatewayTargetInput{GatewayIdentifier: gw.GatewayId, TargetId: tgt.TargetId})
			require.NoError(t, err)
			assert.Equal(t, "first", aws.ToString(got.Description), "undocumented members merge")
			assert.Empty(t, got.CertificateConfigurations, "documented: omitted certificateConfigurations clears")
			assert.NotEmpty(t, got.CredentialProviderConfigurations)
			assert.Equal(t, "t1", aws.ToString(got.Name), "the name is retained")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.backend.SetReferences(fakeRefs{
				functions: map[string]bool{lambdaARN: true}, repos: map[string]bool{"agent": true},
			})
			f.backend.SetLifecycleDelay(time.Minute)
			tt.run(t, f)
		})
	}
}
