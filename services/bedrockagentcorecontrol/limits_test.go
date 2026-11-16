package bedrockagentcorecontrol_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strategies(n int) []types.MemoryStrategyInput {
	out := make([]types.MemoryStrategyInput, 0, n)
	for i := range n {
		out = append(out, &types.MemoryStrategyInputMemberSummaryMemoryStrategy{
			Value: types.SummaryMemoryStrategyInput{Name: aws.String("s" + strconv.Itoa(i))},
		})
	}

	return out
}

func TestMemoryStrategyQuota(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		want  string
		count int
		add   int
	}{
		{name: "six at create", count: 6},
		{name: "seven at create", count: 7, want: "ServiceQuotaExceededException"},
		{name: "update past six", count: 5, add: 2, want: "ServiceQuotaExceededException"},
		{name: "update to six", count: 5, add: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)

			created, err := f.client.CreateMemory(t.Context(), &sdk.CreateMemoryInput{
				Name: aws.String("quota_mem"), EventExpiryDuration: aws.Int32(30),
				MemoryStrategies: strategies(tt.count),
			})
			if tt.add == 0 {
				assert.Equal(t, tt.want, apiCode(err))

				return
			}

			require.NoError(t, err)

			add := make([]types.MemoryStrategyInput, 0, tt.add)
			for i := range tt.add {
				add = append(add, &types.MemoryStrategyInputMemberSummaryMemoryStrategy{
					Value: types.SummaryMemoryStrategyInput{Name: aws.String("extra" + strconv.Itoa(i))},
				})
			}

			f.clock.advance()

			_, err = f.client.UpdateMemory(t.Context(), &sdk.UpdateMemoryInput{
				MemoryId: created.Memory.Id,
				MemoryStrategies: &types.ModifyMemoryStrategies{
					AddMemoryStrategies: add,
				},
			})
			assert.Equal(t, tt.want, apiCode(err))
		})
	}
}

func TestEnvironmentVariablesSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		size int
	}{
		{name: "small", size: 100},
		{name: "over 4 KB", size: 5000, want: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)

			_, err := f.client.CreateAgentRuntime(t.Context(), &sdk.CreateAgentRuntimeInput{
				AgentRuntimeName: aws.String("env_rt"), AgentRuntimeArtifact: containerArtifact(testImage),
				RoleArn:              aws.String(testRole),
				NetworkConfiguration: &types.NetworkConfiguration{NetworkMode: types.NetworkModePublic},
				EnvironmentVariables: map[string]string{"K": strings.Repeat("v", tt.size)},
			})
			assert.Equal(t, tt.want, apiCode(err))
		})
	}
}

func TestToolSchemaLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		toolName string
		want     string
	}{
		{name: "normal", toolName: "ok"},
		{name: "long name", toolName: strings.Repeat("n", 257), want: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := newMCPFixture(t)
			gid := aws.ToString(createGateway(t, m.client, "Limits-Gw").GatewayId)

			_, err := m.client.CreateGatewayTarget(t.Context(), &sdk.CreateGatewayTargetInput{
				GatewayIdentifier: aws.String(gid), Name: aws.String("tgt"),
				TargetConfiguration: lambdaTools(tt.toolName),
				CredentialProviderConfigurations: []types.CredentialProviderConfiguration{
					{CredentialProviderType: types.CredentialProviderTypeGatewayIamRole},
				},
			})
			assert.Equal(t, tt.want, apiCode(err))
		})
	}
}
