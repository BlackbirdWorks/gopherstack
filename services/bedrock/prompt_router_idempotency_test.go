package bedrock_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	brtypes "github.com/aws/aws-sdk-go-v2/service/bedrock/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// CreatePromptRouterInput.ClientRequestToken is an idempotency token (api_op_CreatePromptRouter.go).
func TestCreatePromptRouter_ClientRequestToken(t *testing.T) {
	t.Parallel()

	const model = "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2"

	input := func(name, token string, diff float64) *bedrocksdk.CreatePromptRouterInput {
		return &bedrocksdk.CreatePromptRouterInput{
			PromptRouterName:   aws.String(name),
			ClientRequestToken: aws.String(token),
			FallbackModel:      &brtypes.PromptRouterTargetModel{ModelArn: aws.String(model)},
			Models:             []brtypes.PromptRouterTargetModel{{ModelArn: aws.String(model)}},
			RoutingCriteria:    &brtypes.RoutingCriteria{ResponseQualityDifference: aws.Float64(diff)},
		}
	}

	tests := []struct {
		second    *bedrocksdk.CreatePromptRouterInput
		name      string
		wantSame  bool
		wantError bool
	}{
		{name: "same-params-replays", second: input("r1", "tok", 0.5), wantSame: true},
		{name: "changed-params-conflict", second: input("r1", "tok", 0.9), wantError: true},
		{name: "different-token-same-name-conflict", second: input("r1", "tok2", 0.5), wantError: true},
		{name: "different-token-different-name", second: input("r2", "tok2", 0.5)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestBedrockClient(
				t,
				bedrock.NewHandler(bedrock.NewInMemoryBackend("000000000000", "us-east-1")),
			)
			ctx := t.Context()

			first, err := client.CreatePromptRouter(ctx, input("r1", "tok", 0.5))
			require.NoError(t, err)

			second, err := client.CreatePromptRouter(ctx, tt.second)
			if tt.wantError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ConflictException")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantSame, aws.ToString(first.PromptRouterArn) == aws.ToString(second.PromptRouterArn))

			list, err := client.ListPromptRouters(ctx, &bedrocksdk.ListPromptRoutersInput{})
			require.NoError(t, err)

			want := 2
			if tt.wantSame {
				want = 1
			}

			assert.Len(t, list.PromptRouterSummaries, want)
		})
	}
}
