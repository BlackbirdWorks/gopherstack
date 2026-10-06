package bedrock_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

type bedrockSeed struct{ policy, workflow string }

type bedrockPager func(
	ctx context.Context, c *bedrocksdk.Client, s bedrockSeed, size int32, token *string,
) (int, *string, error)

// TestListOps_HonourMaxResultsAndNextToken covers maxResults/nextToken (bedrock@v1.66.4 serializers.go:5814-7285).
func TestListOps_HonourMaxResultsAndNextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list  bedrockPager
		seed  func(t *testing.T, b *bedrock.InMemoryBackend) bedrockSeed
		name  string
		total int
	}{
		{
			name:  "guardrails",
			total: 3,
			seed: func(t *testing.T, b *bedrock.InMemoryBackend) bedrockSeed {
				t.Helper()

				for _, n := range []string{"g-a", "g-b", "g-c"} {
					_, err := b.CreateGuardrail(n, "", "blocked", "blocked", nil)
					require.NoError(t, err)
				}

				return bedrockSeed{}
			},
			list: func(ctx context.Context, c *bedrocksdk.Client, _ bedrockSeed, size int32, tok *string) (int, *string, error) {
				out, err := c.ListGuardrails(
					ctx,
					&bedrocksdk.ListGuardrailsInput{MaxResults: aws.Int32(size), NextToken: tok},
				)
				if err != nil {
					return 0, nil, err
				}

				return len(out.Guardrails), out.NextToken, nil
			},
		},
		{
			name:  "inference_profiles",
			total: 3,
			seed: func(t *testing.T, b *bedrock.InMemoryBackend) bedrockSeed {
				t.Helper()

				for _, n := range []string{"ip-a", "ip-b", "ip-c"} {
					_, err := b.CreateInferenceProfile(n, "", "arn:aws:bedrock:us-east-1::foundation-model/m", nil)
					require.NoError(t, err)
				}

				return bedrockSeed{}
			},
			list: func(ctx context.Context, c *bedrocksdk.Client, _ bedrockSeed, size int32, tok *string) (int, *string, error) {
				out, err := c.ListInferenceProfiles(ctx, &bedrocksdk.ListInferenceProfilesInput{
					MaxResults: aws.Int32(size), NextToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.InferenceProfileSummaries), out.NextToken, nil
			},
		},
		{
			name:  "marketplace_endpoints",
			total: 3,
			seed: func(t *testing.T, b *bedrock.InMemoryBackend) bedrockSeed {
				t.Helper()

				for _, n := range []string{"me-a", "me-b", "me-c"} {
					_, err := b.CreateMarketplaceModelEndpoint(
						n,
						"arn:aws:sagemaker:us-east-1:111111111111:hub-content/x",
						nil,
						nil,
					)
					require.NoError(t, err)
				}

				return bedrockSeed{}
			},
			list: func(ctx context.Context, c *bedrocksdk.Client, _ bedrockSeed, size int32, tok *string) (int, *string, error) {
				out, err := c.ListMarketplaceModelEndpoints(ctx, &bedrocksdk.ListMarketplaceModelEndpointsInput{
					MaxResults: aws.Int32(size), NextToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.MarketplaceModelEndpoints), out.NextToken, nil
			},
		},
		{
			name:  "prompt_routers",
			total: 3,
			seed: func(t *testing.T, b *bedrock.InMemoryBackend) bedrockSeed {
				t.Helper()

				for _, n := range []string{"pr-a", "pr-b", "pr-c"} {
					_, err := b.CreatePromptRouter(
						n,
						"",
						"arn:aws:bedrock:us-east-1::foundation-model/f",
						[]string{"arn:aws:bedrock:us-east-1::foundation-model/m"},
						0.5,
						nil,
					)
					require.NoError(t, err)
				}

				return bedrockSeed{}
			},
			list: func(ctx context.Context, c *bedrocksdk.Client, _ bedrockSeed, size int32, tok *string) (int, *string, error) {
				out, err := c.ListPromptRouters(
					ctx,
					&bedrocksdk.ListPromptRoutersInput{MaxResults: aws.Int32(size), NextToken: tok},
				)
				if err != nil {
					return 0, nil, err
				}

				return len(out.PromptRouterSummaries), out.NextToken, nil
			},
		},
		{
			name:  "ar_build_workflows",
			total: 3,
			seed:  seedARP,
			list: func(ctx context.Context, c *bedrocksdk.Client, s bedrockSeed, size int32, tok *string) (int, *string, error) {
				out, err := c.ListAutomatedReasoningPolicyBuildWorkflows(ctx,
					&bedrocksdk.ListAutomatedReasoningPolicyBuildWorkflowsInput{
						PolicyArn: aws.String(s.policy), MaxResults: aws.Int32(size), NextToken: tok,
					})
				if err != nil {
					return 0, nil, err
				}

				return len(out.AutomatedReasoningPolicyBuildWorkflowSummaries), out.NextToken, nil
			},
		},
		{
			name:  "ar_test_cases",
			total: 3,
			seed:  seedARP,
			list: func(ctx context.Context, c *bedrocksdk.Client, s bedrockSeed, size int32, tok *string) (int, *string, error) {
				out, err := c.ListAutomatedReasoningPolicyTestCases(ctx,
					&bedrocksdk.ListAutomatedReasoningPolicyTestCasesInput{
						PolicyArn: aws.String(s.policy), MaxResults: aws.Int32(size), NextToken: tok,
					})
				if err != nil {
					return 0, nil, err
				}

				return len(out.TestCases), out.NextToken, nil
			},
		},
		{
			name:  "ar_test_results",
			total: 3,
			seed:  seedARP,
			list: func(ctx context.Context, c *bedrocksdk.Client, s bedrockSeed, size int32, tok *string) (int, *string, error) {
				out, err := c.ListAutomatedReasoningPolicyTestResults(ctx,
					&bedrocksdk.ListAutomatedReasoningPolicyTestResultsInput{
						PolicyArn: aws.String(s.policy), BuildWorkflowId: aws.String(s.workflow),
						MaxResults: aws.Int32(size), NextToken: tok,
					})
				if err != nil {
					return 0, nil, err
				}

				return len(out.TestResults), out.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := bedrock.NewInMemoryBackend("111111111111", "us-east-1")
			seeded := tt.seed(t, backend)
			client := newTestBedrockClient(t, bedrock.NewHandler(backend))

			seen := 0

			var token *string

			for range tt.total + 1 {
				n, next, err := tt.list(t.Context(), client, seeded, 2, token)
				require.NoError(t, err)

				seen += n
				token = next

				if token == nil {
					break
				}

				assert.Equal(t, 2, n, "a truncated page is full")
			}

			assert.Equal(t, tt.total, seen)
			assert.Nil(t, token)
		})
	}
}

func seedARP(t *testing.T, b *bedrock.InMemoryBackend) bedrockSeed {
	t.Helper()

	p, err := b.CreateAutomatedReasoningPolicy("arp-paged", "", nil)
	require.NoError(t, err)

	var wf *bedrock.AutomatedReasoningPolicyBuildWorkflow

	for range 3 {
		wf, err = b.StartAutomatedReasoningPolicyBuildWorkflow(p.PolicyArn, "INGEST_CONTENT", nil)
		require.NoError(t, err)

		_, err = b.CreateAutomatedReasoningPolicyTestCase(p.PolicyArn)
		require.NoError(t, err)
	}

	return bedrockSeed{policy: p.PolicyArn, workflow: wf.BuildWorkflowID}
}
