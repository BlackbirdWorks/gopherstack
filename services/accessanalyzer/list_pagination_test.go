package accessanalyzer_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestList_HonoursMaxResultsAndNextToken pins the query-bound maxResults and
// nextToken members (accessanalyzer@v1.51.4 serializers.go).
func TestList_HonoursMaxResultsAndNextToken(t *testing.T) {
	t.Parallel()

	const badPolicy = `{"Version":"1999-01-01","Statement":[{"Effect":"Permit","Action":"*","Resource":"*"},` +
		`{"Effect":"Allow","Action":"*","Resource":"*"}]}`

	type listFn func(ctx context.Context, c *aasdk.Client, size *int32, token *string) (int, *string, error)

	tests := []struct {
		setup func(t *testing.T, c *aasdk.Client)
		list  listFn
		name  string
	}{
		{
			name: "analyzers",
			setup: func(t *testing.T, c *aasdk.Client) {
				t.Helper()

				for i := range 3 {
					_, err := c.CreateAnalyzer(t.Context(), &aasdk.CreateAnalyzerInput{
						AnalyzerName: aws.String(fmt.Sprintf("an-%d", i)), Type: aatypes.TypeAccount,
					})
					require.NoError(t, err)
				}
			},
			list: func(ctx context.Context, c *aasdk.Client, s *int32, tok *string) (int, *string, error) {
				o, err := c.ListAnalyzers(ctx, &aasdk.ListAnalyzersInput{MaxResults: s, NextToken: tok})
				if err != nil {
					return 0, nil, err
				}

				return len(o.Analyzers), o.NextToken, nil
			},
		},
		{
			name: "archive_rules",
			setup: func(t *testing.T, c *aasdk.Client) {
				t.Helper()

				_, err := c.CreateAnalyzer(t.Context(), &aasdk.CreateAnalyzerInput{
					AnalyzerName: aws.String("an"), Type: aatypes.TypeAccount,
				})
				require.NoError(t, err)

				for i := range 3 {
					_, err = c.CreateArchiveRule(t.Context(), &aasdk.CreateArchiveRuleInput{
						AnalyzerName: aws.String("an"),
						RuleName:     aws.String(fmt.Sprintf("rule-%d", i)),
						Filter:       map[string]aatypes.Criterion{"resourceType": {Eq: []string{"AWS::S3::Bucket"}}},
					})
					require.NoError(t, err)
				}
			},
			list: func(ctx context.Context, c *aasdk.Client, s *int32, tok *string) (int, *string, error) {
				o, err := c.ListArchiveRules(ctx, &aasdk.ListArchiveRulesInput{
					AnalyzerName: aws.String("an"), MaxResults: s, NextToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(o.ArchiveRules), o.NextToken, nil
			},
		},
		{
			name:  "validate_policy",
			setup: func(*testing.T, *aasdk.Client) {},
			list: func(ctx context.Context, c *aasdk.Client, s *int32, tok *string) (int, *string, error) {
				o, err := c.ValidatePolicy(ctx, &aasdk.ValidatePolicyInput{
					PolicyDocument: aws.String(badPolicy), PolicyType: aatypes.PolicyTypeIdentityPolicy,
					MaxResults: s, NextToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(o.Findings), o.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAccessAnalyzerClient(t, accessanalyzer.NewHandler(
				accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")))
			tt.setup(t, client)

			total, _, err := tt.list(t.Context(), client, nil, nil)
			require.NoError(t, err)
			require.GreaterOrEqual(t, total, 2)

			seen, pages := 0, 0

			var token *string

			for {
				n, next, listErr := tt.list(t.Context(), client, aws.Int32(1), token)
				require.NoError(t, listErr)
				require.Equal(t, 1, n)

				seen++
				pages++

				if next == nil || *next == "" {
					break
				}

				token = next
			}

			require.Equal(t, total, seen)
			require.Equal(t, total, pages)
		})
	}
}
