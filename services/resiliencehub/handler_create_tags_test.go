package resiliencehub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	resiliencehubsdk "github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/aws/aws-sdk-go-v2/service/resiliencehub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateOpsWithTags_RoundTrip drives every resiliencehub Create* op
// whose real Input struct accepts Tags (resiliencehub@v1.38.3:
// api_op_CreateApp.go, api_op_CreateRecommendationTemplate.go,
// api_op_CreateResiliencyPolicy.go, all `Tags map[string]string`) through
// the real SDK client and asserts ListTagsForResource sees what was
// supplied at creation (gopherstack-2mwl). CreateAppVersionAppComponent and
// CreateAppVersionResource take no Tags field in the real SDK and are
// excluded.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *resiliencehubsdk.Client) string
		name  string
	}{
		{
			name: "app",
			setup: func(t *testing.T, client *resiliencehubsdk.Client) string {
				t.Helper()
				out, err := client.CreateApp(t.Context(), &resiliencehubsdk.CreateAppInput{
					Name: aws.String("tagged-app"),
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.App.AppArn)
			},
		},
		{
			name: "resiliency policy",
			setup: func(t *testing.T, client *resiliencehubsdk.Client) string {
				t.Helper()
				out, err := client.CreateResiliencyPolicy(t.Context(), &resiliencehubsdk.CreateResiliencyPolicyInput{
					PolicyName: aws.String("tagged-policy"),
					Tier:       types.ResiliencyPolicyTierCritical,
					Policy: map[string]types.FailurePolicy{
						"Software": {RtoInSecs: 60, RpoInSecs: 60},
						"Hardware": {RtoInSecs: 60, RpoInSecs: 60},
						"AZ":       {RtoInSecs: 60, RpoInSecs: 60},
						"Region":   {RtoInSecs: 60, RpoInSecs: 60},
					},
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Policy.PolicyArn)
			},
		},
		{
			name: "recommendation template",
			setup: func(t *testing.T, client *resiliencehubsdk.Client) string {
				t.Helper()
				appOut, err := client.CreateApp(t.Context(), &resiliencehubsdk.CreateAppInput{
					Name: aws.String("app-for-template"),
				})
				require.NoError(t, err)

				started, err := client.StartAppAssessment(t.Context(), &resiliencehubsdk.StartAppAssessmentInput{
					AppArn:         appOut.App.AppArn,
					AppVersion:     aws.String("draft"),
					AssessmentName: aws.String("a1"),
				})
				require.NoError(t, err)

				out, err := client.CreateRecommendationTemplate(
					t.Context(),
					&resiliencehubsdk.CreateRecommendationTemplateInput{
						AssessmentArn: started.Assessment.AssessmentArn,
						Name:          aws.String("tagged-template"),
						Tags:          map[string]string{"env": "prod"},
					},
				)
				require.NoError(t, err)

				return aws.ToString(out.RecommendationTemplate.RecommendationTemplateArn)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)

			arn := tc.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.ListTagsForResource(t.Context(), &resiliencehubsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(arn),
			})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "prod"}, got.Tags)
		})
	}
}
