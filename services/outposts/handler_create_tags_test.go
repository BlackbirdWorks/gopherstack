package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateOpsWithTags_RoundTrip drives every outposts Create* op whose
// real Input struct accepts Tags (outposts@v1.66.1: api_op_CreateOutpost.go,
// api_op_CreateSite.go, both `Tags map[string]string`) through the real SDK
// client and asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl). CreateOrder/CreateQuote/CreateRenewal take no Tags
// field in the real SDK and are excluded.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *outpostssdk.Client) string
		name  string
	}{
		{
			name: "site",
			setup: func(t *testing.T, client *outpostssdk.Client) string {
				t.Helper()
				out, err := client.CreateSite(t.Context(), &outpostssdk.CreateSiteInput{
					Name: aws.String("tagged-site"),
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Site.SiteArn)
			},
		},
		{
			name: "outpost",
			setup: func(t *testing.T, client *outpostssdk.Client) string {
				t.Helper()
				siteID := createTestSite(t, client)

				out, err := client.CreateOutpost(t.Context(), &outpostssdk.CreateOutpostInput{
					Name:                  aws.String("tagged-outpost"),
					SiteId:                aws.String(siteID),
					SupportedHardwareType: types.SupportedHardwareTypeRack,
					Tags:                  map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Outpost.OutpostArn)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)

			arn := tc.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.ListTagsForResource(t.Context(), &outpostssdk.ListTagsForResourceInput{
				ResourceArn: aws.String(arn),
			})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "prod"}, got.Tags)
		})
	}
}
