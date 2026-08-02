package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/require"
)

// createTestSite creates a minimal Site via the real SDK client and returns
// its ID.
func createTestSite(t *testing.T, client *outpostssdk.Client) string {
	t.Helper()

	out, err := client.CreateSite(t.Context(), &outpostssdk.CreateSiteInput{
		Name: aws.String("test-site"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Site)

	return aws.ToString(out.Site.SiteId)
}

// createTestOutpost creates a minimal Outpost belonging to siteID via the
// real SDK client and returns it.
func createTestOutpost(t *testing.T, client *outpostssdk.Client, siteID string) *types.Outpost {
	t.Helper()

	out, err := client.CreateOutpost(t.Context(), &outpostssdk.CreateOutpostInput{
		Name:                  aws.String("test-outpost"),
		SiteId:                aws.String(siteID),
		SupportedHardwareType: types.SupportedHardwareTypeRack,
	})
	require.NoError(t, err)
	require.NotNil(t, out.Outpost)

	return out.Outpost
}
