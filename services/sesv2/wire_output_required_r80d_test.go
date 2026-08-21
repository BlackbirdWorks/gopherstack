package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/require"
)

// gopherstack-r80d batch 21: types.TrackingOptions.CustomRedirectDomain is
// required (aws-sdk-go-v2/service/sesv2/types/types.go's TrackingOptions),
// but PutConfigurationSetTrackingOptionsInput only requires
// ConfigurationSetName -- CustomRedirectDomain and HttpsPolicy are both
// optional on input (api_op_PutConfigurationSetTrackingOptions.go), so a
// real client can set HttpsPolicy alone. handler_configuration_sets.go's
// trackingOptionsOutput tagged CustomRedirectDomain `,omitempty`, dropping
// the required key entirely whenever it was reachably empty.
func TestGetConfigurationSet_TrackingOptionsHttpsPolicyOnly_RealClient(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	client := newSESv2SDKClient(t, h)
	ctx := t.Context()

	_, err := client.CreateConfigurationSet(ctx, &sesv2sdk.CreateConfigurationSetInput{
		ConfigurationSetName: aws.String("tracking-https-only"),
	})
	require.NoError(t, err)

	_, err = client.PutConfigurationSetTrackingOptions(ctx, &sesv2sdk.PutConfigurationSetTrackingOptionsInput{
		ConfigurationSetName: aws.String("tracking-https-only"),
		HttpsPolicy:          sesv2types.HttpsPolicyRequire,
	})
	require.NoError(t, err)

	out, err := client.GetConfigurationSet(ctx, &sesv2sdk.GetConfigurationSetInput{
		ConfigurationSetName: aws.String("tracking-https-only"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.TrackingOptions)
	require.NotNil(
		t,
		out.TrackingOptions.CustomRedirectDomain,
		"CustomRedirectDomain is required on TrackingOptions; the key must be present (even empty), not omitted",
	)
	require.Empty(t, aws.ToString(out.TrackingOptions.CustomRedirectDomain))
	require.Equal(t, sesv2types.HttpsPolicyRequire, out.TrackingOptions.HttpsPolicy)
}
