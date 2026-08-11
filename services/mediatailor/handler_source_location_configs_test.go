package mediatailor_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediatailorsdk "github.com/aws/aws-sdk-go-v2/service/mediatailor"
	"github.com/aws/aws-sdk-go-v2/service/mediatailor/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// TestCreateSourceLocation_AccessAndSegmentDeliveryConfig_RoundTrip verifies
// AccessConfiguration, DefaultSegmentDeliveryConfiguration, and
// SegmentDeliveryConfigurations -- previously unmodeled fields on
// SourceLocation (gopherstack-vdrs item 1) -- survive a real
// CreateSourceLocation -> DescribeSourceLocation round trip through the
// aws-sdk-go-v2 client.
func TestCreateSourceLocation_AccessAndSegmentDeliveryConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := mediatailor.NewInMemoryBackend("000000000000", mediatailorTagsRTRegion)
	client := newTestMediaTailorClient(t, mediatailor.NewHandler(backend))

	_, err := client.CreateSourceLocation(t.Context(), &mediatailorsdk.CreateSourceLocationInput{
		SourceLocationName: aws.String("sl-configs"),
		HttpConfiguration: &types.HttpConfiguration{
			BaseUrl: aws.String("https://example.com"),
		},
		AccessConfiguration: &types.AccessConfiguration{
			AccessType: types.AccessTypeSecretsManagerAccessToken,
			SecretsManagerAccessTokenConfiguration: &types.SecretsManagerAccessTokenConfiguration{
				HeaderName:      aws.String("X-Access-Token"),
				SecretArn:       aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret"),
				SecretStringKey: aws.String("token"),
			},
		},
		DefaultSegmentDeliveryConfiguration: &types.DefaultSegmentDeliveryConfiguration{
			BaseUrl: aws.String("https://cdn.example.com"),
		},
		SegmentDeliveryConfigurations: []types.SegmentDeliveryConfiguration{
			{Name: aws.String("primary"), BaseUrl: aws.String("https://cdn1.example.com")},
			{Name: aws.String("secondary"), BaseUrl: aws.String("https://cdn2.example.com")},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeSourceLocation(t.Context(), &mediatailorsdk.DescribeSourceLocationInput{
		SourceLocationName: aws.String("sl-configs"),
	})
	require.NoError(t, err)

	require.NotNil(t, out.AccessConfiguration)
	assert.Equal(t, types.AccessTypeSecretsManagerAccessToken, out.AccessConfiguration.AccessType)
	require.NotNil(t, out.AccessConfiguration.SecretsManagerAccessTokenConfiguration)
	smat := out.AccessConfiguration.SecretsManagerAccessTokenConfiguration
	assert.Equal(t, "X-Access-Token", aws.ToString(smat.HeaderName))
	assert.Equal(t, "token", aws.ToString(smat.SecretStringKey))

	require.NotNil(t, out.DefaultSegmentDeliveryConfiguration)
	assert.Equal(t, "https://cdn.example.com", aws.ToString(out.DefaultSegmentDeliveryConfiguration.BaseUrl))

	require.Len(t, out.SegmentDeliveryConfigurations, 2)
	assert.Equal(t, "primary", aws.ToString(out.SegmentDeliveryConfigurations[0].Name))
	assert.Equal(t, "secondary", aws.ToString(out.SegmentDeliveryConfigurations[1].Name))
}

// TestCreateSourceLocation_InvalidAccessType rejects an AccessType outside
// the real S3_SIGV4/SECRETS_MANAGER_ACCESS_TOKEN/AUTODETECT_SIGV4 enum
// instead of silently accepting it (mediatailor@v1.63.4's AccessType enum,
// types/enums.go).
func TestCreateSourceLocation_InvalidAccessType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "POST", "/sourceLocation/bad-access-type", map[string]any{
		"HttpConfiguration": map[string]any{"BaseUrl": "https://example.com"},
		"AccessConfiguration": map[string]any{
			"AccessType": "NOT_A_REAL_TYPE",
		},
	})
	assert.Equal(t, 400, rec.Code, rec.Body.String())
}
