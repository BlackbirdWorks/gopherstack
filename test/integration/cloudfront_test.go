package integration_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func minimalCFDistributionConfig(callerRef, comment string) *cftypes.DistributionConfig {
	return &cftypes.DistributionConfig{
		CallerReference: aws.String(callerRef),
		Comment:         aws.String(comment),
		Enabled:         aws.Bool(true),
		Origins: &cftypes.Origins{
			Quantity: aws.Int32(1),
			Items: []cftypes.Origin{
				{
					Id:         aws.String("origin-1"),
					DomainName: aws.String("example.com"),
					S3OriginConfig: &cftypes.S3OriginConfig{
						OriginAccessIdentity: aws.String(""),
					},
				},
			},
		},
		DefaultCacheBehavior: &cftypes.DefaultCacheBehavior{
			ViewerProtocolPolicy: cftypes.ViewerProtocolPolicyAllowAll,
			TargetOriginId:       aws.String("origin-1"),
			ForwardedValues: &cftypes.ForwardedValues{
				QueryString: aws.Bool(false),
				Cookies: &cftypes.CookiePreference{
					Forward: cftypes.ItemSelectionNone,
				},
			},
			MinTTL: aws.Int64(0),
		},
	}
}

func TestIntegration_CloudFront_DistributionLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFrontClient(t)
	ctx := t.Context()

	callerRef := fmt.Sprintf("ref-%s", uuid.NewString()[:8])
	comment := fmt.Sprintf("test-dist-%s", uuid.NewString()[:8])

	// CreateDistribution
	createOut, err := client.CreateDistribution(ctx, &cloudfront.CreateDistributionInput{
		DistributionConfig: minimalCFDistributionConfig(callerRef, comment),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Distribution)

	distID := aws.ToString(createOut.Distribution.Id)
	require.NotEmpty(t, distID)
	assert.Equal(t, "Deployed", aws.ToString(createOut.Distribution.Status))

	t.Cleanup(func() {
		getOut, gErr := client.GetDistribution(ctx, &cloudfront.GetDistributionInput{
			Id: aws.String(distID),
		})
		if gErr != nil {
			return
		}

		_, _ = client.DeleteDistribution(ctx, &cloudfront.DeleteDistributionInput{
			Id:      aws.String(distID),
			IfMatch: getOut.ETag,
		})
	})

	// GetDistribution
	getOut, err := client.GetDistribution(ctx, &cloudfront.GetDistributionInput{
		Id: aws.String(distID),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Distribution)
	assert.Equal(t, distID, aws.ToString(getOut.Distribution.Id))
	assert.Equal(t, comment, aws.ToString(getOut.Distribution.DistributionConfig.Comment))

	// ListDistributions
	listOut, err := client.ListDistributions(ctx, &cloudfront.ListDistributionsInput{})
	require.NoError(t, err)
	require.NotNil(t, listOut.DistributionList)

	found := false

	for _, d := range listOut.DistributionList.Items {
		if aws.ToString(d.Id) == distID {
			found = true

			break
		}
	}

	assert.True(t, found, "created distribution should appear in list")

	// DeleteDistribution
	_, err = client.DeleteDistribution(ctx, &cloudfront.DeleteDistributionInput{
		Id:      aws.String(distID),
		IfMatch: getOut.ETag,
	})
	require.NoError(t, err)

	// Verify deleted
	listOut2, err := client.ListDistributions(ctx, &cloudfront.ListDistributionsInput{})
	require.NoError(t, err)

	for _, d := range listOut2.DistributionList.Items {
		assert.NotEqual(t, distID, aws.ToString(d.Id), "deleted distribution should not appear in list")
	}
}

func TestIntegration_CloudFront_GetDistributionNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCloudFrontClient(t)
	ctx := t.Context()

	_, err := client.GetDistribution(ctx, &cloudfront.GetDistributionInput{
		Id: aws.String("DOESNOTEXIST"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchDistribution")
}
