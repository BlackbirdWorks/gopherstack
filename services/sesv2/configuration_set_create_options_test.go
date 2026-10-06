package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateConfigurationSet_PersistsOptionBlocks(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in     *sesv2sdk.CreateConfigurationSetInput
		assert func(t *testing.T, got *sesv2sdk.GetConfigurationSetOutput)
		name   string
	}{
		{
			name: "all option blocks round-trip",
			in: &sesv2sdk.CreateConfigurationSetInput{
				SendingOptions:    &types.SendingOptions{SendingEnabled: false},
				ReputationOptions: &types.ReputationOptions{ReputationMetricsEnabled: true},
				DeliveryOptions: &types.DeliveryOptions{
					TlsPolicy:       types.TlsPolicyRequire,
					SendingPoolName: aws.String("pool"),
				},
				TrackingOptions: &types.TrackingOptions{CustomRedirectDomain: aws.String("track.example.com")},
				SuppressionOptions: &types.SuppressionOptions{
					SuppressedReasons: []types.SuppressionListReason{types.SuppressionListReasonBounce},
				},
				ArchivingOptions: &types.ArchivingOptions{
					ArchiveArn: aws.String("arn:aws:ses:us-east-1:000000000000:mailmanager-archive/a"),
				},
			},
			assert: func(t *testing.T, got *sesv2sdk.GetConfigurationSetOutput) {
				t.Helper()
				assert.False(t, got.SendingOptions.SendingEnabled)
				assert.True(t, got.ReputationOptions.ReputationMetricsEnabled)
				assert.Equal(t, types.TlsPolicyRequire, got.DeliveryOptions.TlsPolicy)
				assert.Equal(t, "pool", aws.ToString(got.DeliveryOptions.SendingPoolName))
				assert.Equal(t, "track.example.com", aws.ToString(got.TrackingOptions.CustomRedirectDomain))
				assert.Equal(
					t,
					[]types.SuppressionListReason{types.SuppressionListReasonBounce},
					got.SuppressionOptions.SuppressedReasons,
				)
				assert.NotEmpty(t, aws.ToString(got.ArchivingOptions.ArchiveArn))
			},
		},
		{
			name: "omitted blocks keep sending enabled",
			in:   &sesv2sdk.CreateConfigurationSetInput{},
			assert: func(t *testing.T, got *sesv2sdk.GetConfigurationSetOutput) {
				t.Helper()
				assert.True(t, got.SendingOptions.SendingEnabled)
				assert.Nil(t, got.TrackingOptions)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newSESv2TestHandler(t)
			c := newSESv2SDKClient(t, h)
			ctx := t.Context()

			tc.in.ConfigurationSetName = aws.String("cs")
			_, err := c.CreateConfigurationSet(ctx, tc.in)
			require.NoError(t, err)

			got, err := c.GetConfigurationSet(
				ctx,
				&sesv2sdk.GetConfigurationSetInput{ConfigurationSetName: aws.String("cs")},
			)
			require.NoError(t, err)
			tc.assert(t, got)
		})
	}
}
