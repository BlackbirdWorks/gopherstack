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

func TestPlaybackConfiguration_DocumentedDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in           *mediatailorsdk.PutPlaybackConfigurationInput
		name         string
		wantMpd      string
		wantManifest types.OriginManifestType
		wantAdsReq   int32
		wantLive     int32
		wantVod      int32
		wantMaxConc  int32
	}{
		{
			name:         "omitted dash config gets defaults",
			in:           &mediatailorsdk.PutPlaybackConfigurationInput{},
			wantMpd:      "EMT_DEFAULT",
			wantManifest: types.OriginManifestTypeMultiPeriod,
		},
		{
			name: "partial members fill the rest",
			in: &mediatailorsdk.PutPlaybackConfigurationInput{
				DashConfiguration: &types.DashConfigurationForPut{MpdLocation: aws.String("DISABLED")},
				AdsPersonalizationConcurrency: &types.AdsPersonalizationConcurrency{
					EnableVodVastParallelization: aws.Bool(true),
				},
				AdsPersonalizationTimeouts: &types.AdsPersonalizationTimeouts{
					AdsRequestTimeoutMilliseconds: aws.Int32(500),
				},
			},
			wantMpd:      "DISABLED",
			wantManifest: types.OriginManifestTypeMultiPeriod,
			wantAdsReq:   500, wantLive: 10000, wantVod: 10000, wantMaxConc: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestMediaTailorClient(t, mediatailor.NewHandler(
				mediatailor.NewInMemoryBackend("123456789012", "us-east-1")))

			tc.in.Name = aws.String("pc")
			_, err := client.PutPlaybackConfiguration(ctx, tc.in)
			require.NoError(t, err)

			got, err := client.GetPlaybackConfiguration(ctx, &mediatailorsdk.GetPlaybackConfigurationInput{
				Name: aws.String("pc"),
			})
			require.NoError(t, err)

			require.NotNil(t, got.DashConfiguration)
			assert.Equal(t, tc.wantMpd, aws.ToString(got.DashConfiguration.MpdLocation))
			assert.Equal(t, tc.wantManifest, got.DashConfiguration.OriginManifestType)
			assert.Contains(t, aws.ToString(got.DashConfiguration.ManifestEndpointPrefix), "/v1/dash/")

			if tc.in.AdsPersonalizationTimeouts == nil {
				return
			}

			to := got.AdsPersonalizationTimeouts
			require.NotNil(t, to)
			assert.Equal(t, tc.wantAdsReq, aws.ToInt32(to.AdsRequestTimeoutMilliseconds))
			assert.Equal(t, tc.wantLive, aws.ToInt32(to.LiveMaximumAdsPersonalizationTimeMilliseconds))
			assert.Equal(t, tc.wantVod, aws.ToInt32(to.VodMaximumAdsPersonalizationTimeMilliseconds))
			require.NotNil(t, got.AdsPersonalizationConcurrency)
			assert.Equal(t, tc.wantMaxConc, aws.ToInt32(got.AdsPersonalizationConcurrency.MaxConcurrentAdsRequests))
			assert.True(t, aws.ToBool(got.AdsPersonalizationConcurrency.EnableVodVastParallelization))
		})
	}
}
