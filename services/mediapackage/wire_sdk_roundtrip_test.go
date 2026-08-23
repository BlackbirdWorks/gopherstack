package mediapackage_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediapackagesdk "github.com/aws/aws-sdk-go-v2/service/mediapackage"
	mediapackagetypes "github.com/aws/aws-sdk-go-v2/service/mediapackage/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediapackage"
)

// TestCreateOriginEndpoint_AuthorizationMssPackage_SDKRoundTrip drives
// CreateOriginEndpoint/DescribeOriginEndpoint through the real aws-sdk-go-v2
// client (not gopherstack's own JSON tags) for every field on Authorization
// and the full MssPackage->MssEncryption->SpekeKeyProvider->
// EncryptionContractConfiguration/StreamSelection chain -- the two packaging
// blocks this service models to full SDK depth (mediapackage@v1.42.4
// types/types.go:10-26,563-590,681-736). Confirms the real client's own
// serializer/deserializer round-trips every leaf field, not just that
// gopherstack's Go structs match by inspection.
func TestCreateOriginEndpoint_AuthorizationMssPackage_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := mediapackage.NewInMemoryBackend("000000000000", mediapackageTagsRTRegion)
	client := newTestMediaPackageClient(t, mediapackage.NewHandler(backend))

	_, err := client.CreateChannel(t.Context(), &mediapackagesdk.CreateChannelInput{
		Id: aws.String("mss-auth-channel"),
	})
	require.NoError(t, err)

	in := &mediapackagesdk.CreateOriginEndpointInput{
		ChannelId: aws.String("mss-auth-channel"),
		Id:        aws.String("mss-auth-endpoint"),
		Authorization: &mediapackagetypes.Authorization{
			CdnIdentifierSecret: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:cdn"),
			SecretsRoleArn:      aws.String("arn:aws:iam::000000000000:role/CDNRole"),
		},
		MssPackage: &mediapackagetypes.MssPackage{
			ManifestWindowSeconds:  aws.Int32(60),
			SegmentDurationSeconds: aws.Int32(4),
			Encryption: &mediapackagetypes.MssEncryption{
				SpekeKeyProvider: &mediapackagetypes.SpekeKeyProvider{
					ResourceId:     aws.String("res-1"),
					RoleArn:        aws.String("arn:aws:iam::000000000000:role/SpekeRole"),
					Url:            aws.String("https://speke.example.com"),
					CertificateArn: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/abc"),
					SystemIds:      []string{"sys-1", "sys-2"},
					EncryptionContractConfiguration: &mediapackagetypes.EncryptionContractConfiguration{
						PresetSpeke20Audio: mediapackagetypes.PresetSpeke20AudioPresetAudio1,
						PresetSpeke20Video: mediapackagetypes.PresetSpeke20VideoPresetVideo1,
					},
				},
			},
			StreamSelection: &mediapackagetypes.StreamSelection{
				StreamOrder:           mediapackagetypes.StreamOrderVideoBitrateDescending,
				MaxVideoBitsPerSecond: aws.Int32(5000000),
				MinVideoBitsPerSecond: aws.Int32(100000),
			},
		},
	}

	created, err := client.CreateOriginEndpoint(t.Context(), in)
	require.NoError(t, err)

	described, err := client.DescribeOriginEndpoint(t.Context(), &mediapackagesdk.DescribeOriginEndpointInput{
		Id: aws.String("mss-auth-endpoint"),
	})
	require.NoError(t, err)

	for _, out := range []*mediapackagesdk.DescribeOriginEndpointOutput{
		{
			Arn: created.Arn, Authorization: created.Authorization, MssPackage: created.MssPackage,
		},
		described,
	} {
		require.NotNil(t, out.Authorization)
		assert.Equal(t, "arn:aws:secretsmanager:us-east-1:000000000000:secret:cdn",
			aws.ToString(out.Authorization.CdnIdentifierSecret))
		assert.Equal(t, "arn:aws:iam::000000000000:role/CDNRole", aws.ToString(out.Authorization.SecretsRoleArn))

		require.NotNil(t, out.MssPackage)
		assert.Equal(t, int32(60), aws.ToInt32(out.MssPackage.ManifestWindowSeconds))
		assert.Equal(t, int32(4), aws.ToInt32(out.MssPackage.SegmentDurationSeconds))

		require.NotNil(t, out.MssPackage.Encryption)
		speke := out.MssPackage.Encryption.SpekeKeyProvider
		require.NotNil(t, speke)
		assert.Equal(t, "res-1", aws.ToString(speke.ResourceId))
		assert.Equal(t, "arn:aws:iam::000000000000:role/SpekeRole", aws.ToString(speke.RoleArn))
		assert.Equal(t, "https://speke.example.com", aws.ToString(speke.Url))
		assert.Equal(t, "arn:aws:acm:us-east-1:000000000000:certificate/abc", aws.ToString(speke.CertificateArn))
		assert.Equal(t, []string{"sys-1", "sys-2"}, speke.SystemIds)

		require.NotNil(t, speke.EncryptionContractConfiguration)
		assert.Equal(
			t,
			mediapackagetypes.PresetSpeke20AudioPresetAudio1,
			speke.EncryptionContractConfiguration.PresetSpeke20Audio,
		)
		assert.Equal(
			t,
			mediapackagetypes.PresetSpeke20VideoPresetVideo1,
			speke.EncryptionContractConfiguration.PresetSpeke20Video,
		)

		require.NotNil(t, out.MssPackage.StreamSelection)
		assert.Equal(t, mediapackagetypes.StreamOrderVideoBitrateDescending, out.MssPackage.StreamSelection.StreamOrder)
		assert.Equal(t, int32(5000000), aws.ToInt32(out.MssPackage.StreamSelection.MaxVideoBitsPerSecond))
		assert.Equal(t, int32(100000), aws.ToInt32(out.MssPackage.StreamSelection.MinVideoBitsPerSecond))
	}
}
