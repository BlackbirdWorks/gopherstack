package lightsail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	"github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInstance_DefaultsAndMetadataOptionsUpdate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		update       *lightsailsdk.UpdateInstanceMetadataOptionsInput
		name         string
		wantTokens   types.HttpTokens
		wantHops     int32
		fromSnapshot bool
	}{
		{
			name:       "tokens omitted resets to optional",
			update:     &lightsailsdk.UpdateInstanceMetadataOptionsInput{HttpPutResponseHopLimit: aws.Int32(2)},
			wantTokens: types.HttpTokensOptional,
			wantHops:   2,
		},
		{
			name:       "tokens required applies",
			update:     &lightsailsdk.UpdateInstanceMetadataOptionsInput{HttpTokens: types.HttpTokensRequired},
			wantTokens: types.HttpTokensRequired,
		},
		{name: "restored from snapshot", fromSnapshot: true, wantTokens: types.HttpTokensOptional},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestClient(t)
			_, err := c.CreateInstances(t.Context(), &lightsailsdk.CreateInstancesInput{
				AvailabilityZone: aws.String("us-east-1a"), BlueprintId: aws.String("amazon_linux_2023"),
				BundleId: aws.String("nano_3_0"), InstanceNames: []string{"i"},
			})
			require.NoError(t, err)

			name := aws.String("i")

			if tc.fromSnapshot {
				_, err = c.CreateInstanceSnapshot(t.Context(), &lightsailsdk.CreateInstanceSnapshotInput{
					InstanceName: name, InstanceSnapshotName: aws.String("snap"),
				})
				require.NoError(t, err)

				_, err = c.CreateInstancesFromSnapshot(t.Context(), &lightsailsdk.CreateInstancesFromSnapshotInput{
					AvailabilityZone: aws.String("us-east-1a"), BundleId: aws.String("nano_3_0"),
					InstanceNames: []string{"dst"}, InstanceSnapshotName: aws.String("snap"),
				})
				require.NoError(t, err)

				name = aws.String("dst")
			}

			got, err := c.GetInstance(t.Context(), &lightsailsdk.GetInstanceInput{InstanceName: name})
			require.NoError(t, err)
			assert.Equal(t, types.IpAddressTypeDualstack, got.Instance.IpAddressType)
			assert.Equal(t, types.HttpTokensOptional, got.Instance.MetadataOptions.HttpTokens)

			if tc.update == nil {
				return
			}

			tc.update.InstanceName = name
			_, err = c.UpdateInstanceMetadataOptions(t.Context(), tc.update)
			require.NoError(t, err)

			got, err = c.GetInstance(t.Context(), &lightsailsdk.GetInstanceInput{InstanceName: name})
			require.NoError(t, err)
			assert.Equal(t, tc.wantTokens, got.Instance.MetadataOptions.HttpTokens)
			assert.Equal(t, tc.wantHops, aws.ToInt32(got.Instance.MetadataOptions.HttpPutResponseHopLimit))
		})
	}
}
