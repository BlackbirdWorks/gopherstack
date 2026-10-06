package appstream_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFleet_StreamViewDefaultAndPartialUpdateKeepsIdleTimeout(t *testing.T) {
	t.Parallel()

	cases := []struct {
		update   *appstreamsdk.UpdateFleetInput
		name     string
		wantIdle int32
	}{
		{
			name:     "display name only keeps idle timeout",
			update:   &appstreamsdk.UpdateFleetInput{DisplayName: aws.String("d")},
			wantIdle: 120,
		},
		{
			name:     "explicit zero disables idle timeout",
			update:   &appstreamsdk.UpdateFleetInput{IdleDisconnectTimeoutInSeconds: aws.Int32(0)},
			wantIdle: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newRealClient(t)
			_, err := c.CreateFleet(t.Context(), &appstreamsdk.CreateFleetInput{
				Name: aws.String("f"), InstanceType: aws.String("stream.standard.medium"), ImageName: aws.String("img"),
				ComputeCapacity:                &types.ComputeCapacity{DesiredInstances: aws.Int32(1)},
				IdleDisconnectTimeoutInSeconds: aws.Int32(120),
			})
			require.NoError(t, err)

			tc.update.Name = aws.String("f")
			_, err = c.UpdateFleet(t.Context(), tc.update)
			require.NoError(t, err)

			d, err := c.DescribeFleets(t.Context(), &appstreamsdk.DescribeFleetsInput{Names: []string{"f"}})
			require.NoError(t, err)
			assert.Equal(t, types.StreamViewApp, d.Fleets[0].StreamView)
			assert.Equal(t, tc.wantIdle, aws.ToInt32(d.Fleets[0].IdleDisconnectTimeoutInSeconds))
		})
	}
}

func TestStack_UserSettingsDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		settings []types.UserSetting
		wantLen  int
		wantMax  int32
	}{
		{name: "omitted enables defaults", wantLen: 6, wantMax: 20971520},
		{
			name: "clipboard max length defaults",
			settings: []types.UserSetting{
				{Action: types.ActionClipboardCopyToLocalDevice, Permission: types.PermissionEnabled},
			},
			wantLen: 1,
			wantMax: 20971520,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newRealClient(t)
			_, err := c.CreateStack(
				t.Context(),
				&appstreamsdk.CreateStackInput{Name: aws.String("s"), UserSettings: tc.settings},
			)
			require.NoError(t, err)

			d, err := c.DescribeStacks(t.Context(), &appstreamsdk.DescribeStacksInput{Names: []string{"s"}})
			require.NoError(t, err)

			got := d.Stacks[0].UserSettings
			require.Len(t, got, tc.wantLen)

			for _, us := range got {
				assert.Equal(t, types.PermissionEnabled, us.Permission)
			}

			assert.Equal(t, tc.wantMax, aws.ToInt32(got[0].MaximumLength))
		})
	}
}
