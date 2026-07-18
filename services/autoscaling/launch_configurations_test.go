package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestInMemoryBackend_LaunchConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "create_launch_configuration",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				lc, err := b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "my-lc",
					ImageID:                 "ami-12345678",
					InstanceType:            "t2.micro",
				})
				require.NoError(t, err)
				assert.Equal(t, "my-lc", lc.LaunchConfigurationName)
				assert.Equal(t, "ami-12345678", lc.ImageID)
				assert.Equal(t, "t2.micro", lc.InstanceType)
				assert.NotEmpty(t, lc.LaunchConfigurationARN)
			},
		},
		{
			name: "create_launch_configuration_duplicate",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "dup-lc",
					ImageID:                 "ami-12345678",
					InstanceType:            "t2.micro",
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "dup-lc",
					ImageID:                 "ami-12345678",
					InstanceType:            "t2.micro",
				})
				require.Error(t, err)
			},
		},
		{
			name: "describe_launch_configurations",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "lc-1",
					ImageID:                 "ami-aaa",
					InstanceType:            "t2.micro",
				})
				_, _ = b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "lc-2",
					ImageID:                 "ami-bbb",
					InstanceType:            "t2.small",
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				lcs, err := b.DescribeLaunchConfigurations(nil)
				require.NoError(t, err)
				require.Len(t, lcs, 2)
				assert.Equal(t, "lc-1", lcs[0].LaunchConfigurationName)
				assert.Equal(t, "lc-2", lcs[1].LaunchConfigurationName)
			},
		},
		{
			name: "delete_launch_configuration",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "del-lc",
					ImageID:                 "ami-12345678",
					InstanceType:            "t2.micro",
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				err := b.DeleteLaunchConfiguration("del-lc")
				require.NoError(t, err)

				lcs, err := b.DescribeLaunchConfigurations(nil)
				require.NoError(t, err)
				assert.Empty(t, lcs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			tt.run(t, b)
		})
	}
}

func TestInMemoryBackend_CreateLaunchConfigurationExtended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *autoscaling.InMemoryBackend)
		name string
	}{
		{
			name: "spot_price_stored",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "spot-lc",
					ImageID:                 "ami-abc",
					InstanceType:            "t2.micro",
					SpotPrice:               "0.05",
				})
				require.NoError(t, err)

				lcs, err := b.DescribeLaunchConfigurations([]string{"spot-lc"})
				require.NoError(t, err)
				assert.Equal(t, "0.05", lcs[0].SpotPrice)
			},
		},
		{
			name: "block_device_mapping_with_ebs_stored",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				bdm := autoscaling.BlockDeviceMapping{
					DeviceName: "/dev/sda1",
					Ebs: &autoscaling.EbsBlockDevice{
						VolumeType:          "gp3",
						VolumeSize:          50,
						Iops:                3000,
						DeleteOnTermination: true,
					},
				}

				_, err := b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName: "ebs-lc",
					ImageID:                 "ami-abc",
					InstanceType:            "t2.micro",
					BlockDeviceMappings:     []autoscaling.BlockDeviceMapping{bdm},
				})
				require.NoError(t, err)

				lcs, err := b.DescribeLaunchConfigurations([]string{"ebs-lc"})
				require.NoError(t, err)
				require.Len(t, lcs[0].BlockDeviceMappings, 1)
				require.NotNil(t, lcs[0].BlockDeviceMappings[0].Ebs)
				assert.Equal(t, "gp3", lcs[0].BlockDeviceMappings[0].Ebs.VolumeType)
				assert.Equal(t, int32(50), lcs[0].BlockDeviceMappings[0].Ebs.VolumeSize)
			},
		},
		{
			name: "associate_public_ip_address_stored",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
					LaunchConfigurationName:  "pubip-lc",
					ImageID:                  "ami-abc",
					InstanceType:             "t2.micro",
					AssociatePublicIPAddress: true,
					EbsOptimized:             true,
					InstanceMonitoring:       true,
				})
				require.NoError(t, err)

				lcs, err := b.DescribeLaunchConfigurations([]string{"pubip-lc"})
				require.NoError(t, err)
				assert.True(t, lcs[0].AssociatePublicIPAddress)
				assert.True(t, lcs[0].EbsOptimized)
				assert.True(t, lcs[0].InstanceMonitoring)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			tt.run(t, b)
		})
	}
}
