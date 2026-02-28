package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/awsconfig"
)

func TestAWSConfigBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, b *awsconfig.InMemoryBackend)
	}{
		{
			name: "PutConfigurationRecorder",
			run: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				err := b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config")
				require.NoError(t, err)

				recorders := b.DescribeConfigurationRecorders()
				require.Len(t, recorders, 1)
				assert.Equal(t, "default", recorders[0].Name)
				assert.Equal(t, "PENDING", recorders[0].Status)
			},
		},
		{
			name: "StartConfigurationRecorder",
			run: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config"))

				err := b.StartConfigurationRecorder("default")
				require.NoError(t, err)

				recorders := b.DescribeConfigurationRecorders()
				require.Len(t, recorders, 1)
				assert.Equal(t, "ACTIVE", recorders[0].Status)
			},
		},
		{
			name: "StartConfigurationRecorder/NotFound",
			run: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				err := b.StartConfigurationRecorder("nonexistent")
				require.Error(t, err)
				assert.ErrorIs(t, err, awsconfig.ErrNotFound)
			},
		},
		{
			name: "PutDeliveryChannel",
			run: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				err := b.PutDeliveryChannel("default", "my-bucket", "arn:aws:sns:us-east-1:000000000000:my-topic")
				require.NoError(t, err)

				channels := b.DescribeDeliveryChannels()
				require.Len(t, channels, 1)
				assert.Equal(t, "default", channels[0].Name)
				assert.Equal(t, "my-bucket", channels[0].S3Bucket)
			},
		},
		{
			name: "DescribeDeliveryChannels/Empty",
			run: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				channels := b.DescribeDeliveryChannels()
				assert.Empty(t, channels)
			},
		},
		{
			name: "DescribeConfigurationRecorders/Empty",
			run: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				recorders := b.DescribeConfigurationRecorders()
				assert.Empty(t, recorders)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := awsconfig.NewInMemoryBackend()
			tt.run(t, b)
		})
	}
}
