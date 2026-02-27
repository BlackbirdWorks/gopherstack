package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/awsconfig"
)

func TestAWSConfig_PutConfigurationRecorder(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	err := b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config")
	require.NoError(t, err)

	recorders := b.DescribeConfigurationRecorders()
	require.Len(t, recorders, 1)
	assert.Equal(t, "default", recorders[0].Name)
	assert.Equal(t, "PENDING", recorders[0].Status)
}

func TestAWSConfig_StartConfigurationRecorder(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config"))

	err := b.StartConfigurationRecorder("default")
	require.NoError(t, err)

	recorders := b.DescribeConfigurationRecorders()
	require.Len(t, recorders, 1)
	assert.Equal(t, "ACTIVE", recorders[0].Status)
}

func TestAWSConfig_StartConfigurationRecorder_NotFound(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	err := b.StartConfigurationRecorder("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, awsconfig.ErrNotFound)
}

func TestAWSConfig_PutDeliveryChannel(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	err := b.PutDeliveryChannel("default", "my-bucket", "arn:aws:sns:us-east-1:000000000000:my-topic")
	require.NoError(t, err)

	channels := b.DescribeDeliveryChannels()
	require.Len(t, channels, 1)
	assert.Equal(t, "default", channels[0].Name)
	assert.Equal(t, "my-bucket", channels[0].S3Bucket)
}

func TestAWSConfig_DescribeDeliveryChannels_Empty(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	channels := b.DescribeDeliveryChannels()
	assert.Empty(t, channels)
}

func TestAWSConfig_DescribeConfigurationRecorders_Empty(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	recorders := b.DescribeConfigurationRecorders()
	assert.Empty(t, recorders)
}
