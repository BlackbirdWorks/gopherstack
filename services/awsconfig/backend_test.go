package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestAWSConfigBackend_PutConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		recName    string
		roleARN    string
		wantName   string
		wantStatus string
		wantLen    int
	}{
		{
			name:       "success",
			recName:    "default",
			roleARN:    "arn:aws:iam::000000000000:role/config",
			wantLen:    1,
			wantName:   "default",
			wantStatus: "PENDING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			err := b.PutConfigurationRecorder(tt.recName, tt.roleARN)
			require.NoError(t, err)

			recorders := b.DescribeConfigurationRecorders()
			require.Len(t, recorders, tt.wantLen)
			assert.Equal(t, tt.wantName, recorders[0].Name)
			assert.Equal(t, tt.wantStatus, recorders[0].Status)
		})
	}
}

func TestAWSConfigBackend_StartConfigurationRecorder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		recName    string
		setup      func(t *testing.T, b *awsconfig.InMemoryBackend)
		wantErr    error
		wantStatus string
	}{
		{
			name:    "success",
			recName: "default",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config"))
			},
			wantStatus: "ACTIVE",
		},
		{
			name:    "not_found",
			recName: "nonexistent",
			wantErr: awsconfig.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.StartConfigurationRecorder(tt.recName)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			recorders := b.DescribeConfigurationRecorders()
			require.Len(t, recorders, 1)
			assert.Equal(t, tt.wantStatus, recorders[0].Status)
		})
	}
}

func TestAWSConfigBackend_PutDeliveryChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		chanName   string
		bucket     string
		topic      string
		wantName   string
		wantBucket string
		wantLen    int
	}{
		{
			name:       "success",
			chanName:   "default",
			bucket:     "my-bucket",
			topic:      "arn:aws:sns:us-east-1:000000000000:my-topic",
			wantLen:    1,
			wantName:   "default",
			wantBucket: "my-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			err := b.PutDeliveryChannel(tt.chanName, tt.bucket, tt.topic)
			require.NoError(t, err)

			channels := b.DescribeDeliveryChannels()
			require.Len(t, channels, tt.wantLen)
			assert.Equal(t, tt.wantName, channels[0].Name)
			assert.Equal(t, tt.wantBucket, channels[0].S3Bucket)
		})
	}
}

func TestAWSConfigBackend_DescribeDeliveryChannels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *awsconfig.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name: "one_channel",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(
					t,
					b.PutDeliveryChannel("default", "my-bucket", "arn:aws:sns:us-east-1:000000000000:my-topic"),
				)
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			channels := b.DescribeDeliveryChannels()
			assert.Len(t, channels, tt.wantCount)
		})
	}
}

func TestAWSConfigBackend_DescribeConfigurationRecorders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *awsconfig.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name: "one_recorder",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationRecorder("default", "arn:aws:iam::000000000000:role/config"))
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			recorders := b.DescribeConfigurationRecorders()
			assert.Len(t, recorders, tt.wantCount)
		})
	}
}
