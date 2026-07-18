package firehose_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func TestUpdateDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		setup      func(b *firehose.InMemoryBackend)
		newDest    *firehose.S3DestinationDescription
		name       string
		streamName string
	}{
		{
			name:       "success",
			streamName: "update-stream",
			setup: func(b *firehose.InMemoryBackend) {
				_, _ = b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "update-stream"})
			},
			newDest: &firehose.S3DestinationDescription{BucketARN: "arn:aws:s3:::new-bucket"},
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			wantErr:    firehose.ErrNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}
			err := b.UpdateDestination(
				context.TODO(), tt.streamName, "1", firehose.UpdateDestinationInput{S3Destination: tt.newDest},
			)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			s, descErr := b.DescribeDeliveryStream(context.TODO(), tt.streamName)
			require.NoError(t, descErr)
			require.NotNil(t, s.S3Destination)
			assert.Equal(t, "arn:aws:s3:::new-bucket", s.S3Destination.BucketARN)
		})
	}
}

// TestUpdateDestination_VersionCheck verifies CurrentDeliveryStreamVersionId matching
// rules: empty is rejected, mismatched is rejected, matching succeeds.
func TestUpdateDestination_VersionCheck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		currentVersionID string
		wantErr          bool
	}{
		{
			name:             "matching_version",
			currentVersionID: "1",
			wantErr:          false,
		},
		{
			name:             "empty_version_rejected",
			currentVersionID: "",
			wantErr:          true,
		},
		{
			name:             "mismatched_version",
			currentVersionID: "99",
			wantErr:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "ver-stream"})
			require.NoError(t, err)

			err = b.UpdateDestination(
				context.TODO(),
				"ver-stream",
				tt.currentVersionID,
				firehose.UpdateDestinationInput{
					S3Destination: &firehose.S3DestinationDescription{
						BucketARN: "arn:aws:s3:::new-bucket",
					},
				},
			)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, firehose.ErrValidation)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestUpdateDestination_IncrementsVersionID verifies that a successful update bumps
// the stream's VersionID.
func TestUpdateDestination_IncrementsVersionID(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "ver-inc"})
	require.NoError(t, err)

	err = b.UpdateDestination(context.TODO(), "ver-inc", "1", firehose.UpdateDestinationInput{
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::bucket",
		},
	})
	require.NoError(t, err)

	s, err := b.DescribeDeliveryStream(context.TODO(), "ver-inc")
	require.NoError(t, err)
	assert.Equal(t, "2", s.VersionID)
}

// TestUpdateDestination_SwitchType verifies UpdateDestination can switch a stream's
// destination type, clearing the previous destination.
func TestUpdateDestination_SwitchType(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name:          "switch-stream",
		S3Destination: &firehose.S3DestinationDescription{BucketARN: "arn:aws:s3:::orig"},
	})
	require.NoError(t, err)

	err = b.UpdateDestination(context.TODO(), "switch-stream", "1", firehose.UpdateDestinationInput{
		HTTPEndpointDestination: &firehose.HTTPEndpointDestinationDescription{
			EndpointConfiguration: &firehose.HTTPEndpointConfiguration{URL: "https://example.com"},
		},
	})
	require.NoError(t, err)

	s, err := b.DescribeDeliveryStream(context.TODO(), "switch-stream")
	require.NoError(t, err)
	assert.Nil(t, s.S3Destination, "S3 destination should be cleared after switching to HTTP")
	require.NotNil(t, s.HTTPEndpointDestination)
	assert.Equal(t, "https://example.com", s.HTTPEndpointDestination.EndpointConfiguration.URL)
	assert.Equal(t, "2", s.VersionID)
}

// TestUpdateDestination_RequiresVersion verifies the CurrentDeliveryStreamVersionId
// is mandatory and multiple/zero destination updates are rejected.
func TestUpdateDestination_RequiresVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   firehose.UpdateDestinationInput
		name    string
		version string
	}{
		{
			name:    "empty_version",
			version: "",
			input: firehose.UpdateDestinationInput{
				S3Destination: &firehose.S3DestinationDescription{BucketARN: "arn:aws:s3:::b"},
			},
		},
		{
			name:    "no_destination",
			version: "1",
			input:   firehose.UpdateDestinationInput{},
		},
		{
			name:    "two_destinations",
			version: "1",
			input: firehose.UpdateDestinationInput{
				S3Destination:           &firehose.S3DestinationDescription{BucketARN: "arn:aws:s3:::b"},
				HTTPEndpointDestination: &firehose.HTTPEndpointDestinationDescription{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
				Name:          "ver-req",
				S3Destination: &firehose.S3DestinationDescription{BucketARN: "arn:aws:s3:::orig"},
			})
			require.NoError(t, err)

			updErr := b.UpdateDestination(context.TODO(), "ver-req", tt.version, tt.input)
			require.Error(t, updErr)
			assert.ErrorIs(t, updErr, firehose.ErrValidation)
		})
	}
}
