package s3control_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3csdk "github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/aws/aws-sdk-go-v2/service/s3control/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

func TestBucketVersioning_NoStatusUntilConfigured(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		put  types.BucketVersioningStatus
		want types.BucketVersioningStatus
	}{
		{name: "never configured", want: ""},
		{name: "enabled", put: types.BucketVersioningStatusEnabled, want: types.BucketVersioningStatusEnabled},
		{name: "suspended", put: types.BucketVersioningStatusSuspended, want: types.BucketVersioningStatusSuspended},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := s3control.NewInMemoryBackend()
			backend.CreateBucket("123456789012", "vb")
			c := newTestS3ControlClient(t, s3control.NewHandler(backend))

			if tc.put != "" {
				_, err := c.PutBucketVersioning(t.Context(), &s3csdk.PutBucketVersioningInput{
					AccountId: aws.String("123456789012"), Bucket: aws.String("vb"),
					VersioningConfiguration: &types.VersioningConfiguration{Status: tc.put},
				})
				require.NoError(t, err)
			}

			got, err := c.GetBucketVersioning(t.Context(), &s3csdk.GetBucketVersioningInput{
				AccountId: aws.String("123456789012"), Bucket: aws.String("vb"),
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.Status)
		})
	}
}
