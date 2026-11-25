package s3_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestCreateBucket_RecreateOwned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		region  string
	}{
		{name: "us-east-1 succeeds", region: ""},
		{name: "other region conflicts", region: "eu-west-1", wantErr: s3.ErrBucketAlreadyOwnedByYou},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend(t)
			ctx := t.Context()
			in := &sdk_s3.CreateBucketInput{Bucket: aws.String("again")}
			if tt.region != "" {
				in.CreateBucketConfiguration = &types.CreateBucketConfiguration{
					LocationConstraint: types.BucketLocationConstraint(tt.region),
				}
			}

			_, err := backend.CreateBucket(ctx, in)
			require.NoError(t, err)
			mustPutObject(t, backend, "again", "k", []byte("v"))
			require.NoError(t, backend.PutBucketACL(ctx, "again", "public-read"))

			out, err := backend.CreateBucket(ctx, in)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				acl, aclErr := backend.GetBucketACL(ctx, "again")
				require.NoError(t, aclErr)
				assert.Equal(t, "public-read", acl)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "/again", aws.ToString(out.Location))

			acl, err := backend.GetBucketACL(ctx, "again")
			require.NoError(t, err)
			assert.Equal(t, "private", acl)

			_, err = backend.HeadObject(ctx, &sdk_s3.HeadObjectInput{
				Bucket: aws.String("again"), Key: aws.String("k"),
			})
			require.NoError(t, err)
		})
	}
}
