package s3control_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3csdk "github.com/aws/aws-sdk-go-v2/service/s3control"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

// TestGetBucketTagging_NoSuchTagSet proves the required-output-member sweep
// fix: GetBucketTaggingOutput.TagSet is required (api_op_GetBucketTagging.go),
// but a bucket that has never had PutBucketTagging called (or had it removed
// via DeleteBucketTagging) has no tag set at all, not an empty one -- the
// real op documents this as its own special error, "Error code:
// NoSuchTagSetError -- There is no tag set associated with the bucket"
// (api_op_GetBucketTagging.go doc comment). Before the fix, gopherstack
// returned 200 with a TagSet built from a nil map lookup, which -- combined
// with the "TagSet>member" nested-path XML tag on a nil slice -- silently
// dropped the required TagSet element from the wire entirely rather than
// returning the documented error.
func TestGetBucketTagging_NoSuchTagSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tag  bool
	}{
		{name: "never tagged", tag: false},
		{name: "tagged", tag: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackendWithConfig(createTagsTestAccountID, createTagsTestRegion)
			b.CreateBucket(createTagsTestAccountID, "resp-bucket")
			if tt.tag {
				require.NoError(t, b.PutBucketTagging("resp-bucket", s3control.TagSet{"env": "prod"}))
			}

			client := newTestS3ControlClient(t, s3control.NewHandler(b))

			out, err := client.GetBucketTagging(t.Context(), &s3csdk.GetBucketTaggingInput{
				AccountId: aws.String(createTagsTestAccountID),
				Bucket:    aws.String("resp-bucket"),
			})

			if !tt.tag {
				require.Error(t, err)

				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Contains(t, apiErr.ErrorCode(), "NoSuchTagSetError")

				return
			}

			require.NoError(t, err)
			require.Len(t, out.TagSet, 1)
			assert.Equal(t, "env", aws.ToString(out.TagSet[0].Key))
			assert.Equal(t, "prod", aws.ToString(out.TagSet[0].Value))
		})
	}
}
