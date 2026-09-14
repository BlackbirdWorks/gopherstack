package s3_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExpires_PutObject_RealClient covers gopherstack-l4ywn part 2(a): the
// object-level Expires header (PutObject/CopyObject/CreateMultipartUpload,
// s3@v1.111.0 serializers.go:461,1032,8598) was dropped entirely -- never
// stored, never echoed on GetObject/HeadObject (deserializers.go:6936,8899).
func TestExpires_PutObject_RealClient(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := "expires-put-bucket"

	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	tests := []struct {
		name       string
		key        string
		setExpires bool
	}{
		{name: "expires set", key: "with-expires", setExpires: true},
		{name: "expires omitted", key: "without-expires", setExpires: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var wantExpiresString *string
			putInput := &sdk_s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(tc.key),
				Body:   strings.NewReader("payload"),
			}
			if tc.setExpires {
				exp := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
				putInput.Expires = aws.Time(exp)
				wantExpiresString = aws.String(exp.Format(http.TimeFormat))
			}

			_, putErr := client.PutObject(t.Context(), putInput)
			require.NoError(t, putErr)

			getOut, getErr := client.GetObject(t.Context(), &sdk_s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(tc.key),
			})
			require.NoError(t, getErr)
			defer getOut.Body.Close()

			if wantExpiresString == nil {
				assert.Nil(t, getOut.ExpiresString)
			} else {
				require.NotNil(t, getOut.ExpiresString)
				assert.Equal(t, *wantExpiresString, *getOut.ExpiresString)
			}

			headOut, headErr := client.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(tc.key),
			})
			require.NoError(t, headErr)

			if wantExpiresString == nil {
				assert.Nil(t, headOut.ExpiresString)
			} else {
				require.NotNil(t, headOut.ExpiresString)
				assert.Equal(t, *wantExpiresString, *headOut.ExpiresString)
			}
		})
	}
}

// TestExpires_CopyObject_RealClient covers the x-amz-metadata-directive
// handling for Expires: COPY (default) preserves the source's Expires;
// REPLACE requires the copy request's own Expires header.
func TestExpires_CopyObject_RealClient(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := "expires-copy-bucket"

	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	srcExpires := time.Now().UTC().Add(48 * time.Hour).Truncate(time.Second)
	_, err = client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String("src"),
		Body:    strings.NewReader("payload"),
		Expires: aws.Time(srcExpires),
	})
	require.NoError(t, err)

	t.Run("copy directive preserves source Expires", func(t *testing.T) {
		t.Parallel()

		_, copyErr := client.CopyObject(t.Context(), &sdk_s3.CopyObjectInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String("dest-copy"),
			CopySource: aws.String(bucket + "/src"),
		})
		require.NoError(t, copyErr)

		out, getErr := client.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("dest-copy"),
		})
		require.NoError(t, getErr)
		require.NotNil(t, out.ExpiresString)
		assert.Equal(t, srcExpires.Format(http.TimeFormat), *out.ExpiresString)
	})

	t.Run("replace directive sets new Expires", func(t *testing.T) {
		t.Parallel()

		newExpires := time.Now().UTC().Add(1 * time.Hour).Truncate(time.Second)
		_, copyErr := client.CopyObject(t.Context(), &sdk_s3.CopyObjectInput{
			Bucket:            aws.String(bucket),
			Key:               aws.String("dest-replace"),
			CopySource:        aws.String(bucket + "/src"),
			MetadataDirective: types.MetadataDirectiveReplace,
			Expires:           aws.Time(newExpires),
			ContentType:       aws.String("text/plain"),
		})
		require.NoError(t, copyErr)

		out, getErr := client.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("dest-replace"),
		})
		require.NoError(t, getErr)
		require.NotNil(t, out.ExpiresString)
		assert.Equal(t, newExpires.Format(http.TimeFormat), *out.ExpiresString)
	})
}

// TestObjectLockHeaders_RealClient covers gopherstack-l4ywn part 2(c):
// GetObject/HeadObject never surfaced x-amz-object-lock-mode,
// x-amz-object-lock-legal-hold, or x-amz-object-lock-retain-until-date
// (s3@v1.111.0 deserializers.go:6969-6989, :8933-8952) even though the
// underlying retention/legal-hold state already existed in this backend via
// PutObjectRetention/PutObjectLegalHold.
func TestObjectLockHeaders_RealClient(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := "object-lock-headers-bucket"

	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("locked"),
		Body:   strings.NewReader("payload"),
	})
	require.NoError(t, err)

	retainUntil := time.Now().UTC().Add(72 * time.Hour).Truncate(time.Second)
	_, err = client.PutObjectRetention(t.Context(), &sdk_s3.PutObjectRetentionInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("locked"),
		Retention: &types.ObjectLockRetention{
			Mode:            types.ObjectLockRetentionModeGovernance,
			RetainUntilDate: aws.Time(retainUntil),
		},
	})
	require.NoError(t, err)

	_, err = client.PutObjectLegalHold(t.Context(), &sdk_s3.PutObjectLegalHoldInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("locked"),
		LegalHold: &types.ObjectLockLegalHold{
			Status: types.ObjectLockLegalHoldStatusOn,
		},
	})
	require.NoError(t, err)

	t.Run("GetObject", func(t *testing.T) {
		t.Parallel()

		out, getErr := client.GetObject(t.Context(), &sdk_s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("locked"),
		})
		require.NoError(t, getErr)
		defer out.Body.Close()

		assert.Equal(t, types.ObjectLockModeGovernance, out.ObjectLockMode)
		assert.Equal(t, types.ObjectLockLegalHoldStatusOn, out.ObjectLockLegalHoldStatus)
		require.NotNil(t, out.ObjectLockRetainUntilDate)
		assert.True(t, retainUntil.Equal(*out.ObjectLockRetainUntilDate))
	})

	t.Run("HeadObject", func(t *testing.T) {
		t.Parallel()

		out, headErr := client.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("locked"),
		})
		require.NoError(t, headErr)

		assert.Equal(t, types.ObjectLockModeGovernance, out.ObjectLockMode)
		assert.Equal(t, types.ObjectLockLegalHoldStatusOn, out.ObjectLockLegalHoldStatus)
		require.NotNil(t, out.ObjectLockRetainUntilDate)
		assert.True(t, retainUntil.Equal(*out.ObjectLockRetainUntilDate))
	})

	t.Run("no lock configured omits headers", func(t *testing.T) {
		t.Parallel()

		_, putErr := client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("unlocked"),
			Body:   strings.NewReader("payload"),
		})
		require.NoError(t, putErr)

		out, getErr := client.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("unlocked"),
		})
		require.NoError(t, getErr)

		assert.Equal(t, types.ObjectLockMode(""), out.ObjectLockMode)
		assert.Equal(t, types.ObjectLockLegalHoldStatus(""), out.ObjectLockLegalHoldStatus)
		assert.Nil(t, out.ObjectLockRetainUntilDate)
	})
}

// TestAbortIncompleteMultipartUpload_RealClient covers gopherstack-l4ywn part
// 2(b): x-amz-abort-date and x-amz-abort-rule-id (CreateMultipartUpload
// deserializers.go:1124, ListParts :11737) were never computed from the
// bucket's AbortIncompleteMultipartUpload lifecycle rule.
func TestAbortIncompleteMultipartUpload_RealClient(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := "abort-mpu-bucket"

	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	days := int32(3)
	_, err = client.PutBucketLifecycleConfiguration(t.Context(), &sdk_s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("abort-uploads"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{
						Prefix: aws.String("incoming/"),
					},
					AbortIncompleteMultipartUpload: &types.AbortIncompleteMultipartUpload{
						DaysAfterInitiation: aws.Int32(days),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	t.Run("matching prefix gets abort headers", func(t *testing.T) {
		t.Parallel()

		beforeInit := time.Now().UTC()

		created, createErr := client.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("incoming/upload1"),
		})
		require.NoError(t, createErr)
		require.NotNil(t, created.AbortDate)
		require.NotNil(t, created.AbortRuleId)
		assert.Equal(t, "abort-uploads", *created.AbortRuleId)
		assert.True(t, created.AbortDate.After(beforeInit.Add(time.Duration(days)*24*time.Hour-time.Minute)))

		listed, listErr := client.ListParts(t.Context(), &sdk_s3.ListPartsInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String("incoming/upload1"),
			UploadId: created.UploadId,
		})
		require.NoError(t, listErr)
		require.NotNil(t, listed.AbortDate)
		require.NotNil(t, listed.AbortRuleId)
		assert.Equal(t, "abort-uploads", *listed.AbortRuleId)
		assert.True(t, listed.AbortDate.Equal(*created.AbortDate))
	})

	t.Run("non-matching prefix omits abort headers", func(t *testing.T) {
		t.Parallel()

		created, createErr := client.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("other/upload2"),
		})
		require.NoError(t, createErr)
		assert.Nil(t, created.AbortDate)
		assert.Nil(t, created.AbortRuleId)

		listed, listErr := client.ListParts(t.Context(), &sdk_s3.ListPartsInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String("other/upload2"),
			UploadId: created.UploadId,
		})
		require.NoError(t, listErr)
		assert.Nil(t, listed.AbortDate)
		assert.Nil(t, listed.AbortRuleId)
	})
}
