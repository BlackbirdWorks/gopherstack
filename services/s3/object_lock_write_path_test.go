package s3_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func apiCode(err error) string {
	var apiErr smithy.APIError
	if err == nil || !errors.As(err, &apiErr) {
		return ""
	}

	return apiErr.ErrorCode()
}

func TestObjectLockBucketCreation(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	ctx := t.Context()

	_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{
		Bucket: aws.String("lock-bkt"), ObjectLockEnabledForBucket: aws.Bool(true),
	})
	require.NoError(t, err)

	ver, err := client.GetBucketVersioning(ctx, &sdk_s3.GetBucketVersioningInput{Bucket: aws.String("lock-bkt")})
	require.NoError(t, err)
	assert.Equal(t, types.BucketVersioningStatusEnabled, ver.Status)

	cfg, err := client.GetObjectLockConfiguration(ctx, &sdk_s3.GetObjectLockConfigurationInput{
		Bucket: aws.String("lock-bkt"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockEnabledEnabled, cfg.ObjectLockConfiguration.ObjectLockEnabled)
	assert.Nil(t, cfg.ObjectLockConfiguration.Rule)

	_, err = client.PutBucketVersioning(ctx, &sdk_s3.PutBucketVersioningInput{
		Bucket: aws.String("lock-bkt"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusSuspended,
		},
	})
	assert.Equal(t, "InvalidBucketState", apiCode(err))

	_, err = client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String("plain-bkt")})
	require.NoError(t, err)

	_, err = client.GetObjectLockConfiguration(ctx, &sdk_s3.GetObjectLockConfigurationInput{
		Bucket: aws.String("plain-bkt"),
	})
	assert.Equal(t, "ObjectLockConfigurationNotFoundError", apiCode(err))
}

func TestObjectLockEnableOnExistingBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErr    string
		versioning bool
	}{
		{name: "versioned", versioning: true},
		{name: "unversioned", wantErr: "InvalidBucketState"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			ctx := t.Context()

			_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{Bucket: aws.String("existing")})
			require.NoError(t, err)

			if tt.versioning {
				_, err = client.PutBucketVersioning(ctx, &sdk_s3.PutBucketVersioningInput{
					Bucket: aws.String("existing"),
					VersioningConfiguration: &types.VersioningConfiguration{
						Status: types.BucketVersioningStatusEnabled,
					},
				})
				require.NoError(t, err)
			}

			_, err = client.PutObjectLockConfiguration(ctx, &sdk_s3.PutObjectLockConfigurationInput{
				Bucket: aws.String("existing"),
				ObjectLockConfiguration: &types.ObjectLockConfiguration{
					ObjectLockEnabled: types.ObjectLockEnabledEnabled,
				},
			})
			assert.Equal(t, tt.wantErr, apiCode(err))
		})
	}
}

func TestPutObjectLockHeaders(t *testing.T) {
	t.Parallel()

	future := time.Now().Add(48 * time.Hour).UTC().Truncate(time.Second)

	tests := []struct {
		in         func(*sdk_s3.PutObjectInput)
		name       string
		wantErr    string
		wantMode   types.ObjectLockMode
		wantHold   types.ObjectLockLegalHoldStatus
		defaultDay int32
		bucketLock bool
		wantUntil  bool
	}{
		{
			name: "explicit governance", bucketLock: true, wantMode: types.ObjectLockModeGovernance, wantUntil: true,
			in: func(in *sdk_s3.PutObjectInput) {
				in.ObjectLockMode = types.ObjectLockModeGovernance
				in.ObjectLockRetainUntilDate = aws.Time(future)
			},
		},
		{
			name: "legal hold", bucketLock: true, wantHold: types.ObjectLockLegalHoldStatusOn,
			in: func(in *sdk_s3.PutObjectInput) { in.ObjectLockLegalHoldStatus = types.ObjectLockLegalHoldStatusOn },
		},
		{
			name: "default retention", bucketLock: true, defaultDay: 1, wantMode: types.ObjectLockModeCompliance,
			wantUntil: true, in: func(*sdk_s3.PutObjectInput) {},
		},
		{
			name: "mode without date", bucketLock: true, wantErr: "InvalidArgument",
			in: func(in *sdk_s3.PutObjectInput) { in.ObjectLockMode = types.ObjectLockModeGovernance },
		},
		{
			name: "past date", bucketLock: true, wantErr: "InvalidArgument",
			in: func(in *sdk_s3.PutObjectInput) {
				in.ObjectLockMode = types.ObjectLockModeCompliance
				in.ObjectLockRetainUntilDate = aws.Time(time.Now().Add(-time.Hour))
			},
		},
		{
			name: "bucket without lock", wantErr: "InvalidRequest",
			in: func(in *sdk_s3.PutObjectInput) { in.ObjectLockLegalHoldStatus = types.ObjectLockLegalHoldStatusOn },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealS3ClientTest(t)
			ctx := t.Context()

			_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{
				Bucket: aws.String("lock-put"), ObjectLockEnabledForBucket: aws.Bool(tt.bucketLock),
			})
			require.NoError(t, err)

			if tt.defaultDay > 0 {
				_, err = client.PutObjectLockConfiguration(ctx, &sdk_s3.PutObjectLockConfigurationInput{
					Bucket: aws.String("lock-put"),
					ObjectLockConfiguration: &types.ObjectLockConfiguration{
						ObjectLockEnabled: types.ObjectLockEnabledEnabled,
						Rule: &types.ObjectLockRule{DefaultRetention: &types.DefaultRetention{
							Mode: types.ObjectLockRetentionModeCompliance, Days: aws.Int32(tt.defaultDay),
						}},
					},
				})
				require.NoError(t, err)
			}

			in := &sdk_s3.PutObjectInput{
				Bucket: aws.String("lock-put"),
				Key:    aws.String("k"),
				Body:   strings.NewReader("x"),
			}
			tt.in(in)

			_, err = client.PutObject(ctx, in)
			if tt.wantErr != "" {
				assert.Equal(t, tt.wantErr, apiCode(err))

				return
			}

			require.NoError(t, err)

			head, err := client.HeadObject(
				ctx,
				&sdk_s3.HeadObjectInput{Bucket: aws.String("lock-put"), Key: aws.String("k")},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantMode, head.ObjectLockMode)
			assert.Equal(t, tt.wantHold, head.ObjectLockLegalHoldStatus)
			assert.Equal(t, tt.wantUntil, head.ObjectLockRetainUntilDate != nil)

			if tt.wantMode != "" {
				_, err = client.DeleteObject(ctx, &sdk_s3.DeleteObjectInput{
					Bucket: aws.String("lock-put"), Key: aws.String("k"), VersionId: head.VersionId,
				})
				assert.Error(t, err)
			}
		})
	}
}

func TestMultipartObjectLockHeaders(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	ctx := t.Context()

	_, err := client.CreateBucket(ctx, &sdk_s3.CreateBucketInput{
		Bucket: aws.String("mpu-lock"), ObjectLockEnabledForBucket: aws.Bool(true),
	})
	require.NoError(t, err)

	mpu, err := client.CreateMultipartUpload(ctx, &sdk_s3.CreateMultipartUploadInput{
		Bucket:                    aws.String("mpu-lock"),
		Key:                       aws.String("k"),
		ObjectLockMode:            types.ObjectLockModeGovernance,
		ObjectLockRetainUntilDate: aws.Time(time.Now().Add(time.Hour)),
		ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatusOn,
	})
	require.NoError(t, err)

	part, err := client.UploadPart(ctx, &sdk_s3.UploadPartInput{
		Bucket: aws.String("mpu-lock"), Key: aws.String("k"), UploadId: mpu.UploadId,
		PartNumber: aws.Int32(1), Body: strings.NewReader("data"),
	})
	require.NoError(t, err)

	_, err = client.CompleteMultipartUpload(ctx, &sdk_s3.CompleteMultipartUploadInput{
		Bucket: aws.String("mpu-lock"), Key: aws.String("k"), UploadId: mpu.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}},
		},
	})
	require.NoError(t, err)

	head, err := client.HeadObject(ctx, &sdk_s3.HeadObjectInput{Bucket: aws.String("mpu-lock"), Key: aws.String("k")})
	require.NoError(t, err)
	assert.Equal(t, types.ObjectLockModeGovernance, head.ObjectLockMode)
	assert.Equal(t, types.ObjectLockLegalHoldStatusOn, head.ObjectLockLegalHoldStatus)
	assert.NotNil(t, head.ObjectLockRetainUntilDate)
}
