package s3_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestTransitionDefaultMinimumObjectSize is a regression test for
// gopherstack-xhu2t: PutBucketLifecycleConfiguration's
// TransitionDefaultMinimumObjectSize header (s3@v1.111.0 serializers.go:7411,
// X-Amz-Transition-Default-Minimum-Object-Size) was read nowhere, so small
// objects always transitioned regardless of the setting. Real S3's doc
// (api_op_PutBucketLifecycleConfiguration.go): all_storage_classes_128K
// (the default) blocks transition of objects under 128 KB to ANY storage
// class; varies_by_storage_class exempts GLACIER/DEEP_ARCHIVE from that
// minimum. A rule's own explicit ObjectSizeGreaterThan/-LessThan filter
// always takes precedence over either default.
func TestTransitionDefaultMinimumObjectSize(t *testing.T) {
	t.Parallel()

	const smallSize = 1024 // 1 KiB, well under the 128 KiB default minimum
	const bigSize = 200 * 1024

	tests := []struct {
		verify                         func(t *testing.T, b *s3.InMemoryBackend, bucket string)
		name                           string
		transitionDefaultMinObjectSize string
		lcXML                          string
	}{
		{
			name:                           "default_blocks_small_object_transition",
			transitionDefaultMinObjectSize: "",
			lcXML: `<LifecycleConfiguration>
<Rule>
  <ID>r</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition>
</Rule>
</LifecycleConfiguration>`,
			verify: func(t *testing.T, b *s3.InMemoryBackend, bucket string) {
				t.Helper()
				requireStorageClassStable(t, b, bucket, "small.txt")
			},
		},
		{
			name:                           "default_allows_large_object_transition",
			transitionDefaultMinObjectSize: "",
			lcXML: `<LifecycleConfiguration>
<Rule>
  <ID>r</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition>
</Rule>
</LifecycleConfiguration>`,
			verify: func(t *testing.T, b *s3.InMemoryBackend, bucket string) {
				t.Helper()
				requireStorageClassEventually(t, b, bucket, "big.bin", "GLACIER")
			},
		},
		{
			name:                           "varies_by_storage_class_exempts_glacier",
			transitionDefaultMinObjectSize: string(types.TransitionDefaultMinimumObjectSizeVariesByStorageClass),
			lcXML: `<LifecycleConfiguration>
<Rule>
  <ID>r</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition>
</Rule>
</LifecycleConfiguration>`,
			verify: func(t *testing.T, b *s3.InMemoryBackend, bucket string) {
				t.Helper()
				requireStorageClassEventually(t, b, bucket, "small.txt", "GLACIER")
			},
		},
		{
			name:                           "varies_by_storage_class_still_blocks_standard_ia",
			transitionDefaultMinObjectSize: string(types.TransitionDefaultMinimumObjectSizeVariesByStorageClass),
			lcXML: `<LifecycleConfiguration>
<Rule>
  <ID>r</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Transition><Days>1</Days><StorageClass>STANDARD_IA</StorageClass></Transition>
</Rule>
</LifecycleConfiguration>`,
			verify: func(t *testing.T, b *s3.InMemoryBackend, bucket string) {
				t.Helper()
				requireStorageClassStable(t, b, bucket, "small.txt")
			},
		},
		{
			name:                           "custom_filter_overrides_default",
			transitionDefaultMinObjectSize: "",
			lcXML: `<LifecycleConfiguration>
<Rule>
  <ID>r</ID>
  <Status>Enabled</Status>
  <Filter><ObjectSizeGreaterThan>10</ObjectSizeGreaterThan></Filter>
  <Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition>
</Rule>
</LifecycleConfiguration>`,
			verify: func(t *testing.T, b *s3.InMemoryBackend, bucket string) {
				t.Helper()
				requireStorageClassEventually(t, b, bucket, "small.txt", "GLACIER")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bucket := "tdmos-" + tt.name
			b := s3.NewInMemoryBackend(nil)
			mustCreateBucket(t, b, bucket)
			mustPutObject(t, b, bucket, "small.txt", bytes.Repeat([]byte("a"), smallSize))
			mustPutObject(t, b, bucket, "big.bin", bytes.Repeat([]byte("b"), bigSize))
			s3.BackdateObjectForTest(b, bucket, "small.txt", time.Now().Add(-48*time.Hour))
			s3.BackdateObjectForTest(b, bucket, "big.bin", time.Now().Add(-48*time.Hour))

			err := b.PutBucketLifecycleConfiguration(t.Context(), bucket, tt.lcXML, tt.transitionDefaultMinObjectSize)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			go newFastJanitor(b).Run(ctx)

			tt.verify(t, b, bucket)
		})
	}
}

// TestTransitionDefaultMinimumObjectSize_Echoed proves the header round-trips
// through a real typed client: set on PutBucketLifecycleConfiguration, read
// back on GetBucketLifecycleConfiguration.
func TestTransitionDefaultMinimumObjectSize_Echoed(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := "tdmos-echo"

	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = client.PutBucketLifecycleConfiguration(t.Context(), &sdk_s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("r"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
					Transitions: []types.Transition{
						{Days: aws.Int32(1), StorageClass: types.TransitionStorageClassGlacier},
					},
				},
			},
		},
		TransitionDefaultMinimumObjectSize: types.TransitionDefaultMinimumObjectSizeVariesByStorageClass,
	})
	require.NoError(t, err)

	out, err := client.GetBucketLifecycleConfiguration(t.Context(), &sdk_s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Equal(
		t,
		types.TransitionDefaultMinimumObjectSizeVariesByStorageClass,
		out.TransitionDefaultMinimumObjectSize,
	)
}

func requireStorageClassEventually(t *testing.T, b *s3.InMemoryBackend, bucket, key, want string) {
	t.Helper()

	require.Eventually(t, func() bool {
		out, err := b.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})

		return err == nil && string(out.StorageClass) == want
	}, 500*time.Millisecond, 10*time.Millisecond, "object %s must reach storage class %s", key, want)
}

// requireStorageClassStable asserts the object's storage class never
// transitions across a window long enough for the fast test janitor to have
// swept it multiple times.
func requireStorageClassStable(t *testing.T, b *s3.InMemoryBackend, bucket, key string) {
	t.Helper()

	time.Sleep(100 * time.Millisecond)

	out, err := b.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	require.NotEqual(t, "GLACIER", string(out.StorageClass), "object %s must not transition", key)
	require.NotEqual(t, "STANDARD_IA", string(out.StorageClass), "object %s must not transition", key)
}
