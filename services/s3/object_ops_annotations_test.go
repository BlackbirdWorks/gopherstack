package s3_test

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestObjectAnnotations_Lifecycle drives the full Object Annotations family
// (gopherstack-zi7k) through the real aws-sdk-go-v2 client end to end:
// put -> get -> list -> delete -> list. A routing bug in this family would
// most likely surface as ListObjectAnnotations silently returning an empty
// collection (it shares its GET /{Key+}?annotation route with
// GetObjectAnnotation, disambiguated only by the annotationName query param),
// so this asserts non-empty, exact-count results at each step rather than
// just "no error".
func TestObjectAnnotations_Lifecycle(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := "annotation-lifecycle-bucket"
	key := "doc.txt"

	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	_, err = client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("hello world"),
	})
	require.NoError(t, err)

	putOut, err := client.PutObjectAnnotation(t.Context(), &sdk_s3.PutObjectAnnotationInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		AnnotationName:    aws.String("review-status"),
		AnnotationPayload: strings.NewReader("approved"),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	require.NoError(t, err)
	assert.Equal(t, "review-status", aws.ToString(putOut.AnnotationName))
	assert.NotEmpty(t, aws.ToString(putOut.ETag))
	assert.NotNil(t, putOut.ChecksumSHA256, "server should compute the requested checksum algorithm")

	getOut, err := client.GetObjectAnnotation(t.Context(), &sdk_s3.GetObjectAnnotationInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(key),
		AnnotationName: aws.String("review-status"),
	})
	require.NoError(t, err)
	payload, err := io.ReadAll(getOut.AnnotationPayload)
	require.NoError(t, err)
	require.NoError(t, getOut.AnnotationPayload.Close())
	assert.Equal(t, "approved", string(payload))
	assert.Equal(t, aws.ToString(putOut.ETag), aws.ToString(getOut.ETag))
	assert.Equal(t, aws.ToString(putOut.ChecksumSHA256), aws.ToString(getOut.ChecksumSHA256))

	_, err = client.PutObjectAnnotation(t.Context(), &sdk_s3.PutObjectAnnotationInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		AnnotationName:    aws.String("owner"),
		AnnotationPayload: strings.NewReader("alice"),
	})
	require.NoError(t, err)

	listOut, err := client.ListObjectAnnotations(t.Context(), &sdk_s3.ListObjectAnnotationsInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Annotations, 2, "both annotations must come back, not an empty routing-bug collection")
	assert.Equal(t, int32(2), aws.ToInt32(listOut.AnnotationCount))

	names := make([]string, 0, len(listOut.Annotations))
	for _, a := range listOut.Annotations {
		names = append(names, aws.ToString(a.AnnotationName))
	}
	assert.ElementsMatch(t, []string{"review-status", "owner"}, names)

	_, err = client.DeleteObjectAnnotation(t.Context(), &sdk_s3.DeleteObjectAnnotationInput{
		Bucket: aws.String(bucket), Key: aws.String(key), AnnotationName: aws.String("review-status"),
	})
	require.NoError(t, err)

	_, err = client.GetObjectAnnotation(t.Context(), &sdk_s3.GetObjectAnnotationInput{
		Bucket: aws.String(bucket), Key: aws.String(key), AnnotationName: aws.String("review-status"),
	})
	require.Error(t, err)
	var nsa *types.NoSuchAnnotation
	require.ErrorAs(t, err, &nsa)

	listOut2, err := client.ListObjectAnnotations(t.Context(), &sdk_s3.ListObjectAnnotationsInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	require.NoError(t, err)
	require.Len(t, listOut2.Annotations, 1, "exactly the surviving annotation should remain")
	assert.Equal(t, "owner", aws.ToString(listOut2.Annotations[0].AnnotationName))

	// DeleteObjectAnnotation's own error switch (s3@v1.106.5 deserializers.go)
	// declares no NoSuchAnnotation case: deleting an already-gone name must
	// succeed, matching real S3 delete-object idempotency.
	_, err = client.DeleteObjectAnnotation(t.Context(), &sdk_s3.DeleteObjectAnnotationInput{
		Bucket: aws.String(bucket), Key: aws.String(key), AnnotationName: aws.String("review-status"),
	})
	require.NoError(t, err)
}

func TestObjectAnnotations_Errors(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := "annotation-errors-bucket"
	key := "doc.txt"

	_, setupErr := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, setupErr)
	_, setupErr = client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("hello"),
	})
	require.NoError(t, setupErr)

	t.Run("get missing annotation", func(t *testing.T) {
		t.Parallel()

		_, err := client.GetObjectAnnotation(t.Context(), &sdk_s3.GetObjectAnnotationInput{
			Bucket: aws.String(bucket), Key: aws.String(key), AnnotationName: aws.String("missing"),
		})
		require.Error(t, err)

		var nsa *types.NoSuchAnnotation
		assert.ErrorAs(t, err, &nsa)
	})

	t.Run("get on missing key", func(t *testing.T) {
		t.Parallel()

		_, err := client.GetObjectAnnotation(t.Context(), &sdk_s3.GetObjectAnnotationInput{
			Bucket: aws.String(bucket), Key: aws.String("no-such-key.txt"), AnnotationName: aws.String("x"),
		})
		require.Error(t, err)

		var nsk *types.NoSuchKey
		assert.ErrorAs(t, err, &nsk)
	})

	t.Run("put on missing bucket", func(t *testing.T) {
		t.Parallel()

		_, err := client.PutObjectAnnotation(t.Context(), &sdk_s3.PutObjectAnnotationInput{
			Bucket: aws.String("no-such-bucket-annotations"), Key: aws.String(key),
			AnnotationName: aws.String("x"), AnnotationPayload: strings.NewReader("v"),
		})
		require.Error(t, err)

		var nsb *types.NoSuchBucket
		assert.ErrorAs(t, err, &nsb)
	})

	t.Run("name too long", func(t *testing.T) {
		t.Parallel()

		_, err := client.PutObjectAnnotation(t.Context(), &sdk_s3.PutObjectAnnotationInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
			AnnotationName:    aws.String(strings.Repeat("a", 513)),
			AnnotationPayload: strings.NewReader("v"),
		})
		require.Error(t, err)

		var tooLong *types.AnnotationNameTooLong
		assert.ErrorAs(t, err, &tooLong)
	})

	t.Run("reserved name prefix", func(t *testing.T) {
		t.Parallel()

		_, err := client.PutObjectAnnotation(t.Context(), &sdk_s3.PutObjectAnnotationInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
			AnnotationName:    aws.String("aws-internal"),
			AnnotationPayload: strings.NewReader("v"),
		})
		require.Error(t, err)

		var invalid *types.InvalidAnnotationName
		assert.ErrorAs(t, err, &invalid)
	})

	t.Run("invalid utf8 payload", func(t *testing.T) {
		t.Parallel()

		_, err := client.PutObjectAnnotation(t.Context(), &sdk_s3.PutObjectAnnotationInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
			AnnotationName:    aws.String("binary"),
			AnnotationPayload: bytes.NewReader([]byte{0xff, 0xfe, 0xfd}),
		})
		require.Error(t, err)

		var umt *types.UnsupportedMediaType
		assert.ErrorAs(t, err, &umt)
	})
}

// TestObjectAnnotations_VersionScoped verifies annotations attach to a
// specific object version, not the object as a whole (PutObjectAnnotationInput/
// GetObjectAnnotationInput both carry an optional VersionId; s3@v1.106.5
// api_op_PutObjectAnnotation.go: "The version ID of the object to attach the
// annotation to.").
func TestObjectAnnotations_VersionScoped(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := "annotation-version-bucket"
	key := "doc.txt"

	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	_, err = client.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	require.NoError(t, err)

	v1, err := client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("v1"),
	})
	require.NoError(t, err)
	v2, err := client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader("v2"),
	})
	require.NoError(t, err)
	require.NotEqual(t, aws.ToString(v1.VersionId), aws.ToString(v2.VersionId))

	_, err = client.PutObjectAnnotation(t.Context(), &sdk_s3.PutObjectAnnotationInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		VersionId:         v1.VersionId,
		AnnotationName:    aws.String("note"),
		AnnotationPayload: strings.NewReader("first version"),
	})
	require.NoError(t, err)

	// The annotation lives on v1, not v2.
	_, err = client.GetObjectAnnotation(t.Context(), &sdk_s3.GetObjectAnnotationInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: v2.VersionId, AnnotationName: aws.String("note"),
	})
	require.Error(t, err)
	var nsa *types.NoSuchAnnotation
	require.ErrorAs(t, err, &nsa)

	getOut, err := client.GetObjectAnnotation(t.Context(), &sdk_s3.GetObjectAnnotationInput{
		Bucket: aws.String(bucket), Key: aws.String(key), VersionId: v1.VersionId, AnnotationName: aws.String("note"),
	})
	require.NoError(t, err)
	payload, err := io.ReadAll(getOut.AnnotationPayload)
	require.NoError(t, err)
	require.NoError(t, getOut.AnnotationPayload.Close())
	assert.Equal(t, "first version", string(payload))
}

// TestObjectAnnotations_LimitExceeded proves the documented 1,000-annotations-
// per-object cap (s3@v1.106.5 api_op_PutObjectAnnotation.go: "Each object can
// have up to 1,000 annotations.") using the backend directly -- 1,000 real
// HTTP round trips through SigV4 signing would make this test needlessly
// slow, and the backend method is the same code path the handler calls.
func TestObjectAnnotations_LimitExceeded(t *testing.T) {
	t.Parallel()

	const wantMaxAnnotations = 1000

	_, backend := newTestHandler(t)
	bucket := "annotation-limit-bucket"
	key := "doc.txt"
	mustCreateBucket(t, backend, bucket)
	mustPutObject(t, backend, bucket, key, []byte("hello"))

	ctx := t.Context()
	for i := range wantMaxAnnotations {
		_, err := backend.PutObjectAnnotation(ctx, &sdk_s3.PutObjectAnnotationInput{
			Bucket:            aws.String(bucket),
			Key:               aws.String(key),
			AnnotationName:    aws.String("ann-" + strconv.Itoa(i)),
			AnnotationPayload: strings.NewReader("v"),
		})
		require.NoError(t, err)
	}

	_, err := backend.PutObjectAnnotation(ctx, &sdk_s3.PutObjectAnnotationInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		AnnotationName:    aws.String("one-too-many"),
		AnnotationPayload: strings.NewReader("v"),
	})
	require.ErrorIs(t, err, s3.ErrAnnotationLimitExceeded)

	// Overwriting an existing annotation at the cap must still be allowed
	// (the cap is on distinct names, not on writes).
	_, err = backend.PutObjectAnnotation(ctx, &sdk_s3.PutObjectAnnotationInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		AnnotationName:    aws.String("ann-0"),
		AnnotationPayload: strings.NewReader("updated"),
	})
	require.NoError(t, err)
}

// TestObjectAnnotations_SnapshotRoundtrip proves annotations survive a
// Snapshot/Restore cycle, since they are per-object-version state stored
// alongside the object.
func TestObjectAnnotations_SnapshotRoundtrip(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	bucket := "annotation-snapshot-bucket"
	key := "doc.txt"
	mustCreateBucket(t, backend, bucket)
	mustPutObject(t, backend, bucket, key, []byte("hello"))

	ctx := t.Context()
	putOut, err := backend.PutObjectAnnotation(ctx, &sdk_s3.PutObjectAnnotationInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		AnnotationName:    aws.String("persisted"),
		AnnotationPayload: strings.NewReader("survives restart"),
	})
	require.NoError(t, err)

	snap := backend.Snapshot(ctx)
	require.NotEmpty(t, snap)

	restored := s3.NewInMemoryBackend(nil)
	require.NoError(t, restored.Restore(ctx, snap))

	getOut, err := restored.GetObjectAnnotation(ctx, &sdk_s3.GetObjectAnnotationInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(key),
		AnnotationName: aws.String("persisted"),
	})
	require.NoError(t, err)
	payload, err := io.ReadAll(getOut.AnnotationPayload)
	require.NoError(t, err)
	require.NoError(t, getOut.AnnotationPayload.Close())
	assert.Equal(t, "survives restart", string(payload))
	assert.Equal(t, aws.ToString(putOut.ETag), aws.ToString(getOut.ETag))
}

// TestUpdateBucketMetadataAnnotationTableConfiguration exercises the
// bucket-level op through the real client (route key "metadataAnnotationTable",
// verified against s3@v1.106.5 serializers.go:
// awsRestxml_serializeOpUpdateBucketMetadataAnnotationTableConfiguration's
// httpbinding.SplitURI("/?metadataAnnotationTable")). There is no dedicated Get
// op for this sub-config in the pinned SDK, so success/NoSuchBucket are all
// that's independently observable over the wire.
//
// This op's own deserializeOpError switch (s3@v1.106.5 deserializers.go)
// declares no typed error cases at all -- every failure decodes as a
// smithy.GenericAPIError carrying whatever code/message the server sent,
// rather than a *types.NoSuchBucket -- so the error case below asserts via
// smithy.APIError.ErrorCode() instead of ErrorAs on a typed error.
func TestUpdateBucketMetadataAnnotationTableConfiguration(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := "annotation-table-config-bucket"

	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = client.UpdateBucketMetadataAnnotationTableConfiguration(
		t.Context(),
		&sdk_s3.UpdateBucketMetadataAnnotationTableConfigurationInput{
			Bucket: aws.String(bucket),
			AnnotationTableConfiguration: &types.AnnotationTableConfigurationUpdates{
				ConfigurationState: types.AnnotationConfigurationStateEnabled,
			},
		},
	)
	require.NoError(t, err)

	_, err = client.UpdateBucketMetadataAnnotationTableConfiguration(
		t.Context(),
		&sdk_s3.UpdateBucketMetadataAnnotationTableConfigurationInput{
			Bucket: aws.String("no-such-bucket-for-annotation-table"),
			AnnotationTableConfiguration: &types.AnnotationTableConfigurationUpdates{
				ConfigurationState: types.AnnotationConfigurationStateDisabled,
			},
		},
	)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "NoSuchBucket", apiErr.ErrorCode())
}
