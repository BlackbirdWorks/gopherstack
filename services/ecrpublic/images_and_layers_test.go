package ecrpublic_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrpublicsdk "github.com/aws/aws-sdk-go-v2/service/ecrpublic"
	"github.com/aws/aws-sdk-go-v2/service/ecrpublic/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func layerDigest(data []byte) string {
	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

// pushLayer runs the full Initiate/Upload/Complete flow for a single-part
// layer and returns the digest CompleteLayerUpload recorded.
func pushLayer(
	ctx context.Context, t *testing.T, client *ecrpublicsdk.Client, repo string, data []byte,
) string {
	t.Helper()

	initiated, err := client.InitiateLayerUpload(ctx, &ecrpublicsdk.InitiateLayerUploadInput{
		RepositoryName: aws.String(repo),
	})
	require.NoError(t, err)

	_, err = client.UploadLayerPart(ctx, &ecrpublicsdk.UploadLayerPartInput{
		RepositoryName: aws.String(repo),
		UploadId:       initiated.UploadId,
		PartFirstByte:  aws.Int64(0),
		PartLastByte:   aws.Int64(int64(len(data) - 1)),
		LayerPartBlob:  data,
	})
	require.NoError(t, err)

	completed, err := client.CompleteLayerUpload(ctx, &ecrpublicsdk.CompleteLayerUploadInput{
		RepositoryName: aws.String(repo),
		UploadId:       initiated.UploadId,
		LayerDigests:   []string{layerDigest(data)},
	})
	require.NoError(t, err)

	return aws.ToString(completed.LayerDigest)
}

func buildManifest(configDigest string, layerDigests ...string) string {
	type layer struct {
		Digest string `json:"digest"`
	}

	m := struct {
		Config struct {
			Digest string `json:"digest"`
		} `json:"config"`
		Layers []layer `json:"layers"`
	}{}
	m.Config.Digest = configDigest

	for _, d := range layerDigests {
		m.Layers = append(m.Layers, layer{Digest: d})
	}

	b, _ := json.Marshal(m)

	return string(b)
}

func TestImagePushLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("push-repo")})
	require.NoError(t, err)

	configDigest := pushLayer(ctx, t, client, "push-repo", []byte("config-blob"))
	layerADigest := pushLayer(ctx, t, client, "push-repo", []byte("layer-a-blob"))

	avail, err := client.BatchCheckLayerAvailability(ctx, &ecrpublicsdk.BatchCheckLayerAvailabilityInput{
		RepositoryName: aws.String("push-repo"),
		LayerDigests:   []string{configDigest, layerADigest, "sha256:" + hex.EncodeToString(make([]byte, 32))},
	})
	require.NoError(t, err)
	assert.Len(t, avail.Layers, 2)
	require.Len(t, avail.Failures, 1)
	assert.Equal(t, types.LayerFailureCodeMissingLayerDigest, avail.Failures[0].FailureCode)

	manifest := buildManifest(configDigest, layerADigest)

	putOut, err := client.PutImage(ctx, &ecrpublicsdk.PutImageInput{
		RepositoryName: aws.String("push-repo"),
		ImageManifest:  aws.String(manifest),
		ImageTag:       aws.String("v1"),
	})
	require.NoError(t, err)
	digest := aws.ToString(putOut.Image.ImageId.ImageDigest)
	assert.Equal(t, layerDigest([]byte(manifest)), digest)

	described, err := client.DescribeImages(ctx, &ecrpublicsdk.DescribeImagesInput{
		RepositoryName: aws.String("push-repo"),
	})
	require.NoError(t, err)
	require.Len(t, described.ImageDetails, 1)
	assert.Equal(t, []string{"v1"}, described.ImageDetails[0].ImageTags)
	assert.Equal(t, digest, aws.ToString(described.ImageDetails[0].ImageDigest))

	tags, err := client.DescribeImageTags(ctx, &ecrpublicsdk.DescribeImageTagsInput{
		RepositoryName: aws.String("push-repo"),
	})
	require.NoError(t, err)
	require.Len(t, tags.ImageTagDetails, 1)
	assert.Equal(t, "v1", aws.ToString(tags.ImageTagDetails[0].ImageTag))
	assert.Equal(t, digest, aws.ToString(tags.ImageTagDetails[0].ImageDetail.ImageDigest))

	delByTag, err := client.BatchDeleteImage(ctx, &ecrpublicsdk.BatchDeleteImageInput{
		RepositoryName: aws.String("push-repo"),
		ImageIds:       []types.ImageIdentifier{{ImageTag: aws.String("v1")}},
	})
	require.NoError(t, err)
	assert.Len(t, delByTag.ImageIds, 1)
	assert.Empty(t, delByTag.Failures)

	afterUntag, err := client.DescribeImages(ctx, &ecrpublicsdk.DescribeImagesInput{
		RepositoryName: aws.String("push-repo"),
	})
	require.NoError(t, err)
	require.Len(t, afterUntag.ImageDetails, 1)
	assert.Empty(t, afterUntag.ImageDetails[0].ImageTags)

	delByDigest, err := client.BatchDeleteImage(ctx, &ecrpublicsdk.BatchDeleteImageInput{
		RepositoryName: aws.String("push-repo"),
		ImageIds:       []types.ImageIdentifier{{ImageDigest: aws.String(digest)}},
	})
	require.NoError(t, err)
	assert.Len(t, delByDigest.ImageIds, 1)

	empty, err := client.DescribeImages(ctx, &ecrpublicsdk.DescribeImagesInput{
		RepositoryName: aws.String("push-repo"),
	})
	require.NoError(t, err)
	assert.Empty(t, empty.ImageDetails)
}

func TestBatchDeleteImage_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("bdi-repo")})
	require.NoError(t, err)

	out, err := client.BatchDeleteImage(ctx, &ecrpublicsdk.BatchDeleteImageInput{
		RepositoryName: aws.String("bdi-repo"),
		ImageIds:       []types.ImageIdentifier{{ImageTag: aws.String("missing")}},
	})
	require.NoError(t, err)
	assert.Empty(t, out.ImageIds)
	require.Len(t, out.Failures, 1)
	assert.Equal(t, types.ImageFailureCodeImageNotFound, out.Failures[0].FailureCode)
}

func TestPutImage_LayersNotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(
		ctx,
		&ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("missing-layers")},
	)
	require.NoError(t, err)

	manifest := buildManifest(
		"sha256:"+hex.EncodeToString(make([]byte, 32)),
		"sha256:"+hex.EncodeToString(make([]byte, 32)),
	)

	_, err = client.PutImage(ctx, &ecrpublicsdk.PutImageInput{
		RepositoryName: aws.String("missing-layers"),
		ImageManifest:  aws.String(manifest),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "LayersNotFoundException", apiErr.ErrorCode())
}

func TestPutImage_AlreadyExistsAndTagConflict(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, createErr := client.CreateRepository(
		ctx,
		&ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("conflict-repo")},
	)
	require.NoError(t, createErr)

	manifestA := `{"schemaVersion":2,"unique":"a"}`
	manifestB := `{"schemaVersion":2,"unique":"b"}`

	_, pushErr := client.PutImage(ctx, &ecrpublicsdk.PutImageInput{
		RepositoryName: aws.String("conflict-repo"),
		ImageManifest:  aws.String(manifestA),
		ImageTag:       aws.String("latest"),
	})
	require.NoError(t, pushErr)

	t.Run("same tag and digest", func(t *testing.T) {
		t.Parallel()

		_, err := client.PutImage(ctx, &ecrpublicsdk.PutImageInput{
			RepositoryName: aws.String("conflict-repo"),
			ImageManifest:  aws.String(manifestA),
			ImageTag:       aws.String("latest"),
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "ImageAlreadyExistsException", apiErr.ErrorCode())
	})

	t.Run("same tag different digest", func(t *testing.T) {
		t.Parallel()

		_, err := client.PutImage(ctx, &ecrpublicsdk.PutImageInput{
			RepositoryName: aws.String("conflict-repo"),
			ImageManifest:  aws.String(manifestB),
			ImageTag:       aws.String("latest"),
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "ImageTagAlreadyExistsException", apiErr.ErrorCode())
	})
}

func TestUploadLayerPart_InvalidLayerPart(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("gap-repo")})
	require.NoError(t, err)

	initiated, err := client.InitiateLayerUpload(ctx, &ecrpublicsdk.InitiateLayerUploadInput{
		RepositoryName: aws.String("gap-repo"),
	})
	require.NoError(t, err)

	_, err = client.UploadLayerPart(ctx, &ecrpublicsdk.UploadLayerPartInput{
		RepositoryName: aws.String("gap-repo"),
		UploadId:       initiated.UploadId,
		PartFirstByte:  aws.Int64(5),
		PartLastByte:   aws.Int64(10),
		LayerPartBlob:  []byte("123456"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "InvalidLayerPartException", apiErr.ErrorCode())
}

func TestCompleteLayerUpload_UploadNotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(
		ctx,
		&ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("no-upload-repo")},
	)
	require.NoError(t, err)

	_, err = client.CompleteLayerUpload(ctx, &ecrpublicsdk.CompleteLayerUploadInput{
		RepositoryName: aws.String("no-upload-repo"),
		UploadId:       aws.String("bogus-upload-id"),
		LayerDigests:   []string{"sha256:" + hex.EncodeToString(make([]byte, 32))},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "UploadNotFoundException", apiErr.ErrorCode())
}

func TestCompleteLayerUpload_EmptyUpload(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(
		ctx,
		&ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("empty-upload-repo")},
	)
	require.NoError(t, err)

	initiated, err := client.InitiateLayerUpload(ctx, &ecrpublicsdk.InitiateLayerUploadInput{
		RepositoryName: aws.String("empty-upload-repo"),
	})
	require.NoError(t, err)

	_, err = client.CompleteLayerUpload(ctx, &ecrpublicsdk.CompleteLayerUploadInput{
		RepositoryName: aws.String("empty-upload-repo"),
		UploadId:       initiated.UploadId,
		LayerDigests:   []string{"sha256:" + hex.EncodeToString(make([]byte, 32))},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "EmptyUploadException", apiErr.ErrorCode())
}

func TestCompleteLayerUpload_LayerAlreadyExists(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(
		ctx,
		&ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("dup-layer-repo")},
	)
	require.NoError(t, err)

	pushLayer(ctx, t, client, "dup-layer-repo", []byte("same-content"))

	initiated, err := client.InitiateLayerUpload(ctx, &ecrpublicsdk.InitiateLayerUploadInput{
		RepositoryName: aws.String("dup-layer-repo"),
	})
	require.NoError(t, err)

	_, err = client.UploadLayerPart(ctx, &ecrpublicsdk.UploadLayerPartInput{
		RepositoryName: aws.String("dup-layer-repo"),
		UploadId:       initiated.UploadId,
		PartFirstByte:  aws.Int64(0),
		PartLastByte:   aws.Int64(11),
		LayerPartBlob:  []byte("same-content"),
	})
	require.NoError(t, err)

	_, err = client.CompleteLayerUpload(ctx, &ecrpublicsdk.CompleteLayerUploadInput{
		RepositoryName: aws.String("dup-layer-repo"),
		UploadId:       initiated.UploadId,
		LayerDigests:   []string{layerDigest([]byte("same-content"))},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "LayerAlreadyExistsException", apiErr.ErrorCode())
}
