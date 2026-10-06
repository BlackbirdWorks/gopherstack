package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ecrpublicsdk "github.com/aws/aws-sdk-go-v2/service/ecrpublic"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ecrPublicManifestMediaType = "application/vnd.oci.image.manifest.v1+json"
	ecrPublicConfigMediaType   = "application/vnd.oci.image.config.v1+json"
	ecrPublicIndexMediaType    = "application/vnd.oci.image.index.v1+json"
)

func createECRPublicClient(t *testing.T) *ecrpublicsdk.Client {
	t.Helper()

	// Amazon ECR Public is a us-east-1-only API.
	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err, "unable to load SDK config")

	return ecrpublicsdk.NewFromConfig(cfg, func(o *ecrpublicsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func ecrPublicPushLayer(t *testing.T, client *ecrpublicsdk.Client, repo string, data []byte) string {
	t.Helper()

	ctx := t.Context()

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

	sum := sha256.Sum256(data)

	completed, err := client.CompleteLayerUpload(ctx, &ecrpublicsdk.CompleteLayerUploadInput{
		RepositoryName: aws.String(repo),
		UploadId:       initiated.UploadId,
		LayerDigests:   []string{"sha256:" + hex.EncodeToString(sum[:])},
	})
	require.NoError(t, err)

	return aws.ToString(completed.LayerDigest)
}

// TestIntegration_ECRPublic_RepositoryImageLifecycle pushes layers, an image and an OCI
// index through the real SDK client, reads them back and deletes the repository.
func TestIntegration_ECRPublic_RepositoryImageLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECRPublicClient(t)
	ctx := t.Context()
	repo := "it-pub-" + uuid.NewString()[:8]

	created, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{
		RepositoryName: aws.String(repo),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(created.Repository.RepositoryUri), repo)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteRepository(cleanupCtx, &ecrpublicsdk.DeleteRepositoryInput{
			RepositoryName: aws.String(repo),
			Force:          true,
		})
	})

	configDigest := ecrPublicPushLayer(t, client, repo, []byte("it-config"))
	layerDigest := ecrPublicPushLayer(t, client, repo, []byte("it-layer"))

	manifest, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     ecrPublicManifestMediaType,
		"config":        map[string]any{"mediaType": ecrPublicConfigMediaType, "digest": configDigest},
		"layers":        []map[string]any{{"digest": layerDigest}},
	})
	require.NoError(t, err)

	put, err := client.PutImage(ctx, &ecrpublicsdk.PutImageInput{
		RepositoryName: aws.String(repo),
		ImageManifest:  aws.String(string(manifest)),
		ImageTag:       aws.String("v1"),
	})
	require.NoError(t, err)
	childDigest := aws.ToString(put.Image.ImageId.ImageDigest)

	index, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     ecrPublicIndexMediaType,
		"manifests":     []map[string]any{{"digest": childDigest}},
	})
	require.NoError(t, err)

	_, err = client.PutImage(ctx, &ecrpublicsdk.PutImageInput{
		RepositoryName: aws.String(repo),
		ImageManifest:  aws.String(string(index)),
		ImageTag:       aws.String("multi"),
	})
	require.NoError(t, err)

	described, err := client.DescribeImages(ctx, &ecrpublicsdk.DescribeImagesInput{RepositoryName: aws.String(repo)})
	require.NoError(t, err)
	assert.Len(t, described.ImageDetails, 2)

	tags, err := client.DescribeImageTags(ctx, &ecrpublicsdk.DescribeImageTagsInput{RepositoryName: aws.String(repo)})
	require.NoError(t, err)
	assert.Len(t, tags.ImageTagDetails, 2)

	_, err = client.PutImage(ctx, &ecrpublicsdk.PutImageInput{
		RepositoryName: aws.String(repo),
		ImageManifest: aws.String(
			`{"schemaVersion":2,"mediaType":"` + ecrPublicIndexMediaType + `","manifests":[{"digest":"sha256:` +
				"0000000000000000000000000000000000000000000000000000000000000000" + `"}]}`,
		),
	})
	require.Error(t, err)
	assert.True(t, hasAPIErrorCode(err, "ReferencedImagesNotFoundException"))

	_, err = client.SetRepositoryPolicy(ctx, &ecrpublicsdk.SetRepositoryPolicyInput{
		RepositoryName: aws.String(repo),
		PolicyText:     aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	_, err = client.DeleteRepository(ctx, &ecrpublicsdk.DeleteRepositoryInput{RepositoryName: aws.String(repo)})
	require.Error(t, err, "non-empty repository needs force")
	assert.True(t, hasAPIErrorCode(err, "RepositoryNotEmptyException"))

	_, err = client.DeleteRepository(ctx, &ecrpublicsdk.DeleteRepositoryInput{
		RepositoryName: aws.String(repo),
		Force:          true,
	})
	require.NoError(t, err)

	_, err = client.DescribeImages(ctx, &ecrpublicsdk.DescribeImagesInput{RepositoryName: aws.String(repo)})
	assert.True(t, hasAPIErrorCode(err, "RepositoryNotFoundException"))
}
