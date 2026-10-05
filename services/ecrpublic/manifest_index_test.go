package ecrpublic_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrpublicsdk "github.com/aws/aws-sdk-go-v2/service/ecrpublic"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildIndex(t *testing.T, childDigests ...string) string {
	t.Helper()

	type child struct {
		Digest string `json:"digest"`
	}

	doc := struct {
		MediaType string  `json:"mediaType"`
		Manifests []child `json:"manifests"`
	}{MediaType: ociIndexMediaType}

	for _, d := range childDigests {
		doc.Manifests = append(doc.Manifests, child{Digest: d})
	}

	b, err := json.Marshal(doc)
	require.NoError(t, err)

	return string(b)
}

func TestPutImageManifestIndex(t *testing.T) {
	t.Parallel()

	missingChild := "sha256:" + strings.Repeat("0", 64)

	tests := []struct {
		name     string
		wantCode string
		useChild bool
	}{
		{name: "children_present", useChild: true},
		{name: "child_missing", wantCode: "ReferencedImagesNotFoundException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			ctx := t.Context()

			_, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{
				RepositoryName: aws.String("idx-repo"),
			})
			require.NoError(t, err)

			configDigest := pushLayer(ctx, t, client, "idx-repo", []byte("cfg"))
			layerDigestA := pushLayer(ctx, t, client, "idx-repo", []byte("layer"))

			childManifest := buildManifest(configDigest, layerDigestA)
			child, err := client.PutImage(ctx, &ecrpublicsdk.PutImageInput{
				RepositoryName: aws.String("idx-repo"),
				ImageManifest:  aws.String(childManifest),
			})
			require.NoError(t, err)

			ref := missingChild
			if tt.useChild {
				ref = aws.ToString(child.Image.ImageId.ImageDigest)
			}

			put, err := client.PutImage(ctx, &ecrpublicsdk.PutImageInput{
				RepositoryName: aws.String("idx-repo"),
				ImageManifest:  aws.String(buildIndex(t, ref)),
				ImageTag:       aws.String("multi"),
			})

			if tt.wantCode != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, ociIndexMediaType, aws.ToString(put.Image.ImageManifestMediaType))
		})
	}
}

func TestPutImageMediaTypeRequirement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		manifest  string
		mediaType string
		wantCode  string
		wantType  string
	}{
		{
			name:     "from_manifest",
			manifest: `{"schemaVersion":2,"mediaType":"` + ociManifestMediaType + `"}`,
			wantType: ociManifestMediaType,
		},
		{
			name:      "from_request",
			manifest:  `{"schemaVersion":2}`,
			mediaType: ociManifestMediaType,
			wantType:  ociManifestMediaType,
		},
		{name: "absent", manifest: `{"schemaVersion":2}`, wantCode: "InvalidParameterException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			ctx := t.Context()

			_, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{
				RepositoryName: aws.String("mt-repo"),
			})
			require.NoError(t, err)

			in := &ecrpublicsdk.PutImageInput{
				RepositoryName: aws.String("mt-repo"),
				ImageManifest:  aws.String(tt.manifest),
			}
			if tt.mediaType != "" {
				in.ImageManifestMediaType = aws.String(tt.mediaType)
			}

			out, err := client.PutImage(ctx, in)

			if tt.wantCode != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantType, aws.ToString(out.Image.ImageManifestMediaType))
		})
	}
}

func TestDescribeImagesArtifactMediaType(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("art-repo")})
	require.NoError(t, err)

	configDigest := pushLayer(ctx, t, client, "art-repo", []byte("cfg"))
	layerDigestA := pushLayer(ctx, t, client, "art-repo", []byte("layer"))

	_, err = client.PutImage(ctx, &ecrpublicsdk.PutImageInput{
		RepositoryName: aws.String("art-repo"),
		ImageManifest:  aws.String(buildManifest(configDigest, layerDigestA)),
		ImageTag:       aws.String("v1"),
	})
	require.NoError(t, err)

	out, err := client.DescribeImages(ctx, &ecrpublicsdk.DescribeImagesInput{RepositoryName: aws.String("art-repo")})
	require.NoError(t, err)
	require.Len(t, out.ImageDetails, 1)
	assert.Equal(t, ociConfigMediaType, aws.ToString(out.ImageDetails[0].ArtifactMediaType))
	assert.Equal(t, ociManifestMediaType, aws.ToString(out.ImageDetails[0].ImageManifestMediaType))
}
