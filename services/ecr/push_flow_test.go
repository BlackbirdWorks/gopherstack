package ecr_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

const dockerManifest = `{"schemaVersion":2,` +
	`"mediaType":"application/vnd.docker.distribution.manifest.v2+json",` +
	`"config":{"mediaType":"application/vnd.docker.container.image.v1+json","size":10,"digest":"sha256:aa"},` +
	`"layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":100,"digest":"sha256:bb"}]}`

const ociManifest = `{"schemaVersion":2,` +
	`"config":{"mediaType":"application/vnd.oci.image.config.v1+json","size":7,"digest":"sha256:cc"},` +
	`"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","size":3,"digest":"sha256:dd"}]}`

func TestPutImage_PushFlow(t *testing.T) {
	t.Parallel()

	const (
		dockerMT = "application/vnd.docker.distribution.manifest.v2+json"
		ociMT    = "application/vnd.oci.image.manifest.v1+json"
		invalid  = "InvalidParameterException"
	)

	tests := []struct {
		name         string
		manifest     string
		tag          string
		wantMedia    string
		wantErr      string
		wantSize     int64
		scanOnPush   bool
		wantScanDone bool
	}{
		{name: "docker v2", manifest: dockerManifest, tag: "v1", wantMedia: dockerMT, wantSize: 110},
		{name: "oci inferred", manifest: ociManifest, tag: "v1", wantMedia: ociMT, wantSize: 10},
		{
			name: "scan on push", manifest: dockerManifest, tag: "v1", scanOnPush: true,
			wantMedia: dockerMT, wantSize: 110, wantScanDone: true,
		},
		{name: "no scan by default", manifest: dockerManifest, tag: "v1", wantMedia: dockerMT, wantSize: 110},
		{name: "tag with space", manifest: dockerManifest, tag: "bad tag", wantErr: invalid},
		{name: "tag leading dash", manifest: dockerManifest, tag: "-x", wantErr: invalid},
		{name: "tag too long", manifest: dockerManifest, tag: strings.Repeat("a", 129), wantErr: invalid},
		{
			name: "tag max length", manifest: dockerManifest, tag: strings.Repeat("a", 128),
			wantMedia: dockerMT, wantSize: 110,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			client := newTestECRClient(t, ecr.NewHandler(b, nil))

			_, err := client.CreateRepository(t.Context(), &ecrsdk.CreateRepositoryInput{
				RepositoryName: aws.String("app"),
				ImageScanningConfiguration: &ecrtypes.ImageScanningConfiguration{
					ScanOnPush: tt.scanOnPush,
				},
			})
			require.NoError(t, err)

			put, err := client.PutImage(t.Context(), &ecrsdk.PutImageInput{
				RepositoryName: aws.String("app"),
				ImageManifest:  aws.String(tt.manifest),
				ImageTag:       aws.String(tt.tag),
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantMedia, aws.ToString(put.Image.ImageManifestMediaType))

			got, err := client.BatchGetImage(t.Context(), &ecrsdk.BatchGetImageInput{
				RepositoryName: aws.String("app"),
				ImageIds:       []ecrtypes.ImageIdentifier{{ImageTag: aws.String(tt.tag)}},
			})
			require.NoError(t, err)
			require.Len(t, got.Images, 1)
			assert.Equal(t, tt.wantMedia, aws.ToString(got.Images[0].ImageManifestMediaType))

			desc, err := client.DescribeImages(t.Context(), &ecrsdk.DescribeImagesInput{
				RepositoryName: aws.String("app"),
			})
			require.NoError(t, err)
			require.Len(t, desc.ImageDetails, 1)
			assert.Equal(t, tt.wantSize, aws.ToInt64(desc.ImageDetails[0].ImageSizeInBytes))

			if tt.wantScanDone {
				require.NotNil(t, desc.ImageDetails[0].ImageScanStatus)
				assert.Equal(t, ecrtypes.ScanStatusComplete, desc.ImageDetails[0].ImageScanStatus.Status)
			} else {
				assert.Nil(t, desc.ImageDetails[0].ImageScanStatus)
			}
		})
	}
}

func TestStartImageScan_EchoesRequestedTag(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	client := newTestECRClient(t, ecr.NewHandler(b, nil))

	_, err := client.CreateRepository(t.Context(), &ecrsdk.CreateRepositoryInput{RepositoryName: aws.String("app")})
	require.NoError(t, err)

	for _, tag := range []string{"v1", "v2"} {
		_, err = client.PutImage(t.Context(), &ecrsdk.PutImageInput{
			RepositoryName: aws.String("app"), ImageManifest: aws.String(dockerManifest), ImageTag: aws.String(tag),
		})
		require.NoError(t, err)
	}

	out, err := client.StartImageScan(t.Context(), &ecrsdk.StartImageScanInput{
		RepositoryName: aws.String("app"),
		ImageId:        &ecrtypes.ImageIdentifier{ImageTag: aws.String("v1")},
	})
	require.NoError(t, err)
	assert.Equal(t, "v1", aws.ToString(out.ImageId.ImageTag))
}
