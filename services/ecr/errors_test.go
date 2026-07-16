package ecr_test

// errors_test.go — verifies AWS-accurate error precedence for the
// image-scoped scan/storage ops: a missing repository yields
// RepositoryNotFoundException, while a missing image in an existing repository
// yields ImageNotFoundException, and an unscanned image yields
// ScanNotFoundException.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestErrorPrecedence_RepoVsImageNotFound(t *testing.T) {
	t.Parallel()

	const digest = "sha256:present"

	tests := []struct {
		call      func(b *ecr.InMemoryBackend, repo string, id ecr.ImageIdentifier) error
		wantErrIs error
		name      string
		repoName  string
		imageID   ecr.ImageIdentifier
	}{
		{
			name:      "StartImageScan missing repo -> RepositoryNotFound",
			repoName:  "nope",
			imageID:   ecr.ImageIdentifier{ImageDigest: digest},
			wantErrIs: ecr.ErrRepositoryNotFound,
			call: func(b *ecr.InMemoryBackend, repo string, id ecr.ImageIdentifier) error {
				_, err := b.StartImageScan(context.Background(), repo, id)

				return err
			},
		},
		{
			name:      "StartImageScan missing image -> ImageNotFound",
			repoName:  "repo",
			imageID:   ecr.ImageIdentifier{ImageDigest: "sha256:absent"},
			wantErrIs: ecr.ErrImageNotFound,
			call: func(b *ecr.InMemoryBackend, repo string, id ecr.ImageIdentifier) error {
				_, err := b.StartImageScan(context.Background(), repo, id)

				return err
			},
		},
		{
			name:      "DescribeImageScanFindings missing repo -> RepositoryNotFound",
			repoName:  "nope",
			imageID:   ecr.ImageIdentifier{ImageDigest: digest},
			wantErrIs: ecr.ErrRepositoryNotFound,
			call: func(b *ecr.InMemoryBackend, repo string, id ecr.ImageIdentifier) error {
				_, _, err := b.DescribeImageScanFindings(context.Background(), repo, id, 0, "")

				return err
			},
		},
		{
			name:      "DescribeImageScanFindings missing image -> ImageNotFound",
			repoName:  "repo",
			imageID:   ecr.ImageIdentifier{ImageDigest: "sha256:absent"},
			wantErrIs: ecr.ErrImageNotFound,
			call: func(b *ecr.InMemoryBackend, repo string, id ecr.ImageIdentifier) error {
				_, _, err := b.DescribeImageScanFindings(context.Background(), repo, id, 0, "")

				return err
			},
		},
		{
			name:      "DescribeImageScanFindings present image unscanned -> ScanNotFound",
			repoName:  "repo",
			imageID:   ecr.ImageIdentifier{ImageDigest: digest},
			wantErrIs: ecr.ErrScanNotFoundException,
			call: func(b *ecr.InMemoryBackend, repo string, id ecr.ImageIdentifier) error {
				_, _, err := b.DescribeImageScanFindings(context.Background(), repo, id, 0, "")

				return err
			},
		},
		{
			name:      "UpdateImageStorageClass missing repo -> RepositoryNotFound",
			repoName:  "nope",
			imageID:   ecr.ImageIdentifier{ImageDigest: digest},
			wantErrIs: ecr.ErrRepositoryNotFound,
			call: func(b *ecr.InMemoryBackend, repo string, id ecr.ImageIdentifier) error {
				_, err := b.UpdateImageStorageClass(context.Background(), repo, id, "ARCHIVE")

				return err
			},
		},
		{
			name:      "UpdateImageStorageClass missing image -> ImageNotFound",
			repoName:  "repo",
			imageID:   ecr.ImageIdentifier{ImageDigest: "sha256:absent"},
			wantErrIs: ecr.ErrImageNotFound,
			call: func(b *ecr.InMemoryBackend, repo string, id ecr.ImageIdentifier) error {
				_, err := b.UpdateImageStorageClass(context.Background(), repo, id, "ARCHIVE")

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			b.CreateRepoInternal("repo")
			b.AddImageInternal("repo", makeImage(digest, "v1"))

			err := tt.call(b, tt.repoName, tt.imageID)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErrIs)
		})
	}
}
