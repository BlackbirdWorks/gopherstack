package azurecontainerregistry

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
)

// ManifestDetail summarises one stored manifest for inspection.
type ManifestDetail struct {
	Digest       string
	MediaType    string
	Architecture string
	OS           string
	Children     int
	Size         int64
	Index        bool
}

// DescribeManifest reads the manifest at repo:ref (a tag or digest) and summarises it.
func (b *InMemoryBackend) DescribeManifest(ctx context.Context, reg, repo, ref string) (ManifestDetail, error) {
	app, ok := b.app(reg)
	if !ok {
		return ManifestDetail{}, ErrRegistryNotFound
	}

	c := ociregistry.NewClient(app)

	m, err := c.GetManifest(ctx, repo, ref)
	if err != nil {
		return ManifestDetail{}, ErrNotFound
	}

	info := c.Describe(ctx, repo, m)

	return ManifestDetail{
		Digest: info.Digest, MediaType: info.MediaType, Architecture: info.Architecture, OS: info.OS,
		Children: len(info.Children), Size: info.Size, Index: info.Index,
	}, nil
}

// DeleteTag removes one tag, honouring the repository and tag delete attributes; the manifest is kept
// when other references remain.
func (b *InMemoryBackend) DeleteTag(ctx context.Context, reg, repo, tag string) error {
	if ociregistry.IsDigest(tag) {
		return ErrNotFound
	}

	if !b.Allowed(reg, repo, tag, ociregistry.OpDelete) {
		return ErrDenied
	}

	app, ok := b.app(reg)
	if !ok {
		return ErrRegistryNotFound
	}

	if err := ociregistry.NewClient(app).DeleteManifest(ctx, repo, tag); err != nil {
		return ErrNotFound
	}

	b.RecordManifestDelete(reg, repo, tag)

	return nil
}
