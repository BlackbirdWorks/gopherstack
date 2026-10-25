package gcpartifactregistry

import (
	"context"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
)

// versionDigest resolves a tag's version field to a digest of the same package.
func versionDigest(p pkgRef, version string) (string, error) {
	l, err := parseLeaf(version, "version")
	if err != nil {
		return "", err
	}

	if l.pkg != p {
		return "", errInvalid("Version %q does not belong to package %q.", version, p.full())
	}

	return l.leaf, nil
}

func (b *InMemoryBackend) retag(ctx context.Context, p pkgRef, tag, digest string) error {
	c := b.client()
	repo := p.repo.dockerKey() + "/" + p.image

	m, err := c.GetManifest(ctx, repo, digest)
	if err != nil {
		return errNotFound("Version %q not found.", p.full()+"/versions/"+digest)
	}

	if _, err = c.PutManifest(ctx, repo, tag, m.MediaType, m.Body); err != nil {
		return errInternal(err)
	}

	b.RecordManifestPut(repo, tag, digest)

	return nil
}

func (b *InMemoryBackend) immutable(p pkgRef) bool {
	b.mu.RLock("immutable")
	defer b.mu.RUnlock()

	rec, ok := b.repos[p.repo.full()]

	return ok && rec.pb.GetDockerConfig().GetImmutableTags()
}

// CreateTag points a new tag at an existing version.
func (b *InMemoryBackend) CreateTag(
	ctx context.Context, in *artifactregistrypb.CreateTagRequest,
) (*artifactregistrypb.Tag, error) {
	p, err := parsePackage(in.GetParent())
	if err != nil {
		return nil, err
	}

	if in.GetTagId() == "" {
		return nil, errInvalid("tag_id is required.")
	}

	if b.isNPMRepo(p.repo) {
		return b.npmSetTag(p, in.GetTagId(), in.GetTag().GetVersion(), true)
	}

	digest, err := versionDigest(p, in.GetTag().GetVersion())
	if err != nil {
		return nil, err
	}

	s, err := b.snapOfPackage(p)
	if err != nil {
		return nil, err
	}

	if _, exists := s.tags[in.GetTagId()]; exists {
		return nil, errExists("Tag %q already exists.", p.full()+"/tags/"+in.GetTagId())
	}

	if err = b.retag(ctx, p, in.GetTagId(), digest); err != nil {
		return nil, err
	}

	return b.tagPB(p, in.GetTagId(), digest), nil
}

// UpdateTag moves an existing tag to another version.
func (b *InMemoryBackend) UpdateTag(
	ctx context.Context, in *artifactregistrypb.UpdateTagRequest,
) (*artifactregistrypb.Tag, error) {
	l, err := parseLeaf(in.GetTag().GetName(), "tag")
	if err != nil {
		return nil, err
	}

	for _, path := range in.GetUpdateMask().GetPaths() {
		if path != "version" {
			return nil, errInvalid("Update mask path %q is not an updatable field.", path)
		}
	}

	if b.isNPMRepo(l.pkg.repo) {
		return b.npmSetTag(l.pkg, l.leaf, in.GetTag().GetVersion(), false)
	}

	digest, err := versionDigest(l.pkg, in.GetTag().GetVersion())
	if err != nil {
		return nil, err
	}

	s, err := b.snapOfPackage(l.pkg)
	if err != nil {
		return nil, err
	}

	if _, ok := s.tags[l.leaf]; !ok {
		return nil, errNotFound("Tag %q not found.", in.GetTag().GetName())
	}

	if b.immutable(l.pkg) && s.tags[l.leaf] != digest {
		return nil, errPrecondition("Tag %q is immutable.", in.GetTag().GetName())
	}

	if err = b.retag(ctx, l.pkg, l.leaf, digest); err != nil {
		return nil, err
	}

	return b.tagPB(l.pkg, l.leaf, digest), nil
}

// DeleteTag removes a tag, leaving its version in place.
func (b *InMemoryBackend) DeleteTag(ctx context.Context, name string) error {
	l, err := parseLeaf(name, "tag")
	if err != nil {
		return err
	}

	if b.isNPMRepo(l.pkg.repo) {
		return b.npmDeleteTag(l)
	}

	s, err := b.snapOfPackage(l.pkg)
	if err != nil {
		return err
	}

	if _, ok := s.tags[l.leaf]; !ok {
		return errNotFound("Tag %q not found.", name)
	}

	repo := l.pkg.repo.dockerKey() + "/" + l.pkg.image
	if err = b.client().DeleteManifest(ctx, repo, l.leaf); err != nil {
		return errInternal(err)
	}

	b.RecordManifestDelete(repo, l.leaf)

	return nil
}
