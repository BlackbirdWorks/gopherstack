package gcpartifactregistry

import (
	"context"
	"sort"
	"strconv"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
)

// imageSnap is a copy of one image's ledger entry, safe to use without the backend lock.
type imageSnap struct {
	manifests map[string]ociregistry.ManifestEntry
	tags      map[string]string
	image     string
}

func (s imageSnap) digests() []string {
	out := make([]string, 0, len(s.manifests))
	for d := range s.manifests {
		out = append(out, d)
	}

	sort.Strings(out)

	return out
}

func (s imageSnap) tagsOf(digest string) []string {
	var out []string

	for t, d := range s.tags {
		if d == digest {
			out = append(out, t)
		}
	}

	sort.Strings(out)

	return out
}

func (s imageSnap) span() (*timestamppb.Timestamp, *timestamppb.Timestamp) {
	var lo, hi int64

	for _, m := range s.manifests {
		if c := m.Created.UnixNano(); lo == 0 || c < lo {
			lo = c
		}

		if u := m.Updated.UnixNano(); u > hi {
			hi = u
		}
	}

	return timestamppb.New(timeFromNanos(lo)), timestamppb.New(timeFromNanos(hi))
}

func snapOf(image string, r *ociregistry.RepoEntry) imageSnap {
	s := imageSnap{
		image: image, manifests: make(map[string]ociregistry.ManifestEntry, len(r.Manifests)),
		tags: make(map[string]string, len(r.Tags)),
	}

	for d, m := range r.Manifests {
		s.manifests[d] = *m
	}

	for t, e := range r.Tags {
		s.tags[t] = e.Digest
	}

	return s
}

// imageSnaps copies the ledger entries of the repository's images; only limits it to one image when non-empty.
func (b *InMemoryBackend) imageSnaps(ref repoRef, only string) []imageSnap {
	b.mu.RLock("imageSnaps")
	defer b.mu.RUnlock()

	var out []imageSnap

	for _, image := range b.imagesLocked(ref) {
		if only == "" || image == only {
			out = append(out, snapOf(image, b.ledger[ref.dockerKey()+"/"+image]))
		}
	}

	return out
}

func (b *InMemoryBackend) requireDocker(ref repoRef) error {
	b.mu.RLock("requireDocker")
	defer b.mu.RUnlock()

	rec, ok := b.repos[ref.full()]
	if !ok {
		return errNotFound("Repository %q not found.", ref.full())
	}

	if rec.pb.GetFormat() != artifactregistrypb.Repository_DOCKER {
		return errPrecondition("Repository %q is not a DOCKER repository.", ref.full())
	}

	return nil
}

type manifestInfo struct {
	mediaType string
	size      int64
}

func (b *InMemoryBackend) describe(ctx context.Context, ref repoRef, image, digest string) manifestInfo {
	c := b.client()
	repo := ref.dockerKey() + "/" + image

	m, err := c.GetManifest(ctx, repo, digest)
	if err != nil {
		return manifestInfo{}
	}

	info := c.Describe(ctx, repo, m)

	return manifestInfo{mediaType: info.MediaType, size: info.Size}
}

func (b *InMemoryBackend) repoSize(ref repoRef) int64 {
	ctx := b.ctxOrBackground()

	var total int64

	for _, s := range b.imageSnaps(ref, "") {
		for d := range s.manifests {
			total += b.describe(ctx, ref, s.image, d).size
		}
	}

	return total
}

func (b *InMemoryBackend) ctxOrBackground() context.Context {
	b.mu.RLock("ctx")
	defer b.mu.RUnlock()

	return context.WithoutCancel(b.ctx)
}

func (b *InMemoryBackend) purgeImages(ctx context.Context, ref repoRef, images []string) {
	c := b.client()

	for _, image := range images {
		repo := ref.dockerKey() + "/" + image

		var digests []string

		b.mu.RLock("purgeImages")
		if r, ok := b.ledger[repo]; ok {
			digests = snapOf(image, r).digests()
		}
		b.mu.RUnlock()

		for _, d := range digests {
			_ = c.DeleteManifest(ctx, repo, d)

			b.RecordManifestDelete(repo, d)
		}
	}
}

func (b *InMemoryBackend) imageURI(ref repoRef, image, digest string) string {
	b.mu.RLock("imageURI")
	defer b.mu.RUnlock()

	return b.registryURI(ref) + "/" + image + "@" + digest
}

func dockerImageName(ref repoRef, image, digest string) string {
	return ref.full() + "/dockerImages/" + escapeImage(image) + "@" + digest
}

func (b *InMemoryBackend) dockerImage(
	ctx context.Context, ref repoRef, s imageSnap, digest string,
) *artifactregistrypb.DockerImage {
	m := s.manifests[digest]
	info := b.describe(ctx, ref, s.image, digest)

	return &artifactregistrypb.DockerImage{
		Name: dockerImageName(ref, s.image, digest), Uri: b.imageURI(ref, s.image, digest), Tags: s.tagsOf(digest),
		ImageSizeBytes: info.size, MediaType: info.mediaType,
		UploadTime: timestamppb.New(m.Created), UpdateTime: timestamppb.New(m.Updated),
	}
}

func (b *InMemoryBackend) pkg(ref repoRef, s imageSnap) *artifactregistrypb.Package {
	created, updated := s.span()

	return &artifactregistrypb.Package{
		Name: ref.packageName(s.image), DisplayName: s.image, CreateTime: created, UpdateTime: updated,
	}
}

func (b *InMemoryBackend) version(
	ctx context.Context, p pkgRef, s imageSnap, digest string, full bool,
) *artifactregistrypb.Version {
	m := s.manifests[digest]
	name := p.full() + "/versions/" + digest

	v := &artifactregistrypb.Version{
		Name: name, CreateTime: timestamppb.New(m.Created), UpdateTime: timestamppb.New(m.Updated),
	}

	for _, t := range s.tagsOf(digest) {
		v.RelatedTags = append(v.RelatedTags, &artifactregistrypb.Tag{Name: p.full() + "/tags/" + t, Version: name})
	}

	if full {
		info := b.describe(ctx, p.repo, s.image, digest)
		v.Metadata, _ = structpb.NewStruct(map[string]any{
			"imageSizeBytes": strconv.FormatInt(info.size, 10), "mediaType": info.mediaType,
			"name": dockerImageName(p.repo, s.image, digest),
		})
	}

	return v
}
