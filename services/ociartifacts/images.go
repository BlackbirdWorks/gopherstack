package ociartifacts

import (
	"cmp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
)

const unknownVersion = "unknown"

type updateTagsIn struct {
	Tags
}

type removeVersionIn struct {
	Version string `json:"version"`
}

type restoreImageIn struct {
	Version *string `json:"version"`
}

type lookupIn struct {
	ImageURI string `json:"imageUri"`
}

func (im *Image) latest() string {
	if n := len(im.Versions); n > 0 {
		return im.Versions[n-1].Version
	}

	return ""
}

func (im *Image) hasVersion(v string) bool {
	return slices.ContainsFunc(im.Versions, func(x Version) bool { return x.Version == v })
}

func (b *InMemoryBackend) imageName(im *Image, repo *Repo) string {
	if v := im.latest(); v != "" {
		return repo.Name + ":" + v
	}

	last := im.LastVersion
	if last == "" {
		last = unknownVersion
	}

	return repo.Name + ":" + last + "@" + im.Digest
}

func (im *Image) versionPtr() *string {
	if v := im.latest(); v != "" {
		return &v
	}

	return nil
}

func (b *InMemoryBackend) imageView(im *Image, repo *Repo) imageWire {
	layers := make([]layerWire, 0, len(im.Layers))
	for _, l := range im.Layers {
		layers = append(layers, layerWire{TimeCreated: l.TimeCreated, Digest: l.Digest, SizeInBytes: l.Size})
	}

	slices.SortFunc(layers, func(a, c layerWire) int { return cmp.Compare(a.Digest, c.Digest) })

	versions := make([]versionWire, 0, len(im.Versions))
	for _, v := range im.Versions {
		versions = append(versions, versionWire{TimeCreated: v.TimeCreated, CreatedBy: v.CreatedBy, Version: v.Version})
	}

	return imageWire{
		TimeCreated: im.TimeCreated, TimeLastPulled: im.TimeLastPulled, CompartmentID: repo.CompartmentID,
		CreatedBy: im.CreatedBy, Digest: im.Digest, DisplayName: b.imageName(im, repo), ID: im.ID,
		LifecycleState: im.State, RepositoryID: repo.ID, RepositoryName: repo.Name, Version: im.versionPtr(),
		Layers: layers, Versions: versions, LayersSizeInBytes: im.LayersSize, PullCount: im.PullCount,
		ManifestSizeInBytes: im.ManifestSize, FreeformTags: im.Tags.free(), DefinedTags: im.Tags.defined(),
		SystemTags: map[string]map[string]any{},
	}
}

func (b *InMemoryBackend) imageSummary(im *Image, repo *Repo) imageSummaryWire {
	return imageSummaryWire{
		TimeCreated: im.TimeCreated, CompartmentID: repo.CompartmentID, Digest: im.Digest,
		DisplayName: b.imageName(im, repo), ID: im.ID, LifecycleState: im.State, RepositoryID: repo.ID,
		RepositoryName: repo.Name, Version: im.versionPtr(), FreeformTags: im.Tags.free(),
		DefinedTags: im.Tags.defined(), SystemTags: map[string]map[string]any{},
	}
}

func (b *InMemoryBackend) image(id string) (*Image, *Repo, *APIError) {
	im, ok := b.images[id]
	if !ok {
		return nil, nil, errNotFound(id)
	}

	repo, ok := b.repos[im.RepoID]
	if !ok {
		return nil, nil, errNotFound(id)
	}

	return im, repo, nil
}

func (b *InMemoryBackend) availableImage(id string) (*Image, *Repo, *APIError) {
	im, repo, err := b.image(id)
	if err != nil {
		return nil, nil, err
	}

	if im.State != stateAvailable {
		return nil, nil, errNotFound(id)
	}

	return im, repo, nil
}

func (b *InMemoryBackend) imageByDigest(repoID, digest string) *Image {
	for _, im := range b.images {
		if im.RepoID == repoID && im.Digest == digest {
			return im
		}
	}

	return nil
}

// imageByRef resolves a tag or digest reference to the repository's image.
func (b *InMemoryBackend) imageByRef(repoID, ref string) *Image {
	if ociregistry.IsDigest(ref) {
		return b.imageByDigest(repoID, ref)
	}

	for _, im := range b.images {
		if im.RepoID == repoID && im.State == stateAvailable && im.hasVersion(ref) {
			return im
		}
	}

	return nil
}

type imageFilter struct {
	isVersioned *bool
	compartment string
	name        string
	id          string
	repoID      string
	repoName    string
	version     string
	state       string
	digest      string
}

func (f imageFilter) match(b *InMemoryBackend, im *Image, repo *Repo, subtree bool) bool {
	return b.inCompartment(repo.CompartmentID, f.compartment, subtree) && f.matchIdentity(im, repo) &&
		f.matchState(im) && f.matchNames(b, im, repo) && f.matchVersions(im)
}

func (f imageFilter) matchIdentity(im *Image, repo *Repo) bool {
	return (f.id == "" || f.id == im.ID) && (f.repoID == "" || f.repoID == repo.ID) &&
		(f.digest == "" || f.digest == im.Digest)
}

func (f imageFilter) matchState(im *Image) bool {
	if f.state == "" {
		return im.State != stateDeleted
	}

	return strings.EqualFold(f.state, im.State)
}

func (f imageFilter) matchNames(b *InMemoryBackend, im *Image, repo *Repo) bool {
	return (f.repoName == "" || matchWild(f.repoName, repo.Name)) &&
		(f.name == "" || f.name == b.imageName(im, repo))
}

func (f imageFilter) matchVersions(im *Image) bool {
	if f.isVersioned != nil && *f.isVersioned != (len(im.Versions) > 0) {
		return false
	}

	return f.version == "" ||
		slices.ContainsFunc(im.Versions, func(v Version) bool { return matchWild(f.version, v.Version) })
}

// ListContainerImages lists the images of a compartment.
func (b *InMemoryBackend) ListContainerImages(f imageFilter, o ListOpts) (imageCollectionWire, string, error) {
	if f.compartment == "" {
		return imageCollectionWire{}, "", errMissing("compartmentId is required")
	}

	b.mu.RLock("ListContainerImages")
	defer b.mu.RUnlock()

	var all []*Image

	for _, im := range b.images {
		if repo, ok := b.repos[im.RepoID]; ok && f.match(b, im, repo, o.Subtree) {
			all = append(all, im)
		}
	}

	pg, remaining, err := paginate(all, o, func(im *Image) sortKey {
		return sortKey{created: im.TimeCreated, name: b.imageName(im, b.repos[im.RepoID]), seq: im.Seq}
	})
	if err != nil {
		return imageCollectionWire{}, "", err
	}

	out := imageCollectionWire{Items: make([]imageSummaryWire, 0, len(pg.Data)), RemainingItemsCount: remaining}
	for _, im := range pg.Data {
		out.Items = append(out.Items, b.imageSummary(im, b.repos[im.RepoID]))
	}

	return out, pg.Next, nil
}

// GetContainerImage returns an image (including DELETED ones) and its etag.
func (b *InMemoryBackend) GetContainerImage(id string) (imageWire, string, error) {
	b.mu.RLock("GetContainerImage")
	defer b.mu.RUnlock()

	im, repo, err := b.image(id)
	if err != nil {
		return imageWire{}, "", err
	}

	return b.imageView(im, repo), im.Etag, nil
}

// UpdateContainerImage changes an image's tags.
func (b *InMemoryBackend) UpdateContainerImage(id, ifMatch string, in updateTagsIn) (imageWire, string, error) {
	b.mu.Lock("UpdateContainerImage")
	defer b.mu.Unlock()

	im, repo, err := b.availableImage(id)
	if err != nil {
		return imageWire{}, "", err
	}

	if err = checkEtag(im.Etag, ifMatch); err != nil {
		return imageWire{}, "", err
	}

	im.Tags.merge(in.Tags)
	im.Etag = newEtag()

	return b.imageView(im, repo), im.Etag, nil
}

// DeleteContainerImage marks an image DELETED; it stops resolving on pull until restored.
func (b *InMemoryBackend) DeleteContainerImage(id, ifMatch string) error {
	b.mu.Lock("DeleteContainerImage")
	defer b.mu.Unlock()

	im, _, err := b.availableImage(id)
	if err != nil {
		return err
	}

	if err = checkEtag(im.Etag, ifMatch); err != nil {
		return err
	}

	im.State, im.Etag = stateDeleted, newEtag()

	return nil
}

// RestoreContainerImage brings a DELETED image back, optionally under a new version.
func (b *InMemoryBackend) RestoreContainerImage(id, ifMatch string, in restoreImageIn) (imageWire, string, error) {
	b.mu.Lock("RestoreContainerImage")
	defer b.mu.Unlock()

	im, repo, err := b.image(id)
	if err != nil {
		return imageWire{}, "", err
	}

	if err = checkEtag(im.Etag, ifMatch); err != nil {
		return imageWire{}, "", err
	}

	if im.State != stateDeleted {
		return imageWire{}, "", errState("The container image is not deleted")
	}

	version := str(in.Version)
	if in.Version != nil {
		if version == "" || strings.Contains(version, ":") {
			return imageWire{}, "", errParam("version is not a valid tag")
		}

		if owner := b.imageByRef(repo.ID, version); owner != nil && owner != im && repo.IsImmutable {
			return imageWire{}, "", errConflict("version " + version + " already exists in an immutable repository")
		}
	}

	kept := im.Versions[:0:0]

	for _, v := range im.Versions {
		if owner := b.imageByRef(repo.ID, v.Version); owner == nil || owner == im {
			kept = append(kept, v)
		}
	}

	im.State, im.Versions, im.Etag = stateAvailable, kept, newEtag()

	if version != "" {
		b.restoreTag(repo, im, version)
	}

	return b.imageView(im, repo), im.Etag, nil
}

func (b *InMemoryBackend) restoreTag(repo *Repo, im *Image, version string) {
	b.moveTag(repo.ID, im, version, im.CreatedBy)

	app := b.registryLocked()
	client := ociregistry.NewClient(app)

	m, err := client.GetManifest(b.ctx, b.regName(repo), im.Digest)
	if err != nil {
		return
	}

	_, _ = client.PutManifest(b.ctx, b.regName(repo), version, m.MediaType, m.Body)
}

// RemoveContainerVersion removes one version from an image.
func (b *InMemoryBackend) RemoveContainerVersion(id, ifMatch string, in removeVersionIn) (imageWire, string, error) {
	if in.Version == "" {
		return imageWire{}, "", errMissing("version is required")
	}

	b.mu.Lock("RemoveContainerVersion")
	defer b.mu.Unlock()

	im, repo, err := b.availableImage(id)
	if err != nil {
		return imageWire{}, "", err
	}

	if err = checkEtag(im.Etag, ifMatch); err != nil {
		return imageWire{}, "", err
	}

	if !im.hasVersion(in.Version) {
		return imageWire{}, "", errNotFound(in.Version)
	}

	b.dropVersion(im, in.Version)
	im.Etag = newEtag()

	_ = ociregistry.NewClient(b.registryLocked()).DeleteManifest(b.ctx, b.regName(repo), in.Version)

	return b.imageView(im, repo), im.Etag, nil
}

func (b *InMemoryBackend) dropVersion(im *Image, version string) {
	im.Versions = slices.DeleteFunc(im.Versions, func(v Version) bool { return v.Version == version })
	im.LastVersion = version

	if l := im.latest(); l != "" {
		im.LastVersion = l
	}
}

// moveTag gives version to im, taking it from any other image of the repository.
func (b *InMemoryBackend) moveTag(repoID string, im *Image, version, by string) {
	for _, other := range b.images {
		if other != im && other.RepoID == repoID && other.hasVersion(version) {
			b.dropVersion(other, version)
			other.Etag = newEtag()
		}
	}

	im.Versions = slices.DeleteFunc(im.Versions, func(v Version) bool { return v.Version == version })
	im.Versions = append(im.Versions, Version{Version: version, CreatedBy: by, TimeCreated: b.nowUTC()})
	im.LastVersion = version
}

// LookupContainerImageByURI finds an image by `namespace/repo:version` or `namespace/repo@digest`.
func (b *InMemoryBackend) LookupContainerImageByURI(uri string) (imageWire, string, error) {
	b.mu.RLock("LookupContainerImageByURI")
	defer b.mu.RUnlock()

	if uri == "" {
		return imageWire{}, "", errMissing("imageUri is required")
	}

	rest, ok := strings.CutPrefix(uri, b.namespace+"/")
	if !ok {
		return imageWire{}, "", errNotFound(uri)
	}

	name, ref := rest, ""

	if i := strings.Index(rest, "@"); i >= 0 {
		name, ref = rest[:i], rest[i+1:]
	} else if i = strings.LastIndex(rest, ":"); i >= 0 && !strings.Contains(rest[i:], "/") {
		name, ref = rest[:i], rest[i+1:]
	}

	repo := b.repoByName(name)
	if repo == nil || ref == "" {
		return imageWire{}, "", errNotFound(uri)
	}

	im := b.imageByRef(repo.ID, ref)
	if im == nil || im.State != stateAvailable {
		return imageWire{}, "", errNotFound(uri)
	}

	return b.imageView(im, repo), im.Etag, nil
}

func (b *InMemoryBackend) regName(r *Repo) string { return strings.ToLower(b.namespace) + "/" + r.Name }

// purgeRepoManifests removes a repository's manifests from the embedded registry.
func (b *InMemoryBackend) purgeRepoManifests(r *Repo) {
	client := ociregistry.NewClient(b.registryLocked())

	for _, im := range b.images {
		if im.RepoID == r.ID {
			_ = client.DeleteManifest(b.ctx, b.regName(r), im.Digest)
		}
	}
}
