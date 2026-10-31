package ociartifacts

import (
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxRepoName = 255
	bytesPerGB  = 1_000_000_000
)

// repoNameRe is one path component of a repository name, as in the Docker distribution grammar.
var repoNameRe = regexp.MustCompile(`^[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*$`)

type readmeIn struct {
	Content *string `json:"content"`
	Format  string  `json:"format"`
}

type createRepoIn struct {
	Tags
	IsImmutable   *bool     `json:"isImmutable"`
	IsPublic      *bool     `json:"isPublic"`
	Readme        *readmeIn `json:"readme"`
	DisplayName   string    `json:"displayName"`
	CompartmentID string    `json:"compartmentId"`
}

type updateRepoIn struct {
	Tags
	IsImmutable *bool     `json:"isImmutable"`
	IsPublic    *bool     `json:"isPublic"`
	Readme      *readmeIn `json:"readme"`
}

type changeCompartmentIn struct {
	CompartmentID string `json:"compartmentId"`
}

func validRepoName(name string) *APIError {
	if name == "" {
		return errMissing("displayName is required")
	}

	if len(name) > maxRepoName {
		return errParam("displayName must be at most 255 characters")
	}

	for part := range strings.SplitSeq(name, "/") {
		if !repoNameRe.MatchString(part) {
			return errParam("displayName must be lowercase path components of letters, digits, '.', '_' and '-' " +
				"separated by '/'")
		}
	}

	return nil
}

func toReadme(in *readmeIn) (*Readme, *APIError) {
	if in == nil {
		return nil, nil
	}

	if in.Content == nil {
		return nil, errMissing("readme.content is required")
	}

	switch in.Format {
	case readmeMarkdown, readmePlain:
		return &Readme{Content: *in.Content, Format: in.Format}, nil
	case "":
		return nil, errMissing("readme.format is required")
	}

	return nil, errParam("readme.format must be TEXT_MARKDOWN or TEXT_PLAIN")
}

type repoStats struct {
	images int
	layers int
	size   int64
}

// stats totals the AVAILABLE images of a repository; shared layers count once.
func (b *InMemoryBackend) stats(repoID string) repoStats {
	var s repoStats

	seen := map[string]struct{}{}

	for _, im := range b.images {
		if im.RepoID != repoID || im.State != stateAvailable {
			continue
		}

		s.images++

		for _, l := range im.Layers {
			if _, dup := seen[l.Digest]; !dup {
				seen[l.Digest] = struct{}{}
				s.layers++
				s.size += l.Size
			}
		}
	}

	return s
}

func billableGBs(size int64) int64 {
	return (size + bytesPerGB - 1) / bytesPerGB
}

func (b *InMemoryBackend) repoView(r *Repo) repoWire {
	s := b.stats(r.ID)
	w := repoWire{
		TimeCreated: r.TimeCreated, TimeLastPushed: r.TimeLastPushed, CompartmentID: r.CompartmentID,
		CreatedBy: r.CreatedBy, DisplayName: r.Name, ID: r.ID, LifecycleState: stateAvailable, Namespace: b.namespace,
		LayersSizeInBytes: s.size, BillableSizeInGBs: billableGBs(s.size), ImageCount: s.images, LayerCount: s.layers,
		IsImmutable: r.IsImmutable, IsPublic: r.IsPublic, FreeformTags: r.Tags.free(), DefinedTags: r.Tags.defined(),
		SystemTags: map[string]map[string]any{},
	}

	if r.Readme != nil {
		w.Readme = &readmeWire{Content: r.Readme.Content, Format: r.Readme.Format}
	}

	return w
}

func (b *InMemoryBackend) repoSummary(r *Repo) repoSummaryWire {
	s := b.stats(r.ID)

	return repoSummaryWire{
		TimeCreated: r.TimeCreated, CompartmentID: r.CompartmentID, DisplayName: r.Name, ID: r.ID,
		LifecycleState: stateAvailable, Namespace: b.namespace, LayersSizeInBytes: s.size,
		BillableSizeInGBs: billableGBs(s.size), ImageCount: s.images, LayerCount: s.layers, IsPublic: r.IsPublic,
		FreeformTags: r.Tags.free(), DefinedTags: r.Tags.defined(), SystemTags: map[string]map[string]any{},
	}
}

func (b *InMemoryBackend) repoByName(name string) *Repo {
	for _, r := range b.repos {
		if strs.Equal(r.Name, name) {
			return r
		}
	}

	return nil
}

func (b *InMemoryBackend) repo(id string) (*Repo, *APIError) {
	r, ok := b.repos[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return r, nil
}

func (b *InMemoryBackend) newRepo(name, compartment, createdBy string) *Repo {
	r := &Repo{
		ID: newOCID("containerrepo"), Name: name, CompartmentID: compartment, CreatedBy: createdBy,
		TimeCreated: b.nowUTC(), Etag: newEtag(), Seq: b.nextSeq(),
	}
	b.repos[r.ID] = r

	return r
}

// CreateContainerRepository creates a repository; createdBy is the signing user's OCID.
func (b *InMemoryBackend) CreateContainerRepository(in createRepoIn, createdBy string) (repoWire, string, error) {
	readme, rerr := toReadme(in.Readme)

	b.mu.Lock("CreateContainerRepository")
	defer b.mu.Unlock()

	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return repoWire{}, "", err
	}

	if err := validRepoName(in.DisplayName); err != nil {
		return repoWire{}, "", err
	}

	if rerr != nil {
		return repoWire{}, "", rerr
	}

	if b.repoByName(in.DisplayName) != nil {
		return repoWire{}, "", errConflict("A container repository named " + in.DisplayName + " already exists")
	}

	r := b.newRepo(in.DisplayName, in.CompartmentID, createdBy)
	r.IsImmutable, r.IsPublic = in.IsImmutable != nil && *in.IsImmutable, in.IsPublic != nil && *in.IsPublic
	r.Readme, r.Tags = readme, in.Tags

	return b.repoView(r), r.Etag, nil
}

// GetContainerRepository returns a repository and its etag.
func (b *InMemoryBackend) GetContainerRepository(id string) (repoWire, string, error) {
	b.mu.RLock("GetContainerRepository")
	defer b.mu.RUnlock()

	r, err := b.repo(id)
	if err != nil {
		return repoWire{}, "", err
	}

	return b.repoView(r), r.Etag, nil
}

type repoFilter struct {
	isPublic    *bool
	compartment string
	id          string
	name        string
	state       string
}

// ListContainerRepositories lists repositories of a compartment.
func (b *InMemoryBackend) ListContainerRepositories(f repoFilter, o ListOpts) (repoCollectionWire, string, error) {
	if f.compartment == "" {
		return repoCollectionWire{}, "", errMissing("compartmentId is required")
	}

	b.mu.RLock("ListContainerRepositories")
	defer b.mu.RUnlock()

	var all []*Repo

	for _, r := range b.repos {
		if f.state != "" && !strings.EqualFold(f.state, stateAvailable) {
			continue
		}

		if b.inCompartment(r.CompartmentID, f.compartment, o.Subtree) && (f.id == "" || f.id == r.ID) &&
			(f.name == "" || strs.Equal(f.name, r.Name)) && (f.isPublic == nil || *f.isPublic == r.IsPublic) {
			all = append(all, r)
		}
	}

	pg, remaining, err := paginate(all, o, func(r *Repo) sortKey {
		return sortKey{created: r.TimeCreated, name: r.Name, seq: r.Seq}
	})
	if err != nil {
		return repoCollectionWire{}, "", err
	}

	out := repoCollectionWire{Items: make([]repoSummaryWire, 0, len(pg.Data)), RemainingItemsCount: remaining}

	for _, r := range pg.Data {
		out.Items = append(out.Items, b.repoSummary(r))
	}

	out.RepositoryCount = len(all)
	b.addTotals(&out, all)

	return out, pg.Next, nil
}

// addTotals fills the collection-wide image, layer and size counts over every matching repository.
func (b *InMemoryBackend) addTotals(out *repoCollectionWire, repos []*Repo) {
	ids := map[string]struct{}{}
	for _, r := range repos {
		ids[r.ID] = struct{}{}
	}

	seen := map[string]struct{}{}

	for _, im := range b.images {
		if _, ok := ids[im.RepoID]; !ok || im.State != stateAvailable {
			continue
		}

		out.ImageCount++

		for _, l := range im.Layers {
			if _, dup := seen[l.Digest]; !dup {
				seen[l.Digest] = struct{}{}
				out.LayerCount++
				out.LayersSizeInBytes += l.Size
			}
		}
	}
}

// UpdateContainerRepository changes the mutable attributes of a repository.
func (b *InMemoryBackend) UpdateContainerRepository(id, ifMatch string, in updateRepoIn) (repoWire, string, error) {
	readme, rerr := toReadme(in.Readme)

	b.mu.Lock("UpdateContainerRepository")
	defer b.mu.Unlock()

	r, err := b.repo(id)
	if err != nil {
		return repoWire{}, "", err
	}

	if err = checkEtag(r.Etag, ifMatch); err != nil {
		return repoWire{}, "", err
	}

	if rerr != nil {
		return repoWire{}, "", rerr
	}

	if in.IsImmutable != nil {
		r.IsImmutable = *in.IsImmutable
	}

	if in.IsPublic != nil {
		r.IsPublic = *in.IsPublic
	}

	if readme != nil {
		r.Readme = readme
	}

	r.Tags.merge(in.Tags)
	r.Etag = newEtag()

	return b.repoView(r), r.Etag, nil
}

// DeleteContainerRepository deletes a repository with its images and signatures, and their stored blobs.
func (b *InMemoryBackend) DeleteContainerRepository(id, ifMatch string) error {
	b.mu.Lock("DeleteContainerRepository")
	defer b.mu.Unlock()

	r, err := b.repo(id)
	if err != nil {
		return err
	}

	if err = checkEtag(r.Etag, ifMatch); err != nil {
		return err
	}

	b.purgeRepoManifests(r)

	for imageID, im := range b.images {
		if im.RepoID != r.ID {
			continue
		}

		for sid, s := range b.sigs {
			if s.ImageID == imageID {
				delete(b.sigs, sid)
			}
		}

		delete(b.images, imageID)
	}

	delete(b.repos, id)

	return nil
}

// ChangeContainerRepositoryCompartment moves a repository.
func (b *InMemoryBackend) ChangeContainerRepositoryCompartment(id, ifMatch string, in changeCompartmentIn) error {
	b.mu.Lock("ChangeContainerRepositoryCompartment")
	defer b.mu.Unlock()

	r, err := b.repo(id)
	if err != nil {
		return err
	}

	if err = checkEtag(r.Etag, ifMatch); err != nil {
		return err
	}

	if err = b.checkCompartment(in.CompartmentID); err != nil {
		return err
	}

	r.CompartmentID, r.Etag = in.CompartmentID, newEtag()

	return nil
}

type updateConfigIn struct {
	IsRepositoryCreatedOnFirstPush *bool `json:"isRepositoryCreatedOnFirstPush"`
}

// GetContainerConfiguration returns the tenancy's container configuration.
func (b *InMemoryBackend) GetContainerConfiguration(compartment string) (configWire, string, error) {
	b.mu.RLock("GetContainerConfiguration")
	defer b.mu.RUnlock()

	if err := b.checkCompartment(compartment); err != nil {
		return configWire{}, "", err
	}

	return configWire{Namespace: b.namespace, IsRepositoryCreatedOnFirstPush: b.createOnPush}, b.configEtag, nil
}

// UpdateContainerConfiguration changes the container configuration.
func (b *InMemoryBackend) UpdateContainerConfiguration(
	compartment, ifMatch string, in updateConfigIn,
) (configWire, string, error) {
	b.mu.Lock("UpdateContainerConfiguration")
	defer b.mu.Unlock()

	if err := b.checkCompartment(compartment); err != nil {
		return configWire{}, "", err
	}

	if err := checkEtag(b.configEtag, ifMatch); err != nil {
		return configWire{}, "", err
	}

	if in.IsRepositoryCreatedOnFirstPush != nil {
		b.createOnPush = *in.IsRepositoryCreatedOnFirstPush
	}

	b.configEtag = newEtag()

	return configWire{Namespace: b.namespace, IsRepositoryCreatedOnFirstPush: b.createOnPush}, b.configEtag, nil
}
