package ociartifacts

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxGenericName    = 255
	maxArtifactPath   = 255
	maxArtifactVer    = 255
	maxArtifactBytes  = 256 << 20
	repositoryTypeGen = "GENERIC"
)

type createGenericRepoIn struct {
	Tags
	IsImmutable    *bool   `json:"isImmutable"`
	DisplayName    *string `json:"displayName"`
	Description    *string `json:"description"`
	CompartmentID  string  `json:"compartmentId"`
	RepositoryType string  `json:"repositoryType"`
}

type updateGenericRepoIn struct {
	Tags
	DisplayName    *string `json:"displayName"`
	Description    *string `json:"description"`
	RepositoryType string  `json:"repositoryType"`
}

func (r *GenericRepo) view() genericRepoWire {
	return genericRepoWire{
		TimeCreated: r.TimeCreated, ID: r.ID, DisplayName: r.Name, CompartmentID: r.CompartmentID,
		Description: r.Description, LifecycleState: stateAvailable, RepositoryType: repositoryTypeGen,
		IsImmutable: r.IsImmutable, FreeformTags: r.Tags.free(), DefinedTags: r.Tags.defined(),
	}
}

func (b *InMemoryBackend) artifactView(a *GenericArtifact, repo *GenericRepo) genericArtifactWire {
	name := a.Path + ":" + a.Version
	if len(name) > maxGenericName {
		name = name[:maxGenericName]
	}

	return genericArtifactWire{
		TimeCreated: a.TimeCreated, ID: a.ID, DisplayName: name, CompartmentID: repo.CompartmentID,
		RepositoryID: repo.ID, ArtifactPath: a.Path, Version: a.Version, Sha256: a.Sha256,
		LifecycleState: stateAvailable, SizeInBytes: int64(len(a.Content)), FreeformTags: a.Tags.free(),
		DefinedTags: a.Tags.defined(),
	}
}

func validGenericName(name string) *APIError {
	if name == "" || len(name) > maxGenericName || strings.ContainsFunc(name, unicode.IsControl) {
		return errParam("displayName must be 1 to 255 printable characters")
	}

	return nil
}

func (b *InMemoryBackend) genericRepo(id string) (*GenericRepo, *APIError) {
	r, ok := b.gens[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return r, nil
}

// CreateRepository creates a generic artifacts repository.
func (b *InMemoryBackend) CreateRepository(in createGenericRepoIn) (genericRepoWire, string, error) {
	if in.RepositoryType != repositoryTypeGen {
		return genericRepoWire{}, "", errParam("repositoryType must be GENERIC")
	}

	if in.IsImmutable == nil {
		return genericRepoWire{}, "", errMissing("isImmutable is required")
	}

	b.mu.Lock("CreateRepository")
	defer b.mu.Unlock()

	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return genericRepoWire{}, "", err
	}

	r := &GenericRepo{
		ID: newOCID("artifactrepository"), CompartmentID: in.CompartmentID, IsImmutable: *in.IsImmutable,
		Description: str(in.Description), TimeCreated: b.nowUTC(), Etag: newEtag(), Seq: b.nextSeq(), Tags: in.Tags,
	}

	if in.DisplayName != nil {
		if err := validGenericName(*in.DisplayName); err != nil {
			return genericRepoWire{}, "", err
		}

		r.Name = *in.DisplayName
	} else {
		r.Name = "repository" + r.TimeCreated.Format("20060102150405")
	}

	b.gens[r.ID] = r

	return r.view(), r.Etag, nil
}

// GetRepository returns a generic repository and its etag.
func (b *InMemoryBackend) GetRepository(id string) (genericRepoWire, string, error) {
	b.mu.RLock("GetRepository")
	defer b.mu.RUnlock()

	r, err := b.genericRepo(id)
	if err != nil {
		return genericRepoWire{}, "", err
	}

	return r.view(), r.Etag, nil
}

type genericRepoFilter struct {
	isImmutable *bool
	compartment string
	id          string
	name        string
	state       string
}

// ListRepositories lists the generic repositories of a compartment.
func (b *InMemoryBackend) ListRepositories(f genericRepoFilter, o ListOpts) (genericRepoCollectionWire, string, error) {
	if f.compartment == "" {
		return genericRepoCollectionWire{}, "", errMissing("compartmentId is required")
	}

	b.mu.RLock("ListRepositories")
	defer b.mu.RUnlock()

	var all []*GenericRepo

	for _, r := range b.gens {
		if (f.state == "" || strings.EqualFold(f.state, stateAvailable)) && r.CompartmentID == f.compartment &&
			(f.id == "" || f.id == r.ID) && (f.name == "" || strs.Equal(f.name, r.Name)) &&
			(f.isImmutable == nil || *f.isImmutable == r.IsImmutable) {
			all = append(all, r)
		}
	}

	pg, _, err := paginate(all, o, func(r *GenericRepo) sortKey {
		return sortKey{created: r.TimeCreated, name: r.Name, seq: r.Seq}
	})
	if err != nil {
		return genericRepoCollectionWire{}, "", err
	}

	out := genericRepoCollectionWire{Items: make([]genericRepoWire, 0, len(pg.Data))}
	for _, r := range pg.Data {
		out.Items = append(out.Items, r.view())
	}

	return out, pg.Next, nil
}

// UpdateRepository changes a generic repository's name, description and tags.
func (b *InMemoryBackend) UpdateRepository(
	id, ifMatch string,
	in updateGenericRepoIn,
) (genericRepoWire, string, error) {
	if in.RepositoryType != repositoryTypeGen {
		return genericRepoWire{}, "", errParam("repositoryType must be GENERIC")
	}

	b.mu.Lock("UpdateRepository")
	defer b.mu.Unlock()

	r, err := b.genericRepo(id)
	if err != nil {
		return genericRepoWire{}, "", err
	}

	if err = checkEtag(r.Etag, ifMatch); err != nil {
		return genericRepoWire{}, "", err
	}

	if in.DisplayName != nil {
		if err = validGenericName(*in.DisplayName); err != nil {
			return genericRepoWire{}, "", err
		}

		r.Name = *in.DisplayName
	}

	if in.Description != nil {
		r.Description = *in.Description
	}

	r.Tags.merge(in.Tags)
	r.Etag = newEtag()

	return r.view(), r.Etag, nil
}

// DeleteRepository deletes a generic repository together with its artifacts.
func (b *InMemoryBackend) DeleteRepository(id, ifMatch string) error {
	b.mu.Lock("DeleteRepository")
	defer b.mu.Unlock()

	r, err := b.genericRepo(id)
	if err != nil {
		return err
	}

	if err = checkEtag(r.Etag, ifMatch); err != nil {
		return err
	}

	for aid, a := range b.arts {
		if a.RepoID == id {
			delete(b.arts, aid)
		}
	}

	delete(b.gens, id)

	return nil
}

// ChangeRepositoryCompartment moves a generic repository.
func (b *InMemoryBackend) ChangeRepositoryCompartment(id, ifMatch string, in changeCompartmentIn) error {
	b.mu.Lock("ChangeRepositoryCompartment")
	defer b.mu.Unlock()

	r, err := b.genericRepo(id)
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

func (b *InMemoryBackend) artifact(id string) (*GenericArtifact, *GenericRepo, *APIError) {
	a, ok := b.arts[id]
	if !ok {
		return nil, nil, errNotFound(id)
	}

	repo, ok := b.gens[a.RepoID]
	if !ok {
		return nil, nil, errNotFound(id)
	}

	return a, repo, nil
}

func (b *InMemoryBackend) artifactByPath(repoID, path, version string) (*GenericArtifact, *GenericRepo, *APIError) {
	repo, err := b.genericRepo(repoID)
	if err != nil {
		return nil, nil, err
	}

	for _, a := range b.arts {
		if a.RepoID == repoID && a.Path == path && a.Version == version {
			return a, repo, nil
		}
	}

	return nil, nil, errNotFound(path + ":" + version)
}

// GetGenericArtifact returns an artifact by id.
func (b *InMemoryBackend) GetGenericArtifact(id string) (genericArtifactWire, string, error) {
	b.mu.RLock("GetGenericArtifact")
	defer b.mu.RUnlock()

	a, repo, err := b.artifact(id)
	if err != nil {
		return genericArtifactWire{}, "", err
	}

	return b.artifactView(a, repo), a.Etag, nil
}

// GetGenericArtifactByPath returns an artifact by repository, path and version.
func (b *InMemoryBackend) GetGenericArtifactByPath(repoID, path, version string) (genericArtifactWire, string, error) {
	b.mu.RLock("GetGenericArtifactByPath")
	defer b.mu.RUnlock()

	a, repo, err := b.artifactByPath(repoID, path, version)
	if err != nil {
		return genericArtifactWire{}, "", err
	}

	return b.artifactView(a, repo), a.Etag, nil
}

// GenericContent is an artifact body with its etag.
type GenericContent struct {
	Etag    string
	Sha256  string
	Content []byte
}

// GetGenericArtifactContent returns an artifact's bytes by id.
func (b *InMemoryBackend) GetGenericArtifactContent(id string) (GenericContent, error) {
	b.mu.RLock("GetGenericArtifactContent")
	defer b.mu.RUnlock()

	a, _, err := b.artifact(id)
	if err != nil {
		return GenericContent{}, err
	}

	return GenericContent{Etag: a.Etag, Sha256: a.Sha256, Content: a.Content}, nil
}

// GetGenericArtifactContentByPath returns an artifact's bytes by repository, path and version.
func (b *InMemoryBackend) GetGenericArtifactContentByPath(repoID, path, version string) (GenericContent, error) {
	b.mu.RLock("GetGenericArtifactContentByPath")
	defer b.mu.RUnlock()

	a, _, err := b.artifactByPath(repoID, path, version)
	if err != nil {
		return GenericContent{}, err
	}

	return GenericContent{Etag: a.Etag, Sha256: a.Sha256, Content: a.Content}, nil
}

func validArtifactKey(path, version string) *APIError {
	if path == "" || len(path) > maxArtifactPath || strings.ContainsFunc(path, unicode.IsControl) {
		return errParam("artifactPath must be 1 to 255 printable characters")
	}

	if version == "" || len(version) > maxArtifactVer || strings.ContainsFunc(version, unicode.IsControl) {
		return errParam("version must be 1 to 255 printable characters")
	}

	return nil
}

// PutGenericArtifactContentByPath uploads an artifact, replacing the version in a mutable repository.
func (b *InMemoryBackend) PutGenericArtifactContentByPath(
	repoID, path, version, ifMatch string, content []byte,
) (genericArtifactWire, string, error) {
	if err := validArtifactKey(path, version); err != nil {
		return genericArtifactWire{}, "", err
	}

	if len(content) > maxArtifactBytes {
		return genericArtifactWire{}, "", errTooLarge()
	}

	sum := sha256.Sum256(content)

	b.mu.Lock("PutGenericArtifactContentByPath")
	defer b.mu.Unlock()

	repo, err := b.genericRepo(repoID)
	if err != nil {
		return genericArtifactWire{}, "", err
	}

	a, _, lookupErr := b.artifactByPath(repoID, path, version)
	if lookupErr == nil {
		if repo.IsImmutable {
			return genericArtifactWire{}, "", errConflict(
				fmt.Sprintf("artifact %s:%s already exists in an immutable repository", path, version))
		}

		if err = checkEtag(a.Etag, ifMatch); err != nil {
			return genericArtifactWire{}, "", err
		}
	} else {
		a = &GenericArtifact{
			ID: newOCID("genericartifact"), RepoID: repoID, Path: path, Version: version,
			TimeCreated: b.nowUTC(), Seq: b.nextSeq(),
		}
		b.arts[a.ID] = a
	}

	a.Content, a.Sha256, a.Etag = append(
		[]byte(nil),
		content...), strings.ToUpper(
		hex.EncodeToString(sum[:]),
	), newEtag()

	return b.artifactView(a, repo), a.Etag, nil
}

type genericArtifactFilter struct {
	compartment string
	repoID      string
	id          string
	name        string
	path        string
	version     string
	sha256      string
	state       string
}

// ListGenericArtifacts lists the artifacts of a repository.
func (b *InMemoryBackend) ListGenericArtifacts(
	f genericArtifactFilter, o ListOpts,
) (genericArtifactCollectionWire, string, error) {
	if f.compartment == "" {
		return genericArtifactCollectionWire{}, "", errMissing("compartmentId is required")
	}

	if f.repoID == "" {
		return genericArtifactCollectionWire{}, "", errMissing("repositoryId is required")
	}

	b.mu.RLock("ListGenericArtifacts")
	defer b.mu.RUnlock()

	repo, err := b.genericRepo(f.repoID)
	if err != nil {
		return genericArtifactCollectionWire{}, "", err
	}

	var all []*GenericArtifact

	for _, a := range b.arts {
		if a.RepoID == f.repoID && repo.CompartmentID == f.compartment && b.artifactMatch(a, repo, f) {
			all = append(all, a)
		}
	}

	pg, _, perr := paginate(all, o, func(a *GenericArtifact) sortKey {
		return sortKey{created: a.TimeCreated, name: b.artifactView(a, repo).DisplayName, seq: a.Seq}
	})
	if perr != nil {
		return genericArtifactCollectionWire{}, "", perr
	}

	out := genericArtifactCollectionWire{Items: make([]genericArtifactWire, 0, len(pg.Data))}
	for _, a := range pg.Data {
		out.Items = append(out.Items, b.artifactView(a, repo))
	}

	return out, pg.Next, nil
}

func (b *InMemoryBackend) artifactMatch(a *GenericArtifact, repo *GenericRepo, f genericArtifactFilter) bool {
	return (f.state == "" || strings.EqualFold(f.state, stateAvailable)) && (f.id == "" || f.id == a.ID) &&
		(f.name == "" || f.name == b.artifactView(a, repo).DisplayName) &&
		strings.HasPrefix(a.Path, f.path) && strings.HasPrefix(a.Version, f.version) &&
		(f.sha256 == "" || strings.EqualFold(f.sha256, a.Sha256))
}

func (b *InMemoryBackend) updateArtifact(
	a *GenericArtifact, repo *GenericRepo, ifMatch string, in updateTagsIn,
) (genericArtifactWire, string, error) {
	if err := checkEtag(a.Etag, ifMatch); err != nil {
		return genericArtifactWire{}, "", err
	}

	a.Tags.merge(in.Tags)
	a.Etag = newEtag()

	return b.artifactView(a, repo), a.Etag, nil
}

// UpdateGenericArtifact changes an artifact's tags.
func (b *InMemoryBackend) UpdateGenericArtifact(
	id, ifMatch string,
	in updateTagsIn,
) (genericArtifactWire, string, error) {
	b.mu.Lock("UpdateGenericArtifact")
	defer b.mu.Unlock()

	a, repo, err := b.artifact(id)
	if err != nil {
		return genericArtifactWire{}, "", err
	}

	return b.updateArtifact(a, repo, ifMatch, in)
}

// UpdateGenericArtifactByPath changes an artifact's tags.
func (b *InMemoryBackend) UpdateGenericArtifactByPath(
	repoID, path, version, ifMatch string, in updateTagsIn,
) (genericArtifactWire, string, error) {
	b.mu.Lock("UpdateGenericArtifactByPath")
	defer b.mu.Unlock()

	a, repo, err := b.artifactByPath(repoID, path, version)
	if err != nil {
		return genericArtifactWire{}, "", err
	}

	return b.updateArtifact(a, repo, ifMatch, in)
}

// DeleteGenericArtifact deletes an artifact by id.
func (b *InMemoryBackend) DeleteGenericArtifact(id, ifMatch string) error {
	b.mu.Lock("DeleteGenericArtifact")
	defer b.mu.Unlock()

	a, _, err := b.artifact(id)
	if err != nil {
		return err
	}

	if err = checkEtag(a.Etag, ifMatch); err != nil {
		return err
	}

	delete(b.arts, id)

	return nil
}

// DeleteGenericArtifactByPath deletes an artifact by repository, path and version.
func (b *InMemoryBackend) DeleteGenericArtifactByPath(repoID, path, version, ifMatch string) error {
	b.mu.Lock("DeleteGenericArtifactByPath")
	defer b.mu.Unlock()

	a, _, err := b.artifactByPath(repoID, path, version)
	if err != nil {
		return err
	}

	if err = checkEtag(a.Etag, ifMatch); err != nil {
		return err
	}

	delete(b.arts, a.ID)

	return nil
}
