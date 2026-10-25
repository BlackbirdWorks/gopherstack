package gcpartifactregistry

import (
	"context"
	"slices"
	"sort"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var mutableFields = []string{ //nolint:gochecknoglobals // immutable catalog
	"description", "labels", "cleanup_policies", "cleanup_policy_dry_run", "docker_config",
	"vulnerability_scanning_config",
}

// done records an already finished operation under parent and returns it.
func (b *InMemoryBackend) done(parent string, resp proto.Message) (*longrunningpb.Operation, error) {
	name := parent + "/operations/operation-" + uuid.NewString()

	op, err := gcplro.Done(name, &artifactregistrypb.OperationMetadata{}, resp)
	if err != nil {
		return nil, errInternal(err)
	}

	return b.ops.Put(op), nil
}

func validateFormat(f artifactregistrypb.Repository_Format) error {
	if f == artifactregistrypb.Repository_FORMAT_UNSPECIFIED {
		return errInvalid("repository.format is required.")
	}

	if _, known := artifactregistrypb.Repository_Format_name[int32(f)]; !known {
		return errInvalid("Unknown repository format.")
	}

	return nil
}

func validateRepository(in *artifactregistrypb.Repository) error {
	if err := validateFormat(in.GetFormat()); err != nil {
		return err
	}

	if in.GetDockerConfig() != nil && in.GetFormat() != artifactregistrypb.Repository_DOCKER {
		return errInvalid("docker_config is only valid for DOCKER repositories.")
	}

	if in.GetMode() == artifactregistrypb.Repository_VIRTUAL_REPOSITORY && in.GetVirtualRepositoryConfig() == nil {
		return errInvalid("virtual_repository_config is required for VIRTUAL_REPOSITORY mode.")
	}

	if in.GetMode() == artifactregistrypb.Repository_REMOTE_REPOSITORY && in.GetRemoteRepositoryConfig() == nil {
		return errInvalid("remote_repository_config is required for REMOTE_REPOSITORY mode.")
	}

	return nil
}

// CreateRepository stores a repository and returns the finished operation.
func (b *InMemoryBackend) CreateRepository(
	in *artifactregistrypb.CreateRepositoryRequest,
) (*longrunningpb.Operation, error) {
	parent, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if parent.location == allLocs {
		return nil, errInvalid("A concrete location is required to create a repository.")
	}

	if err = validateID(in.GetRepositoryId()); err != nil {
		return nil, err
	}

	if in.GetRepository() == nil {
		return nil, errInvalid("repository is required.")
	}

	if err = validateRepository(in.GetRepository()); err != nil {
		return nil, err
	}

	parent.id = in.GetRepositoryId()

	pb := b.buildRepository(parent, in.GetRepository())

	b.mu.Lock("CreateRepository")

	if _, _, exists := b.repoByKey(parent.project, parent.id); exists {
		b.mu.Unlock()

		return nil, errExists("Repository %q already exists.", parent.full())
	}

	if len(b.repos) >= maxRepositories {
		b.mu.Unlock()

		return nil, errPrecondition("At most %d repositories can exist in the emulator.", maxRepositories)
	}

	b.repos[parent.full()] = &repoRec{pb: pb}
	out := cloneRepo(pb)
	b.mu.Unlock()

	return b.done(parent.parent(), out)
}

func (b *InMemoryBackend) buildRepository(
	ref repoRef,
	in *artifactregistrypb.Repository,
) *artifactregistrypb.Repository {
	pb := cloneRepo(in)
	now := b.ts()

	pb.Name, pb.CreateTime, pb.UpdateTime, pb.SizeBytes, pb.RegistryUri = ref.full(), now, now, 0, ""

	if pb.GetMode() == artifactregistrypb.Repository_MODE_UNSPECIFIED {
		pb.Mode = artifactregistrypb.Repository_STANDARD_REPOSITORY
	}

	return pb
}

// GetRepository returns a repository with its size and registry_uri filled in.
func (b *InMemoryBackend) GetRepository(name string) (*artifactregistrypb.Repository, error) {
	ref, err := parseRepo(name)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetRepository")
	rec, ok := b.repos[ref.full()]

	var pb *artifactregistrypb.Repository
	if ok {
		pb = cloneRepo(rec.pb)
	}
	b.mu.RUnlock()

	if !ok {
		return nil, errNotFound("Repository %q not found.", ref.full())
	}

	return b.decorate(ref, pb), nil
}

// decorate adds the computed registry_uri and size_bytes.
func (b *InMemoryBackend) decorate(ref repoRef, pb *artifactregistrypb.Repository) *artifactregistrypb.Repository {
	if storesFormat(pb.GetFormat()) {
		pb.SizeBytes = b.genericRepoSize(ref)

		return pb
	}

	if pb.GetFormat() != artifactregistrypb.Repository_DOCKER {
		return pb
	}

	b.mu.RLock("decorate")
	pb.RegistryUri = b.registryURI(ref)
	b.mu.RUnlock()

	pb.SizeBytes = b.repoSize(ref)

	return pb
}

// ListRepositories lists repositories under a location, or every location with "-".
func (b *InMemoryBackend) ListRepositories(
	in *artifactregistrypb.ListRepositoriesRequest,
) ([]*artifactregistrypb.Repository, string, error) {
	parent, err := parseParent(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	if in.GetPageSize() < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	if err = page.ValidateToken(in.GetPageToken()); err != nil {
		return nil, "", errInvalid("Invalid page_token.")
	}

	if in.GetFilter() != "" {
		return nil, "", errInvalid("filter is not supported.")
	}

	prefix := "projects/" + parent.project + "/locations/"
	if parent.location != allLocs {
		prefix += parent.location + "/"
	}

	names := b.repoNames(prefix)
	all := make([]*artifactregistrypb.Repository, 0, len(names))

	for _, name := range names {
		if pb, getErr := b.GetRepository(name); getErr == nil {
			all = append(all, pb)
		}
	}

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultPage)

	return pg.Data, pg.Next, nil
}

func (b *InMemoryBackend) repoNames(prefix string) []string {
	b.mu.RLock("repoNames")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.repos))

	for name := range b.repos {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}

	sort.Strings(names)

	return names
}

// UpdateRepository applies the masked fields (all set fields when the mask is empty).
func (b *InMemoryBackend) UpdateRepository(
	in *artifactregistrypb.UpdateRepositoryRequest,
) (*artifactregistrypb.Repository, error) {
	ref, err := parseRepo(in.GetRepository().GetName())
	if err != nil {
		return nil, err
	}

	paths := in.GetUpdateMask().GetPaths()

	b.mu.Lock("UpdateRepository")

	rec, ok := b.repos[ref.full()]
	if !ok {
		b.mu.Unlock()

		return nil, errNotFound("Repository %q not found.", ref.full())
	}

	if err = applyMask(rec.pb, in.GetRepository(), paths); err != nil {
		b.mu.Unlock()

		return nil, err
	}

	rec.pb.UpdateTime = b.ts()
	out := cloneRepo(rec.pb)
	b.mu.Unlock()

	return b.decorate(ref, out), nil
}

// applyMask copies the masked fields of src onto dst; an empty mask copies every mutable field src has set.
func applyMask(dst, src *artifactregistrypb.Repository, paths []string) error {
	d, s := dst.ProtoReflect(), src.ProtoReflect()

	if len(paths) == 0 {
		for _, f := range mutableFields {
			copyField(d, s, f, false)
		}

		return nil
	}

	for _, p := range paths {
		name, _, _ := strings.Cut(p, ".")
		if !slices.Contains(mutableFields, name) {
			return errInvalid("Update mask path %q is not an updatable field.", p)
		}

		copyField(d, s, name, true)
	}

	return nil
}

func copyField(d, s protoreflect.Message, name string, clearMissing bool) {
	fd := d.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return
	}

	switch {
	case s.Has(fd):
		d.Set(fd, s.Get(fd))
	case clearMissing:
		d.Clear(fd)
	}
}

// DeleteRepository removes a repository with every image in it.
func (b *InMemoryBackend) DeleteRepository(
	ctx context.Context, name string,
) (*longrunningpb.Operation, error) {
	ref, err := parseRepo(name)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteRepository")

	rec, ok := b.repos[ref.full()]
	if !ok {
		b.mu.Unlock()

		return nil, errNotFound("Repository %q not found.", ref.full())
	}

	images := b.imagesLocked(ref)
	docker := rec.pb.GetFormat() == artifactregistrypb.Repository_DOCKER

	delete(b.repos, ref.full())
	b.dropRepoDataLocked(ref)
	b.mu.Unlock()

	if docker {
		b.purgeImages(ctx, ref, images)
	}

	return b.done(ref.parent(), &emptypb.Empty{})
}
