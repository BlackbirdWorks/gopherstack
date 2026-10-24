package gcpartifactregistry

import (
	"context"
	"sort"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func validatePaging(size int32, token, filter string) error {
	if size < 0 {
		return errInvalid("page_size must not be negative.")
	}

	if err := page.ValidateToken(token); err != nil {
		return errInvalid("Invalid page_token.")
	}

	if filter != "" {
		return errInvalid("filter is not supported.")
	}

	return nil
}

func paged[T any](all []T, token string, size int32) ([]T, string) {
	pg := page.New(all, token, int(size), defaultPage)

	return pg.Data, pg.Next
}

// ListDockerImages lists every manifest of the repository's images.
func (b *InMemoryBackend) ListDockerImages(
	ctx context.Context, in *artifactregistrypb.ListDockerImagesRequest,
) ([]*artifactregistrypb.DockerImage, string, error) {
	ref, err := parseRepo(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	if err = validatePaging(in.GetPageSize(), in.GetPageToken(), ""); err != nil {
		return nil, "", err
	}

	if err = b.requireDocker(ref); err != nil {
		return nil, "", err
	}

	var all []*artifactregistrypb.DockerImage

	for _, s := range b.imageSnaps(ref, "") {
		for _, d := range s.digests() {
			all = append(all, b.dockerImage(ctx, ref, s, d))
		}
	}

	out, next := paged(all, in.GetPageToken(), in.GetPageSize())

	return out, next, nil
}

// GetDockerImage returns one manifest of an image, addressed by digest or tag.
func (b *InMemoryBackend) GetDockerImage(
	ctx context.Context, name string,
) (*artifactregistrypb.DockerImage, error) {
	ref, image, id, err := parseImageName(name)
	if err != nil {
		return nil, err
	}

	if err = b.requireDocker(ref); err != nil {
		return nil, err
	}

	snaps := b.imageSnaps(ref, image)
	if len(snaps) == 0 {
		return nil, errNotFound("Docker image %q not found.", name)
	}

	digest := id
	if !strings.Contains(id, ":") {
		digest = snaps[0].tags[id]
	}

	if _, ok := snaps[0].manifests[digest]; !ok {
		return nil, errNotFound("Docker image %q not found.", name)
	}

	return b.dockerImage(ctx, ref, snaps[0], digest), nil
}

// ListPackages lists the images of a DOCKER repository as packages.
func (b *InMemoryBackend) ListPackages(
	in *artifactregistrypb.ListPackagesRequest,
) ([]*artifactregistrypb.Package, string, error) {
	ref, err := parseRepo(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	if err = validatePaging(in.GetPageSize(), in.GetPageToken(), in.GetFilter()); err != nil {
		return nil, "", err
	}

	if generic, gErr := b.isGeneric(ref); gErr != nil {
		return nil, "", gErr
	} else if generic {
		out, next := b.genListPackages(ref, in.GetPageToken(), in.GetPageSize())

		return out, next, nil
	}

	if err = b.requireDocker(ref); err != nil {
		return nil, "", err
	}

	var all []*artifactregistrypb.Package
	for _, s := range b.imageSnaps(ref, "") {
		all = append(all, b.pkg(ref, s))
	}

	out, next := paged(all, in.GetPageToken(), in.GetPageSize())

	return out, next, nil
}

func (b *InMemoryBackend) snapOfPackage(p pkgRef) (imageSnap, error) {
	if err := b.requireDocker(p.repo); err != nil {
		return imageSnap{}, err
	}

	snaps := b.imageSnaps(p.repo, p.image)
	if len(snaps) == 0 {
		return imageSnap{}, errNotFound("Package %q not found.", p.full())
	}

	return snaps[0], nil
}

// GetPackage returns one image as a package.
func (b *InMemoryBackend) GetPackage(name string) (*artifactregistrypb.Package, error) {
	p, err := parsePackage(name)
	if err != nil {
		return nil, err
	}

	if generic, gErr := b.isGeneric(p.repo); gErr != nil {
		return nil, gErr
	} else if generic {
		return b.genGetPackage(p)
	}

	s, err := b.snapOfPackage(p)
	if err != nil {
		return nil, err
	}

	return b.pkg(p.repo, s), nil
}

// DeletePackage deletes every version of an image.
func (b *InMemoryBackend) DeletePackage(ctx context.Context, name string) (*longrunningpb.Operation, error) {
	p, err := parsePackage(name)
	if err != nil {
		return nil, err
	}

	if generic, gErr := b.isGeneric(p.repo); gErr != nil {
		return nil, gErr
	} else if generic {
		return b.genDeletePackage(p.repo, p)
	}

	if _, err = b.snapOfPackage(p); err != nil {
		return nil, err
	}

	b.purgeImages(ctx, p.repo, []string{p.image})

	return b.done(p.repo.parent(), &emptypb.Empty{})
}

// ListVersions lists the manifests of an image as versions.
func (b *InMemoryBackend) ListVersions(
	ctx context.Context, in *artifactregistrypb.ListVersionsRequest,
) ([]*artifactregistrypb.Version, string, error) {
	p, err := parsePackage(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	if err = validatePaging(in.GetPageSize(), in.GetPageToken(), in.GetFilter()); err != nil {
		return nil, "", err
	}

	if generic, gErr := b.isGeneric(p.repo); gErr != nil {
		return nil, "", gErr
	} else if generic {
		return b.genListVersions(p, in.GetOrderBy(), in.GetPageToken(), in.GetPageSize())
	}

	s, err := b.snapOfPackage(p)
	if err != nil {
		return nil, "", err
	}

	full := in.GetView() == artifactregistrypb.VersionView_FULL
	digests := orderDigests(s, in.GetOrderBy())
	all := make([]*artifactregistrypb.Version, 0, len(digests))

	for _, d := range digests {
		all = append(all, b.version(ctx, p, s, d, full))
	}

	out, next := paged(all, in.GetPageToken(), in.GetPageSize())

	return out, next, nil
}

func orderDigests(s imageSnap, orderBy string) []string {
	digests := s.digests()
	field, desc := strings.TrimSpace(orderBy), false

	if f, dir, ok := strings.Cut(field, " "); ok {
		field, desc = f, strings.EqualFold(strings.TrimSpace(dir), "desc")
	}

	key := func(d string) int64 {
		m := s.manifests[d]
		if field == fieldUpdSnake || field == fieldUpdated {
			return m.Updated.UnixNano()
		}

		return m.Created.UnixNano()
	}

	if field == "create_time" || field == "createTime" || field == fieldUpdSnake || field == fieldUpdated {
		sort.SliceStable(digests, func(i, j int) bool {
			if desc {
				return key(digests[i]) > key(digests[j])
			}

			return key(digests[i]) < key(digests[j])
		})
	} else if desc {
		sort.Sort(sort.Reverse(sort.StringSlice(digests)))
	}

	return digests
}

// GetVersion returns one manifest as a version.
func (b *InMemoryBackend) GetVersion(
	ctx context.Context, in *artifactregistrypb.GetVersionRequest,
) (*artifactregistrypb.Version, error) {
	l, err := parseLeaf(in.GetName(), "version")
	if err != nil {
		return nil, err
	}

	if generic, gErr := b.isGeneric(l.pkg.repo); gErr != nil {
		return nil, gErr
	} else if generic {
		return b.genGetVersion(l)
	}

	s, err := b.snapOfPackage(l.pkg)
	if err != nil {
		return nil, err
	}

	if _, ok := s.manifests[l.leaf]; !ok {
		return nil, errNotFound("Version %q not found.", in.GetName())
	}

	return b.version(ctx, l.pkg, s, l.leaf, in.GetView() == artifactregistrypb.VersionView_FULL), nil
}

func (b *InMemoryBackend) deleteVersion(ctx context.Context, l leafRef, force bool) error {
	s, err := b.snapOfPackage(l.pkg)
	if err != nil {
		return err
	}

	if _, ok := s.manifests[l.leaf]; !ok {
		return errNotFound("Version %q not found.", l.pkg.full()+"/versions/"+l.leaf)
	}

	if tags := s.tagsOf(l.leaf); len(tags) > 0 && !force {
		return errPrecondition("Version %q has tags %v; set force to delete it.", l.leaf, tags)
	}

	repo := l.pkg.repo.dockerKey() + "/" + l.pkg.image
	if err = b.client().DeleteManifest(ctx, repo, l.leaf); err != nil {
		return errInternal(err)
	}

	b.RecordManifestDelete(repo, l.leaf)
	b.dropMeta(l.pkg.full() + "/versions/" + l.leaf)
	b.dropMeta(l.pkg.repo.fileName(l.leaf))

	return nil
}

// DeleteVersion deletes one manifest; force is required when tags point at it.
func (b *InMemoryBackend) DeleteVersion(
	ctx context.Context, in *artifactregistrypb.DeleteVersionRequest,
) (*longrunningpb.Operation, error) {
	l, err := parseLeaf(in.GetName(), "version")
	if err != nil {
		return nil, err
	}

	if generic, gErr := b.isGeneric(l.pkg.repo); gErr != nil {
		return nil, gErr
	} else if generic {
		return b.genDeleteVersion(l)
	}

	if err = b.deleteVersion(ctx, l, in.GetForce()); err != nil {
		return nil, err
	}

	return b.done(l.pkg.repo.parent(), &emptypb.Empty{})
}

// BatchDeleteVersions deletes several manifests of one package, force-deleting tagged ones.
func (b *InMemoryBackend) BatchDeleteVersions(
	ctx context.Context, in *artifactregistrypb.BatchDeleteVersionsRequest,
) (*longrunningpb.Operation, error) {
	p, err := parsePackage(in.GetParent())
	if err != nil {
		return nil, err
	}

	leaves := make([]leafRef, 0, len(in.GetNames()))

	for _, name := range in.GetNames() {
		l, parseErr := parseLeaf(name, "version")
		if parseErr != nil {
			return nil, parseErr
		}

		if l.pkg != p {
			return nil, errInvalid("Version %q does not belong to %q.", name, in.GetParent())
		}

		leaves = append(leaves, l)
	}

	if generic, gErr := b.isGeneric(p.repo); gErr != nil {
		return nil, gErr
	} else if generic {
		return b.genBatchDelete(p, leaves, in.GetValidateOnly())
	}

	s, err := b.snapOfPackage(p)
	if err != nil {
		return nil, err
	}

	for _, l := range leaves {
		if _, ok := s.manifests[l.leaf]; !ok {
			return nil, errNotFound("Version %q not found.", p.full()+"/versions/"+l.leaf)
		}
	}

	if !in.GetValidateOnly() {
		for _, l := range leaves {
			if err = b.deleteVersion(ctx, l, true); err != nil {
				return nil, err
			}
		}
	}

	return b.done(p.repo.parent(), &emptypb.Empty{})
}

func (b *InMemoryBackend) tagPB(p pkgRef, tag, digest string) *artifactregistrypb.Tag {
	return &artifactregistrypb.Tag{Name: p.full() + "/tags/" + tag, Version: p.full() + "/versions/" + digest}
}

// ListTags lists an image's tags, optionally only those on one version.
func (b *InMemoryBackend) ListTags(in *artifactregistrypb.ListTagsRequest) ([]*artifactregistrypb.Tag, string, error) {
	p, err := parsePackage(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	if err = validatePaging(in.GetPageSize(), in.GetPageToken(), ""); err != nil {
		return nil, "", err
	}

	version, err := versionFilter(in.GetFilter())
	if err != nil {
		return nil, "", err
	}

	s, err := b.snapOfPackage(p)
	if err != nil {
		return nil, "", err
	}

	names := make([]string, 0, len(s.tags))
	for t := range s.tags {
		names = append(names, t)
	}

	sort.Strings(names)

	var all []*artifactregistrypb.Tag

	for _, t := range names {
		if pb := b.tagPB(p, t, s.tags[t]); version == "" || pb.GetVersion() == version {
			all = append(all, pb)
		}
	}

	out, next := paged(all, in.GetPageToken(), in.GetPageSize())

	return out, next, nil
}

// versionFilter reads the only supported tag filter, version="<name>".
func versionFilter(filter string) (string, error) {
	if filter == "" {
		return "", nil
	}

	v, ok := strings.CutPrefix(strings.TrimSpace(filter), "version=")
	if !ok {
		return "", errInvalid("Only the version=\"<name>\" filter is supported.")
	}

	return strings.Trim(v, `"`), nil
}

// GetTag returns one tag.
func (b *InMemoryBackend) GetTag(name string) (*artifactregistrypb.Tag, error) {
	l, err := parseLeaf(name, "tag")
	if err != nil {
		return nil, err
	}

	s, err := b.snapOfPackage(l.pkg)
	if err != nil {
		return nil, err
	}

	digest, ok := s.tags[l.leaf]
	if !ok {
		return nil, errNotFound("Tag %q not found.", name)
	}

	return b.tagPB(l.pkg, l.leaf, digest), nil
}
