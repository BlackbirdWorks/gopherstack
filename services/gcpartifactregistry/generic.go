package gcpartifactregistry

import (
	"context"
	"crypto/sha256"
	"maps"
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxGenericBytes = 128 << 20

var (
	genericPackageRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]{0,254}[A-Za-z0-9])?$`)
	genericVersionRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9.+~:-]{0,126}[a-z0-9])?$`)
	genericFileRe    = regexp.MustCompile(`^[a-zA-Z0-9._~@-]+$`)
)

// genFile is one uploaded generic file with its content.
type genFile struct {
	Created     time.Time         `json:"created"`
	Updated     time.Time         `json:"updated"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Owner       string            `json:"owner"`
	Data        []byte            `json:"data"`
}

// isGeneric reports whether the repository exists and holds generic artifacts.
func (b *InMemoryBackend) isGeneric(ref repoRef) (bool, error) {
	b.mu.RLock("isGeneric")
	defer b.mu.RUnlock()

	rec, ok := b.repos[ref.full()]
	if !ok {
		return false, errNotFound("Repository %q not found.", ref.full())
	}

	return rec.pb.GetFormat() == artifactregistrypb.Repository_GENERIC, nil
}

func genericPackagePB(name string, m *resMeta) *artifactregistrypb.Package {
	c := m.clone()

	return &artifactregistrypb.Package{
		Name: name, DisplayName: c.DisplayName, CreateTime: timestamppb.New(c.Created),
		UpdateTime: timestamppb.New(c.Updated), Annotations: c.Annotations,
	}
}

func genericVersionPB(name string, m *resMeta) *artifactregistrypb.Version {
	c := m.clone()

	return &artifactregistrypb.Version{
		Name: name, Description: c.Description, CreateTime: timestamppb.New(c.Created),
		UpdateTime: timestamppb.New(c.Updated), Annotations: c.Annotations,
	}
}

func (b *InMemoryBackend) genListPackages(
	ref repoRef, token string, size int32,
) ([]*artifactregistrypb.Package, string) {
	b.mu.RLock("genListPackages")

	prefix := ref.full() + "/packages/"

	var all []*artifactregistrypb.Package

	for name, m := range b.genPkgs {
		if strings.HasPrefix(name, prefix) {
			all = append(all, genericPackagePB(name, m))
		}
	}
	b.mu.RUnlock()

	sort.Slice(all, func(i, j int) bool { return all[i].GetName() < all[j].GetName() })

	return paged(all, token, size)
}

func (b *InMemoryBackend) genGetPackage(p pkgRef) (*artifactregistrypb.Package, error) {
	b.mu.RLock("genGetPackage")
	defer b.mu.RUnlock()

	m, ok := b.genPkgs[p.full()]
	if !ok {
		return nil, errNotFound("Package %q not found.", p.full())
	}

	return genericPackagePB(p.full(), m), nil
}

func (b *InMemoryBackend) genVersionsOf(p pkgRef) []string {
	prefix := p.full() + "/versions/"

	var out []string

	for name := range b.genVers {
		if strings.HasPrefix(name, prefix) {
			out = append(out, name)
		}
	}

	return out
}

func (b *InMemoryBackend) genListVersions(
	p pkgRef, orderBy, token string, size int32,
) ([]*artifactregistrypb.Version, string, error) {
	b.mu.RLock("genListVersions")

	if _, ok := b.genPkgs[p.full()]; !ok {
		b.mu.RUnlock()

		return nil, "", errNotFound("Package %q not found.", p.full())
	}

	var all []*artifactregistrypb.Version
	for _, name := range b.genVersionsOf(p) {
		all = append(all, genericVersionPB(name, b.genVers[name]))
	}
	b.mu.RUnlock()

	field, desc := parseOrder(orderBy)
	if err := sortByField(all, field, desc, (*artifactregistrypb.Version).GetName,
		(*artifactregistrypb.Version).GetCreateTime, (*artifactregistrypb.Version).GetUpdateTime); err != nil {
		return nil, "", err
	}

	out, next := paged(all, token, size)

	return out, next, nil
}

func (b *InMemoryBackend) genGetVersion(l leafRef) (*artifactregistrypb.Version, error) {
	name := l.pkg.full() + "/versions/" + l.leaf

	b.mu.RLock("genGetVersion")
	defer b.mu.RUnlock()

	m, ok := b.genVers[name]
	if !ok {
		return nil, errNotFound("Version %q not found.", name)
	}

	return genericVersionPB(name, m), nil
}

// dropGenericVersionLocked removes a version with its files. Must hold b.mu for writing.
func (b *InMemoryBackend) dropGenericVersionLocked(name string) {
	delete(b.genVers, name)

	for fn, f := range b.genFiles {
		if f.Owner == name {
			delete(b.genFiles, fn)
			delete(b.meta, fn)
		}
	}
}

func (b *InMemoryBackend) genDeletePackage(parent repoRef, p pkgRef) (*longrunningpb.Operation, error) {
	b.mu.Lock("genDeletePackage")

	if _, ok := b.genPkgs[p.full()]; !ok {
		b.mu.Unlock()

		return nil, errNotFound("Package %q not found.", p.full())
	}

	for _, v := range b.genVersionsOf(p) {
		b.dropGenericVersionLocked(v)
	}

	delete(b.genPkgs, p.full())
	b.mu.Unlock()

	return b.done(parent.parent(), &emptypb.Empty{})
}

func (b *InMemoryBackend) genDeleteVersions(names []string, validateOnly bool) error {
	b.mu.Lock("genDeleteVersions")
	defer b.mu.Unlock()

	for _, n := range names {
		if _, ok := b.genVers[n]; !ok {
			return errNotFound("Version %q not found.", n)
		}
	}

	if validateOnly {
		return nil
	}

	for _, n := range names {
		b.dropGenericVersionLocked(n)
	}

	return nil
}

func (b *InMemoryBackend) genDeleteVersion(l leafRef) (*longrunningpb.Operation, error) {
	name := l.pkg.full() + "/versions/" + l.leaf

	if err := b.genDeleteVersions([]string{name}, false); err != nil {
		return nil, err
	}

	return b.done(l.pkg.repo.parent(), &emptypb.Empty{})
}

func genFileID(pkg, version, filename string) string { return pkg + ":" + version + ":" + filename }

type uploadReq struct {
	PackageID string `json:"packageId"`
	VersionID string `json:"versionId"`
	Filename  string `json:"filename"`
}

func (u uploadReq) validate() error {
	switch {
	case !genericPackageRe.MatchString(u.PackageID):
		return errInvalid("package_id %q must start and end with a letter or number, contain only letters, "+
			"numbers, hyphens, underscores and periods, and not exceed 256 characters.", u.PackageID)
	case !genericVersionRe.MatchString(u.VersionID) || u.VersionID == "latest":
		return errInvalid("version_id %q must match [a-z0-9-.+~:], start and end with a letter or number, not "+
			"exceed 128 characters and not be \"latest\".", u.VersionID)
	case !genericFileRe.MatchString(u.Filename):
		return errInvalid("filename %q is limited to letters, numbers and the characters [-_.~@].", u.Filename)
	}

	return nil
}

// UploadGenericArtifact stores a file of a generic repository, creating its package and version.
func (b *InMemoryBackend) UploadGenericArtifact(
	_ context.Context, parent string, in uploadReq, data []byte,
) (*longrunningpb.Operation, error) {
	ref, err := parseRepo(parent)
	if err != nil {
		return nil, err
	}

	if err = in.validate(); err != nil {
		return nil, err
	}

	generic, err := b.isGeneric(ref)
	if err != nil {
		return nil, err
	}

	if !generic {
		return nil, errPrecondition("Repository %q is not a GENERIC repository.", ref.full())
	}

	pkg := ref.packageName(in.PackageID)
	ver := pkg + "/versions/" + in.VersionID
	file := ref.fileName(genFileID(in.PackageID, in.VersionID, in.Filename))
	now := b.now().UTC()

	b.mu.Lock("UploadGenericArtifact")

	if _, exists := b.genFiles[file]; exists {
		b.mu.Unlock()

		return nil, errExists("File %q already exists.", file)
	}

	touch(b.genPkgs, pkg, resMeta{Created: now, Updated: now, DisplayName: in.PackageID})
	touch(b.genVers, ver, resMeta{Created: now, Updated: now})

	b.genFiles[file] = &genFile{Created: now, Updated: now, Owner: ver, Data: data}
	b.mu.Unlock()

	art := &artifactregistrypb.GenericArtifact{
		Name:    ref.full() + "/genericArtifacts/" + in.PackageID + ":" + in.VersionID,
		Version: in.VersionID, CreateTime: timestamppb.New(now), UpdateTime: timestamppb.New(now),
	}

	return b.done(ref.parent(), art)
}

// touch inserts fresh under name, or bumps the update time of the existing entry.
func touch(store map[string]*resMeta, name string, fresh resMeta) {
	if m, ok := store[name]; ok {
		m.Updated = fresh.Updated

		return
	}

	store[name] = &fresh
}

func (b *InMemoryBackend) genericFilePB(name string, f *genFile) *artifactregistrypb.File {
	sum := sha256.Sum256(f.Data)

	return &artifactregistrypb.File{
		Name: name, SizeBytes: int64(len(f.Data)), CreateTime: timestamppb.New(f.Created),
		UpdateTime: timestamppb.New(f.Updated), Owner: f.Owner, Annotations: maps.Clone(f.Annotations),
		Hashes: []*artifactregistrypb.Hash{{Type: artifactregistrypb.Hash_SHA256, Value: sum[:]}},
	}
}

// genericFiles lists the repository's generic files, unsorted.
func (b *InMemoryBackend) genericFiles(ref repoRef) []*artifactregistrypb.File {
	b.mu.RLock("genericFiles")
	defer b.mu.RUnlock()

	prefix := ref.full() + "/files/"

	var out []*artifactregistrypb.File

	for name, f := range b.genFiles {
		if strings.HasPrefix(name, prefix) {
			out = append(out, b.genericFilePB(name, f))
		}
	}

	return out
}

func (b *InMemoryBackend) genericRepoSize(ref repoRef) int64 {
	var total int64
	for _, f := range b.genericFiles(ref) {
		total += f.GetSizeBytes()
	}

	return total
}

// dropRepoDataLocked forgets everything stored under a deleted repository. Must hold b.mu for writing.
func (b *InMemoryBackend) dropRepoDataLocked(ref repoRef) {
	prefix := ref.full() + "/"

	for _, store := range []map[string]*resMeta{b.meta, b.genPkgs, b.genVers} {
		for k := range store {
			if strings.HasPrefix(k, prefix) {
				delete(store, k)
			}
		}
	}

	for k := range b.genFiles {
		if strings.HasPrefix(k, prefix) {
			delete(b.genFiles, k)
		}
	}

	for k := range b.rules {
		if strings.HasPrefix(k, prefix) {
			delete(b.rules, k)
		}
	}
}

func (b *InMemoryBackend) genBatchDelete(
	p pkgRef,
	leaves []leafRef,
	validateOnly bool,
) (*longrunningpb.Operation, error) {
	names := make([]string, 0, len(leaves))
	for _, l := range leaves {
		names = append(names, p.full()+"/versions/"+l.leaf)
	}

	if err := b.genDeleteVersions(names, validateOnly); err != nil {
		return nil, err
	}

	return b.done(p.repo.parent(), &emptypb.Empty{})
}
