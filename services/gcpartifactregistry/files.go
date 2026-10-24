package gcpartifactregistry

import (
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"maps"
	"regexp"
	"slices"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type blobDesc struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

type manifestBlobs struct {
	Layers []blobDesc `json:"layers"`
	Config blobDesc   `json:"config"`
}

type fileSource struct {
	snap   imageSnap
	owner  string
	digest string
}

func (b *InMemoryBackend) dockerFile(ref repoRef, id string, size int64, src fileSource) *artifactregistrypb.File {
	name := ref.fileName(id)
	m := src.snap.manifests[src.digest]
	edits := b.metaOf(name)

	f := &artifactregistrypb.File{
		Name: name, SizeBytes: size, Owner: src.owner, CreateTime: timestamppb.New(m.Created),
		UpdateTime: laterOf(timestamppb.New(m.Updated), edits.Updated), Annotations: edits.Annotations,
	}

	if raw, err := hex.DecodeString(strings.TrimPrefix(id, "sha256:")); err == nil && strings.HasPrefix(id, "sha256:") {
		f.Hashes = []*artifactregistrypb.Hash{{Type: artifactregistrypb.Hash_SHA256, Value: raw}}
	}

	return f
}

// dockerFiles exposes a DOCKER repository's content as files named after their digests: each manifest, and the
// config and layer blobs it references. A blob shared by several manifests belongs to the first one.
func (b *InMemoryBackend) dockerFiles(ctx context.Context, ref repoRef) []*artifactregistrypb.File {
	c := b.client()
	seen := map[string]bool{}

	var out []*artifactregistrypb.File

	add := func(id string, size int64, src fileSource) {
		if id != "" && !seen[id] {
			seen[id] = true
			out = append(out, b.dockerFile(ref, id, size, src))
		}
	}

	for _, s := range b.imageSnaps(ref, "") {
		for _, d := range s.digests() {
			m, err := c.GetManifest(ctx, ref.dockerKey()+"/"+s.image, d)
			if err != nil {
				continue
			}

			src := fileSource{snap: s, digest: d, owner: ref.packageName(s.image) + "/versions/" + d}
			add(d, int64(len(m.Body)), src)

			var doc manifestBlobs
			if json.Unmarshal(m.Body, &doc) != nil {
				continue
			}

			for _, blob := range append([]blobDesc{doc.Config}, doc.Layers...) {
				add(blob.Digest, blob.Size, src)
			}
		}
	}

	return out
}

func (b *InMemoryBackend) filesOf(ctx context.Context, ref repoRef) ([]*artifactregistrypb.File, error) {
	b.mu.RLock("filesOf")
	rec, ok := b.repos[ref.full()]

	var format artifactregistrypb.Repository_Format
	if ok {
		format = rec.pb.GetFormat()
	}
	b.mu.RUnlock()

	switch {
	case !ok:
		return nil, errNotFound("Repository %q not found.", ref.full())
	case format == artifactregistrypb.Repository_DOCKER:
		return b.dockerFiles(ctx, ref), nil
	case format == artifactregistrypb.Repository_GENERIC:
		return b.genericFiles(ref), nil
	}

	return nil, nil
}

var (
	filterClauseRe = regexp.MustCompile(
		"^(name|owner|annotations(?:\\.(?:`[^`]+`|[^:=\\s]+))?)\\s*(?:([=:])\\s*(.+))?$",
	)
	filterAndRe = regexp.MustCompile(`(?i)\s+AND\s+`)
)

func unquote(v string) string {
	v = strings.TrimSpace(v)
	if len(v) >= 2 && (v[0] == '"' || v[0] == '`') && v[len(v)-1] == v[0] {
		return v[1 : len(v)-1]
	}

	return v
}

func wildcard(pattern string) *regexp.Regexp {
	parts := strings.Split(pattern, "*")
	for i := range parts {
		parts[i] = regexp.QuoteMeta(parts[i])
	}

	return regexp.MustCompile("(?is)^" + strings.Join(parts, ".*") + "$")
}

type fileMatcher func(*artifactregistrypb.File) bool

func parseFileFilter(filter string) ([]fileMatcher, error) {
	if strings.TrimSpace(filter) == "" {
		return nil, nil
	}

	var out []fileMatcher

	for _, raw := range filterAndRe.Split(strings.TrimSpace(filter), -1) {
		m := filterClauseRe.FindStringSubmatch(strings.TrimSpace(raw))
		if m == nil {
			return nil, errInvalid(
				"Unsupported filter %q: use name, owner or annotations.<key> with = or :, joined by AND.", filter)
		}

		if m[1] == fieldAnnot {
			return nil, errInvalid("Filter %q: name the annotation key, annotations.<key>.", raw)
		}

		var re *regexp.Regexp
		if m[2] != "" {
			re = wildcard(unquote(m[3]))
		}

		out = append(out, clauseMatcher(m[1], re))
	}

	return out, nil
}

func clauseMatcher(field string, re *regexp.Regexp) fileMatcher {
	switch field {
	case fieldName:
		return func(f *artifactregistrypb.File) bool { return re != nil && re.MatchString(f.GetName()) }
	case "owner":
		return func(f *artifactregistrypb.File) bool { return re != nil && re.MatchString(f.GetOwner()) }
	}

	key := unquote(strings.TrimPrefix(strings.TrimPrefix(field, fieldAnnot), "."))

	return func(f *artifactregistrypb.File) bool {
		for k, v := range f.GetAnnotations() {
			if strings.EqualFold(k, key) {
				return re == nil || re.MatchString(v)
			}
		}

		return false
	}
}

func parseOrder(orderBy string) (string, bool) {
	field, dir, _ := strings.Cut(strings.TrimSpace(orderBy), " ")

	return field, strings.EqualFold(strings.TrimSpace(dir), "desc")
}

func sortByField[T any](
	items []T, field string, desc bool, name func(T) string, created, updated func(T) *timestamppb.Timestamp,
) error {
	var key func(a, b T) int

	switch field {
	case "", fieldName:
		key = func(a, b T) int { return cmp.Compare(name(a), name(b)) }
	case "create_time", "createTime":
		key = func(a, b T) int { return created(a).AsTime().Compare(created(b).AsTime()) }
	case fieldUpdSnake, fieldUpdated:
		key = func(a, b T) int { return updated(a).AsTime().Compare(updated(b).AsTime()) }
	default:
		return errInvalid("order_by %q is not supported: use name, create_time or update_time.", field)
	}

	slices.SortStableFunc(items, func(a, b T) int {
		if desc {
			return key(b, a)
		}

		return key(a, b)
	})

	return nil
}

// ListFiles lists a repository's files, filtered by name, owner and annotations.
func (b *InMemoryBackend) ListFiles(
	ctx context.Context, in *artifactregistrypb.ListFilesRequest,
) ([]*artifactregistrypb.File, string, error) {
	ref, err := parseRepo(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	if in.GetPageSize() < 0 {
		return nil, "", errInvalid("page_size must not be negative.")
	}

	if err = page.ValidateToken(in.GetPageToken()); err != nil {
		return nil, "", errInvalid("Invalid page_token.")
	}

	matchers, err := parseFileFilter(in.GetFilter())
	if err != nil {
		return nil, "", err
	}

	all, err := b.filesOf(ctx, ref)
	if err != nil {
		return nil, "", err
	}

	kept := make([]*artifactregistrypb.File, 0, len(all))

	for _, f := range all {
		if !slices.ContainsFunc(matchers, func(m fileMatcher) bool { return !m(f) }) {
			kept = append(kept, f)
		}
	}

	field, desc := parseOrder(in.GetOrderBy())
	if err = sortByField(kept, field, desc, (*artifactregistrypb.File).GetName,
		(*artifactregistrypb.File).GetCreateTime, (*artifactregistrypb.File).GetUpdateTime); err != nil {
		return nil, "", err
	}

	out, next := paged(kept, in.GetPageToken(), in.GetPageSize())

	return out, next, nil
}

// GetFile returns one file.
func (b *InMemoryBackend) GetFile(ctx context.Context, name string) (*artifactregistrypb.File, error) {
	fr, err := parseFile(name)
	if err != nil {
		return nil, err
	}

	all, err := b.filesOf(ctx, fr.repo)
	if err != nil {
		return nil, err
	}

	want := fr.repo.fileName(fr.id)

	for _, f := range all {
		if f.GetName() == want {
			return f, nil
		}
	}

	return nil, errNotFound("File %q not found.", name)
}

// DeleteFile deletes a generic file; other formats refuse, as AR allows it on generic repositories only.
func (b *InMemoryBackend) DeleteFile(_ context.Context, name string) (*longrunningpb.Operation, error) {
	fr, err := parseFile(name)
	if err != nil {
		return nil, err
	}

	generic, err := b.isGeneric(fr.repo)
	if err != nil {
		return nil, err
	}

	if !generic {
		return nil, errPrecondition("Files can only be deleted from GENERIC repositories.")
	}

	want := fr.repo.fileName(fr.id)

	b.mu.Lock("DeleteFile")

	if _, ok := b.genFiles[want]; !ok {
		b.mu.Unlock()

		return nil, errNotFound("File %q not found.", name)
	}

	delete(b.genFiles, want)
	b.mu.Unlock()

	return b.done(fr.repo.parent(), &emptypb.Empty{})
}

// UpdateFile replaces the annotations of a file.
func (b *InMemoryBackend) UpdateFile(
	ctx context.Context, in *artifactregistrypb.UpdateFileRequest,
) (*artifactregistrypb.File, error) {
	fr, err := parseFile(in.GetFile().GetName())
	if err != nil {
		return nil, err
	}

	paths := maskPaths(in.GetUpdateMask())
	if err = checkPaths(paths, []string{fieldAnnot}); err != nil {
		return nil, err
	}

	cur, err := b.GetFile(ctx, in.GetFile().GetName())
	if err != nil {
		return nil, err
	}

	if len(paths) > 0 || len(in.GetFile().GetAnnotations()) > 0 {
		if err = b.setFileAnnotations(fr.repo, cur.GetName(), maps.Clone(in.GetFile().GetAnnotations())); err != nil {
			return nil, err
		}
	}

	return b.GetFile(ctx, in.GetFile().GetName())
}

func (b *InMemoryBackend) setFileAnnotations(ref repoRef, name string, ann map[string]string) error {
	generic, err := b.isGeneric(ref)
	if err != nil {
		return err
	}

	if !generic {
		b.editMeta(name, func(m *resMeta) { m.Annotations = ann })

		return nil
	}

	b.mu.Lock("setFileAnnotations")
	defer b.mu.Unlock()

	if f, ok := b.genFiles[name]; ok {
		f.Annotations, f.Updated = ann, b.now().UTC()
	}

	return nil
}
