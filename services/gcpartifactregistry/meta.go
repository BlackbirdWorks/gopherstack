package gcpartifactregistry

import (
	"maps"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// resMeta is the client-editable state of a package, version or file: the docker-backed ones keep only the
// edits, generic ones are the whole resource.
type resMeta struct {
	Created     time.Time         `json:"created"`
	Updated     time.Time         `json:"updated"`
	Annotations map[string]string `json:"annotations,omitempty"`
	DisplayName string            `json:"displayName,omitempty"`
	Description string            `json:"description,omitempty"`
}

func (m *resMeta) clone() resMeta {
	if m == nil {
		return resMeta{}
	}

	out := *m
	out.Annotations = maps.Clone(m.Annotations)

	return out
}

func (b *InMemoryBackend) metaOf(name string) resMeta {
	b.mu.RLock("metaOf")
	defer b.mu.RUnlock()

	return b.meta[name].clone()
}

func (b *InMemoryBackend) editMeta(name string, edit func(*resMeta)) {
	b.mu.Lock("editMeta")
	defer b.mu.Unlock()

	m := b.meta[name]
	if m == nil {
		m = &resMeta{}
		b.meta[name] = m
	}

	edit(m)
	m.Updated = b.now().UTC()
}

// dropMeta forgets the edits of name and everything below it.
func (b *InMemoryBackend) dropMeta(name string) {
	b.mu.Lock("dropMeta")
	defer b.mu.Unlock()

	for k := range b.meta {
		if k == name || strings.HasPrefix(k, name+"/") {
			delete(b.meta, k)
		}
	}
}

func laterOf(a *timestamppb.Timestamp, t time.Time) *timestamppb.Timestamp {
	if t.After(a.AsTime()) {
		return timestamppb.New(t)
	}

	return a
}

func checkPaths(paths, allowed []string) error {
	for _, p := range paths {
		if !slices.Contains(allowed, p) {
			return errInvalid("Update mask path %q is not an updatable field.", p)
		}
	}

	return nil
}

func wants(paths []string, field string) bool {
	return len(paths) == 0 || slices.Contains(paths, field)
}

func maskPaths(m *fieldmaskpb.FieldMask) []string { return m.GetPaths() }

// UpdatePackage changes the display name and annotations of a package.
func (b *InMemoryBackend) UpdatePackage(
	in *artifactregistrypb.UpdatePackageRequest,
) (*artifactregistrypb.Package, error) {
	p, err := parsePackage(in.GetPackage().GetName())
	if err != nil {
		return nil, err
	}

	paths := maskPaths(in.GetUpdateMask())
	if err = checkPaths(paths, []string{"display_name", fieldAnnot}); err != nil {
		return nil, err
	}

	edit := func(m *resMeta) {
		if wants(paths, "display_name") && (len(paths) > 0 || in.GetPackage().GetDisplayName() != "") {
			m.DisplayName = in.GetPackage().GetDisplayName()
		}

		if wants(paths, fieldAnnot) && (len(paths) > 0 || len(in.GetPackage().GetAnnotations()) > 0) {
			m.Annotations = maps.Clone(in.GetPackage().GetAnnotations())
		}
	}

	if err = b.editPackage(p, edit); err != nil {
		return nil, err
	}

	return b.GetPackage(p.full())
}

func (b *InMemoryBackend) editPackage(p pkgRef, edit func(*resMeta)) error {
	generic, err := b.isGeneric(p.repo)
	if err != nil {
		return err
	}

	if !generic {
		if _, err = b.snapOfPackage(p); err != nil {
			return err
		}

		b.editMeta(p.full(), edit)

		return nil
	}

	return b.editGeneric(b.genPkgs, p.full(), "Package", edit)
}

func (b *InMemoryBackend) editGeneric(store map[string]*resMeta, name, kind string, edit func(*resMeta)) error {
	b.mu.Lock("editGeneric")
	defer b.mu.Unlock()

	m, ok := store[name]
	if !ok {
		return errNotFound("%s %q not found.", kind, name)
	}

	edit(m)
	m.Updated = b.now().UTC()

	return nil
}

// UpdateVersion changes the description and annotations of a version.
func (b *InMemoryBackend) UpdateVersion(
	in *artifactregistrypb.UpdateVersionRequest,
) (*artifactregistrypb.Version, error) {
	l, err := parseLeaf(in.GetVersion().GetName(), "version")
	if err != nil {
		return nil, err
	}

	paths := maskPaths(in.GetUpdateMask())
	if err = checkPaths(paths, []string{"description", fieldAnnot}); err != nil {
		return nil, err
	}

	edit := func(m *resMeta) {
		if wants(paths, "description") && (len(paths) > 0 || in.GetVersion().GetDescription() != "") {
			m.Description = in.GetVersion().GetDescription()
		}

		if wants(paths, fieldAnnot) && (len(paths) > 0 || len(in.GetVersion().GetAnnotations()) > 0) {
			m.Annotations = maps.Clone(in.GetVersion().GetAnnotations())
		}
	}

	generic, err := b.isGeneric(l.pkg.repo)
	if err != nil {
		return nil, err
	}

	name := l.pkg.full() + "/versions/" + l.leaf

	if generic {
		err = b.editGeneric(b.genVers, name, "Version", edit)
	} else {
		var s imageSnap
		if s, err = b.snapOfPackage(l.pkg); err == nil {
			if _, ok := s.manifests[l.leaf]; !ok {
				return nil, errNotFound("Version %q not found.", name)
			}

			b.editMeta(name, edit)
		}
	}

	if err != nil {
		return nil, err
	}

	return b.GetVersion(b.ctxOrBackground(), &artifactregistrypb.GetVersionRequest{Name: name})
}
