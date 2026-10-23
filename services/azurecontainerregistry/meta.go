package azurecontainerregistry

import (
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

type attrs = ociregistry.Attrs

// RecordManifestPut notes a stored manifest, and the tag when ref is one.
func (b *InMemoryBackend) RecordManifestPut(reg, repo, ref, digest string) {
	b.mu.Lock("RecordManifestPut")
	defer b.mu.Unlock()

	if r, ok := b.regs[strs.Fold(reg)]; ok {
		r.Repos.RecordPut(repo, ref, digest, b.now().UTC())
	}
}

// RecordManifestDelete forgets a deleted manifest with its tags, or just the tag when ref is one.
func (b *InMemoryBackend) RecordManifestDelete(reg, repo, ref string) {
	b.mu.Lock("RecordManifestDelete")
	defer b.mu.Unlock()

	if r, ok := b.regs[strs.Fold(reg)]; ok {
		r.Repos.RecordDelete(repo, ref)
	}
}

// Allowed reports whether op (read, write or delete) may touch repo:ref.
func (b *InMemoryBackend) Allowed(reg, repo, ref, op string) bool {
	b.mu.RLock("Allowed")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]

	return !ok || r.Repos.Allowed(repo, ref, op)
}

// RepoView is a repository's listing entry.
type RepoView struct {
	Created       time.Time
	Updated       time.Time
	Name          string
	Attrs         attrs
	ManifestCount int
	TagCount      int
}

// TagView is a tag's listing entry.
type TagView struct {
	Created time.Time
	Updated time.Time
	Name    string
	Digest  string
	Attrs   attrs
}

// ManifestView is a manifest's listing entry.
type ManifestView struct {
	Created time.Time
	Updated time.Time
	Digest  string
	Tags    []string
	Attrs   attrs
}

func (b *InMemoryBackend) repoOf(reg, repo string) (*ociregistry.RepoEntry, bool) {
	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return nil, false
	}

	rm, ok := r.Repos[repo]

	return rm, ok
}

// Repositories lists the repositories holding at least one manifest, in lexical order.
func (b *InMemoryBackend) Repositories(reg string) []RepoView {
	b.mu.RLock("Repositories")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return nil
	}

	out := make([]RepoView, 0, len(r.Repos))
	for name, rm := range r.Repos {
		out = append(out, repoView(name, rm))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

func repoView(name string, rm *ociregistry.RepoEntry) RepoView {
	return RepoView{
		Name: name, Created: rm.Created, Updated: rm.Updated, Attrs: rm.Attrs,
		ManifestCount: len(rm.Manifests), TagCount: len(rm.Tags),
	}
}

// Repository returns one repository's entry.
func (b *InMemoryBackend) Repository(reg, repo string) (RepoView, bool) {
	b.mu.RLock("Repository")
	defer b.mu.RUnlock()

	rm, ok := b.repoOf(reg, repo)
	if !ok {
		return RepoView{}, false
	}

	return repoView(repo, rm), true
}

// Tags lists a repository's tags in lexical order.
func (b *InMemoryBackend) Tags(reg, repo string) ([]TagView, bool) {
	b.mu.RLock("Tags")
	defer b.mu.RUnlock()

	rm, ok := b.repoOf(reg, repo)
	if !ok {
		return nil, false
	}

	out := make([]TagView, 0, len(rm.Tags))
	for name, t := range rm.Tags {
		out = append(out, TagView{Name: name, Digest: t.Digest, Created: t.Created, Updated: t.Updated, Attrs: t.Attrs})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out, true
}

// Manifests lists a repository's manifests in lexical digest order.
func (b *InMemoryBackend) Manifests(reg, repo string) ([]ManifestView, bool) {
	b.mu.RLock("Manifests")
	defer b.mu.RUnlock()

	rm, ok := b.repoOf(reg, repo)
	if !ok {
		return nil, false
	}

	out := make([]ManifestView, 0, len(rm.Manifests))
	for digest, m := range rm.Manifests {
		out = append(out, ManifestView{Digest: digest, Created: m.Created, Updated: m.Updated, Attrs: m.Attrs})
	}

	for i := range out {
		for name, t := range rm.Tags {
			if t.Digest == out[i].Digest {
				out[i].Tags = append(out[i].Tags, name)
			}
		}

		sort.Strings(out[i].Tags)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Digest < out[j].Digest })

	return out, true
}

// AttrPatch carries the attributes a PATCH sets; nil leaves one unchanged.
type AttrPatch struct {
	Delete *bool
	Write  *bool
	Read   *bool
	List   *bool
}

func (p AttrPatch) apply(a *attrs) {
	set := func(dst *bool, v *bool) {
		if v != nil {
			*dst = !*v
		}
	}

	set(&a.DeleteDisabled, p.Delete)
	set(&a.WriteDisabled, p.Write)
	set(&a.ReadDisabled, p.Read)
	set(&a.ListDisabled, p.List)
}

// PatchTag updates a tag's attributes.
func (b *InMemoryBackend) PatchTag(reg, repo, tag string, p AttrPatch) error {
	b.mu.Lock("PatchTag")
	defer b.mu.Unlock()

	rm, ok := b.repoOf(reg, repo)
	if !ok {
		return ErrNotFound
	}

	t, ok := rm.Tags[tag]
	if !ok {
		return ErrNotFound
	}

	p.apply(&t.Attrs)

	return nil
}

// PatchManifest updates a manifest's attributes.
func (b *InMemoryBackend) PatchManifest(reg, repo, digest string, p AttrPatch) error {
	b.mu.Lock("PatchManifest")
	defer b.mu.Unlock()

	rm, ok := b.repoOf(reg, repo)
	if !ok {
		return ErrNotFound
	}

	m, ok := rm.Manifests[digest]
	if !ok {
		return ErrNotFound
	}

	p.apply(&m.Attrs)

	return nil
}

// PatchRepository updates a repository's attributes.
func (b *InMemoryBackend) PatchRepository(reg, repo string, p AttrPatch) error {
	b.mu.Lock("PatchRepository")
	defer b.mu.Unlock()

	rm, ok := b.repoOf(reg, repo)
	if !ok {
		return ErrNotFound
	}

	p.apply(&rm.Attrs)

	return nil
}

// Forgotten lists what ForgetRepository removed.
type Forgotten struct {
	Digests []string
	Tags    []string
}

// ForgetRepository drops a repository's metadata and returns the digests and tags it held.
func (b *InMemoryBackend) ForgetRepository(reg, repo string) (Forgotten, bool) {
	b.mu.Lock("ForgetRepository")
	defer b.mu.Unlock()

	r, found := b.regs[strs.Fold(reg)]
	if !found {
		return Forgotten{}, false
	}

	rm, found := r.Repos[repo]
	if !found {
		return Forgotten{}, false
	}

	var out Forgotten

	for d := range rm.Manifests {
		out.Digests = append(out.Digests, d)
	}

	for t := range rm.Tags {
		out.Tags = append(out.Tags, t)
	}

	sort.Strings(out.Digests)
	sort.Strings(out.Tags)
	delete(r.Repos, repo)

	return out, true
}
