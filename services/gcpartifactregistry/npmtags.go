package gcpartifactregistry

import (
	"maps"
	"slices"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
)

// isNPMRepo reports whether ref is an existing NPM repository; any other repository is left to the Docker path.
func (b *InMemoryBackend) isNPMRepo(ref repoRef) bool {
	b.mu.RLock("isNPMRepo")
	defer b.mu.RUnlock()

	rec, ok := b.repos[ref.full()]

	return ok && rec.pb.GetFormat() == artifactregistrypb.Repository_NPM
}

// npmDistTags returns a copy of a package's dist-tags, failing when the package does not exist.
func (b *InMemoryBackend) npmDistTags(p pkgRef) (map[string]string, error) {
	b.mu.RLock("npmDistTags")
	defer b.mu.RUnlock()

	if _, ok := b.genPkgs[p.full()]; !ok {
		return nil, errNotFound("Package %q not found.", p.full())
	}

	return maps.Clone(b.pkgTags[p.full()]), nil
}

func (b *InMemoryBackend) npmListTags(in *artifactregistrypb.ListTagsRequest, p pkgRef) (
	[]*artifactregistrypb.Tag, string, error,
) {
	if err := validatePaging(in.GetPageSize(), in.GetPageToken(), ""); err != nil {
		return nil, "", err
	}

	version, err := versionFilter(in.GetFilter())
	if err != nil {
		return nil, "", err
	}

	tags, err := b.npmDistTags(p)
	if err != nil {
		return nil, "", err
	}

	var all []*artifactregistrypb.Tag

	for _, t := range slices.Sorted(maps.Keys(tags)) {
		if pb := b.tagPB(p, t, tags[t]); version == "" || pb.GetVersion() == version {
			all = append(all, pb)
		}
	}

	out, next := paged(all, in.GetPageToken(), in.GetPageSize())

	return out, next, nil
}

func (b *InMemoryBackend) npmGetTag(l leafRef) (*artifactregistrypb.Tag, error) {
	tags, err := b.npmDistTags(l.pkg)
	if err != nil {
		return nil, err
	}

	v, ok := tags[l.leaf]
	if !ok {
		return nil, errNotFound("Tag %q not found.", l.pkg.full()+"/tags/"+l.leaf)
	}

	return b.tagPB(l.pkg, l.leaf, v), nil
}

// npmSetTag points a dist-tag at an existing version. A new tag must not exist yet and an updated one must.
func (b *InMemoryBackend) npmSetTag(p pkgRef, tag, versionName string, create bool) (*artifactregistrypb.Tag, error) {
	version, err := versionDigest(p, versionName)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("npmSetTag")
	defer b.mu.Unlock()

	name := p.full()
	if _, ok := b.genPkgs[name]; !ok {
		return nil, errNotFound("Package %q not found.", name)
	}

	if _, ok := b.genVers[name+"/versions/"+version]; !ok {
		return nil, errNotFound("Version %q not found.", name+"/versions/"+version)
	}

	_, exists := b.pkgTags[name][tag]

	switch {
	case create && exists:
		return nil, errExists("Tag %q already exists.", name+"/tags/"+tag)
	case !create && !exists:
		return nil, errNotFound("Tag %q not found.", name+"/tags/"+tag)
	}

	if b.pkgTags[name] == nil {
		b.pkgTags[name] = map[string]string{}
	}

	b.pkgTags[name][tag] = version

	return b.tagPB(p, tag, version), nil
}

func (b *InMemoryBackend) npmDeleteTag(l leafRef) error {
	b.mu.Lock("npmDeleteTag")
	defer b.mu.Unlock()

	name := l.pkg.full()
	if _, ok := b.genPkgs[name]; !ok {
		return errNotFound("Package %q not found.", name)
	}

	if _, ok := b.pkgTags[name][l.leaf]; !ok {
		return errNotFound("Tag %q not found.", name+"/tags/"+l.leaf)
	}

	delete(b.pkgTags[name], l.leaf)

	if len(b.pkgTags[name]) == 0 {
		delete(b.pkgTags, name)
	}

	return nil
}
