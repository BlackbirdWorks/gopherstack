package codebuild

import "maps"

// ListTagsForResource returns the tags for a CodeBuild resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if matches := b.projectsByARN.Get(resourceARN); len(matches) > 0 {
		p := matches[0]
		out := make(map[string]string, len(p.Tags))
		maps.Copy(out, p.Tags)

		return out, nil
	}

	if matches := b.buildsByARN.Get(resourceARN); len(matches) > 0 {
		build := matches[0]
		out := make(map[string]string, len(build.Tags))
		maps.Copy(out, build.Tags)

		return out, nil
	}

	if matches := b.fleetsByARN.Get(resourceARN); len(matches) > 0 {
		f := matches[0]
		out := make(map[string]string, len(f.Tags))
		maps.Copy(out, f.Tags)

		return out, nil
	}

	if matches := b.reportGroupsByARN.Get(resourceARN); len(matches) > 0 {
		rg := matches[0]
		out := make(map[string]string, len(rg.Tags))
		maps.Copy(out, rg.Tags)

		return out, nil
	}

	return nil, ErrNotFound
}

// TagResource adds or updates tags on a CodeBuild resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	if matches := b.projectsByARN.Get(resourceARN); len(matches) > 0 {
		p := matches[0]
		if p.Tags == nil {
			p.Tags = make(map[string]string)
		}

		maps.Copy(p.Tags, tagsCopy)

		return nil
	}

	if matches := b.buildsByARN.Get(resourceARN); len(matches) > 0 {
		build := matches[0]
		if build.Tags == nil {
			build.Tags = make(map[string]string)
		}

		maps.Copy(build.Tags, tagsCopy)

		return nil
	}

	if matches := b.fleetsByARN.Get(resourceARN); len(matches) > 0 {
		f := matches[0]
		if f.Tags == nil {
			f.Tags = make(map[string]string)
		}

		maps.Copy(f.Tags, tagsCopy)

		return nil
	}

	if matches := b.reportGroupsByARN.Get(resourceARN); len(matches) > 0 {
		rg := matches[0]
		if rg.Tags == nil {
			rg.Tags = make(map[string]string)
		}

		maps.Copy(rg.Tags, tagsCopy)

		return nil
	}

	return ErrNotFound
}

// UntagResource removes tags from a CodeBuild resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if matches := b.projectsByARN.Get(resourceARN); len(matches) > 0 {
		p := matches[0]
		for _, k := range tagKeys {
			delete(p.Tags, k)
		}

		return nil
	}

	if matches := b.buildsByARN.Get(resourceARN); len(matches) > 0 {
		build := matches[0]
		for _, k := range tagKeys {
			delete(build.Tags, k)
		}

		return nil
	}

	if matches := b.fleetsByARN.Get(resourceARN); len(matches) > 0 {
		f := matches[0]
		for _, k := range tagKeys {
			delete(f.Tags, k)
		}

		return nil
	}

	if matches := b.reportGroupsByARN.Get(resourceARN); len(matches) > 0 {
		rg := matches[0]
		for _, k := range tagKeys {
			delete(rg.Tags, k)
		}

		return nil
	}

	return ErrNotFound
}
