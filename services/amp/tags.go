package amp

import (
	"context"
	"maps"
	"strings"
)

const arnResourceParts = 3

// tagsOfLocked resolves a taggable ARN to its tag map holder.
func (b *InMemoryBackend) tagsOfLocked(sc scope, resourceARN string) (*map[string]string, error) {
	_, rest, ok := strings.Cut(resourceARN, ":aps:")
	if !ok {
		return nil, invalid("resourceArn %q is not an AMP resource ARN", resourceARN)
	}

	parts := strings.SplitN(rest, ":", arnResourceParts)
	if len(parts) != arnResourceParts || parts[0] != sc.region || parts[1] != sc.account {
		return nil, notFound("resource", resourceARN, "Resource %s does not exist", resourceARN)
	}

	kind, path, _ := strings.Cut(parts[2], "/")
	segs := strings.Split(path, "/")

	holder, taggable := b.tagHolderLocked(sc, kind, segs)
	if !taggable {
		return nil, invalid("resourceArn %q is not a taggable AMP resource", resourceARN)
	}

	if holder == nil {
		return nil, notFound("resource", resourceARN, "Resource %s does not exist", resourceARN)
	}

	return holder, nil
}

// tagHolderLocked returns the tag map of the named resource; taggable is false for unknown kinds or shapes.
func (b *InMemoryBackend) tagHolderLocked(
	sc scope,
	kind string,
	segs []string,
) (*map[string]string, bool) {
	switch {
	case kind == resWorkspace && len(segs) == 1:
		if w, err := b.workspaceLocked(sc, segs[0]); err == nil {
			return &w.Tags, true
		}
	case kind == resNamespace && len(segs) == 2:
		if n, err := b.namespaceLocked(sc, segs[0], segs[1]); err == nil {
			return &n.Tags, true
		}
	case kind == resScraper && len(segs) == 1:
		if s, err := b.scraperLocked(sc, segs[0]); err == nil {
			return &s.Tags, true
		}
	case kind == resDetector && len(segs) == 2:
		if d, err := b.detectorLocked(sc, segs[0], segs[1]); err == nil {
			return &d.Tags, true
		}
	default:
		return nil, false
	}

	return nil, true
}

// TagResource adds or replaces tags.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceARN string, add map[string]string) error {
	if err := validateTags(add); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	b.purgeLocked()

	t, err := b.tagsOfLocked(b.scopeOf(ctx), resourceARN)
	if err != nil {
		return err
	}

	merged := maps.Clone(*t)
	if merged == nil {
		merged = make(map[string]string, len(add))
	}

	maps.Copy(merged, add)

	if ierr := validateTags(merged); ierr != nil {
		return ierr
	}

	*t = merged

	return nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	b.purgeLocked()

	t, err := b.tagsOfLocked(b.scopeOf(ctx), resourceARN)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(*t, k)
	}

	return nil
}

// ListTagsForResource returns a resource's tags.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, err := b.tagsOfLocked(b.scopeOf(ctx), resourceARN)
	if err != nil {
		return nil, err
	}

	return maps.Clone(*t), nil
}
