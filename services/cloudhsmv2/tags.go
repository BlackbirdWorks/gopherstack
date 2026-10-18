package cloudhsmv2

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// tagsOfLocked resolves a cluster or backup ID (or ARN) to its tag map.
func (b *InMemoryBackend) tagsOfLocked(sc scope, ref string) (*map[string]string, error) {
	if ref == "" {
		return nil, fail(ErrInvalidRequest, "ResourceId is required")
	}

	id, _, _ := splitRef(ref)

	switch {
	case strings.HasPrefix(id, "cluster-"):
		c, err := b.clusterLocked(sc, id)
		if err != nil {
			return nil, err
		}

		return &c.Tags, nil
	case strings.HasPrefix(id, "backup-"):
		bk, err := b.backupLocked(sc, id)
		if err != nil {
			return nil, err
		}

		return &bk.Tags, nil
	default:
		return nil, fail(ErrResourceNotFound, "resource %s not found", ref)
	}
}

// TagResource adds or replaces tags on a cluster or backup.
func (b *InMemoryBackend) TagResource(ctx context.Context, ref string, tagList []wireTag) error {
	add, err := validateTags(tagList)
	if err != nil {
		return err
	}

	sc := b.scopeOf(ctx)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(sc, ref)
	if err != nil {
		return err
	}

	merged := maps.Clone(*t)
	if merged == nil {
		merged = make(map[string]string, len(add))
	}

	maps.Copy(merged, add)

	if len(merged) > maxTags {
		return fail(ErrTag, "a resource can have at most %d tags", maxTags)
	}

	*t = merged

	return nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(ctx context.Context, ref string, keys []string) error {
	sc := b.scopeOf(ctx)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(sc, ref)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(*t, k)
	}

	return nil
}

// ListTags pages the tags of a cluster or backup.
func (b *InMemoryBackend) ListTags(
	ctx context.Context, ref, token string, maxResults *int32,
) (page.Page[wireTag], error) {
	limit, err := validatePaging(token, maxResults)
	if err != nil {
		return page.Page[wireTag]{}, err
	}

	sc := b.scopeOf(ctx)

	b.mu.RLock("ListTags")
	defer b.mu.RUnlock()

	t, err := b.tagsOfLocked(sc, ref)
	if err != nil {
		return page.Page[wireTag]{}, err
	}

	return page.New(slices.Clone(wireTags(*t)), token, limit, defaultPage), nil
}
