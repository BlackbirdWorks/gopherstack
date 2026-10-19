package timestreaminfluxdb

import (
	"context"
	"maps"
	"strings"
)

const arnParts = 3

func (b *InMemoryBackend) tagsOfLocked(sc scope, resourceARN string) (*map[string]string, error) {
	_, rest, ok := strings.Cut(resourceARN, ":"+serviceName+":")
	if !ok {
		return nil, invalid("resourceArn %q is not a Timestream for InfluxDB ARN", resourceARN)
	}

	parts := strings.SplitN(rest, ":", arnParts)
	if len(parts) != arnParts || parts[0] != sc.region || parts[1] != sc.account {
		return nil, notFound("resource", resourceARN)
	}

	kind, id, _ := strings.Cut(parts[2], "/")

	var holder *map[string]string

	switch kind {
	case "db-instance":
		if i, err := b.instanceLocked(sc, id); err == nil {
			holder = &i.Tags
		}
	case "db-cluster":
		if c, err := b.clusterLocked(sc, id); err == nil {
			holder = &c.Tags
		}
	case "db-parameter-group":
		if g, err := b.groupLocked(sc, id); err == nil {
			holder = &g.Tags
		}
	case "db-backup":
		if bk, err := b.backupLocked(sc, id); err == nil {
			holder = &bk.Tags
		}
	default:
		return nil, invalid("resourceArn %q is not a taggable resource", resourceARN)
	}

	if holder == nil {
		return nil, notFound("resource", resourceARN)
	}

	return holder, nil
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
