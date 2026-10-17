package route53recoverycontrolconfig

import (
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const maxTags = 50

// tagsFor returns the live tag map of a taggable resource (cluster, control panel or safety rule).
func (b *InMemoryBackend) tagsFor(arn string) (*map[string]string, error) {
	if c, err := b.getCluster(arn); err == nil {
		return &c.Tags, nil
	}

	if p, err := b.getPanel(arn); err == nil {
		return &p.Tags, nil
	}

	if r, err := b.getRule(arn); err == nil {
		return &r.Tags, nil
	}

	if _, err := b.getControl(arn); err == nil {
		return nil, restroute.Validation("Routing controls do not support tags")
	}

	return nil, restroute.NotFound("Resource %s was not found", arn)
}

// ListTagsForResource returns a resource's tags.
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, err := b.tagsFor(arn)
	if err != nil {
		return nil, err
	}

	return maps.Clone(*t), nil
}

// TagResource adds or overwrites tags.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.tagsFor(arn)
	if err != nil {
		return err
	}

	merged := maps.Clone(*t)
	if merged == nil {
		merged = map[string]string{}
	}

	maps.Copy(merged, tags)

	if len(merged) > maxTags {
		return restroute.Validation("A resource can have at most %d tags", maxTags)
	}

	*t = merged

	return nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.tagsFor(arn)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(*t, k)
	}

	return nil
}

// requireResource backs GetResourcePolicy: policies only arrive through RAM sharing, so an existing resource has none.
func (b *InMemoryBackend) requireResource(arn string) error {
	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	_, e1 := b.getCluster(arn)
	_, e2 := b.getPanel(arn)
	_, e3 := b.getControl(arn)
	_, e4 := b.getRule(arn)

	if e1 == nil || e2 == nil || e3 == nil || e4 == nil {
		return nil
	}

	return restroute.NotFound("Resource %s was not found", arn)
}
