package route53recoveryreadiness

import (
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (b *InMemoryBackend) tagsFor(arn string) (*map[string]string, error) {
	for _, c := range b.cells.All() {
		if c.ARN == arn {
			return &c.Tags, nil
		}
	}

	for _, g := range b.groups.All() {
		if g.ARN == arn {
			return &g.Tags, nil
		}
	}

	for _, s := range b.sets.All() {
		if s.ARN == arn {
			return &s.Tags, nil
		}
	}

	for _, c := range b.checks.All() {
		if c.ARN == arn {
			return &c.Tags, nil
		}
	}

	return nil, restroute.NotFound("Resource %s was not found", arn)
}

// ListTagsForResource returns a resource's tags.
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResources")
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

// CreateCrossAccountAuthorization records an authorization (idempotent).
func (b *InMemoryBackend) CreateCrossAccountAuthorization(value string) error {
	if value == "" {
		return restroute.Validation("crossAccountAuthorization is required")
	}

	b.mu.Lock("CreateCrossAccountAuthorization")
	defer b.mu.Unlock()

	b.auths.Put(&Authorization{Value: value})

	return nil
}

// DeleteCrossAccountAuthorization removes an authorization.
func (b *InMemoryBackend) DeleteCrossAccountAuthorization(value string) error {
	b.mu.Lock("DeleteCrossAccountAuthorization")
	defer b.mu.Unlock()

	if !b.auths.Delete(value) {
		return restroute.NotFound("Cross-account authorization %s was not found", value)
	}

	return nil
}

// ListCrossAccountAuthorizations lists authorizations.
func (b *InMemoryBackend) ListCrossAccountAuthorizations(token string, maxResults int) ([]string, string, error) {
	b.mu.RLock("ListCrossAccountAuthorizations")
	defer b.mu.RUnlock()

	pg, err := restroute.Paginate(sorted(b.auths.All(), func(a *Authorization) string { return a.Value }),
		token, maxResults, listDefault, listMax)
	if err != nil {
		return nil, "", err
	}

	out := make([]string, 0, len(pg.Data))
	for _, a := range pg.Data {
		out = append(out, a.Value)
	}

	return out, pg.Next, nil
}
