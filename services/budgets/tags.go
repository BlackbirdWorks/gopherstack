package budgets

import (
	"maps"
	"strings"
)

// tagsOfLocked resolves a budget or action ARN to its tag map.
func (b *InMemoryBackend) tagsOfLocked(resource string) (*map[string]string, error) {
	_, rest, ok := strings.Cut(resource, ":budgets::")
	if !ok {
		return nil, fail(ErrNotFound, "resource %s not found", resource)
	}

	account, path, _ := strings.Cut(rest, ":budget/")
	if name, id, isAction := strings.Cut(path, "/action/"); isAction {
		r, err := b.actionLocked(account, name, id)
		if err != nil {
			return nil, err
		}

		return &r.Tags, nil
	}

	r, err := b.budgetLocked(account, path)
	if err != nil {
		return nil, err
	}

	return &r.Tags, nil
}

// TagResource adds or replaces tags on a budget or action.
func (b *InMemoryBackend) TagResource(resource string, add []ResourceTag) error {
	m, err := validateTags(add)
	if err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(resource)
	if err != nil {
		return err
	}

	merged := maps.Clone(*t)
	if merged == nil {
		merged = make(map[string]string, len(m))
	}

	maps.Copy(merged, m)

	if len(merged) > maxTags {
		return fail(ErrCreationLimitExceeded, "a resource can have at most %d tags", maxTags)
	}

	*t = merged

	return nil
}

// UntagResource removes tags by key.
func (b *InMemoryBackend) UntagResource(resource string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.tagsOfLocked(resource)
	if err != nil {
		return err
	}

	for _, k := range keys {
		delete(*t, k)
	}

	return nil
}

// ListTagsForResource returns a resource's tags.
func (b *InMemoryBackend) ListTagsForResource(resource string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, err := b.tagsOfLocked(resource)
	if err != nil {
		return nil, err
	}

	return maps.Clone(*t), nil
}
