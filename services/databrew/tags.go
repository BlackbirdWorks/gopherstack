package databrew

import (
	"context"
	"maps"
)

// FindTagsByArn searches all resources in the request region for a specific ARN and returns its tags.
func (b *InMemoryBackend) FindTagsByArn(
	ctx context.Context,
	arnVal string,
) (map[string]string, error) {
	b.mu.RLock("FindTagsByArn")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	for _, ds := range b.datasetsTable(region).All() {
		if ds.Arn == arnVal {
			return maps.Clone(ds.Tags), nil
		}
	}
	for _, r := range b.recipesTable(region).All() {
		if r.Arn == arnVal {
			return maps.Clone(r.Tags), nil
		}
	}
	for _, p := range b.projectsTable(region).All() {
		if p.Arn == arnVal {
			return maps.Clone(p.Tags), nil
		}
	}
	for _, j := range b.jobsTable(region).All() {
		if j.Arn == arnVal {
			return maps.Clone(j.Tags), nil
		}
	}
	for _, rs := range b.rulesetsTable(region).All() {
		if rs.Arn == arnVal {
			return maps.Clone(rs.Tags), nil
		}
	}
	for _, sc := range b.schedulesTable(region).All() {
		if sc.Arn == arnVal {
			return maps.Clone(sc.Tags), nil
		}
	}

	return nil, ErrNotFound
}

// UpdateTagsByArn searches all resources in the request region and applies tags additions/removals.
func (b *InMemoryBackend) UpdateTagsByArn(
	ctx context.Context,
	arnVal string,
	add map[string]string,
	remove []string,
) error {
	b.mu.Lock("UpdateTagsByArn")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	applyTags := func(tags map[string]string) map[string]string {
		if tags == nil {
			tags = make(map[string]string)
		}
		maps.Copy(tags, add)
		for _, k := range remove {
			delete(tags, k)
		}

		return tags
	}

	if b.updateDatasetTags(region, arnVal, applyTags) {
		return nil
	}
	if b.updateRecipeTags(region, arnVal, applyTags) {
		return nil
	}
	if b.updateProjectTags(region, arnVal, applyTags) {
		return nil
	}
	if b.updateJobTags(region, arnVal, applyTags) {
		return nil
	}
	if b.updateRulesetTags(region, arnVal, applyTags) {
		return nil
	}
	if b.updateScheduleTags(region, arnVal, applyTags) {
		return nil
	}

	return ErrNotFound
}

func (b *InMemoryBackend) updateDatasetTags(
	region, arnVal string,
	apply func(map[string]string) map[string]string,
) bool {
	for _, x := range b.datasetsTable(region).All() {
		if x.Arn == arnVal {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateRecipeTags(
	region, arnVal string,
	apply func(map[string]string) map[string]string,
) bool {
	for _, x := range b.recipesTable(region).All() {
		if x.Arn == arnVal {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateProjectTags(
	region, arnVal string,
	apply func(map[string]string) map[string]string,
) bool {
	for _, x := range b.projectsTable(region).All() {
		if x.Arn == arnVal {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateJobTags(
	region, arnVal string,
	apply func(map[string]string) map[string]string,
) bool {
	for _, x := range b.jobsTable(region).All() {
		if x.Arn == arnVal {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateRulesetTags(
	region, arnVal string,
	apply func(map[string]string) map[string]string,
) bool {
	for _, x := range b.rulesetsTable(region).All() {
		if x.Arn == arnVal {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) updateScheduleTags(
	region, arnVal string,
	apply func(map[string]string) map[string]string,
) bool {
	for _, x := range b.schedulesTable(region).All() {
		if x.Arn == arnVal {
			x.Tags = apply(x.Tags)

			return true
		}
	}

	return false
}
