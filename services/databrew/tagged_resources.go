package databrew

import (
	"context"
	"maps"
)

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every tagged resource in the request region.
func (b *InMemoryBackend) TaggedResources(region string) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	add := func(arnVal string, tags map[string]string) {
		if len(tags) > 0 {
			out = append(out, TaggedEntry{ARN: arnVal, Tags: maps.Clone(tags)})
		}
	}

	for _, x := range b.datasetsTable(region).All() {
		add(x.Arn, x.Tags)
	}

	for _, x := range b.recipesTable(region).All() {
		add(x.Arn, x.Tags)
	}

	for _, x := range b.projectsTable(region).All() {
		add(x.Arn, x.Tags)
	}

	for _, x := range b.jobsTable(region).All() {
		add(x.Arn, x.Tags)
	}

	for _, x := range b.rulesetsTable(region).All() {
		add(x.Arn, x.Tags)
	}

	for _, x := range b.schedulesTable(region).All() {
		add(x.Arn, x.Tags)
	}

	return out
}

// WithRegion returns ctx routed to region for tag operations.
func WithRegion(ctx context.Context, region string) context.Context {
	return context.WithValue(ctx, regionContextKey{}, region)
}
