package gcplogging

import (
	"strings"

	"cloud.google.com/go/logging/apiv2/loggingpb"
)

// AncestryResolver returns the folders and organization above a project, nearest first.
type AncestryResolver func(project string) []string

// SetAncestryResolver wires the resource hierarchy that include_children sinks aggregate over.
func (b *InMemoryBackend) SetAncestryResolver(r AncestryResolver) {
	b.mu.Lock("SetAncestryResolver")
	defer b.mu.Unlock()

	b.ancestry = r
}

// ancestorsOf resolves the ancestors of every project the entries belong to, outside the backend lock.
func (b *InMemoryBackend) ancestorsOf(entries []*loggingpb.LogEntry) map[string][]string {
	b.mu.RLock("ancestorsOf")
	resolve := b.ancestry
	b.mu.RUnlock()

	if resolve == nil {
		return nil
	}

	out := map[string][]string{}

	for _, e := range entries {
		parent := parentOf(e.GetLogName())
		if _, done := out[parent]; !done && strings.HasPrefix(parent, "projects/") {
			out[parent] = resolve(parent)
		}
	}

	return out
}
