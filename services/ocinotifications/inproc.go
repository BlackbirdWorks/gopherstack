package ocinotifications

import "slices"

// Compartments lists the distinct compartments that hold live topics or subscriptions.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]struct{}{}

	for _, t := range b.topics {
		seen[t.CompartmentID] = struct{}{}
	}

	for _, s := range b.subs {
		seen[s.CompartmentID] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}

	slices.Sort(out)

	return out
}

// TopicActive reports whether id is an ACTIVE topic.
func (b *InMemoryBackend) TopicActive(id string) bool {
	b.mu.Lock("TopicActive")
	defer b.mu.Unlock()

	_, aerr := b.activeTopic(id)

	return aerr == nil
}
