package azureeventgrid

import (
	"maps"
	"slices"
)

// ScopeSubscriptions lists the event subscriptions owned by one Azure Resource Manager scope.
type ScopeSubscriptions struct {
	ID            string
	Subscriptions []string
}

func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

// DomainNames returns the domain names sorted.
func (b *InMemoryBackend) DomainNames() []string {
	b.mu.RLock("DomainNames")
	defer b.mu.RUnlock()

	return sortedKeys(b.domains)
}

// NamespaceNames returns the namespace names sorted.
func (b *InMemoryBackend) NamespaceNames() []string {
	b.mu.RLock("NamespaceNames")
	defer b.mu.RUnlock()

	return sortedKeys(b.namespaces)
}

// NamespaceTopicNames returns a namespace's topic names sorted.
func (b *InMemoryBackend) NamespaceTopicNames(ns string) ([]string, error) {
	b.mu.RLock("NamespaceTopicNames")
	defer b.mu.RUnlock()

	n, err := b.nsLocked(ns)
	if err != nil {
		return nil, err
	}

	return sortedKeys(n.Topics), nil
}

// PullSubscriptionNames returns a namespace topic's subscription names sorted.
func (b *InMemoryBackend) PullSubscriptionNames(ns, topicName string) ([]string, error) {
	b.mu.RLock("PullSubscriptionNames")
	defer b.mu.RUnlock()

	_, t, err := b.nsTopicLocked(ns, topicName)
	if err != nil {
		return nil, err
	}

	return sortedKeys(t.Subs), nil
}

// ScopedSubscriptionNames returns the event subscription names of a custom topic,
// domain, domain topic or resource scope, sorted.
func (b *InMemoryBackend) ScopedSubscriptionNames(sc Scope) ([]string, error) {
	b.mu.RLock("ScopedSubscriptionNames")
	defer b.mu.RUnlock()

	info, err := b.scopeLocked(sc, false)
	if err != nil {
		return nil, err
	}

	return sortedKeys(info.subs), nil
}

// ARMScopes returns the Azure Resource Manager scopes that own event subscriptions, sorted by ID.
func (b *InMemoryBackend) ARMScopes() []ScopeSubscriptions {
	b.mu.RLock("ARMScopes")
	defer b.mu.RUnlock()

	out := make([]ScopeSubscriptions, 0, len(b.armScopes))
	for _, a := range b.armScopes {
		out = append(out, ScopeSubscriptions{ID: a.ID, Subscriptions: sortedKeys(a.Subs)})
	}

	slices.SortFunc(out, func(x, y ScopeSubscriptions) int {
		switch {
		case x.ID < y.ID:
			return -1
		case x.ID > y.ID:
			return 1
		default:
			return 0
		}
	})

	return out
}
