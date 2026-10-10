package transfer

import (
	"fmt"
	"maps"
	"strings"
)

// TagResource applies tags to a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExistsLocked(resourceARN) {
		return fmt.Errorf("%w: resource %s not found", ErrServerNotFound, resourceARN)
	}

	if _, ok := b.tagsStore[resourceARN]; !ok {
		b.tagsStore[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tagsStore[resourceARN], tags)

	return nil
}

// UntagResource removes tag keys from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.resourceExistsLocked(resourceARN) {
		return fmt.Errorf("%w: resource %s not found", ErrServerNotFound, resourceARN)
	}

	if existing, ok := b.tagsStore[resourceARN]; ok {
		for _, k := range tagKeys {
			delete(existing, k)
		}
	}

	return nil
}

// ListTagsForResource returns tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) map[string]string {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing, ok := b.tagsStore[resourceARN]
	if !ok {
		return make(map[string]string)
	}

	out := make(map[string]string, len(existing))
	maps.Copy(out, existing)

	return out
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingTransfer).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every Transfer Family resource ARN (servers,
// users, connectors, certificates, profiles, web apps, workflows,
// agreements, host keys) that currently has at least one tag applied via
// TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tagsStore))

	for arn, t := range b.tagsStore {
		if len(t) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: arn, Tags: maps.Clone(t)})
	}

	return out
}

// initTagsStore seeds tagsStore[resourceARN] with creation-time tags so that
// ListTagsForResource returns them even before any TagResource call.
// Caller must hold b.mu (write lock).
func (b *InMemoryBackend) initTagsStore(resourceARN string, tags map[string]string) {
	if len(tags) == 0 {
		return
	}

	if _, ok := b.tagsStore[resourceARN]; !ok {
		b.tagsStore[resourceARN] = make(map[string]string, len(tags))
	}

	maps.Copy(b.tagsStore[resourceARN], tags)
}

const (
	scopedIDCount    = 2
	arnFieldCount    = 6
	arnResourceField = 5
)

// resourceExistsLocked reports whether resourceARN names a live Transfer Family resource.
func (b *InMemoryBackend) resourceExistsLocked(resourceARN string) bool {
	parts := strings.SplitN(resourceARN, ":", arnFieldCount)
	if len(parts) < arnFieldCount || parts[2] != "transfer" {
		return false
	}

	seg := strings.Split(parts[arnResourceField], "/")
	kind, ids := seg[0], seg[1:]

	if kind == "server" && len(ids) == 3 && ids[1] == "agreement" {
		return b.agreements.Has(agreementKey(ids[0], ids[2]))
	}

	switch {
	case len(ids) == 1:
		return b.singleIDExists(kind, ids[0])
	case len(ids) == scopedIDCount:
		return b.scopedIDExists(kind, ids[0], ids[1])
	default:
		return false
	}
}

func (b *InMemoryBackend) singleIDExists(kind, id string) bool {
	switch kind {
	case "server":
		return b.servers.Has(id)
	case "connector":
		return b.connectors.Has(id)
	case "profile":
		return b.profiles.Has(id)
	case "webapp":
		return b.webApps.Has(id)
	case "workflow":
		return b.workflows.Has(id)
	case "certificate":
		return b.certificates.Has(id)
	default:
		return false
	}
}

func (b *InMemoryBackend) scopedIDExists(kind, serverID, id string) bool {
	switch kind {
	case "user":
		return b.users.Has(userKey(serverID, id))
	case "host-key":
		return b.hostKeys.Has(hostKeyKey(serverID, id))
	default:
		return false
	}
}
