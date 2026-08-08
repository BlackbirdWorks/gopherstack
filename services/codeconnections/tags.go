package codeconnections

import (
	"context"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// findResourceTagsLocked returns the tag map for a resource ARN within the given region.
// Must be called with the appropriate lock held.
func (b *InMemoryBackend) findResourceTagsLocked(
	region, resourceArn string,
) (map[string]string, bool) {
	if conn, ok := b.connections.Get(resourceArn); ok && regionFromARN(resourceArn) == region {
		return conn.Tags, true
	}

	if host, ok := b.hosts.Get(resourceArn); ok && regionFromARN(resourceArn) == region {
		return host.Tags, true
	}

	// Repository links are keyed by ID, not ARN; scan by ARN within the region.
	for _, link := range b.repositoryLinksByRegion.Get(region) {
		if link.RepositoryLinkArn == resourceArn {
			return link.Tags, true
		}
	}

	return nil, false
}

// TagResource adds or updates tags on a connection or host.
func (b *InMemoryBackend) TagResource(
	ctx context.Context,
	resourceArn string,
	tags map[string]string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing, ok := b.findResourceTagsLocked(region, resourceArn)
	if !ok {
		return ErrNotFound
	}

	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from a connection or host.
func (b *InMemoryBackend) UntagResource(
	ctx context.Context,
	resourceArn string,
	tagKeys []string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing, ok := b.findResourceTagsLocked(region, resourceArn)
	if !ok {
		return ErrNotFound
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListTagsForResource returns the tags for a connection or host.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	resourceArn string,
) (map[string]string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing, ok := b.findResourceTagsLocked(region, resourceArn)
	if !ok {
		return nil, ErrNotFound
	}

	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every CodeConnections resource ARN (connections, hosts,
// repository links) that currently has at least one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	b.connections.Range(func(conn *Connection) bool {
		if len(conn.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: conn.ConnectionArn, Tags: cloneCodeConnTags(conn.Tags)})
		}

		return true
	})

	b.hosts.Range(func(host *Host) bool {
		if len(host.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: host.HostArn, Tags: cloneCodeConnTags(host.Tags)})
		}

		return true
	})

	b.repositoryLinks.Range(func(link *RepositoryLink) bool {
		if len(link.Tags) > 0 {
			out = append(out, TaggedEntry{ARN: link.RepositoryLinkArn, Tags: cloneCodeConnTags(link.Tags)})
		}

		return true
	})

	return out
}

func cloneCodeConnTags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out
}

// sortedTagKeys returns the keys of the tags map in sorted order.
func sortedTagKeys(tags map[string]string) []string {
	keys := collections.SortedKeys(tags)

	return keys
}
