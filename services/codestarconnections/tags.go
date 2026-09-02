package codestarconnections

import (
	"context"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// sortedTagKeys returns the keys of the tags map in sorted order for deterministic output.
func sortedTagKeys(tags map[string]string) []string {
	keys := collections.SortedKeys(tags)

	return keys
}

// findResourceTagsLocked returns the tag map for a resource ARN. Connections,
// hosts, and repository links are all real taggable resources (see
// RepositoryLink.Tags doc comment in models.go for why repository links are
// included here). Must be called with at least an RLock held.
func (b *InMemoryBackend) findResourceTagsLocked(resourceArn string) (map[string]string, bool) {
	if conn, ok := b.connections.Get(resourceArn); ok {
		return conn.Tags, true
	}

	if host, ok := b.hosts.Get(resourceArn); ok {
		return host.Tags, true
	}

	if links := b.repositoryLinksByArn.Get(resourceArn); len(links) > 0 {
		return links[0].Tags, true
	}

	return nil, false
}

// ensureTagsLocked returns a non-nil tag map for the resource, initialising it when nil.
// Must be called with a write lock held.
func (b *InMemoryBackend) ensureTagsLocked(resourceArn string) (map[string]string, bool) {
	if conn, ok := b.connections.Get(resourceArn); ok {
		if conn.Tags == nil {
			conn.Tags = make(map[string]string)
		}

		return conn.Tags, true
	}

	if host, ok := b.hosts.Get(resourceArn); ok {
		if host.Tags == nil {
			host.Tags = make(map[string]string)
		}

		return host.Tags, true
	}

	if links := b.repositoryLinksByArn.Get(resourceArn); len(links) > 0 {
		link := links[0]
		if link.Tags == nil {
			link.Tags = make(map[string]string)
		}

		return link.Tags, true
	}

	return nil, false
}

// ListTagsForResource returns the tags for a resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(_ context.Context, resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing, ok := b.findResourceTagsLocked(resourceArn)
	if !ok {
		return nil, fmt.Errorf("%w: resource not found: %s", ErrNotFound, resourceArn)
	}

	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

// TagResource adds or updates tags on a resource.
//
// validateTags's per-key/value checks (empty key, oversized key or value)
// return ErrValidation (InvalidInputException), a code TagResource's own
// switch does not declare ([LimitExceededException, ResourceNotFoundException],
// codestarconnections@v1.38.4 deserializers.go
// deserializeOpErrorTagResource) -- unlike the count-limit check just above
// it in validateTags, which correctly uses ErrTagLimitExceeded
// (LimitExceededException, declared here). No ValidationException
// equivalent exists anywhere in this SDK module, so there is no correct
// code to substitute; recorded, not fixed (gopherstack-6flj/uox6
// error-envelope sweep, found by manual trace beyond errtargetaudit's own
// findings for this op).
func (b *InMemoryBackend) TagResource(_ context.Context, resourceArn string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing, ok := b.ensureTagsLocked(resourceArn)
	if !ok {
		return fmt.Errorf("%w: resource not found: %s", ErrNotFound, resourceArn)
	}

	// Check total count after applying new tags.
	merged := make(map[string]string, len(existing)+len(tags))
	maps.Copy(merged, existing)
	maps.Copy(merged, tags)

	if len(merged) > maxTagsPerResource {
		return fmt.Errorf("%w: cannot have more than %d tags on a resource", ErrTagLimitExceeded, maxTagsPerResource)
	}

	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing, ok := b.findResourceTagsLocked(resourceArn)
	if !ok {
		return fmt.Errorf("%w: resource not found: %s", ErrNotFound, resourceArn)
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}
