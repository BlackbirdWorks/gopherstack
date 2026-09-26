package dms

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// AddTagsToResource adds tags to a DMS resource by ARN.
func (b *InMemoryBackend) AddTagsToResource(ctx context.Context, resourceArn string, kv map[string]string) error {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	t := b.findResourceTags(getRegion(ctx, b.region), resourceArn)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
	}

	t.Merge(kv)

	return nil
}

// ListTagsForResource returns tags for a DMS resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t := b.findResourceTags(getRegion(ctx, b.region), resourceArn)
	if t == nil {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
	}

	return t.Clone(), nil
}

// findResourceTags returns the Tags for a resource ARN within the given region
// (must hold a lock). Returns nil if not found.
func (b *InMemoryBackend) findResourceTags(region, resourceArn string) *tags.Tags {
	if ri, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, resourceArn)); ok {
		return ri.Tags
	}

	if ep, ok := lookupUnique(b.endpointsByARN, regionKey(region, resourceArn)); ok {
		return ep.Tags
	}

	if rt, ok := lookupUnique(b.replicationTasksByARN, regionKey(region, resourceArn)); ok {
		return rt.Tags
	}

	if dm, ok := lookupUnique(b.dataMigrationsByARN, regionKey(region, resourceArn)); ok {
		return dm.Tags
	}

	if dp, ok := lookupUnique(b.dataProvidersByARN, regionKey(region, resourceArn)); ok {
		return dp.Tags
	}

	if ip, ok := lookupUnique(b.instanceProfilesByARN, regionKey(region, resourceArn)); ok {
		return ip.Tags
	}

	if mp, ok := lookupUnique(b.migrationProjectsByARN, regionKey(region, resourceArn)); ok {
		return mp.Tags
	}

	if sg, ok := lookupUnique(b.replicationSubnetGroupsByARN, regionKey(region, resourceArn)); ok {
		return sg.Tags
	}

	if rc, ok := lookupUnique(b.replicationConfigsByARN, regionKey(region, resourceArn)); ok {
		return rc.Tags
	}

	if cert, ok := lookupUnique(b.certificatesByARN, regionKey(region, resourceArn)); ok {
		return cert.Tags
	}

	// Fallback: terraform-provider-aws builds the tag-lookup ARN for some DMS
	// resources (e.g. replication subnet groups, event subscriptions) using
	// its own notion of account id, which is empty under
	// skip_requesting_account_id=true and so can differ from the account id
	// this emulator embeds in ARNs it hands back from Create/Describe --
	// same "type:id" suffix, different (or missing) account segment. Resolve
	// by that suffix instead of failing the whole lookup on a segment the
	// caller never got from us in the first place.
	return b.findResourceTagsBySuffix(region, resourceArn)
}

// findResourceTagsBySuffix resolves resourceArn by its "type:id" suffix
// alone, ignoring the account-id segment. See ListTagsForResource's fallback
// comment for why this is needed. Must be called with b.mu held.
func (b *InMemoryBackend) findResourceTagsBySuffix(region, resourceArn string) *tags.Tags {
	suffix, hasSuffix := arnResourceSuffix(resourceArn)
	if !hasSuffix {
		return nil
	}

	if sgID, isSubnetGroup := strings.CutPrefix(suffix, "subgrp:"); isSubnetGroup {
		sg, found := b.replicationSubnetGroups.Get(regionKey(region, sgID))
		if !found {
			return nil
		}

		return sg.Tags
	}

	if esID, isEventSub := strings.CutPrefix(suffix, "es:"); isEventSub {
		es, found := b.eventSubscriptions.Get(regionKey(region, esID))
		if !found {
			return nil
		}

		return es.Tags
	}

	return nil
}

// arnResourceSuffix returns the "<type>:<id>" (or "<type>/<id>") suffix of a
// 6-field ARN (arn:partition:service:region:account:resource) -- the part
// after the account-id field -- or "", false if resourceArn doesn't have
// that shape.
func arnResourceSuffix(resourceArn string) (string, bool) {
	parts := strings.SplitN(resourceArn, ":", 6) //nolint:mnd // arn:partition:service:region:account:resource
	if len(parts) != 6 {                         //nolint:mnd // same
		return "", false
	}

	return parts[5], true
}

// RemoveTagsFromResource removes tags from a DMS resource by ARN.
func (b *InMemoryBackend) RemoveTagsFromResource(ctx context.Context, resourceArn string, tagKeys []string) error {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	t := b.findResourceTags(getRegion(ctx, b.region), resourceArn)
	if t == nil {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceArn)
	}

	t.DeleteKeys(tagKeys)

	return nil
}
