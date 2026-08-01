package directconnect

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// TaggedEntry is one tagged resource, used by TaggedResources for the
// resourcegroupstaggingapi integration (cli.go's wireTaggingDirectConnect).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// resolveTaggableLocked resolves resourceArn to the tags.Tags instance of
// whichever of the five taggable resource kinds it names: Connection
// ("dxcon/"), Lag ("dxlag/"), VirtualInterface ("dxvif/"), DirectConnectGateway
// ("dx-gateway/"), or Interconnect (reuses the "dxcon/" marker -- see
// store.go's InterconnectARN doc comment; tried after Connection since IDs
// never collide in practice but Connection is checked first for
// determinism). Callers must hold b.mu (either lock).
func (b *InMemoryBackend) resolveTaggableLocked(resourceArn string) (*tags.Tags, bool) {
	if id, ok := resourceIDFromARN(resourceArn, ":dxcon/"); ok {
		if c, found := b.connections.Get(id); found {
			return c.Tags, true
		}

		if ic, found := b.interconnects.Get(id); found {
			return ic.Tags, true
		}
	}

	if id, ok := resourceIDFromARN(resourceArn, ":dxlag/"); ok {
		if l, found := b.lags.Get(id); found {
			return l.Tags, true
		}
	}

	if id, ok := resourceIDFromARN(resourceArn, ":dxvif/"); ok {
		if v, found := b.virtualInterfaces.Get(id); found {
			return v.Tags, true
		}
	}

	if id, ok := resourceIDFromARN(resourceArn, ":dx-gateway/"); ok {
		if g, found := b.gateways.Get(id); found {
			return g.Tags, true
		}
	}

	return nil, false
}

// TagResource adds or updates tags on the named resource. The
// TooManyTagsException count check re-runs here (in addition to
// handler_static.go's handleTagResource) so it also applies to callers that
// invoke this method directly (e.g. cross-service resourcegroupstaggingapi
// wiring in cli.go), not just the native TagResource wire op.
// DuplicateTagKeysException detection, by contrast, is only meaningful at
// the wire layer against the RAW caller-supplied tag list before it
// collapses to this map -- a Go map can never itself hold a duplicate key,
// so mapKeys(newTags) can never trigger that branch from this call path.
func (b *InMemoryBackend) TagResource(_ context.Context, resourceArn string, newTags map[string]string) error {
	if err := validateNewTags(mapKeys(newTags)); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, ok := b.resolveTaggableLocked(resourceArn)
	if !ok {
		return notFoundError(resourceTaggable, resourceArn)
	}

	t.Merge(newTags)

	return nil
}

// UntagResource removes tags from the named resource.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, ok := b.resolveTaggableLocked(resourceArn)
	if !ok {
		return notFoundError(resourceTaggable, resourceArn)
	}

	t.DeleteKeys(tagKeys)

	return nil
}

// DescribeTags is this service's own native, BATCH-shaped tag-read op --
// unlike the single-ARN ListTagsForResource pattern used by outposts/
// resiliencehub, it takes a list of ARNs in one call (PARITY.md).
func (b *InMemoryBackend) DescribeTags(resourceArns []string) []resourceTagWire {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	out := make([]resourceTagWire, 0, len(resourceArns))

	for _, arnStr := range resourceArns {
		t, ok := b.resolveTaggableLocked(arnStr)
		if !ok {
			continue
		}

		out = append(out, resourceTagWire{ResourceArn: arnStr, Tags: toTagWire(t)})
	}

	return out
}

// appendIfTagged appends a TaggedEntry for (arn, t) to out iff t carries at
// least one tag -- the shared single-branch building block
// TaggedResources uses once per taggable resource kind, so that function
// itself stays a flat sequence of calls rather than five copies of the same
// nil/Len check (keeping its cyclomatic complexity down).
func appendIfTagged(out []TaggedEntry, arn string, t *tags.Tags) []TaggedEntry {
	if t == nil || t.Len() == 0 {
		return out
	}

	return append(out, TaggedEntry{ARN: arn, Tags: t.Clone()})
}

// TaggedResources returns every tagged Connection/Lag/Interconnect/
// VirtualInterface/DirectConnectGateway, for the resourcegroupstaggingapi
// integration.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, c := range b.connections.Snapshot() {
		out = appendIfTagged(out, b.ConnectionARN(c.ConnectionID), c.Tags)
	}

	for _, l := range b.lags.Snapshot() {
		out = appendIfTagged(out, b.LagARN(l.LagID), l.Tags)
	}

	for _, ic := range b.interconnects.Snapshot() {
		out = appendIfTagged(out, b.InterconnectARN(ic.InterconnectID), ic.Tags)
	}

	for _, v := range b.virtualInterfaces.Snapshot() {
		out = appendIfTagged(out, b.VifARN(v.VirtualInterfaceID), v.Tags)
	}

	for _, g := range b.gateways.Snapshot() {
		out = appendIfTagged(out, b.GatewayARN(g.DirectConnectGatewayID), g.Tags)
	}

	return out
}

// mapKeys returns the keys of m, for validateNewTags' duplicate-key check
// (moot for a map -- a Go map cannot itself hold a duplicate key -- but
// TagResource's own exception set includes DuplicateTagKeysException per
// PARITY.md, so this still runs the tag-count check on the same helper).
func mapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return keys
}
