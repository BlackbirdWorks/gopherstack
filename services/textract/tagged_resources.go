package textract

import "maps"

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every adapter and adapter version of region that carries at least one tag.
func (b *InMemoryBackend) TaggedResources(region string) []TaggedEntry {
	return b.entries(region, true)
}

// InventoryResources lists every adapter and adapter version of region, tagged or not.
func (b *InMemoryBackend) InventoryResources(region string) []TaggedEntry {
	return b.entries(region, false)
}

func (b *InMemoryBackend) entries(region string, taggedOnly bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, a := range b.adapters.All() {
		if a.Region == region && (!taggedOnly || len(a.Tags) > 0) {
			out = append(out, TaggedEntry{
				ARN:  buildAdapterARN(region, b.accountID, a.AdapterID),
				Tags: maps.Clone(a.Tags),
			})
		}
	}

	for _, v := range b.adapterVersions.All() {
		if v.Region == region && (!taggedOnly || len(v.Tags) > 0) {
			out = append(out, TaggedEntry{
				ARN:  buildAdapterVersionARN(region, b.accountID, v.AdapterID, v.AdapterVersion),
				Tags: maps.Clone(v.Tags),
			})
		}
	}

	return out
}
