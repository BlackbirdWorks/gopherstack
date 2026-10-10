package textract

import "maps"

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every tagged adapter and adapter version in region.
func (b *InMemoryBackend) TaggedResources(region string) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	var out []TaggedEntry

	for _, a := range b.adapters.All() {
		if a.Region == region && len(a.Tags) > 0 {
			out = append(out, TaggedEntry{
				ARN:  buildAdapterARN(region, b.accountID, a.AdapterID),
				Tags: maps.Clone(a.Tags),
			})
		}
	}

	for _, v := range b.adapterVersions.All() {
		if v.Region == region && len(v.Tags) > 0 {
			out = append(out, TaggedEntry{
				ARN:  buildAdapterVersionARN(region, b.accountID, v.AdapterID, v.AdapterVersion),
				Tags: maps.Clone(v.Tags),
			})
		}
	}

	return out
}
