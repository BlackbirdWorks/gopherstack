package outposts

// resolveOutpostLocked resolves idOrARN (an Outpost ID, or a full Outpost
// ARN) to its Outpost. Every Outpost-identifying input field in this API
// accepts either form (per each operation's own doc comment, e.g.
// GetOutpostInput.OutpostId: "The ID or ARN of the Outpost") -- see
// PARITY.md's field-naming-inconsistency note (OutpostId vs
// OutpostIdentifier is a naming difference only, both accept id-or-ARN).
// Callers must hold b.mu.
func (b *InMemoryBackend) resolveOutpostLocked(idOrARN string) (*Outpost, bool) {
	if id, ok := resourceIDFromARN(idOrARN, ":outpost/"); ok {
		return b.outposts.Get(id)
	}

	return b.outposts.Get(idOrARN)
}

// resolveSiteLocked resolves idOrARN (a Site ID, or a full Site ARN) to its
// Site. Callers must hold b.mu.
func (b *InMemoryBackend) resolveSiteLocked(idOrARN string) (*Site, bool) {
	if id, ok := resourceIDFromARN(idOrARN, ":site/"); ok {
		return b.sites.Get(id)
	}

	return b.sites.Get(idOrARN)
}
