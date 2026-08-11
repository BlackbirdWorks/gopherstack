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

// resolveQuoteLocked resolves idOrARN (a Quote ID, or an
// "arn:...:quote/<id>"-shaped identifier) to its Quote. Quote itself has no
// QuoteArn output field, but GetQuote/UpdateQuote/DeleteQuote's
// QuoteIdentifier and CreateOrder's QuoteIdentifier both document (and their
// Pattern regexes confirm) an optional ARN-shaped input form -- see
// store.go's newQuoteID doc comment. Callers must hold b.mu.
func (b *InMemoryBackend) resolveQuoteLocked(idOrARN string) (*Quote, bool) {
	if id, ok := resourceIDFromARN(idOrARN, ":quote/"); ok {
		return b.quotes.Get(id)
	}

	return b.quotes.Get(idOrARN)
}
