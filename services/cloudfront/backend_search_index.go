package cloudfront

// This file implements an inverted token index over distribution raw configs so
// that the ListDistributionsBy* control-plane operations resolve in O(k) (k =
// number of distributions referencing the queried token) instead of scanning
// every distribution's raw config on every call. It also removes the substring
// false positives of the previous strings.Contains scan: a query for an ID or
// ARN now matches only when that value appears as a whole config token.
//
// A "token" is a maximal run of characters that can appear in a CloudFront ID or
// ARN: letters, digits, and the punctuation used by IDs/ARNs (":", "/", ".",
// "_", "-"). XML angle brackets, whitespace, and quotes are treated as
// separators. Because every value passed to ListDistributionsBy* (key group IDs,
// VPC origin IDs, function IDs, anycast IDs, trust store IDs, connection modes,
// and resource ARNs) is itself a complete token, token membership reproduces the
// intended matches while dropping spurious partial-substring hits.

// isTokenByte reports whether c can be part of an ID/ARN token.
func isTokenByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z':
		return true
	case c >= 'A' && c <= 'Z':
		return true
	case c >= '0' && c <= '9':
		return true
	case c == ':' || c == '/' || c == '.' || c == '_' || c == '-':
		return true
	default:
		return false
	}
}

// tokenizeConfig extracts the set of distinct tokens from a raw distribution
// config. The result is a set so repeated tokens collapse to a single entry.
func tokenizeConfig(raw []byte) map[string]struct{} {
	tokens := make(map[string]struct{})

	start := -1
	for i := range raw {
		if isTokenByte(raw[i]) {
			if start < 0 {
				start = i
			}

			continue
		}

		if start >= 0 {
			tokens[string(raw[start:i])] = struct{}{}
			start = -1
		}
	}

	if start >= 0 {
		tokens[string(raw[start:])] = struct{}{}
	}

	return tokens
}

// indexDistributionConfig adds the tokens of distID's raw config to the inverted
// index. Must be called with the write lock held.
func (b *InMemoryBackend) indexDistributionConfig(distID string, raw []byte) {
	tokens := tokenizeConfig(raw)
	b.distSearchTokens[distID] = tokens

	for tok := range tokens {
		set := b.distSearchInverted[tok]
		if set == nil {
			set = make(map[string]struct{})
			b.distSearchInverted[tok] = set
		}

		set[distID] = struct{}{}
	}
}

// deindexDistributionConfig removes distID from the inverted index. Must be
// called with the write lock held.
func (b *InMemoryBackend) deindexDistributionConfig(distID string) {
	for tok := range b.distSearchTokens[distID] {
		set := b.distSearchInverted[tok]
		if set == nil {
			continue
		}

		delete(set, distID)
		if len(set) == 0 {
			delete(b.distSearchInverted, tok)
		}
	}

	delete(b.distSearchTokens, distID)
}

// reindexDistributionConfig replaces the index entry for distID. Must be called
// with the write lock held.
func (b *InMemoryBackend) reindexDistributionConfig(distID string, raw []byte) {
	b.deindexDistributionConfig(distID)
	b.indexDistributionConfig(distID, raw)
}

// rebuildDistributionSearchIndex discards and rebuilds the index from the
// authoritative distributions map. Used after a bulk Restore. Must be called
// with the write lock held.
func (b *InMemoryBackend) rebuildDistributionSearchIndex() {
	b.distSearchTokens = make(map[string]map[string]struct{})
	b.distSearchInverted = make(map[string]map[string]struct{})

	for id, d := range b.distributions {
		b.indexDistributionConfig(id, d.RawConfig)
	}
}

// tokenReferencedByAnyDistribution reports whether searchStr appears as a whole
// token in any indexed distribution's raw config. Used by Delete* methods to
// enforce AWS's "cannot delete a resource still in use" rule (e.g.
// CachePolicyInUse, FunctionInUse) without an O(n) scan over every distribution.
// Must be called with the backend's lock already held (read or write).
func (b *InMemoryBackend) tokenReferencedByAnyDistribution(searchStr string) bool {
	return len(b.distSearchInverted[searchStr]) > 0
}

// distributionsByConfigSearch returns copies of the distributions whose raw
// config contains searchStr as a whole token. Must be called with the read lock
// held.
func (b *InMemoryBackend) distributionsByConfigSearch(searchStr string) []*Distribution {
	ids := b.distSearchInverted[searchStr]
	if len(ids) == 0 {
		return nil
	}

	out := make([]*Distribution, 0, len(ids))
	for id := range ids {
		if d, ok := b.distributions[id]; ok {
			out = append(out, b.copyDistribution(d))
		}
	}

	return out
}
