package ses

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// dkimTokenCount is the number of DKIM tokens issued per domain, matching AWS SES behavior.
const dkimTokenCount = 3

// dkimTokensForIdentity generates deterministic DKIM tokens for an identity.
// Tokens are stable across calls for the same identity, matching AWS SES Pending→Success flow.
func dkimTokensForIdentity(identity string) []string {
	tokens := make([]string, dkimTokenCount)
	for i := range dkimTokenCount {
		key := fmt.Appendf(nil, "%s:dkim:%d", identity, i)
		h := sha256.Sum256(key)
		tokens[i] = hex.EncodeToString(h[:])[:32]
	}

	return tokens
}

// GetIdentityDkimAttributes returns DKIM attributes for each identity.
// Known identities return their persisted DKIM state; unknown identities return NotStarted.
func (b *InMemoryBackend) GetIdentityDkimAttributes(identities []string) map[string]DkimAttributes {
	b.mu.RLock("GetIdentityDkimAttributes")
	defer b.mu.RUnlock()

	out := make(map[string]DkimAttributes, len(identities))

	for _, id := range identities {
		rec, ok := b.identities.Get(id)
		if !ok {
			out[id] = DkimAttributes{DkimVerificationStatus: identityStatusNotStarted}

			continue
		}

		tokens := rec.DkimTokens
		status := identityStatusNotStarted

		if len(tokens) > 0 {
			status = identityStatusSuccess
		}

		out[id] = DkimAttributes{
			DkimEnabled:            rec.DkimEnabled,
			DkimVerificationStatus: status,
			DkimTokens:             tokens,
		}
	}

	return out
}

// SetIdentityDkimEnabled persists the DKIM-enabled flag for an identity.
func (b *InMemoryBackend) SetIdentityDkimEnabled(identity string, enabled bool) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: Identity is required", ErrInvalidParameter)
	}

	b.mu.Lock("SetIdentityDkimEnabled")
	defer b.mu.Unlock()

	b.getOrCreateIdentityLocked(identity).DkimEnabled = enabled

	return nil
}

// VerifyDomainDkim adds a domain as a verified identity and returns deterministic DKIM tokens.
func (b *InMemoryBackend) VerifyDomainDkim(domain string) ([]string, error) {
	if strings.TrimSpace(domain) == "" {
		return nil, fmt.Errorf("%w: Domain is required", ErrInvalidParameter)
	}

	b.mu.Lock("VerifyDomainDkim")
	defer b.mu.Unlock()

	tokens := dkimTokensForIdentity(domain)

	if rec, ok := b.identities.Get(domain); ok {
		rec.Verified = true
		rec.DkimTokens = tokens
	} else {
		b.identities.Put(&IdentityRecord{
			Identity: domain, Verified: true, ForwardingEnabled: true, DkimTokens: tokens,
		})
	}

	return tokens, nil
}
