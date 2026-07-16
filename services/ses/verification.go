package ses

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// VerifyDomainIdentity adds a domain as a verified identity, returning a deterministic verification token.
func (b *InMemoryBackend) VerifyDomainIdentity(domain string) (string, error) {
	if strings.TrimSpace(domain) == "" {
		return "", fmt.Errorf("%w: Domain is required", ErrInvalidParameter)
	}

	b.mu.Lock("VerifyDomainIdentity")
	defer b.mu.Unlock()

	if rec, ok := b.identities.Get(domain); ok {
		rec.Verified = true
	} else {
		b.identities.Put(&IdentityRecord{Identity: domain, Verified: true, ForwardingEnabled: true})
	}

	h := sha256.Sum256([]byte("domain-token:" + domain))

	return hex.EncodeToString(h[:])[:32], nil
}

// VerifyEmailAddress is an alias for VerifyEmailIdentity (legacy API).
func (b *InMemoryBackend) VerifyEmailAddress(email string) error {
	return b.VerifyEmailIdentity(email)
}

// DeleteVerifiedEmailAddress removes a verified email address (legacy API).
func (b *InMemoryBackend) DeleteVerifiedEmailAddress(email string) {
	b.DeleteIdentity(email)
}

// ListVerifiedEmailAddresses returns all verified identities that are email addresses (contain @).
func (b *InMemoryBackend) ListVerifiedEmailAddresses() []string {
	b.mu.RLock("ListVerifiedEmailAddresses")
	defer b.mu.RUnlock()

	var out []string

	for _, rec := range b.identities.All() {
		if rec.Verified && strings.Contains(rec.Identity, "@") {
			out = append(out, rec.Identity)
		}
	}

	sort.Strings(out)

	return out
}
