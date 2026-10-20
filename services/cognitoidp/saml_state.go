package cognitoidp

import (
	"slices"
	"time"
)

const maxSAMLSeen = 20000

// samlLink ties a hosted session to the SAML login that created it, for single logout.
type samlLink struct {
	provider     string
	nameID       string
	sessionIndex string
}

// claimSAMLAssertion records a message ID until it can no longer be replayed; false means it was
// already seen (or the cache is saturated, which fails closed).
func (b *InMemoryBackend) claimSAMLAssertion(poolID, id string, until time.Time) bool {
	b.mu.Lock("ClaimSAMLAssertion")
	defer b.mu.Unlock()

	now := time.Now()
	key := poolID + "|" + id

	if exp, seen := b.samlSeen[key]; seen && exp.After(now) {
		return false
	}

	if len(b.samlSeen) >= maxSAMLSeen {
		for k, exp := range b.samlSeen {
			if !exp.After(now) {
				delete(b.samlSeen, k)
			}
		}
	}

	if len(b.samlSeen) >= maxSAMLSeen {
		return false
	}

	b.samlSeen[key] = until.Add(samlClockSkew)

	return true
}

// samlProviders returns the pool's SAML providers, or every pool's when poolID is empty.
func (b *InMemoryBackend) samlProviders(poolID string) []*IdentityProvider {
	b.mu.RLock("SAMLProviders")
	defer b.mu.RUnlock()

	var out []*IdentityProvider

	for _, idp := range b.identityProviders.All() {
		if idp.ProviderType == providerTypeSAML && (poolID == "" || idp.UserPoolID == poolID) {
			out = append(out, cloneIdentityProvider(idp))
		}
	}

	return out
}

// samlSessionLink returns the SAML login behind a hosted session cookie value.
func (b *InMemoryBackend) samlSessionLink(sessionID, poolID string) (*samlLink, bool) {
	b.mu.RLock("SAMLSessionLink")
	defer b.mu.RUnlock()

	sess, ok := b.hostedSessions[sessionKey(sessionID)]
	if !ok || sess.SAML == nil || sess.PoolID != poolID || !sess.ExpiresAt.After(time.Now()) {
		return nil, false
	}

	link := *sess.SAML

	return &link, true
}

// endSAMLSessions signs out every user whose SAML login (provider + NameID, narrowed by SessionIndex
// when the IdP names any) matches, and returns how many users were signed out.
func (b *InMemoryBackend) endSAMLSessions(poolID, provider, nameID string, indexes []string) int {
	b.mu.Lock("EndSAMLSessions")
	defer b.mu.Unlock()

	users := map[string]struct{}{}

	for _, s := range b.hostedSessions {
		l := s.SAML
		if s.PoolID != poolID || l == nil || l.provider != provider || l.nameID != nameID {
			continue
		}

		if len(indexes) == 0 || slices.Contains(indexes, l.sessionIndex) {
			users[s.Username] = struct{}{}
		}
	}

	if len(indexes) == 0 {
		b.addLinkedSAMLUserLocked(users, poolID, provider, nameID)
	}

	for u := range users {
		b.signOutUserLocked(poolID, u)
	}

	return len(users)
}

func (b *InMemoryBackend) addLinkedSAMLUserLocked(users map[string]struct{}, poolID, provider, nameID string) {
	if u := b.linkedUserLocked(poolID, provider, nameID); u != nil {
		users[u.Username] = struct{}{}

		return
	}

	if u, ok := b.users.Get(userKey(poolID, provider+"_"+nameID)); ok && u.Status == UserStatusExternalProvider {
		users[u.Username] = struct{}{}
	}
}
