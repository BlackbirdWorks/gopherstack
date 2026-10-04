package iot

import "fmt"

const (
	tokenKindAuditSuppression = "auditSuppression"
	tokenKindAuditMitigation  = "auditMitigationTask"
	tokenKindDetectMitigation = "detectMitigationTask"
)

func clientTokenKey(kind, token string) string { return kind + "|" + token }

// claimClientTokenLocked records token for resourceKey, or returns err when another resource already holds it.
func (b *InMemoryBackend) claimClientTokenLocked(kind, token, resourceKey string, err error) error {
	if token == "" {
		return nil
	}

	k := clientTokenKey(kind, token)
	if owner, taken := b.clientRequestTokens[k]; taken {
		return fmt.Errorf("client request token %q is already used by %q: %w", token, owner, err)
	}

	b.clientRequestTokens[k] = resourceKey

	return nil
}

// releaseClientTokensLocked drops every token held by resourceKey of kind.
func (b *InMemoryBackend) releaseClientTokensLocked(kind, resourceKey string) {
	for k, owner := range b.clientRequestTokens {
		if owner == resourceKey && len(k) > len(kind) && k[:len(kind)+1] == kind+"|" {
			delete(b.clientRequestTokens, k)
		}
	}
}
