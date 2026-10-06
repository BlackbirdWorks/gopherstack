package iot

import "fmt"

const (
	tokenKindAuditSuppression = "auditSuppression"
	tokenKindAuditMitigation  = "auditMitigationTask"
	tokenKindDetectMitigation = "detectMitigationTask"
	tokenKindPackage          = "package"
	tokenKindPackageVersion   = "packageVersion"
	tokenKindCertProvider     = "certificateProvider"
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

// CheckCreateToken reports whether token already created resourceKey (a replay); another owner is an error.
func (b *InMemoryBackend) CheckCreateToken(kind, token, resourceKey string) (bool, error) {
	if token == "" {
		return false, nil
	}

	b.mu.RLock("CheckCreateToken")
	defer b.mu.RUnlock()

	owner, taken := b.clientRequestTokens[clientTokenKey(kind, token)]
	switch {
	case !taken:
		return false, nil
	case owner == resourceKey:
		return true, nil
	default:
		return false, fmt.Errorf("client request token %q is already used by %q: %w", token, owner, ErrAlreadyExists)
	}
}

// RecordCreateToken remembers that token created resourceKey; the entry dies with the resource.
func (b *InMemoryBackend) RecordCreateToken(kind, token, resourceKey string) {
	if token == "" {
		return
	}

	b.mu.Lock("RecordCreateToken")
	defer b.mu.Unlock()

	b.clientRequestTokens[clientTokenKey(kind, token)] = resourceKey
}
