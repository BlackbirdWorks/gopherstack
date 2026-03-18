package qldbsession

import "time"

// MaxSessionsForTest exposes the maxSessions cap constant for use in tests.
const MaxSessionsForTest = maxSessions

// SessionCount returns the number of sessions currently stored.
// Used only in tests.
func (b *InMemoryBackend) SessionCount() int {
	b.mu.RLock("SessionCount")
	defer b.mu.RUnlock()

	return len(b.sessions)
}

// InjectSessionsForTest fills the sessions map with n synthetic sessions so
// tests can exercise the eviction path without creating maxSessions real sessions.
func (b *InMemoryBackend) InjectSessionsForTest(n int) []string {
	b.mu.Lock("InjectSessionsForTest")
	defer b.mu.Unlock()

	tokens := make([]string, n)

	for i := range tokens {
		token := newSessionToken()
		sess := &Session{
			Token:          token,
			LedgerName:     "test-ledger",
			CreatedAt:      time.Now(),
			TransactionIDs: make(map[string]bool),
		}
		b.sessions[token] = sess
		b.sessionKeys = append(b.sessionKeys, token)
		tokens[i] = token
	}

	return tokens
}
