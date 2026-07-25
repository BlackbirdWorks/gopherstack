package sagemakerruntime

import "time"

// ExpireSessionForTest forces the named session's ExpiresAt into the past,
// for deterministically testing TouchSession's expiry-driven closure (see
// session_expiry_test.go) without waiting out the real sessionDuration. AWS
// provides no API to force a stateful session's expiry directly -- it is
// entirely model/container-driven -- so this is a test-only backdoor, not a
// simulated SDK operation.
func ExpireSessionForTest(b *InMemoryBackend, sessionID string) {
	b.mu.Lock("ExpireSessionForTest")
	defer b.mu.Unlock()

	if session, ok := b.sessions.Get(sessionID); ok {
		session.ExpiresAt = time.Now().Add(-time.Minute)
	}
}
