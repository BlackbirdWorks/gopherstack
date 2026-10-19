package azurekeyvault

import "time"

// SetClock overrides the backend clock for tests.
func SetClock(b *InMemoryBackend, now func() time.Time) { b.now = now }

// B64URL exposes the base64url encoder to tests.
func B64URL(b []byte) string { return b64u(b) }

// SweepLifetimeActions runs one janitor pass and returns keys rotated and certs renewed.
func SweepLifetimeActions(b *InMemoryBackend) (int, int) {
	s := b.sweepOnce()

	return s.Rotated, s.Renewed
}
