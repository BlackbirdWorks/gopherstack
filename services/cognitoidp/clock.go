package cognitoidp

import "time"

// SetClock replaces the wall clock used for MFA sessions, OTP codes, TOTP windows and passkey
// challenges; tests inject a fake instead of sleeping.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clock = now
}

// now is the injected clock, or the wall clock. Caller holds b.mu.
func (b *InMemoryBackend) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}

	return time.Now()
}
