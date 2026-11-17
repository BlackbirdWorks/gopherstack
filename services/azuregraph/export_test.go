package azuregraph

// SetSignInHooks observes the password sign-in: burned fires for an account answered with dummy work, verified
// after the hash check and before the backend lock is retaken.
func (b *Backend) SetSignInHooks(burned, verified func()) {
	b.hooks = signInHooks{burned: burned, verified: verified}
}

// DeltaSessionCount reports the live delta sessions.
func (b *Backend) DeltaSessionCount() int {
	b.mu.RLock("DeltaSessionCount")
	defer b.mu.RUnlock()

	return len(b.deltaSessions)
}
