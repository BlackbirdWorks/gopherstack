package azuregraph

// SetSignInHooks observes the password sign-in: burned fires for an account answered with dummy work, verified
// after the hash check and before the backend lock is retaken.
func (b *Backend) SetSignInHooks(burned, verified func()) {
	b.hooks = signInHooks{burned: burned, verified: verified}
}
