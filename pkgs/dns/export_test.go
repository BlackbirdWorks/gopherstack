package dns

// IsSelfAddr exposes isSelfAddr for tests.
func IsSelfAddr(upstream, self string) bool { return isSelfAddr(upstream, self) }
