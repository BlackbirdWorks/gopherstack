package azurefunctions

// BeginRequest exposes beginRequest to the external test package.
func (b *InMemoryBackend) BeginRequest(name string) func() { return b.beginRequest(name) }
