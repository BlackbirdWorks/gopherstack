package ocidatabase

// Idle returns once every background operation started so far has finished.
func (b *InMemoryBackend) Idle() { b.wg.Wait() }
