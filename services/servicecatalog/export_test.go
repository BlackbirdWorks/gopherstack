package servicecatalog

// LockContended reports whether another goroutine's attempt to take the backend write lock blocks,
// i.e. whether the lock is held (read or write) by the caller's stack.
func (b *InMemoryBackend) LockContended() bool {
	done := make(chan struct{})

	go func() {
		b.mu.Lock("LockContendedProbe")
		b.mu.Unlock()
		close(done)
	}()

	for {
		select {
		case <-done:
			return false
		default:
		}

		if b.mu.WriteWaiters() > 0 {
			return true
		}
	}
}
