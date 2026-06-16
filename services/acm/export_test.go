package acm

// TimerCountForTest returns the number of pending auto-validation timers
// currently stored in the backend.
func (b *InMemoryBackend) TimerCountForTest() int {
	b.mu.RLock("TimerCountForTest")
	defer b.mu.RUnlock()

	return len(b.timers)
}
