package acm

// TimerCountForTest returns the number of pending auto-validation timers
// currently stored in the backend.
func (b *InMemoryBackend) TimerCountForTest() int {
	b.mu.RLock("TimerCountForTest")
	defer b.mu.RUnlock()

	total := 0
	for _, regionTimers := range b.timers {
		total += len(regionTimers)
	}
	return total
}
