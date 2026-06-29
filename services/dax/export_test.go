package dax

func SetClusterAvailableForTest(b *InMemoryBackend, name string) {
	b.mu.Lock("SetClusterAvailableForTest")
	defer b.mu.Unlock()
	if c, ok := b.clusters[name]; ok {
		c.Status = StatusAvailable
	}
}
