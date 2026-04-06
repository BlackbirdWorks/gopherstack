package cognitoidp

// UserPoolCount returns the number of user pools in the backend. For testing only.
func (b *InMemoryBackend) UserPoolCount() int {
	b.mu.RLock("UserPoolCount")
	defer b.mu.RUnlock()

	return len(b.pools)
}

// UserCount returns the total number of users across all pools. For testing only.
func (b *InMemoryBackend) UserCount() int {
	b.mu.RLock("UserCount")
	defer b.mu.RUnlock()

	total := 0
	for _, poolUsers := range b.users {
		total += len(poolUsers)
	}

	return total
}

// ClientCount returns the number of user pool clients. For testing only.
func (b *InMemoryBackend) ClientCount() int {
	b.mu.RLock("ClientCount")
	defer b.mu.RUnlock()

	return len(b.clients)
}
