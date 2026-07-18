package glacier

// GetDataRetrievalPolicy returns the data retrieval policy for the account.
func (b *InMemoryBackend) GetDataRetrievalPolicy(accountID string) string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.dataRetrievalPolicies[accountID]
}

// SetDataRetrievalPolicy stores the data retrieval policy for the account.
func (b *InMemoryBackend) SetDataRetrievalPolicy(accountID string, policy []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.dataRetrievalPolicies[accountID] = string(policy)
}
