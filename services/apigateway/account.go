package apigateway

// GetAccount returns the mock API Gateway account settings.
func (b *InMemoryBackend) GetAccount() (*Account, error) {
	b.mu.RLock("GetAccount")
	defer b.mu.RUnlock()

	return b.account, nil
}

// UpdateAccount updates the account's throttle settings.
func (b *InMemoryBackend) UpdateAccount(input UpdateAccountInput) (*Account, error) {
	b.mu.Lock("UpdateAccount")
	defer b.mu.Unlock()

	if input.ThrottleSettings != nil {
		b.account.ThrottleSettings = input.ThrottleSettings
	}
	if input.CloudwatchRoleARN != "" {
		b.account.CloudwatchRoleARN = input.CloudwatchRoleARN
	}

	return b.account, nil
}
