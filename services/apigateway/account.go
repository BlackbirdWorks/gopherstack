package apigateway

// GetAccount returns the mock API Gateway account settings.
func (b *InMemoryBackend) GetAccount() (*Account, error) {
	b.mu.RLock("GetAccount")
	defer b.mu.RUnlock()

	cp := *b.account

	return &cp, nil
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
	if input.Features != nil {
		b.account.Features = input.Features
	}

	cp := *b.account

	return &cp, nil
}
