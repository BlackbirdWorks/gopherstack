package codebuild

// ImportSourceCredentials imports source credentials and returns the ARN.
func (b *InMemoryBackend) ImportSourceCredentials(authType, serverType, token string) (string, error) {
	b.mu.Lock("ImportSourceCredentials")
	defer b.mu.Unlock()

	_ = token
	arnStr := "arn:aws:codebuild:" + b.region + ":" + b.accountID + ":token/" + serverType
	b.sourceCredentials.Put(&SourceCredentials{
		Arn:        arnStr,
		ServerType: serverType,
		AuthType:   authType,
	})

	return arnStr, nil
}

// DeleteSourceCredentials removes source credentials by ARN.
func (b *InMemoryBackend) DeleteSourceCredentials(arnStr string) error {
	b.mu.Lock("DeleteSourceCredentials")
	defer b.mu.Unlock()

	if !b.sourceCredentials.Delete(arnStr) {
		return ErrNotFound
	}

	return nil
}

// ListSourceCredentials returns all stored source credentials.
func (b *InMemoryBackend) ListSourceCredentials() []*SourceCredentials {
	b.mu.RLock("ListSourceCredentials")
	defer b.mu.RUnlock()

	items := b.sourceCredentials.All()
	result := make([]*SourceCredentials, 0, len(items))

	for _, sc := range items {
		out := *sc
		result = append(result, &out)
	}

	return result
}
