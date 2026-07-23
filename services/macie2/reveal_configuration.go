package macie2

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

// GetRevealConfiguration returns the sensitive data reveal configuration.
func (b *InMemoryBackend) GetRevealConfiguration() (*RevealConfiguration, error) {
	b.mu.RLock("GetRevealConfiguration")
	defer b.mu.RUnlock()

	if b.revealConfig == nil {
		return &RevealConfiguration{Status: statusDisabled}, nil
	}

	cp := *b.revealConfig

	return &cp, nil
}

// UpdateRevealConfiguration stores the reveal configuration.
func (b *InMemoryBackend) UpdateRevealConfiguration(kmsKeyID, status string) error {
	b.mu.Lock("UpdateRevealConfiguration")
	defer b.mu.Unlock()

	b.revealConfig = &RevealConfiguration{
		KmsKeyID: kmsKeyID,
		Status:   status,
	}

	return nil
}

// GetSensitiveDataOccurrences returns redacted occurrences for a finding.
func (b *InMemoryBackend) GetSensitiveDataOccurrences(findingID string) (map[string]any, error) {
	b.mu.RLock("GetSensitiveDataOccurrences")
	defer b.mu.RUnlock()

	finding, ok := b.findings.Get(findingID)
	if !ok {
		return nil, ErrFindingNotFound
	}

	if finding.Category != categoryClassification {
		return nil, awserr.New("UnprocessableEntityException", awserr.ErrInvalidParameter)
	}

	if b.session == nil || !b.session.Enabled || b.revealConfig == nil || b.revealConfig.Status != statusEnabled {
		return nil, awserr.New("AccessDeniedException", awserr.ErrInvalidParameter)
	}

	return map[string]any{
		"sensitiveDataOccurrences": map[string]any{
			"EMAIL_ADDRESS": []map[string]any{
				{"value": "test@example.com"},
			},
		},
		"status": "SUCCESS",
	}, nil
}

// GetSensitiveDataOccurrencesAvailability reports reveal availability for a finding.
func (b *InMemoryBackend) GetSensitiveDataOccurrencesAvailability(findingID string) (string, []string, error) {
	b.mu.RLock("GetSensitiveDataOccurrencesAvailability")
	defer b.mu.RUnlock()

	finding, ok := b.findings.Get(findingID)
	if !ok {
		return "", nil, ErrFindingNotFound
	}

	if finding.Category != categoryClassification {
		return "UNAVAILABLE", []string{"INVALID_CLASSIFICATION_RESULT"}, nil
	}

	if b.session == nil || !b.session.Enabled || b.revealConfig == nil || b.revealConfig.Status != statusEnabled {
		return "UNAVAILABLE", nil, nil
	}

	return "AVAILABLE", nil, nil
}
