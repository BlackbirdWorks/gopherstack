package macie2

import (
	"fmt"

	"github.com/google/uuid"
)

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

const (
	retrievalModeAssumeRole = "ASSUME_ROLE"
	retrievalModeCaller     = "CALLER_CREDENTIALS"
)

// GetRetrievalConfiguration returns the sensitive-data retrieval settings; CALLER_CREDENTIALS until configured.
func (b *InMemoryBackend) GetRetrievalConfiguration() *RetrievalConfiguration {
	b.mu.RLock("GetRetrievalConfiguration")
	defer b.mu.RUnlock()

	if b.retrievalConfig == nil {
		return &RetrievalConfiguration{RetrievalMode: retrievalModeCaller}
	}

	cp := *b.retrievalConfig

	return &cp
}

// UpdateRetrievalConfiguration applies the retrieval mode; the external ID is generated on first ASSUME_ROLE.
func (b *InMemoryBackend) UpdateRetrievalConfiguration(mode, roleName string) (*RetrievalConfiguration, error) {
	switch mode {
	case retrievalModeAssumeRole:
		if roleName == "" {
			return nil, fmt.Errorf("%w: roleName is required when retrievalMode is ASSUME_ROLE", ErrValidation)
		}
	case retrievalModeCaller:
		roleName = ""
	default:
		return nil, fmt.Errorf("%w: retrievalMode must be ASSUME_ROLE or CALLER_CREDENTIALS", ErrValidation)
	}

	b.mu.Lock("UpdateRetrievalConfiguration")
	defer b.mu.Unlock()

	next := &RetrievalConfiguration{RetrievalMode: mode, RoleName: roleName}

	if mode == retrievalModeAssumeRole {
		next.ExternalID = uuid.NewString()

		if b.retrievalConfig != nil && b.retrievalConfig.ExternalID != "" {
			next.ExternalID = b.retrievalConfig.ExternalID
		}
	}

	b.retrievalConfig = next
	cp := *next

	return &cp, nil
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
		return nil, ErrRevealNotClassification
	}

	if b.session == nil || !b.session.Enabled || b.revealConfig == nil || b.revealConfig.Status != statusEnabled {
		return nil, ErrRevealNotEnabled
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
