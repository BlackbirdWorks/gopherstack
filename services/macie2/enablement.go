package macie2

import (
	"fmt"
	"time"
)

// GetSession returns the current Macie session (may be nil if not enabled).
func (b *InMemoryBackend) GetSession() *Session {
	b.mu.RLock("GetSession")
	defer b.mu.RUnlock()

	return b.session
}

// EnableMacie enables Macie for the account.
func (b *InMemoryBackend) EnableMacie(_, frequency, status string) error {
	b.mu.Lock("EnableMacie")
	defer b.mu.Unlock()

	if b.session != nil && b.session.Enabled {
		return ErrSessionAlreadyExists
	}

	freq := frequency
	if freq == "" {
		freq = defaultFrequency
	}

	st := status
	if st == "" {
		st = statusEnabled
	}

	now := time.Now().UTC()
	b.session = &Session{
		CreatedAt:                  now,
		UpdatedAt:                  now,
		FindingPublishingFrequency: freq,
		ServiceRole: fmt.Sprintf(
			"arn:aws:iam::%s:role/aws-service-role/macie.amazonaws.com/AWSServiceRoleForAmazonMacie",
			b.accountID,
		),
		Status:  st,
		Enabled: true,
	}

	return nil
}

// DisableMacie disables Macie for the account.
func (b *InMemoryBackend) DisableMacie() error {
	b.mu.Lock("DisableMacie")
	defer b.mu.Unlock()

	b.session = nil

	return nil
}

// UpdateMacieSession updates the Macie session configuration.
func (b *InMemoryBackend) UpdateMacieSession(frequency, status string) error {
	b.mu.Lock("UpdateMacieSession")
	defer b.mu.Unlock()

	if b.session == nil {
		return nil
	}

	if frequency != "" {
		b.session.FindingPublishingFrequency = frequency
	}

	if status != "" {
		b.session.Status = status
	}

	b.session.UpdatedAt = time.Now().UTC()

	return nil
}
