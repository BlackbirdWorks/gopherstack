package guardduty

// GetAdministratorAccount returns the administrator account for a detector.
func (b *InMemoryBackend) GetAdministratorAccount(detectorID string) (*AdminAccount, error) {
	b.mu.RLock("GetAdministratorAccount")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	a, ok := b.adminAccounts.Get(detectorID)
	if !ok {
		return &AdminAccount{}, nil
	}

	cp := *a

	return &cp, nil
}

// GetMasterAccount returns the legacy master account for a detector.
func (b *InMemoryBackend) GetMasterAccount(detectorID string) (*AdminAccount, error) {
	b.mu.RLock("GetMasterAccount")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	a, ok := b.adminAccounts.Get(detectorID)
	if !ok {
		return &AdminAccount{}, nil
	}

	cp := *a

	return &cp, nil
}

// DisassociateFromAdministratorAccount removes the administrator relationship.
func (b *InMemoryBackend) DisassociateFromAdministratorAccount(detectorID string) error {
	b.mu.Lock("DisassociateFromAdministratorAccount")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	b.adminAccounts.Delete(detectorID)

	return nil
}

// DisassociateFromMasterAccount removes the legacy master relationship.
func (b *InMemoryBackend) DisassociateFromMasterAccount(detectorID string) error {
	b.mu.Lock("DisassociateFromMasterAccount")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	b.adminAccounts.Delete(detectorID)

	return nil
}
