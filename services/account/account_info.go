package account

import "time"

// GetAccountInformation returns the account's ID, display name, creation
// date, and lifecycle state. AccountState is always ACTIVE: this service has
// no operation that transitions it (account closure belongs to AWS
// Organizations -- see services/organizations CloseAccount).
func (b *InMemoryBackend) GetAccountInformation() (*Info, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return &Info{
		AccountID:          b.accountID,
		AccountName:        b.accountName,
		AccountCreatedDate: b.accountCreatedDate.Format(time.RFC3339),
		AccountState:       StateActive,
	}, nil
}

// GetPrimaryEmail returns the current primary email address.
func (b *InMemoryBackend) GetPrimaryEmail() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.primaryEmail
}

// StartPrimaryEmailUpdate initiates a primary email change.
// Returns a fixed simulation OTP that the caller must pass to AcceptPrimaryEmailUpdate.
func (b *InMemoryBackend) StartPrimaryEmailUpdate(email string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pendingEmail = email
	b.pendingOTP = simOTP
	b.primaryEmailUpdateStatus = PrimaryEmailUpdateStatusPending
	b.primaryEmailUpdateAt = time.Now().UTC()

	return simOTP, nil
}

// AcceptPrimaryEmailUpdate confirms a pending email change using the OTP.
// Real AWS's AcceptPrimaryEmailUpdateOutput reports Status ACCEPTED
// immediately, then asynchronously transitions to COMPLETED once the change
// propagates; like EnableRegion/DisableRegion (see PARITY.md gaps), this
// simulator does not model that async completion tail -- ACCEPTED is the
// terminal status GetPrimaryEmailUpdateStatus reports here.
func (b *InMemoryBackend) AcceptPrimaryEmailUpdate(otp, email string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.pendingEmail == "" {
		return errNoPendingUpdate
	}

	if otp != b.pendingOTP || email != b.pendingEmail {
		return errInvalidOTP
	}

	b.primaryEmail = b.pendingEmail
	b.pendingEmail = ""
	b.pendingOTP = ""
	b.primaryEmailUpdateStatus = PrimaryEmailUpdateStatusAccepted
	b.primaryEmailUpdateAt = time.Now().UTC()

	return nil
}

// GetPrimaryEmailUpdateStatus returns the status of the most recent primary
// email update request and when it last changed.
func (b *InMemoryBackend) GetPrimaryEmailUpdateStatus() (PrimaryEmailUpdateStatus, time.Time, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.primaryEmailUpdateStatus == "" {
		return "", time.Time{}, errNoPrimaryEmailUpdateStatus
	}

	return b.primaryEmailUpdateStatus, b.primaryEmailUpdateAt, nil
}

// GetGovCloudAccountInformation returns the GovCloud account linked to this
// account. This backend models a single, standalone (non-organization-member)
// account -- see the AccountId-targeting gap in PARITY.md -- so no account it
// simulates ever has a linked GovCloud pair; matches real AWS's documented
// ResourceNotFoundException for that case.
func (b *InMemoryBackend) GetGovCloudAccountInformation() (string, State, error) {
	return "", "", errGovCloudNotLinked
}

// PutAccountName updates the account's display name.
func (b *InMemoryBackend) PutAccountName(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.accountName = name

	return nil
}
