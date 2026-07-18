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

	return simOTP, nil
}

// AcceptPrimaryEmailUpdate confirms a pending email change using the OTP.
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

	return nil
}

// PutAccountName updates the account's display name.
func (b *InMemoryBackend) PutAccountName(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.accountName = name

	return nil
}
