package organizations

import (
	"cmp"
	"slices"
	"time"
)

// RegisterDelegatedAdministrator registers a delegated admin for a service.
func (b *InMemoryBackend) RegisterDelegatedAdministrator(accountID, servicePrincipal string) error {
	b.mu.Lock("RegisterDelegatedAdministrator")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	// Reject management account.
	if accountID == b.org.MasterAccountID {
		return ErrInvalidInput
	}

	// Require service access to be enabled first.
	if !b.serviceAccess.Has(servicePrincipal) {
		return ErrServiceNotEnabled
	}

	acct, ok := b.accounts.Get(accountID)
	if !ok {
		return ErrAccountNotFound
	}

	if b.delegatedAdmins.Has(delegatedAdminKey(servicePrincipal, accountID)) {
		return ErrDelegatedAdminAlreadyExists
	}

	b.delegatedAdmins.Put(&DelegatedAdmin{
		AccountID:        accountID,
		ARN:              acct.ARN,
		Name:             acct.Name,
		Email:            acct.Email,
		Status:           accountStatusActive,
		JoinedMethod:     acct.JoinedMethod,
		JoinedAt:         acct.JoinedAt,
		DelegationTime:   time.Now(),
		ServicePrincipal: servicePrincipal,
	})

	return nil
}

// DeregisterDelegatedAdministrator removes a delegated admin.
func (b *InMemoryBackend) DeregisterDelegatedAdministrator(
	accountID, servicePrincipal string,
) error {
	b.mu.Lock("DeregisterDelegatedAdministrator")
	defer b.mu.Unlock()

	if b.org == nil {
		return ErrOrgNotFound
	}

	key := delegatedAdminKey(servicePrincipal, accountID)
	if !b.delegatedAdmins.Has(key) {
		return ErrDelegatedAdminNotFound
	}

	b.delegatedAdmins.Delete(key)

	return nil
}

// ListDelegatedAdministrators lists delegated admins, optionally filtered by service principal.
func (b *InMemoryBackend) ListDelegatedAdministrators(
	servicePrincipal string,
) ([]*DelegatedAdmin, error) {
	b.mu.RLock("ListDelegatedAdministrators")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	var out []*DelegatedAdmin

	if servicePrincipal != "" {
		out = append(out, b.delegatedAdminsByService.Get(servicePrincipal)...)
	} else {
		out = b.delegatedAdmins.All()
	}

	slices.SortFunc(
		out,
		func(a, b *DelegatedAdmin) int { return cmp.Compare(a.AccountID, b.AccountID) },
	)

	return out, nil
}

// ListDelegatedServicesForAccount returns service principals for which an account is a delegated admin.
func (b *InMemoryBackend) ListDelegatedServicesForAccount(
	accountID string,
) ([]DelegatedService, error) {
	b.mu.RLock("ListDelegatedServicesForAccount")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	if !b.accounts.Has(accountID) {
		return nil, ErrAccountNotFound
	}

	var out []DelegatedService

	for _, da := range b.delegatedAdminsByAccount.Get(accountID) {
		out = append(out, DelegatedService{
			ServicePrincipal:      da.ServicePrincipal,
			DelegationEnabledDate: da.DelegationTime,
		})
	}

	slices.SortFunc(
		out,
		func(a, b DelegatedService) int { return cmp.Compare(a.ServicePrincipal, b.ServicePrincipal) },
	)

	return out, nil
}
