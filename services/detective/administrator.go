package detective

import (
	"fmt"
	"time"
)

// EnableOrganizationAdminAccount designates a Detective administrator account.
func (b *InMemoryBackend) EnableOrganizationAdminAccount(accountID string) error {
	if !validateAccountID(accountID) {
		return fmt.Errorf("%w: accountId must be a 12-digit number", ErrValidation)
	}

	b.mu.Lock("EnableOrganizationAdminAccount")
	defer b.mu.Unlock()

	var graphARN string
	if all := b.graphs.All(); len(all) > 0 {
		graphARN = all[0].Arn
	} else {
		// AWS: "If the account does not have Detective enabled, then enables
		// Detective for that account and creates a new behavior graph."
		graphARN = b.createGraphLocked(nil).Arn
	}

	now := time.Now().UTC()
	b.orgAdmins = append(b.orgAdmins, &storedOrgAdmin{
		DelegationTime: now,
		AccountID:      accountID,
		GraphARN:       graphARN,
	})

	return nil
}

// DisableOrganizationAdminAccount removes the Detective administrator account.
func (b *InMemoryBackend) DisableOrganizationAdminAccount() error {
	b.mu.Lock("DisableOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.orgAdmins = nil

	return nil
}

// ListOrganizationAdminAccounts returns Detective organization administrator accounts.
func (b *InMemoryBackend) ListOrganizationAdminAccounts(
	maxResults int32,
	nextToken string,
) ([]*OrgAdmin, string, error) {
	b.mu.RLock("ListOrganizationAdminAccounts")
	defer b.mu.RUnlock()

	admins := b.orgAdmins
	start := 0
	if nextToken != "" {
		for i, a := range admins {
			if a.AccountID == nextToken {
				start = i

				break
			}
		}
	}

	limit := int(maxResults)
	if limit <= 0 || limit > maxOrgAdminsPerPage {
		limit = maxOrgAdminsPerPage
	}

	end := min(start+limit, len(admins))

	result := make([]*OrgAdmin, 0, end-start)
	for _, a := range admins[start:end] {
		cp := OrgAdmin{
			DelegationTime: a.DelegationTime,
			AccountID:      a.AccountID,
			GraphARN:       a.GraphARN,
		}
		result = append(result, &cp)
	}

	var outToken string
	if end < len(admins) {
		outToken = admins[end].AccountID
	}

	return result, outToken, nil
}

// StartMonitoringMember enables monitoring for a member in ACCEPTED_BUT_DISABLED state.
func (b *InMemoryBackend) StartMonitoringMember(graphARN, accountID string) error {
	b.mu.Lock("StartMonitoringMember")
	defer b.mu.Unlock()

	if !b.graphs.Has(graphARN) {
		return ErrGraphNotFound
	}

	m, ok := b.members.Get(memberKey(graphARN, accountID))
	if !ok {
		return ErrMemberNotFound
	}

	if m.Status != memberStatusAcceptedDisabled {
		return fmt.Errorf("%w: member status must be ACCEPTED_BUT_DISABLED", ErrValidation)
	}

	now := time.Now().UTC()
	m.Status = memberStatusEnabled
	m.UpdatedTime = now

	return nil
}
