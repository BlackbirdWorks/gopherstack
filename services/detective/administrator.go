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

	// AWS designates a single Detective administrator account per
	// organization within the current Region -- ListOrganizationAdminAccounts
	// and the underlying Administrator type both describe it in the singular
	// ("Returns information about the Detective administrator account for an
	// organization"). Replace any existing designation instead of
	// accumulating duplicate entries on repeated Enable calls.
	b.orgAdmins = []*storedOrgAdmin{{
		DelegationTime: now,
		AccountID:      accountID,
		GraphARN:       graphARN,
	}}

	return nil
}

// DisableOrganizationAdminAccount removes the Detective administrator account.
// AWS docs: "Removes the Detective administrator account in the current
// Region. Deletes the organization behavior graph." -- so every graph
// referenced by the current org admin(s) is deleted along with its
// dependent state (members, investigations, tags, datasources), not just the
// orgAdmins record.
func (b *InMemoryBackend) DisableOrganizationAdminAccount() error {
	b.mu.Lock("DisableOrganizationAdminAccount")
	defer b.mu.Unlock()

	for _, a := range b.orgAdmins {
		b.deleteGraphLocked(a.GraphARN)
	}

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

	start, err := decodePageToken(nextToken)
	if err != nil {
		return nil, "", err
	}

	if start > len(admins) {
		start = len(admins)
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
		outToken = encodePageToken(end)
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
