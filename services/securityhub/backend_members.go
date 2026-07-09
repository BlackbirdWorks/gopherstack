package securityhub

import (
	"fmt"
	"slices"
	"time"
)

// Member represents a Security Hub member account.
type Member struct {
	AccountId       string `json:"AccountId"`       //nolint:revive,staticcheck // existing issue.
	AdministratorId string `json:"AdministratorId"` //nolint:revive,staticcheck // existing issue.
	MasterId        string `json:"MasterId"`        // deprecated alias //nolint:revive,staticcheck // existing issue.
	Email           string `json:"Email"`
	MemberStatus    string `json:"MemberStatus"`
	InvitedAt       string `json:"InvitedAt"`
	UpdatedAt       string `json:"UpdatedAt"`
}

// Invitation represents a pending invitation.
type Invitation struct {
	AccountId    string `json:"AccountId"`    //nolint:revive,staticcheck // existing issue.
	InvitationId string `json:"InvitationId"` //nolint:revive,staticcheck // existing issue.
	InvitedAt    string `json:"InvitedAt"`
	MemberStatus string `json:"MemberStatus"`
}

// AdminAccount represents the administrator account relationship.
type AdminAccount struct {
	AccountId          string `json:"AccountId"`    //nolint:revive,staticcheck // existing issue.
	InvitationId       string `json:"InvitationId"` //nolint:revive,staticcheck // existing issue.
	InvitedAt          string `json:"InvitedAt"`
	RelationshipStatus string `json:"RelationshipStatus"`
}

// OrgConfig represents the organization configuration.
type OrgConfig struct {
	AutoEnableStandards           string `json:"AutoEnableStandards"`
	OrganizationConfigurationType string `json:"OrganizationConfigurationType"`
	AutoEnable                    bool   `json:"AutoEnable"`
	MemberAccountLimitReached     bool   `json:"MemberAccountLimitReached"`
}

// OrgAdminAccount represents an organization admin account.
type OrgAdminAccount struct {
	AccountId string `json:"AccountId"` //nolint:revive,staticcheck // existing issue.
	Status    string `json:"Status"`
}

// --- Member backend methods ---

func (b *InMemoryBackend) CreateMembers(accounts []map[string]any) ([]*Member, []map[string]any) {
	b.mu.Lock("CreateMembers")
	defer b.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)

	var created []*Member
	var unprocessed []map[string]any

	for _, acc := range accounts {
		accountID, _ := acc["AccountId"].(string)
		email, _ := acc["Email"].(string)

		if accountID == "" {
			unprocessed = append(unprocessed, map[string]any{
				"AccountId":     accountID, //nolint:goconst // existing issue.
				keyErrorCode:    errCodeInvalidInput,
				keyErrorMessage: "AccountId is required",
			})

			continue
		}

		if b.members.Has(accountID) {
			unprocessed = append(unprocessed, map[string]any{
				"AccountId":     accountID,
				keyErrorCode:    "ResourceConflictException",
				keyErrorMessage: "Member account already exists",
			})

			continue
		}

		m := &Member{
			AccountId:       accountID,
			AdministratorId: b.accountID,
			MasterId:        b.accountID,
			Email:           email,
			MemberStatus:    "Created",
			UpdatedAt:       now,
		}
		b.members.Put(m)
		created = append(created, m)
	}

	return created, unprocessed
}

func (b *InMemoryBackend) DeleteMembers(accountIDs []string) ([]string, []map[string]any) {
	b.mu.Lock("DeleteMembers")
	defer b.mu.Unlock()

	var deleted []string
	var unprocessed []map[string]any

	for _, id := range accountIDs {
		if b.members.Delete(id) {
			deleted = append(deleted, id)
		} else {
			unprocessed = append(unprocessed, map[string]any{
				"AccountId":     id,
				keyErrorCode:    "ResourceNotFoundException", //nolint:goconst // existing issue.
				keyErrorMessage: "Member not found",
			})
		}
	}

	return deleted, unprocessed
}

func (b *InMemoryBackend) GetMembers(accountIDs []string) ([]*Member, []map[string]any) {
	b.mu.RLock("GetMembers")
	defer b.mu.RUnlock()

	var found []*Member
	var unprocessed []map[string]any

	for _, id := range accountIDs {
		if m, ok := b.members.Get(id); ok {
			cp := *m
			found = append(found, &cp)
		} else {
			unprocessed = append(unprocessed, map[string]any{
				"AccountId":     id,
				keyErrorCode:    "ResourceNotFoundException",
				keyErrorMessage: "Member not found",
			})
		}
	}

	return found, unprocessed
}

func (b *InMemoryBackend) InviteMembers(accountIDs []string) []map[string]any {
	b.mu.Lock("InviteMembers")
	defer b.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)

	var unprocessed []map[string]any

	for _, id := range accountIDs {
		b.memberSeq++
		invitationID := fmt.Sprintf("%s-invite-%d", b.accountID, b.memberSeq)

		inv := &Invitation{
			AccountId:    id,
			InvitationId: invitationID,
			InvitedAt:    now,
			MemberStatus: "Invited",
		}
		b.invitations.Put(inv)

		if m, ok := b.members.Get(id); ok {
			m.MemberStatus = "Invited"
			m.InvitedAt = now
		}
	}

	return unprocessed
}

func (b *InMemoryBackend) ListMembers(onlyAssociated bool, nextToken string, maxResults int) ([]*Member, string) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()

	var all []*Member

	for _, m := range b.members.All() {
		if onlyAssociated && m.MemberStatus != "Enabled" {
			continue
		}

		cp := *m
		all = append(all, &cp)
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) DisassociateMembers(accountIDs []string) error {
	b.mu.Lock("DisassociateMembers")
	defer b.mu.Unlock()

	for _, id := range accountIDs {
		if m, ok := b.members.Get(id); ok {
			m.MemberStatus = "Removed"
		}
	}

	return nil
}

// --- Invitation backend methods ---

func (b *InMemoryBackend) AcceptAdministratorInvitation(administratorID, invitationID string) error {
	b.mu.Lock("AcceptAdministratorInvitation")
	defer b.mu.Unlock()

	b.adminAccount = &AdminAccount{
		AccountId:          administratorID,
		InvitationId:       invitationID,
		InvitedAt:          time.Now().UTC().Format(time.RFC3339),
		RelationshipStatus: "ENABLED", //nolint:goconst // existing issue.
	}

	return nil
}

func (b *InMemoryBackend) AcceptInvitation(masterID, invitationID string) error {
	return b.AcceptAdministratorInvitation(masterID, invitationID)
}

func (b *InMemoryBackend) DeclineInvitations(accountIDs []string) ([]map[string]any, []map[string]any) {
	b.mu.Lock("DeclineInvitations")
	defer b.mu.Unlock()

	var declined []map[string]any
	var unprocessed []map[string]any

	for _, inv := range b.invitations.All() {
		if slices.Contains(accountIDs, inv.AccountId) {
			inv.MemberStatus = "Resigned"
			declined = append(declined, map[string]any{
				"AccountId":        inv.AccountId,
				"ProcessingResult": "SUCCESS", //nolint:goconst // existing issue.
			})
			b.invitations.Delete(inv.InvitationId)
		}
	}

	for _, id := range accountIDs {
		found := false

		for _, item := range declined {
			if item["AccountId"] == id {
				found = true

				break
			}
		}

		if !found {
			unprocessed = append(unprocessed, map[string]any{
				"AccountId":     id,
				keyErrorCode:    "ResourceNotFoundException",
				keyErrorMessage: "Invitation not found",
			})
		}
	}

	return declined, unprocessed
}

func (b *InMemoryBackend) DeleteInvitations(accountIDs []string) ([]map[string]any, []map[string]any) {
	b.mu.Lock("DeleteInvitations")
	defer b.mu.Unlock()

	var deleted []map[string]any
	var unprocessed []map[string]any

	for _, inv := range b.invitations.All() {
		if slices.Contains(accountIDs, inv.AccountId) {
			deleted = append(deleted, map[string]any{
				"AccountId":        inv.AccountId,
				"ProcessingResult": "SUCCESS",
			})
			b.invitations.Delete(inv.InvitationId)
		}
	}

	for _, id := range accountIDs {
		found := false

		for _, item := range deleted {
			if item["AccountId"] == id {
				found = true

				break
			}
		}

		if !found {
			unprocessed = append(unprocessed, map[string]any{
				"AccountId":     id,
				keyErrorCode:    "ResourceNotFoundException",
				keyErrorMessage: "Invitation not found",
			})
		}
	}

	return deleted, unprocessed
}

func (b *InMemoryBackend) GetInvitationsCount() int {
	b.mu.RLock("GetInvitationsCount")
	defer b.mu.RUnlock()

	return b.invitations.Len()
}

func (b *InMemoryBackend) ListInvitations(nextToken string, maxResults int) ([]*Invitation, string) {
	b.mu.RLock("ListInvitations")
	defer b.mu.RUnlock()

	snap := b.invitations.All()
	all := make([]*Invitation, 0, len(snap))

	for _, inv := range snap {
		cp := *inv
		all = append(all, &cp)
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}

func (b *InMemoryBackend) GetAdministratorAccount() (*AdminAccount, error) {
	b.mu.RLock("GetAdministratorAccount")
	defer b.mu.RUnlock()

	if b.adminAccount == nil {
		return nil, nil //nolint:nilnil // existing issue.
	}

	cp := *b.adminAccount

	return &cp, nil
}

func (b *InMemoryBackend) GetMasterAccount() (*AdminAccount, error) {
	return b.GetAdministratorAccount()
}

func (b *InMemoryBackend) DisassociateFromAdministratorAccount() error {
	b.mu.Lock("DisassociateFromAdministratorAccount")
	defer b.mu.Unlock()

	b.adminAccount = nil

	return nil
}

func (b *InMemoryBackend) DisassociateFromMasterAccount() error {
	return b.DisassociateFromAdministratorAccount()
}

// --- Organization backend methods ---

func (b *InMemoryBackend) DescribeOrganizationConfiguration() *OrgConfig {
	b.mu.RLock("DescribeOrganizationConfiguration")
	defer b.mu.RUnlock()

	if b.orgConfig == nil {
		return &OrgConfig{
			AutoEnable:                    false,
			MemberAccountLimitReached:     false,
			AutoEnableStandards:           "NONE",
			OrganizationConfigurationType: "CENTRAL",
		}
	}

	cp := *b.orgConfig

	return &cp
}

func (b *InMemoryBackend) UpdateOrganizationConfiguration(
	autoEnable bool,
	autoEnableStandards string,
	orgConfigType string,
) error {
	b.mu.Lock("UpdateOrganizationConfiguration")
	defer b.mu.Unlock()

	if b.orgConfig == nil {
		b.orgConfig = &OrgConfig{}
	}

	b.orgConfig.AutoEnable = autoEnable

	if autoEnableStandards != "" {
		b.orgConfig.AutoEnableStandards = autoEnableStandards
	}

	if orgConfigType != "" {
		b.orgConfig.OrganizationConfigurationType = orgConfigType
	}

	return nil
}

func (b *InMemoryBackend) EnableOrganizationAdminAccount(accountID string) error {
	b.mu.Lock("EnableOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.orgAdminAccounts[accountID] = "ENABLED"

	return nil
}

func (b *InMemoryBackend) DisableOrganizationAdminAccount(accountID string) error {
	b.mu.Lock("DisableOrganizationAdminAccount")
	defer b.mu.Unlock()

	delete(b.orgAdminAccounts, accountID)

	return nil
}

func (b *InMemoryBackend) ListOrganizationAdminAccounts(nextToken string, maxResults int) ([]*OrgAdminAccount, string) {
	b.mu.RLock("ListOrganizationAdminAccounts")
	defer b.mu.RUnlock()

	var all []*OrgAdminAccount //nolint:prealloc // existing issue.

	for id, status := range b.orgAdminAccounts {
		all = append(all, &OrgAdminAccount{AccountId: id, Status: status})
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}
