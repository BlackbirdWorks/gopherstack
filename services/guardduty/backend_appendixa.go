package guardduty

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// --- error sentinels ---

var (
	ErrMemberNotFound                = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	ErrMemberAlreadyExists           = awserr.New(errConflictException, awserr.ErrConflict)
	ErrPublishingDestNotFound        = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	ErrPublishingDestAlreadyExists   = awserr.New(errConflictException, awserr.ErrConflict)
	ErrMalwareScanNotFound           = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	ErrMalwareProtPlanNotFound       = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	ErrMalwareProtPlanAlreadyExists  = awserr.New(errConflictException, awserr.ErrConflict)
	ErrThreatEntitySetNotFound       = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	ErrThreatEntitySetAlreadyExists  = awserr.New(errConflictException, awserr.ErrConflict)
	ErrTrustedEntitySetNotFound      = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	ErrTrustedEntitySetAlreadyExists = awserr.New(errConflictException, awserr.ErrConflict)
)

// --- types ---

// Member represents a GuardDuty member account.
type Member struct {
	AccountID          string    `json:"accountId"`
	AdministratorID    string    `json:"administratorId"`
	MasterID           string    `json:"masterId"`
	DetectorID         string    `json:"detectorId"`
	Email              string    `json:"email"`
	RelationshipStatus string    `json:"relationshipStatus"`
	InvitedAt          string    `json:"invitedAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

// Invitation represents a pending GuardDuty invitation.
type Invitation struct {
	AccountID          string `json:"accountId"`
	InvitationID       string `json:"invitationId"`
	InvitedAt          string `json:"invitedAt"`
	RelationshipStatus string `json:"relationshipStatus"`
}

// AdminAccount represents the administrator account relationship for GetAdministratorAccount.
type AdminAccount struct {
	AccountID          string `json:"accountId"`
	InvitationID       string `json:"invitationId"`
	InvitedAt          string `json:"invitedAt"`
	RelationshipStatus string `json:"relationshipStatus"`
}

// OrgAdminAccount represents an organization admin account.
type OrgAdminAccount struct {
	AdminAccountID string `json:"adminAccountId"`
	AdminStatus    string `json:"adminStatus"`
}

// OrgConfig holds org-level GuardDuty configuration.
type OrgConfig struct { //nolint:govet // fieldalignment: bool fields after strings trades padding for readability
	AutoEnable                bool           `json:"autoEnable"`
	MemberAccountLimitReached bool           `json:"memberAccountLimitReached"`
	DataSources               map[string]any `json:"dataSources"`
	Features                  []OrgFeature   `json:"features"`
}

// OrgFeature holds org-level feature configuration.
type OrgFeature struct {
	AutoEnable string `json:"autoEnable"`
	Name       string `json:"name"`
}

// PublishingDestination represents a GuardDuty publishing destination.
type PublishingDestination struct { //nolint:govet // fieldalignment: int64 after strings trades padding for readability
	DestinationID              string                `json:"destinationId"`
	DestinationType            string                `json:"destinationType"`
	Status                     string                `json:"status"`
	ServicePrincipal           string                `json:"servicePrincipal,omitempty"`
	PublishingFailureStartedAt int64                 `json:"publishingFailureStartedAt,omitempty"`
	DestinationProperties      DestinationProperties `json:"destinationProperties"`
	DetectorID                 string                `json:"-"`
}

// DestinationProperties holds properties for a publishing destination.
type DestinationProperties struct {
	DestinationArn string `json:"destinationArn,omitempty"`
	KmsKeyArn      string `json:"kmsKeyArn,omitempty"`
}

// MalwareScan represents a GuardDuty malware scan result.
type MalwareScan struct { //nolint:govet // fieldalignment: time.Time fields after strings
	ScanID          string         `json:"scanId"`
	DetectorID      string         `json:"detectorId"`
	AccountID       string         `json:"accountId"`
	ScanStartTime   time.Time      `json:"scanStartTime"`
	ScanEndTime     time.Time      `json:"scanEndTime"`
	ScanStatus      string         `json:"scanStatus"`
	ScanType        string         `json:"scanType"`
	TriggerDetails  map[string]any `json:"triggerDetails"`
	ResourceDetails map[string]any `json:"resourceDetails"`
	Findings        []any          `json:"findings"`
}

// MalwareScanSettings holds malware scan configuration for a detector.
type MalwareScanSettings struct {
	EbsSnapshotPreservation string         `json:"ebsSnapshotPreservation"`
	ScanResourceCriteria    map[string]any `json:"scanResourceCriteria"`
}

// MalwareProtectionPlan represents a malware protection plan.
type MalwareProtectionPlan struct { //nolint:govet // fieldalignment: time.Time after strings
	MalwareProtectionPlanID string            `json:"malwareProtectionPlanId"`
	Arn                     string            `json:"arn"`
	Role                    string            `json:"role"`
	Status                  string            `json:"status"`
	CreatedAt               time.Time         `json:"createdAt"`
	StatusReasons           []any             `json:"statusReasons"`
	ProtectedResource       map[string]any    `json:"protectedResource"`
	Actions                 map[string]any    `json:"actions"`
	Tags                    map[string]string `json:"tags,omitempty"`
}

// ThreatEntitySet represents a GuardDuty threat entity set.
type ThreatEntitySet struct { //nolint:govet // fieldalignment: time.Time after strings
	ThreatEntitySetID string            `json:"threatEntitySetId"`
	DetectorID        string            `json:"-"`
	Name              string            `json:"name"`
	Format            string            `json:"format"`
	Location          string            `json:"location"`
	Status            string            `json:"status"`
	Tags              map[string]string `json:"tags,omitempty"`
	CreatedAt         time.Time         `json:"createdAt"`
	UpdatedAt         time.Time         `json:"updatedAt"`
}

// TrustedEntitySet represents a GuardDuty trusted entity set.
type TrustedEntitySet struct { //nolint:govet // fieldalignment: time.Time after strings
	TrustedEntitySetID string            `json:"trustedEntitySetId"`
	DetectorID         string            `json:"-"`
	Name               string            `json:"name"`
	Format             string            `json:"format"`
	Location           string            `json:"location"`
	Status             string            `json:"status"`
	Tags               map[string]string `json:"tags,omitempty"`
	CreatedAt          time.Time         `json:"createdAt"`
	UpdatedAt          time.Time         `json:"updatedAt"`
}

// --- member backend methods ---

// CreateMembers creates member accounts for a detector.
func (b *InMemoryBackend) CreateMembers(
	detectorID string,
	accountDetails []map[string]any,
) ([]*Member, []map[string]any) {
	b.mu.Lock("CreateMembers")
	defer b.mu.Unlock()

	now := time.Now().UTC()

	var created []*Member
	var unprocessed []map[string]any

	if _, ok := b.detectors[detectorID]; !ok {
		for _, acc := range accountDetails {
			unprocessed = append(unprocessed, map[string]any{
				"accountId": acc["accountId"],
				"result":    "DetectorNotFound",
			})
		}

		return nil, unprocessed
	}

	if b.members[detectorID] == nil {
		b.members[detectorID] = make(map[string]*Member)
	}

	for _, acc := range accountDetails {
		accountID, _ := acc["accountId"].(string)
		email, _ := acc["email"].(string)

		if accountID == "" {
			unprocessed = append(unprocessed, map[string]any{
				"accountId": accountID,
				"result":    "InvalidInput",
			})

			continue
		}

		if _, exists := b.members[detectorID][accountID]; exists {
			unprocessed = append(unprocessed, map[string]any{
				"accountId": accountID,
				"result":    "ResourceConflictException",
			})

			continue
		}

		m := &Member{
			AccountID:          accountID,
			AdministratorID:    b.accountID,
			MasterID:           b.accountID,
			DetectorID:         detectorID,
			Email:              email,
			RelationshipStatus: "Created",
			UpdatedAt:          now,
		}
		b.members[detectorID][accountID] = m
		created = append(created, m)
	}

	return created, unprocessed
}

// DeleteMembers removes member accounts from a detector.
func (b *InMemoryBackend) DeleteMembers(detectorID string, accountIDs []string) []map[string]any {
	b.mu.Lock("DeleteMembers")
	defer b.mu.Unlock()

	var unprocessed []map[string]any

	if b.members[detectorID] == nil {
		for _, id := range accountIDs {
			unprocessed = append(unprocessed, map[string]any{
				"accountId": id,
				"result":    "ResourceNotFoundException",
			})
		}

		return unprocessed
	}

	for _, id := range accountIDs {
		if _, ok := b.members[detectorID][id]; ok {
			delete(b.members[detectorID], id)
		} else {
			unprocessed = append(unprocessed, map[string]any{
				"accountId": id,
				"result":    "ResourceNotFoundException",
			})
		}
	}

	return unprocessed
}

// GetMembers retrieves member account details.
func (b *InMemoryBackend) GetMembers(detectorID string, accountIDs []string) ([]*Member, []map[string]any) {
	b.mu.RLock("GetMembers")
	defer b.mu.RUnlock()

	var found []*Member
	var unprocessed []map[string]any

	for _, id := range accountIDs {
		if b.members[detectorID] != nil {
			if m, ok := b.members[detectorID][id]; ok {
				cp := *m
				found = append(found, &cp)

				continue
			}
		}

		unprocessed = append(unprocessed, map[string]any{
			"accountId": id,
			"result":    "ResourceNotFoundException",
		})
	}

	return found, unprocessed
}

// InviteMembers sends invitations to member accounts.
func (b *InMemoryBackend) InviteMembers(detectorID string, accountIDs []string) []map[string]any {
	b.mu.Lock("InviteMembers")
	defer b.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)

	var unprocessed []map[string]any

	for _, id := range accountIDs {
		b.memberSeq++
		invitationID := fmt.Sprintf("%s-invite-%d", b.accountID, b.memberSeq)

		b.invitations[invitationID] = &Invitation{
			AccountID:          id,
			InvitationID:       invitationID,
			InvitedAt:          now,
			RelationshipStatus: "Invited",
		}

		if b.members[detectorID] != nil {
			if m, ok := b.members[detectorID][id]; ok {
				m.RelationshipStatus = "Invited"
				m.InvitedAt = now
			}
		}
	}

	return unprocessed
}

// ListMembers returns member accounts for a detector.
func (b *InMemoryBackend) ListMembers(detectorID string, onlyAssociated bool) ([]*Member, error) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	var all []*Member

	for _, m := range b.members[detectorID] {
		if onlyAssociated && m.RelationshipStatus != "Enabled" {
			continue
		}

		cp := *m
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].AccountID < all[j].AccountID })

	return all, nil
}

// StartMonitoringMembers starts monitoring member accounts.
func (b *InMemoryBackend) StartMonitoringMembers(detectorID string, accountIDs []string) []map[string]any {
	b.mu.Lock("StartMonitoringMembers")
	defer b.mu.Unlock()

	var unprocessed []map[string]any

	for _, id := range accountIDs {
		if b.members[detectorID] != nil {
			if m, ok := b.members[detectorID][id]; ok {
				m.RelationshipStatus = "Enabled"

				continue
			}
		}

		unprocessed = append(unprocessed, map[string]any{
			"accountId": id,
			"result":    "ResourceNotFoundException",
		})
	}

	return unprocessed
}

// StopMonitoringMembers stops monitoring member accounts.
func (b *InMemoryBackend) StopMonitoringMembers(detectorID string, accountIDs []string) []map[string]any {
	b.mu.Lock("StopMonitoringMembers")
	defer b.mu.Unlock()

	var unprocessed []map[string]any

	for _, id := range accountIDs {
		if b.members[detectorID] != nil {
			if m, ok := b.members[detectorID][id]; ok {
				m.RelationshipStatus = "Disabled"

				continue
			}
		}

		unprocessed = append(unprocessed, map[string]any{
			"accountId": id,
			"result":    "ResourceNotFoundException",
		})
	}

	return unprocessed
}

// DisassociateMembers disassociates member accounts from a detector.
func (b *InMemoryBackend) DisassociateMembers(detectorID string, accountIDs []string) []map[string]any {
	b.mu.Lock("DisassociateMembers")
	defer b.mu.Unlock()

	var unprocessed []map[string]any

	for _, id := range accountIDs {
		if b.members[detectorID] != nil {
			if m, ok := b.members[detectorID][id]; ok {
				m.RelationshipStatus = "Removed"

				continue
			}
		}

		unprocessed = append(unprocessed, map[string]any{
			"accountId": id,
			"result":    "ResourceNotFoundException",
		})
	}

	return unprocessed
}

// GetMemberDetectors returns detector configurations for member accounts.
func (b *InMemoryBackend) GetMemberDetectors(
	detectorID string,
	accountIDs []string,
) ([]map[string]any, []map[string]any) {
	b.mu.RLock("GetMemberDetectors")
	defer b.mu.RUnlock()

	var memberDetails []map[string]any
	var unprocessed []map[string]any

	for _, id := range accountIDs {
		if b.members[detectorID] != nil {
			if m, ok := b.members[detectorID][id]; ok {
				memberDetails = append(memberDetails, map[string]any{
					"accountId":  m.AccountID,
					"detectorId": m.DetectorID,
					"features":   []any{},
				})

				continue
			}
		}

		unprocessed = append(unprocessed, map[string]any{
			"accountId": id,
			"result":    "ResourceNotFoundException",
		})
	}

	return memberDetails, unprocessed
}

// UpdateMemberDetectors updates detector configurations for member accounts.
func (b *InMemoryBackend) UpdateMemberDetectors(
	detectorID string,
	accountIDs []string,
) []map[string]any {
	b.mu.Lock("UpdateMemberDetectors")
	defer b.mu.Unlock()

	var unprocessed []map[string]any

	for _, id := range accountIDs {
		if b.members[detectorID] == nil || b.members[detectorID][id] == nil {
			unprocessed = append(unprocessed, map[string]any{
				"accountId": id,
				"result":    "ResourceNotFoundException",
			})
		}
	}

	return unprocessed
}

// --- invitation backend methods ---

// AcceptAdministratorInvitation records acceptance of an administrator invitation.
func (b *InMemoryBackend) AcceptAdministratorInvitation(detectorID, administratorID, invitationID string) error {
	b.mu.Lock("AcceptAdministratorInvitation")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)
	b.adminAccounts[detectorID] = &AdminAccount{
		AccountID:          administratorID,
		InvitationID:       invitationID,
		InvitedAt:          now,
		RelationshipStatus: "Enabled",
	}

	return nil
}

// AcceptInvitation records acceptance of a legacy master invitation.
func (b *InMemoryBackend) AcceptInvitation(detectorID, masterID, invitationID string) error {
	b.mu.Lock("AcceptInvitation")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)
	b.adminAccounts[detectorID] = &AdminAccount{
		AccountID:          masterID,
		InvitationID:       invitationID,
		InvitedAt:          now,
		RelationshipStatus: "Enabled",
	}

	return nil
}

// GetAdministratorAccount returns the administrator account for a detector.
func (b *InMemoryBackend) GetAdministratorAccount(detectorID string) (*AdminAccount, error) {
	b.mu.RLock("GetAdministratorAccount")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	a := b.adminAccounts[detectorID]
	if a == nil {
		return &AdminAccount{}, nil
	}

	cp := *a

	return &cp, nil
}

// GetMasterAccount returns the legacy master account for a detector.
func (b *InMemoryBackend) GetMasterAccount(detectorID string) (*AdminAccount, error) {
	b.mu.RLock("GetMasterAccount")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	a := b.adminAccounts[detectorID]
	if a == nil {
		return &AdminAccount{}, nil
	}

	cp := *a

	return &cp, nil
}

// DisassociateFromAdministratorAccount removes the administrator relationship.
func (b *InMemoryBackend) DisassociateFromAdministratorAccount(detectorID string) error {
	b.mu.Lock("DisassociateFromAdministratorAccount")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	delete(b.adminAccounts, detectorID)

	return nil
}

// DisassociateFromMasterAccount removes the legacy master relationship.
func (b *InMemoryBackend) DisassociateFromMasterAccount(detectorID string) error {
	b.mu.Lock("DisassociateFromMasterAccount")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	delete(b.adminAccounts, detectorID)

	return nil
}

// DeclineInvitations declines invitations from specified accounts.
func (b *InMemoryBackend) DeclineInvitations(accountIDs []string) []map[string]any {
	b.mu.Lock("DeclineInvitations")
	defer b.mu.Unlock()

	var unprocessed []map[string]any

	for invID, inv := range b.invitations {
		for _, id := range accountIDs {
			if inv.AccountID == id {
				inv.RelationshipStatus = "Declined"
				delete(b.invitations, invID)
			}
		}
	}

	return unprocessed
}

// DeleteInvitations deletes invitations from specified accounts.
func (b *InMemoryBackend) DeleteInvitations(accountIDs []string) []map[string]any {
	b.mu.Lock("DeleteInvitations")
	defer b.mu.Unlock()

	var unprocessed []map[string]any

	for invID, inv := range b.invitations {
		for _, id := range accountIDs {
			if inv.AccountID == id {
				delete(b.invitations, invID)
			}
		}
	}

	return unprocessed
}

// GetInvitationsCount returns the count of pending invitations.
func (b *InMemoryBackend) GetInvitationsCount() int {
	b.mu.RLock("GetInvitationsCount")
	defer b.mu.RUnlock()

	return len(b.invitations)
}

// ListInvitations returns all pending invitations.
func (b *InMemoryBackend) ListInvitations() []*Invitation {
	b.mu.RLock("ListInvitations")
	defer b.mu.RUnlock()

	all := make([]*Invitation, 0, len(b.invitations))
	for _, inv := range b.invitations {
		cp := *inv
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].InvitationID < all[j].InvitationID })

	return all
}

// --- org admin backend methods ---

// EnableOrganizationAdminAccount designates an account as org admin.
func (b *InMemoryBackend) EnableOrganizationAdminAccount(adminAccountID string) error {
	b.mu.Lock("EnableOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.orgAdminAccounts[adminAccountID] = &OrgAdminAccount{
		AdminAccountID: adminAccountID,
		AdminStatus:    "ENABLED",
	}

	return nil
}

// DisableOrganizationAdminAccount removes an account as org admin.
func (b *InMemoryBackend) DisableOrganizationAdminAccount(adminAccountID string) error {
	b.mu.Lock("DisableOrganizationAdminAccount")
	defer b.mu.Unlock()

	delete(b.orgAdminAccounts, adminAccountID)

	return nil
}

// ListOrganizationAdminAccounts returns all org admin accounts.
func (b *InMemoryBackend) ListOrganizationAdminAccounts() []*OrgAdminAccount {
	b.mu.RLock("ListOrganizationAdminAccounts")
	defer b.mu.RUnlock()

	all := make([]*OrgAdminAccount, 0, len(b.orgAdminAccounts))
	for _, a := range b.orgAdminAccounts {
		cp := *a
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].AdminAccountID < all[j].AdminAccountID })

	return all
}

// DescribeOrganizationConfiguration returns org config for a detector.
func (b *InMemoryBackend) DescribeOrganizationConfiguration(detectorID string) (*OrgConfig, error) {
	b.mu.RLock("DescribeOrganizationConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	cfg := b.orgConfigs[detectorID]
	if cfg == nil {
		return &OrgConfig{DataSources: map[string]any{}, Features: []OrgFeature{}}, nil
	}

	cp := *cfg

	return &cp, nil
}

// UpdateOrganizationConfiguration updates org config for a detector.
func (b *InMemoryBackend) UpdateOrganizationConfiguration(
	detectorID string,
	autoEnable bool,
	features []OrgFeature,
) error {
	b.mu.Lock("UpdateOrganizationConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	existing := b.orgConfigs[detectorID]
	if existing == nil {
		existing = &OrgConfig{DataSources: map[string]any{}}
	}

	existing.AutoEnable = autoEnable
	if features != nil {
		existing.Features = features
	}

	b.orgConfigs[detectorID] = existing

	return nil
}

// GetOrganizationStatistics returns org-level statistics.
func (b *InMemoryBackend) GetOrganizationStatistics() map[string]any {
	b.mu.RLock("GetOrganizationStatistics")
	defer b.mu.RUnlock()

	return map[string]any{
		"organizationDetails": map[string]any{
			"organizationStatistics": map[string]any{
				"totalAccountsCount":   1,
				"memberAccountsCount":  len(b.orgAdminAccounts),
				"enabledAccountsCount": 0,
				"countByFeature":       []any{},
			},
		},
	}
}

// --- publishing destination backend methods ---

// CreatePublishingDestination creates a new publishing destination for a detector.
func (b *InMemoryBackend) CreatePublishingDestination(
	detectorID, destType string,
	props DestinationProperties,
) (*PublishingDestination, error) {
	b.mu.Lock("CreatePublishingDestination")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	dest := &PublishingDestination{
		DestinationID:         id,
		DestinationType:       destType,
		Status:                "PUBLISHING",
		DestinationProperties: props,
		DetectorID:            detectorID,
	}
	b.publishingDestinations[detectorID][id] = dest

	return dest, nil
}

// DeletePublishingDestination removes a publishing destination.
func (b *InMemoryBackend) DeletePublishingDestination(detectorID, destID string) error {
	b.mu.Lock("DeletePublishingDestination")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	if _, ok := b.publishingDestinations[detectorID][destID]; !ok {
		return ErrPublishingDestNotFound
	}

	delete(b.publishingDestinations[detectorID], destID)

	return nil
}

// DescribePublishingDestination retrieves a publishing destination.
func (b *InMemoryBackend) DescribePublishingDestination(detectorID, destID string) (*PublishingDestination, error) {
	b.mu.RLock("DescribePublishingDestination")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	dest, ok := b.publishingDestinations[detectorID][destID]
	if !ok {
		return nil, ErrPublishingDestNotFound
	}

	cp := *dest

	return &cp, nil
}

// ListPublishingDestinations returns publishing destinations for a detector.
func (b *InMemoryBackend) ListPublishingDestinations(detectorID string) ([]*PublishingDestination, error) {
	b.mu.RLock("ListPublishingDestinations")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	all := make([]*PublishingDestination, 0, len(b.publishingDestinations[detectorID]))
	for _, dest := range b.publishingDestinations[detectorID] {
		cp := *dest
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].DestinationID < all[j].DestinationID })

	return all, nil
}

// UpdatePublishingDestination updates a publishing destination.
func (b *InMemoryBackend) UpdatePublishingDestination(
	detectorID, destID string,
	props DestinationProperties,
) error {
	b.mu.Lock("UpdatePublishingDestination")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	dest, ok := b.publishingDestinations[detectorID][destID]
	if !ok {
		return ErrPublishingDestNotFound
	}

	dest.DestinationProperties = props

	return nil
}

// --- malware scan backend methods ---

// DescribeMalwareScans returns malware scans associated with a detector.
func (b *InMemoryBackend) DescribeMalwareScans(detectorID string) ([]*MalwareScan, error) {
	b.mu.RLock("DescribeMalwareScans")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	var all []*MalwareScan

	for _, scan := range b.malwareScans {
		if scan.DetectorID == detectorID {
			cp := *scan
			all = append(all, &cp)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ScanID < all[j].ScanID })

	return all, nil
}

// ListMalwareScans returns all malware scans globally.
func (b *InMemoryBackend) ListMalwareScans() []*MalwareScan {
	b.mu.RLock("ListMalwareScans")
	defer b.mu.RUnlock()

	all := make([]*MalwareScan, 0, len(b.malwareScans))
	for _, scan := range b.malwareScans {
		cp := *scan
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ScanID < all[j].ScanID })

	return all
}

// StartMalwareScan initiates a malware scan.
func (b *InMemoryBackend) StartMalwareScan(resourceARN string) (string, error) {
	b.mu.Lock("StartMalwareScan")
	defer b.mu.Unlock()

	scanID := strings.ReplaceAll(uuid.New().String(), "-", "")
	now := time.Now().UTC()

	b.malwareScans[scanID] = &MalwareScan{
		ScanID:        scanID,
		AccountID:     b.accountID,
		ScanStatus:    "RUNNING",
		ScanType:      "GUARDDUTY_INITIATED",
		ScanStartTime: now,
		TriggerDetails: map[string]any{
			"scanTriggerDetails": map[string]any{"scanInitiatedAt": now.Format(time.RFC3339)},
		},
		ResourceDetails: map[string]any{"instanceArn": resourceARN},
		Findings:        []any{},
	}

	return scanID, nil
}

// GetMalwareScan retrieves a malware scan by ID.
func (b *InMemoryBackend) GetMalwareScan(scanID string) (*MalwareScan, error) {
	b.mu.RLock("GetMalwareScan")
	defer b.mu.RUnlock()

	scan, ok := b.malwareScans[scanID]
	if !ok {
		return nil, ErrMalwareScanNotFound
	}

	cp := *scan

	return &cp, nil
}

// GetMalwareScanSettings returns malware scan settings for a detector.
func (b *InMemoryBackend) GetMalwareScanSettings(detectorID string) (*MalwareScanSettings, error) {
	b.mu.RLock("GetMalwareScanSettings")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	settings := b.malwareScanSettings[detectorID]
	if settings == nil {
		return &MalwareScanSettings{
			EbsSnapshotPreservation: "NO_RETENTION",
			ScanResourceCriteria:    map[string]any{},
		}, nil
	}

	cp := *settings

	return &cp, nil
}

// UpdateMalwareScanSettings updates malware scan settings for a detector.
func (b *InMemoryBackend) UpdateMalwareScanSettings(
	detectorID string,
	settings *MalwareScanSettings,
) error {
	b.mu.Lock("UpdateMalwareScanSettings")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	b.malwareScanSettings[detectorID] = settings

	return nil
}

// GetUsageStatistics returns usage statistics for a detector.
func (b *InMemoryBackend) GetUsageStatistics(detectorID string) (map[string]any, error) {
	b.mu.RLock("GetUsageStatistics")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	return map[string]any{
		"usageStatistics": map[string]any{
			"sumByAccount":    []any{},
			"sumByDataSource": []any{},
			"sumByResource":   []any{},
			"topResources":    []any{},
		},
	}, nil
}

// GetRemainingFreeTrialDays returns remaining free trial days.
func (b *InMemoryBackend) GetRemainingFreeTrialDays(detectorID string) (map[string]any, error) {
	b.mu.RLock("GetRemainingFreeTrialDays")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	return map[string]any{
		"accounts": []any{
			map[string]any{
				"accountId":              b.accountID,
				"dataSources":            map[string]any{},
				"features":               []any{},
				"freeTrialDaysRemaining": 30,
			},
		},
		"unprocessedAccounts": []any{},
	}, nil
}

// GetCoverageStatistics returns coverage statistics for a detector.
func (b *InMemoryBackend) GetCoverageStatistics(detectorID string) (map[string]any, error) {
	b.mu.RLock("GetCoverageStatistics")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	return map[string]any{
		"coverageStatistics": map[string]any{
			"countByResourceType":   map[string]any{},
			"countByCoverageStatus": map[string]any{},
		},
	}, nil
}

// ListCoverage returns coverage resources for a detector.
func (b *InMemoryBackend) ListCoverage(detectorID string) ([]map[string]any, error) {
	b.mu.RLock("ListCoverage")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	return []map[string]any{}, nil
}

// --- malware protection plan backend methods ---

// CreateMalwareProtectionPlan creates a new malware protection plan.
func (b *InMemoryBackend) CreateMalwareProtectionPlan(
	role string,
	protectedResource, actions map[string]any,
	tags map[string]string,
) (*MalwareProtectionPlan, error) {
	b.mu.Lock("CreateMalwareProtectionPlan")
	defer b.mu.Unlock()

	planID := strings.ReplaceAll(uuid.New().String(), "-", "")
	arn := fmt.Sprintf(
		"arn:aws:guardduty:%s:%s:malware-protection-plan/%s",
		b.region, b.accountID, planID,
	)

	plan := &MalwareProtectionPlan{
		MalwareProtectionPlanID: planID,
		Arn:                     arn,
		Role:                    role,
		Status:                  "ACTIVE",
		CreatedAt:               time.Now().UTC(),
		StatusReasons:           []any{},
		ProtectedResource:       protectedResource,
		Actions:                 actions,
		Tags:                    tags,
	}
	b.malwareProtectionPlans[planID] = plan

	if tags != nil {
		b.tags[arn] = maps.Clone(tags)
	}

	return plan, nil
}

// DeleteMalwareProtectionPlan removes a malware protection plan.
func (b *InMemoryBackend) DeleteMalwareProtectionPlan(planID string) error {
	b.mu.Lock("DeleteMalwareProtectionPlan")
	defer b.mu.Unlock()

	plan, ok := b.malwareProtectionPlans[planID]
	if !ok {
		return ErrMalwareProtPlanNotFound
	}

	delete(b.tags, plan.Arn)
	delete(b.malwareProtectionPlans, planID)

	return nil
}

// GetMalwareProtectionPlan retrieves a malware protection plan.
func (b *InMemoryBackend) GetMalwareProtectionPlan(planID string) (*MalwareProtectionPlan, error) {
	b.mu.RLock("GetMalwareProtectionPlan")
	defer b.mu.RUnlock()

	plan, ok := b.malwareProtectionPlans[planID]
	if !ok {
		return nil, ErrMalwareProtPlanNotFound
	}

	cp := *plan

	return &cp, nil
}

// ListMalwareProtectionPlans returns all malware protection plans.
func (b *InMemoryBackend) ListMalwareProtectionPlans() []*MalwareProtectionPlan {
	b.mu.RLock("ListMalwareProtectionPlans")
	defer b.mu.RUnlock()

	all := make([]*MalwareProtectionPlan, 0, len(b.malwareProtectionPlans))
	for _, plan := range b.malwareProtectionPlans {
		cp := *plan
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].MalwareProtectionPlanID < all[j].MalwareProtectionPlanID
	})

	return all
}

// UpdateMalwareProtectionPlan updates a malware protection plan.
func (b *InMemoryBackend) UpdateMalwareProtectionPlan(
	planID, role string,
	protectedResource, actions map[string]any,
) error {
	b.mu.Lock("UpdateMalwareProtectionPlan")
	defer b.mu.Unlock()

	plan, ok := b.malwareProtectionPlans[planID]
	if !ok {
		return ErrMalwareProtPlanNotFound
	}

	if role != "" {
		plan.Role = role
	}

	if protectedResource != nil {
		plan.ProtectedResource = protectedResource
	}

	if actions != nil {
		plan.Actions = actions
	}

	return nil
}

// SendObjectMalwareScan initiates a malware scan on an S3 object.
func (b *InMemoryBackend) SendObjectMalwareScan(s3ObjectDetails map[string]any) (string, error) {
	b.mu.Lock("SendObjectMalwareScan")
	defer b.mu.Unlock()

	scanID := strings.ReplaceAll(uuid.New().String(), "-", "")

	return scanID, nil
}

// --- threat entity set backend methods ---

// CreateThreatEntitySet creates a new threat entity set.
//
//nolint:dupl // ThreatEntitySet and TrustedEntitySet have identical creation patterns
func (b *InMemoryBackend) CreateThreatEntitySet(
	detectorID, name, format, location string,
	activate bool,
	tags map[string]string,
) (*ThreatEntitySet, error) {
	b.mu.Lock("CreateThreatEntitySet")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	for _, existing := range b.threatEntitySets[detectorID] {
		if existing.Name == name {
			return nil, ErrThreatEntitySetAlreadyExists
		}
	}

	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	status := statusInactive
	if activate {
		status = statusActive
	}

	now := time.Now().UTC()
	s := &ThreatEntitySet{
		ThreatEntitySetID: id,
		DetectorID:        detectorID,
		Name:              name,
		Format:            format,
		Location:          location,
		Status:            status,
		Tags:              tags,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	b.threatEntitySets[detectorID][id] = s

	return s, nil
}

// GetThreatEntitySet retrieves a threat entity set.
func (b *InMemoryBackend) GetThreatEntitySet(detectorID, setID string) (*ThreatEntitySet, error) {
	b.mu.RLock("GetThreatEntitySet")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	s, ok := b.threatEntitySets[detectorID][setID]
	if !ok {
		return nil, ErrThreatEntitySetNotFound
	}

	cp := *s

	return &cp, nil
}

// ListThreatEntitySets returns threat entity set IDs for a detector.
func (b *InMemoryBackend) ListThreatEntitySets(detectorID string) ([]string, error) {
	b.mu.RLock("ListThreatEntitySets")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	ids := make([]string, 0, len(b.threatEntitySets[detectorID]))
	for id := range b.threatEntitySets[detectorID] {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids, nil
}

// UpdateThreatEntitySet updates a threat entity set.
func (b *InMemoryBackend) UpdateThreatEntitySet(
	detectorID, setID, name, location string,
	activate *bool,
) error {
	b.mu.Lock("UpdateThreatEntitySet")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	s, ok := b.threatEntitySets[detectorID][setID]
	if !ok {
		return ErrThreatEntitySetNotFound
	}

	if name != "" {
		s.Name = name
	}

	if location != "" {
		s.Location = location
	}

	if activate != nil {
		if *activate {
			s.Status = statusActive
		} else {
			s.Status = statusInactive
		}
	}

	s.UpdatedAt = time.Now().UTC()

	return nil
}

// DeleteThreatEntitySet removes a threat entity set.
func (b *InMemoryBackend) DeleteThreatEntitySet(detectorID, setID string) error {
	b.mu.Lock("DeleteThreatEntitySet")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	if _, ok := b.threatEntitySets[detectorID][setID]; !ok {
		return ErrThreatEntitySetNotFound
	}

	delete(b.threatEntitySets[detectorID], setID)

	return nil
}

// --- trusted entity set backend methods ---

// CreateTrustedEntitySet creates a new trusted entity set.
//
//nolint:dupl // ThreatEntitySet and TrustedEntitySet have identical creation patterns
func (b *InMemoryBackend) CreateTrustedEntitySet(
	detectorID, name, format, location string,
	activate bool,
	tags map[string]string,
) (*TrustedEntitySet, error) {
	b.mu.Lock("CreateTrustedEntitySet")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	for _, existing := range b.trustedEntitySets[detectorID] {
		if existing.Name == name {
			return nil, ErrTrustedEntitySetAlreadyExists
		}
	}

	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	status := statusInactive
	if activate {
		status = statusActive
	}

	now := time.Now().UTC()
	s := &TrustedEntitySet{
		TrustedEntitySetID: id,
		DetectorID:         detectorID,
		Name:               name,
		Format:             format,
		Location:           location,
		Status:             status,
		Tags:               tags,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	b.trustedEntitySets[detectorID][id] = s

	return s, nil
}

// GetTrustedEntitySet retrieves a trusted entity set.
func (b *InMemoryBackend) GetTrustedEntitySet(detectorID, setID string) (*TrustedEntitySet, error) {
	b.mu.RLock("GetTrustedEntitySet")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	s, ok := b.trustedEntitySets[detectorID][setID]
	if !ok {
		return nil, ErrTrustedEntitySetNotFound
	}

	cp := *s

	return &cp, nil
}

// ListTrustedEntitySets returns trusted entity set IDs for a detector.
func (b *InMemoryBackend) ListTrustedEntitySets(detectorID string) ([]string, error) {
	b.mu.RLock("ListTrustedEntitySets")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	ids := make([]string, 0, len(b.trustedEntitySets[detectorID]))
	for id := range b.trustedEntitySets[detectorID] {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids, nil
}

// UpdateTrustedEntitySet updates a trusted entity set.
func (b *InMemoryBackend) UpdateTrustedEntitySet(
	detectorID, setID, name, location string,
	activate *bool,
) error {
	b.mu.Lock("UpdateTrustedEntitySet")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	s, ok := b.trustedEntitySets[detectorID][setID]
	if !ok {
		return ErrTrustedEntitySetNotFound
	}

	if name != "" {
		s.Name = name
	}

	if location != "" {
		s.Location = location
	}

	if activate != nil {
		if *activate {
			s.Status = statusActive
		} else {
			s.Status = statusInactive
		}
	}

	s.UpdatedAt = time.Now().UTC()

	return nil
}

// DeleteTrustedEntitySet removes a trusted entity set.
func (b *InMemoryBackend) DeleteTrustedEntitySet(detectorID, setID string) error {
	b.mu.Lock("DeleteTrustedEntitySet")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	if _, ok := b.trustedEntitySets[detectorID][setID]; !ok {
		return ErrTrustedEntitySetNotFound
	}

	delete(b.trustedEntitySets[detectorID], setID)

	return nil
}
