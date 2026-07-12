package guardduty

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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
	UpdatedAt          time.Time `json:"updatedAt"`
	AccountID          string    `json:"accountId"`
	AdministratorID    string    `json:"administratorId"`
	MasterID           string    `json:"masterId"`
	DetectorID         string    `json:"detectorId"`
	Email              string    `json:"email"`
	RelationshipStatus string    `json:"relationshipStatus"`
	InvitedAt          string    `json:"invitedAt"`
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
	// detectorID is the store.Table composite-key qualifier (see
	// adminAccountTableKeyFn in store_setup.go); it has no wire shape of its
	// own -- AdminAccount carries no identity field, so this was added
	// purely for the table's key -- and is carried through persistence via
	// byDetectorDTO (see persistence.go).
	detectorID string
}

// OrgAdminAccount represents an organization admin account.
type OrgAdminAccount struct {
	AdminAccountID string `json:"adminAccountId"`
	AdminStatus    string `json:"adminStatus"`
}

// OrgConfig holds org-level GuardDuty configuration.
type OrgConfig struct {
	DataSources map[string]any `json:"dataSources"`
	// detectorID is the store.Table composite-key qualifier (see
	// orgConfigTableKeyFn in store_setup.go); see AdminAccount.detectorID.
	detectorID                string
	Features                  []OrgFeature `json:"features"`
	AutoEnable                bool         `json:"autoEnable"`
	MemberAccountLimitReached bool         `json:"memberAccountLimitReached"`
}

// OrgFeature holds org-level feature configuration.
type OrgFeature struct {
	AutoEnable string `json:"autoEnable"`
	Name       string `json:"name"`
}

// PublishingDestination represents a GuardDuty publishing destination.
type PublishingDestination struct {
	DestinationProperties      DestinationProperties `json:"destinationProperties"`
	Tags                       map[string]string     `json:"tags,omitempty"`
	DestinationID              string                `json:"destinationId"`
	DestinationType            string                `json:"destinationType"`
	Status                     string                `json:"status"`
	ServicePrincipal           string                `json:"servicePrincipal,omitempty"`
	DetectorID                 string                `json:"-"`
	PublishingFailureStartedAt int64                 `json:"publishingFailureStartedAt,omitempty"`
}

// DestinationProperties holds properties for a publishing destination.
type DestinationProperties struct {
	DestinationArn string `json:"destinationArn,omitempty"`
	KmsKeyArn      string `json:"kmsKeyArn,omitempty"`
}

// MalwareScan represents a GuardDuty malware scan result.
type MalwareScan struct {
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
	ScanResourceCriteria    map[string]any `json:"scanResourceCriteria"`
	EbsSnapshotPreservation string         `json:"ebsSnapshotPreservation"`
	// detectorID is the store.Table composite-key qualifier (see
	// malwareScanSettingsTableKeyFn in store_setup.go); see
	// AdminAccount.detectorID.
	detectorID string
}

// MalwareProtectionPlan represents a malware protection plan.
type MalwareProtectionPlan struct {
	CreatedAt               time.Time         `json:"createdAt"`
	ProtectedResource       map[string]any    `json:"protectedResource"`
	Actions                 map[string]any    `json:"actions"`
	Tags                    map[string]string `json:"tags,omitempty"`
	MalwareProtectionPlanID string            `json:"malwareProtectionPlanId"`
	Arn                     string            `json:"arn"`
	Role                    string            `json:"role"`
	Status                  string            `json:"status"`
	StatusReasons           []any             `json:"statusReasons"`
}

// ThreatEntitySet represents a GuardDuty threat entity set.
type ThreatEntitySet struct {
	CreatedAt         time.Time         `json:"createdAt"`
	UpdatedAt         time.Time         `json:"updatedAt"`
	Tags              map[string]string `json:"tags,omitempty"`
	ThreatEntitySetID string            `json:"threatEntitySetId"`
	DetectorID        string            `json:"-"`
	Name              string            `json:"name"`
	Format            string            `json:"format"`
	Location          string            `json:"location"`
	Status            string            `json:"status"`
}

// TrustedEntitySet represents a GuardDuty trusted entity set.
type TrustedEntitySet struct {
	CreatedAt          time.Time         `json:"createdAt"`
	UpdatedAt          time.Time         `json:"updatedAt"`
	Tags               map[string]string `json:"tags,omitempty"`
	TrustedEntitySetID string            `json:"trustedEntitySetId"`
	DetectorID         string            `json:"-"`
	Name               string            `json:"name"`
	Format             string            `json:"format"`
	Location           string            `json:"location"`
	Status             string            `json:"status"`
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

	if !b.detectors.Has(detectorID) {
		for _, acc := range accountDetails {
			unprocessed = append(unprocessed, map[string]any{
				"accountId": acc["accountId"],   //nolint:goconst // existing issue.
				"result":    "DetectorNotFound", //nolint:goconst // existing issue.
			})
		}

		return nil, unprocessed
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

		if b.members.Has(detectorKey(detectorID, accountID)) {
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
		b.members.Put(m)
		created = append(created, m)
	}

	return created, unprocessed
}

// DeleteMembers removes member accounts from a detector.
func (b *InMemoryBackend) DeleteMembers(detectorID string, accountIDs []string) []map[string]any {
	b.mu.Lock("DeleteMembers")
	defer b.mu.Unlock()

	var unprocessed []map[string]any

	for _, id := range accountIDs {
		if !b.members.Delete(detectorKey(detectorID, id)) {
			unprocessed = append(unprocessed, map[string]any{
				"accountId": id,
				"result":    "ResourceNotFoundException", //nolint:goconst // existing issue.
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
		if m, ok := b.members.Get(detectorKey(detectorID, id)); ok {
			cp := *m
			found = append(found, &cp)

			continue
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

		b.invitations.Put(&Invitation{
			AccountID:          id,
			InvitationID:       invitationID,
			InvitedAt:          now,
			RelationshipStatus: "Invited",
		})

		if m, ok := b.members.Get(detectorKey(detectorID, id)); ok {
			m.RelationshipStatus = "Invited"
			m.InvitedAt = now
		}
	}

	return unprocessed
}

// ListMembers returns member accounts for a detector.
func (b *InMemoryBackend) ListMembers(detectorID string, onlyAssociated bool) ([]*Member, error) {
	b.mu.RLock("ListMembers")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	var all []*Member

	for _, m := range b.membersByDetector.Get(detectorID) {
		if onlyAssociated && m.RelationshipStatus != "Enabled" { //nolint:goconst // existing issue.
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
		if m, ok := b.members.Get(detectorKey(detectorID, id)); ok {
			m.RelationshipStatus = "Enabled"

			continue
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
		if m, ok := b.members.Get(detectorKey(detectorID, id)); ok {
			m.RelationshipStatus = "Disabled"

			continue
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
		if m, ok := b.members.Get(detectorKey(detectorID, id)); ok {
			m.RelationshipStatus = "Removed"

			continue
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
		if m, ok := b.members.Get(detectorKey(detectorID, id)); ok {
			memberDetails = append(memberDetails, map[string]any{
				"accountId":  m.AccountID,
				"detectorId": m.DetectorID, //nolint:goconst // existing issue.
				"features":   []any{},      //nolint:goconst // existing issue.
			})

			continue
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
		if !b.members.Has(detectorKey(detectorID, id)) {
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

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)
	acc := &AdminAccount{
		AccountID:          administratorID,
		InvitationID:       invitationID,
		InvitedAt:          now,
		RelationshipStatus: "Enabled",
	}
	acc.detectorID = detectorID
	b.adminAccounts.Put(acc)

	return nil
}

// AcceptInvitation records acceptance of a legacy master invitation.
func (b *InMemoryBackend) AcceptInvitation(detectorID, masterID, invitationID string) error {
	b.mu.Lock("AcceptInvitation")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)
	acc := &AdminAccount{
		AccountID:          masterID,
		InvitationID:       invitationID,
		InvitedAt:          now,
		RelationshipStatus: "Enabled",
	}
	acc.detectorID = detectorID
	b.adminAccounts.Put(acc)

	return nil
}

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

// DeclineInvitations declines invitations from specified accounts.
func (b *InMemoryBackend) DeclineInvitations(accountIDs []string) []map[string]any {
	b.mu.Lock("DeclineInvitations")
	defer b.mu.Unlock()

	var unprocessed []map[string]any

	for _, inv := range b.invitations.All() {
		for _, id := range accountIDs {
			if inv.AccountID == id {
				inv.RelationshipStatus = "Declined"
				b.invitations.Delete(inv.InvitationID)
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

	for _, inv := range b.invitations.All() {
		for _, id := range accountIDs {
			if inv.AccountID == id {
				b.invitations.Delete(inv.InvitationID)
			}
		}
	}

	return unprocessed
}

// GetInvitationsCount returns the count of pending invitations.
func (b *InMemoryBackend) GetInvitationsCount() int {
	b.mu.RLock("GetInvitationsCount")
	defer b.mu.RUnlock()

	return b.invitations.Len()
}

// ListInvitations returns all pending invitations.
func (b *InMemoryBackend) ListInvitations() []*Invitation {
	b.mu.RLock("ListInvitations")
	defer b.mu.RUnlock()

	items := b.invitations.Snapshot()
	all := make([]*Invitation, 0, len(items))

	for _, inv := range items {
		cp := *inv
		all = append(all, &cp)
	}

	return all
}

// --- org admin backend methods ---

// EnableOrganizationAdminAccount designates an account as org admin.
func (b *InMemoryBackend) EnableOrganizationAdminAccount(adminAccountID string) error {
	b.mu.Lock("EnableOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.orgAdminAccounts.Put(&OrgAdminAccount{
		AdminAccountID: adminAccountID,
		AdminStatus:    "ENABLED",
	})

	return nil
}

// DisableOrganizationAdminAccount removes an account as org admin.
func (b *InMemoryBackend) DisableOrganizationAdminAccount(adminAccountID string) error {
	b.mu.Lock("DisableOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.orgAdminAccounts.Delete(adminAccountID)

	return nil
}

// ListOrganizationAdminAccounts returns all org admin accounts.
func (b *InMemoryBackend) ListOrganizationAdminAccounts() []*OrgAdminAccount {
	b.mu.RLock("ListOrganizationAdminAccounts")
	defer b.mu.RUnlock()

	items := b.orgAdminAccounts.Snapshot()
	all := make([]*OrgAdminAccount, 0, len(items))

	for _, a := range items {
		cp := *a
		all = append(all, &cp)
	}

	return all
}

// DescribeOrganizationConfiguration returns org config for a detector.
func (b *InMemoryBackend) DescribeOrganizationConfiguration(detectorID string) (*OrgConfig, error) {
	b.mu.RLock("DescribeOrganizationConfiguration")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	cfg, ok := b.orgConfigs.Get(detectorID)
	if !ok {
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

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	existing, ok := b.orgConfigs.Get(detectorID)
	if !ok {
		existing = &OrgConfig{DataSources: map[string]any{}}
		existing.detectorID = detectorID
	}

	existing.AutoEnable = autoEnable
	if features != nil {
		existing.Features = features
	}

	b.orgConfigs.Put(existing)

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
				"memberAccountsCount":  b.orgAdminAccounts.Len(),
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
	tags map[string]string,
) (*PublishingDestination, error) {
	b.mu.Lock("CreatePublishingDestination")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	dest := &PublishingDestination{
		DestinationID:         id,
		DestinationType:       destType,
		Status:                "PUBLISHING",
		DestinationProperties: props,
		DetectorID:            detectorID,
		Tags:                  tags,
	}
	b.publishingDestinations.Put(dest)

	if tags != nil {
		b.tags[b.publishingDestinationARN(detectorID, id)] = maps.Clone(tags)
	}

	return dest, nil
}

// DeletePublishingDestination removes a publishing destination.
func (b *InMemoryBackend) DeletePublishingDestination(detectorID, destID string) error {
	b.mu.Lock("DeletePublishingDestination")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	if !b.publishingDestinations.Delete(detectorKey(detectorID, destID)) {
		return ErrPublishingDestNotFound
	}

	delete(b.tags, b.publishingDestinationARN(detectorID, destID))

	return nil
}

// DescribePublishingDestination retrieves a publishing destination.
func (b *InMemoryBackend) DescribePublishingDestination(detectorID, destID string) (*PublishingDestination, error) {
	b.mu.RLock("DescribePublishingDestination")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	dest, ok := b.publishingDestinations.Get(detectorKey(detectorID, destID))
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

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	items := b.publishingDestinationsByDetector.Get(detectorID)
	all := make([]*PublishingDestination, 0, len(items))

	for _, dest := range items {
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

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	dest, ok := b.publishingDestinations.Get(detectorKey(detectorID, destID))
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

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	var all []*MalwareScan

	for _, scan := range b.malwareScans.All() {
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

	items := b.malwareScans.Snapshot()
	all := make([]*MalwareScan, 0, len(items))

	for _, scan := range items {
		cp := *scan
		all = append(all, &cp)
	}

	return all
}

// StartMalwareScan initiates a malware scan.
func (b *InMemoryBackend) StartMalwareScan(resourceARN string) (string, error) {
	b.mu.Lock("StartMalwareScan")
	defer b.mu.Unlock()

	scanID := strings.ReplaceAll(uuid.New().String(), "-", "")
	now := time.Now().UTC()

	b.malwareScans.Put(&MalwareScan{
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
	})

	return scanID, nil
}

// GetMalwareScan retrieves a malware scan by ID.
func (b *InMemoryBackend) GetMalwareScan(scanID string) (*MalwareScan, error) {
	b.mu.RLock("GetMalwareScan")
	defer b.mu.RUnlock()

	scan, ok := b.malwareScans.Get(scanID)
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

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	settings, ok := b.malwareScanSettings.Get(detectorID)
	if !ok {
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

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	settings.detectorID = detectorID
	b.malwareScanSettings.Put(settings)

	return nil
}

// GetUsageStatistics returns usage statistics for a detector.
func (b *InMemoryBackend) GetUsageStatistics(detectorID string) (map[string]any, error) {
	b.mu.RLock("GetUsageStatistics")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
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

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	return map[string]any{
		"accounts": []any{
			map[string]any{
				"accountId":              b.accountID,
				"dataSources":            map[string]any{},
				"features":               []any{},
				"freeTrialDaysRemaining": 30, //nolint:mnd // existing issue.
			},
		},
		"unprocessedAccounts": []any{}, //nolint:goconst // existing issue.
	}, nil
}

// GetCoverageStatistics returns coverage statistics for a detector.
func (b *InMemoryBackend) GetCoverageStatistics(detectorID string) (map[string]any, error) {
	b.mu.RLock("GetCoverageStatistics")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
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

	if !b.detectors.Has(detectorID) {
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
	planARN := arn.Build(
		"guardduty", b.region, b.accountID,
		fmt.Sprintf("malware-protection-plan/%s", planID),
	)

	plan := &MalwareProtectionPlan{
		MalwareProtectionPlanID: planID,
		Arn:                     planARN,
		Role:                    role,
		Status:                  "ACTIVE",
		CreatedAt:               time.Now().UTC(),
		StatusReasons:           []any{},
		ProtectedResource:       protectedResource,
		Actions:                 actions,
		Tags:                    tags,
	}
	b.malwareProtectionPlans.Put(plan)

	if tags != nil {
		b.tags[planARN] = maps.Clone(tags)
	}

	return plan, nil
}

// DeleteMalwareProtectionPlan removes a malware protection plan.
func (b *InMemoryBackend) DeleteMalwareProtectionPlan(planID string) error {
	b.mu.Lock("DeleteMalwareProtectionPlan")
	defer b.mu.Unlock()

	plan, ok := b.malwareProtectionPlans.Get(planID)
	if !ok {
		return ErrMalwareProtPlanNotFound
	}

	delete(b.tags, plan.Arn)
	b.malwareProtectionPlans.Delete(planID)

	return nil
}

// GetMalwareProtectionPlan retrieves a malware protection plan.
func (b *InMemoryBackend) GetMalwareProtectionPlan(planID string) (*MalwareProtectionPlan, error) {
	b.mu.RLock("GetMalwareProtectionPlan")
	defer b.mu.RUnlock()

	plan, ok := b.malwareProtectionPlans.Get(planID)
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

	items := b.malwareProtectionPlans.Snapshot()
	all := make([]*MalwareProtectionPlan, 0, len(items))

	for _, plan := range items {
		cp := *plan
		all = append(all, &cp)
	}

	return all
}

// UpdateMalwareProtectionPlan updates a malware protection plan.
func (b *InMemoryBackend) UpdateMalwareProtectionPlan(
	planID, role string,
	protectedResource, actions map[string]any,
) error {
	b.mu.Lock("UpdateMalwareProtectionPlan")
	defer b.mu.Unlock()

	plan, ok := b.malwareProtectionPlans.Get(planID)
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
func (b *InMemoryBackend) SendObjectMalwareScan(
	s3ObjectDetails map[string]any, //nolint:revive // existing issue.
) (string, error) {
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

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	for _, existing := range b.threatEntitySetsByDetector.Get(detectorID) {
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
	b.threatEntitySets.Put(s)

	if tags != nil {
		b.tags[b.threatEntitySetARN(detectorID, id)] = maps.Clone(tags)
	}

	return s, nil
}

// GetThreatEntitySet retrieves a threat entity set.
func (b *InMemoryBackend) GetThreatEntitySet(detectorID, setID string) (*ThreatEntitySet, error) {
	b.mu.RLock("GetThreatEntitySet")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	s, ok := b.threatEntitySets.Get(detectorKey(detectorID, setID))
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

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	items := b.threatEntitySetsByDetector.Get(detectorID)
	ids := make([]string, len(items))

	for i, s := range items {
		ids[i] = s.ThreatEntitySetID
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

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	s, ok := b.threatEntitySets.Get(detectorKey(detectorID, setID))
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

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	if !b.threatEntitySets.Delete(detectorKey(detectorID, setID)) {
		return ErrThreatEntitySetNotFound
	}

	delete(b.tags, b.threatEntitySetARN(detectorID, setID))

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

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	for _, existing := range b.trustedEntitySetsByDetector.Get(detectorID) {
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
	b.trustedEntitySets.Put(s)

	if tags != nil {
		b.tags[b.trustedEntitySetARN(detectorID, id)] = maps.Clone(tags)
	}

	return s, nil
}

// GetTrustedEntitySet retrieves a trusted entity set.
func (b *InMemoryBackend) GetTrustedEntitySet(detectorID, setID string) (*TrustedEntitySet, error) {
	b.mu.RLock("GetTrustedEntitySet")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	s, ok := b.trustedEntitySets.Get(detectorKey(detectorID, setID))
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

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	items := b.trustedEntitySetsByDetector.Get(detectorID)
	ids := make([]string, len(items))

	for i, s := range items {
		ids[i] = s.TrustedEntitySetID
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

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	s, ok := b.trustedEntitySets.Get(detectorKey(detectorID, setID))
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

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	if !b.trustedEntitySets.Delete(detectorKey(detectorID, setID)) {
		return ErrTrustedEntitySetNotFound
	}

	delete(b.tags, b.trustedEntitySetARN(detectorID, setID))

	return nil
}
