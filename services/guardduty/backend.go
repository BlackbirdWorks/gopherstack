package guardduty

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	defaultFindingSeverity = 5.0
	severityLowThreshold   = 4.0
	severityHighThreshold  = 7.0

	statusEnabled  = "ENABLED"
	statusDisabled = "DISABLED"
	statusActive   = "ACTIVE"
	statusInactive = "INACTIVE"
	freqSixHours   = "SIX_HOURS"

	errResourceNotFound  = "ResourceNotFoundException"
	errConflictException = "ConflictException"
)

var (
	// ErrDetectorNotFound is returned when a detector does not exist.
	ErrDetectorNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrDetectorAlreadyExists is returned when a detector already exists.
	ErrDetectorAlreadyExists = awserr.New(errConflictException, awserr.ErrConflict)
	// ErrFilterNotFound is returned when a filter does not exist.
	ErrFilterNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrFilterAlreadyExists is returned when a filter already exists.
	ErrFilterAlreadyExists = awserr.New(errConflictException, awserr.ErrConflict)
	// ErrFindingNotFound is returned when a finding does not exist.
	ErrFindingNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrIPSetNotFound is returned when an IP set does not exist.
	ErrIPSetNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrIPSetAlreadyExists is returned when an IP set already exists.
	ErrIPSetAlreadyExists = awserr.New(errConflictException, awserr.ErrConflict)
	// ErrThreatIntelSetNotFound is returned when a threat intel set does not exist.
	ErrThreatIntelSetNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrThreatIntelSetAlreadyExists is returned when a threat intel set already exists.
	ErrThreatIntelSetAlreadyExists = awserr.New(errConflictException, awserr.ErrConflict)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)

// Detector represents a GuardDuty detector.
type Detector struct {
	CreatedAt                  time.Time         `json:"createdAt"`
	UpdatedAt                  time.Time         `json:"updatedAt"`
	DetectorID                 string            `json:"detectorId"`
	Status                     string            `json:"status"`
	FindingPublishingFrequency string            `json:"findingPublishingFrequency,omitempty"`
	ServiceRole                string            `json:"serviceRole"`
	Tags                       map[string]string `json:"tags,omitempty"`
	Features                   []DetectorFeature `json:"features,omitempty"`
}

// DetectorFeature represents a feature configuration for a detector.
type DetectorFeature struct {
	Name                    string             `json:"name"`
	Status                  string             `json:"status"`
	AdditionalConfiguration []AdditionalConfig `json:"additionalConfiguration,omitempty"`
}

// AdditionalConfig holds extra feature-level configuration.
type AdditionalConfig struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

// Filter represents a GuardDuty filter.
type Filter struct {
	CreatedAt       time.Time         `json:"createdAt"`
	UpdatedAt       time.Time         `json:"updatedAt"`
	FindingCriteria map[string]any    `json:"findingCriteria,omitempty"`
	Tags            map[string]string `json:"tags,omitempty"`
	Name            string            `json:"name"`
	Description     string            `json:"description,omitempty"`
	Action          string            `json:"action"`
	DetectorID      string            `json:"-"`
	Rank            int32             `json:"rank"`
}

// Finding represents a GuardDuty finding.
type Finding struct {
	AccountID     string          `json:"accountId"`
	SchemaVersion string          `json:"schemaVersion"`
	CreatedAt     string          `json:"createdAt"`
	Description   string          `json:"description"`
	DetectorID    string          `json:"detectorId"`
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Title         string          `json:"title"`
	Region        string          `json:"region"`
	UpdatedAt     string          `json:"updatedAt"`
	Arn           string          `json:"arn"`
	Resource      FindingResource `json:"resource"`
	Service       FindingService  `json:"service"`
	Severity      float64         `json:"severity"`
}

// FindingService holds service-level metadata for a finding.
type FindingService struct {
	DetectorID     string `json:"detectorId"`
	EventFirstSeen string `json:"eventFirstSeen"`
	EventLastSeen  string `json:"eventLastSeen"`
	ResourceRole   string `json:"resourceRole"`
	ServiceName    string `json:"serviceName"`
	UserFeedback   string `json:"userFeedback,omitempty"`
	Count          int32  `json:"count"`
	Archived       bool   `json:"archived"`
}

// FindingResource describes the AWS resource involved in a finding.
type FindingResource struct {
	ResourceType string `json:"resourceType"`
}

// IPSet represents a GuardDuty IP set.
type IPSet struct {
	CreatedAt  time.Time         `json:"createdAt"`
	UpdatedAt  time.Time         `json:"updatedAt"`
	IPSetID    string            `json:"ipSetId"`
	Name       string            `json:"name"`
	Format     string            `json:"format"`
	Location   string            `json:"location"`
	Status     string            `json:"status"`
	Tags       map[string]string `json:"tags,omitempty"`
	DetectorID string            `json:"-"`
}

// ThreatIntelSet represents a GuardDuty threat intelligence set.
type ThreatIntelSet struct {
	CreatedAt        time.Time         `json:"createdAt"`
	UpdatedAt        time.Time         `json:"updatedAt"`
	ThreatIntelSetID string            `json:"threatIntelSetId"`
	Name             string            `json:"name"`
	Format           string            `json:"format"`
	Location         string            `json:"location"`
	Status           string            `json:"status"`
	Tags             map[string]string `json:"tags,omitempty"`
	DetectorID       string            `json:"-"`
}

// InMemoryBackend implements StorageBackend using pkgs/store tables.
//
// See store_setup.go for how the tables/indexes below are registered and
// persistence.go for how they are snapshotted/restored. tags (a
// map[string]string value map, not *T) and memberSeq (a scalar counter) are
// the only state left as plain fields -- see persistence.go's file doc
// comment for the per-field persistence audit.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	// detectors is a "clean" table: DetectorID is a real, wire-visible
	// identity field, so it is registered directly on registry.
	detectors *store.Table[Detector]

	// filters, ipSets, and threatIntelSets were detector-nested maps
	// (map[string]map[string]*T). Each is now a single flat table keyed by
	// the composite "detectorID|id" string (see detectorKey), with a
	// companion byDetector index replacing the per-detector scans the
	// nested maps used to answer directly. DetectorID is hidden from the
	// wire shape (json:"-"), so these are "dirty" tables -- not registered
	// on registry directly (see persistence.go's DTO wrapping).
	filters           *store.Table[Filter]
	filtersByDetector *store.Index[Filter]

	ipSets           *store.Table[IPSet]
	ipSetsByDetector *store.Index[IPSet]

	threatIntelSets           *store.Table[ThreatIntelSet]
	threatIntelSetsByDetector *store.Index[ThreatIntelSet]

	// findings and members were also detector-nested, but Finding.DetectorID
	// and Member.DetectorID are real (non-hidden) wire fields, so both are
	// "clean" composite-keyed tables registered directly on registry.
	findings           *store.Table[Finding]
	findingsByDetector *store.Index[Finding]

	members           *store.Table[Member]
	membersByDetector *store.Index[Member]

	// tags is a non-*T value map (map[string]string), so it does not fit
	// store.Table's keyed-by-identity-value shape; it remains a plain map,
	// persisted directly (see persistence.go).
	tags map[string]map[string]string

	// invitations and orgAdminAccounts are flat (not detector-nested) maps
	// whose value types carry a real identity field (InvitationID,
	// AdminAccountID), so both are "clean" tables registered directly.
	invitations      *store.Table[Invitation]
	orgAdminAccounts *store.Table[OrgAdminAccount]

	// orgConfigs and adminAccounts were flat maps keyed by detectorID, but
	// OrgConfig and AdminAccount carry no identity field of their own
	// (identity-less). Each gained an unexported detectorID field purely
	// for the table's key; being unexported it never round-trips through a
	// direct json.Marshal of the value, so both are "dirty" tables (see
	// persistence.go's DTO wrapping).
	orgConfigs    *store.Table[OrgConfig]
	adminAccounts *store.Table[AdminAccount]

	// publishingDestinations, threatEntitySets, and trustedEntitySets are
	// detector-nested maps whose value types hide DetectorID (json:"-"),
	// same treatment as filters/ipSets/threatIntelSets above.
	publishingDestinations           *store.Table[PublishingDestination]
	publishingDestinationsByDetector *store.Index[PublishingDestination]

	threatEntitySets           *store.Table[ThreatEntitySet]
	threatEntitySetsByDetector *store.Index[ThreatEntitySet]

	trustedEntitySets           *store.Table[TrustedEntitySet]
	trustedEntitySetsByDetector *store.Index[TrustedEntitySet]

	// malwareScans and malwareProtectionPlans are flat maps whose value
	// types carry a real identity field (ScanID, MalwareProtectionPlanID),
	// so both are "clean" tables registered directly.
	malwareScans           *store.Table[MalwareScan]
	malwareProtectionPlans *store.Table[MalwareProtectionPlan]

	// malwareScanSettings was a flat map keyed by detectorID; like
	// orgConfigs/adminAccounts, MalwareScanSettings is identity-less and
	// gained an unexported detectorID field, making it a "dirty" table.
	malwareScanSettings *store.Table[MalwareScanSettings]

	accountID string
	region    string
	memberSeq int64
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("guardduty"),
		accountID: accountID,
		region:    region,
		registry:  store.NewRegistry(),
		tags:      make(map[string]map[string]string),
	}
	registerAllTables(b)

	return b
}

// detectorKey builds the composite primary key used by every detector-nested
// table (filters, findings, ipSets, threatIntelSets, members,
// publishingDestinations, threatEntitySets, trustedEntitySets).
func detectorKey(detectorID, id string) string { return detectorID + "|" + id }

func (b *InMemoryBackend) detectorARN(id string) string {
	return arn.Build("guardduty", b.region, b.accountID, fmt.Sprintf("detector/%s", id))
}

func (b *InMemoryBackend) filterARN(detectorID, filterName string) string {
	return arn.Build("guardduty", b.region, b.accountID, fmt.Sprintf("detector/%s/filter/%s", detectorID, filterName))
}

func (b *InMemoryBackend) ipSetARN(detectorID, ipSetID string) string {
	return arn.Build("guardduty", b.region, b.accountID, fmt.Sprintf("detector/%s/ipset/%s", detectorID, ipSetID))
}

func (b *InMemoryBackend) threatIntelSetARN(detectorID, setID string) string {
	return arn.Build(
		"guardduty", b.region, b.accountID,
		fmt.Sprintf("detector/%s/threatintelset/%s", detectorID, setID),
	)
}

func (b *InMemoryBackend) findingARN(detectorID, findingID string) string {
	return arn.Build("guardduty", b.region, b.accountID, fmt.Sprintf("detector/%s/finding/%s", detectorID, findingID))
}

// CreateDetector creates a new GuardDuty detector for this account+region.
func (b *InMemoryBackend) CreateDetector(
	enable bool,
	frequency string,
	tags map[string]string,
	features []DetectorFeature,
) (*Detector, error) {
	b.mu.Lock("CreateDetector")
	defer b.mu.Unlock()

	if b.detectors.Len() > 0 {
		return nil, ErrDetectorAlreadyExists
	}

	if frequency == "" {
		frequency = freqSixHours
	}

	status := statusDisabled
	if enable {
		status = statusEnabled
	}

	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	now := time.Now().UTC()
	d := &Detector{
		DetectorID:                 id,
		Status:                     status,
		FindingPublishingFrequency: frequency,
		ServiceRole: fmt.Sprintf(
			"arn:aws:iam::%s:role/aws-service-role/guardduty.amazonaws.com/AWSServiceRoleForAmazonGuardDuty",
			b.accountID,
		),
		CreatedAt: now,
		UpdatedAt: now,
		Tags:      tags,
		Features:  features,
	}
	b.detectors.Put(d)

	arn := b.detectorARN(id)
	if tags != nil {
		b.tags[arn] = maps.Clone(tags)
	}

	return d, nil
}

// GetDetector retrieves a detector by ID.
func (b *InMemoryBackend) GetDetector(detectorID string) (*Detector, error) {
	b.mu.RLock("GetDetector")
	defer b.mu.RUnlock()

	d, ok := b.detectors.Get(detectorID)
	if !ok {
		return nil, ErrDetectorNotFound
	}

	return d, nil
}

// UpdateDetector updates a detector's configuration.
func (b *InMemoryBackend) UpdateDetector(
	detectorID string,
	enable *bool,
	frequency string,
	features []DetectorFeature,
) error {
	b.mu.Lock("UpdateDetector")
	defer b.mu.Unlock()

	d, ok := b.detectors.Get(detectorID)
	if !ok {
		return ErrDetectorNotFound
	}

	if enable != nil {
		if *enable {
			d.Status = statusEnabled
		} else {
			d.Status = statusDisabled
		}
	}

	if frequency != "" {
		d.FindingPublishingFrequency = frequency
	}

	if features != nil {
		d.Features = features
	}

	d.UpdatedAt = time.Now().UTC()

	return nil
}

// DeleteDetector removes a detector.
func (b *InMemoryBackend) DeleteDetector(detectorID string) error {
	b.mu.Lock("DeleteDetector")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	b.detectors.Delete(detectorID)

	// slices.Clone: Index.Get's returned slice mutates under Delete, so it
	// must be cloned before the delete loop below.
	for _, f := range slices.Clone(b.filtersByDetector.Get(detectorID)) {
		b.filters.Delete(detectorKey(detectorID, f.Name))
	}

	for _, f := range slices.Clone(b.findingsByDetector.Get(detectorID)) {
		b.findings.Delete(detectorKey(detectorID, f.ID))
	}

	for _, s := range slices.Clone(b.ipSetsByDetector.Get(detectorID)) {
		b.ipSets.Delete(detectorKey(detectorID, s.IPSetID))
	}

	for _, s := range slices.Clone(b.threatIntelSetsByDetector.Get(detectorID)) {
		b.threatIntelSets.Delete(detectorKey(detectorID, s.ThreatIntelSetID))
	}

	for _, m := range slices.Clone(b.membersByDetector.Get(detectorID)) {
		b.members.Delete(detectorKey(detectorID, m.AccountID))
	}

	for _, d := range slices.Clone(b.publishingDestinationsByDetector.Get(detectorID)) {
		b.publishingDestinations.Delete(detectorKey(detectorID, d.DestinationID))
	}

	for _, s := range slices.Clone(b.threatEntitySetsByDetector.Get(detectorID)) {
		b.threatEntitySets.Delete(detectorKey(detectorID, s.ThreatEntitySetID))
	}

	for _, s := range slices.Clone(b.trustedEntitySetsByDetector.Get(detectorID)) {
		b.trustedEntitySets.Delete(detectorKey(detectorID, s.TrustedEntitySetID))
	}

	delete(b.tags, b.detectorARN(detectorID))

	return nil
}

// ListDetectors returns all detector IDs.
func (b *InMemoryBackend) ListDetectors() []string {
	b.mu.RLock("ListDetectors")
	defer b.mu.RUnlock()

	items := b.detectors.Snapshot()
	ids := make([]string, len(items))

	for i, d := range items {
		ids[i] = d.DetectorID
	}

	return ids
}

// CreateFilter creates a new filter for a detector.
func (b *InMemoryBackend) CreateFilter(
	detectorID, name, description, action string,
	rank int32,
	findingCriteria map[string]any,
	tags map[string]string,
) (*Filter, error) {
	b.mu.Lock("CreateFilter")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	if b.filters.Has(detectorKey(detectorID, name)) {
		return nil, ErrFilterAlreadyExists
	}

	now := time.Now().UTC()
	f := &Filter{
		Name:            name,
		Description:     description,
		Action:          action,
		Rank:            rank,
		FindingCriteria: findingCriteria,
		Tags:            tags,
		DetectorID:      detectorID,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	b.filters.Put(f)

	arn := b.filterARN(detectorID, name)
	if tags != nil {
		b.tags[arn] = maps.Clone(tags)
	}

	return f, nil
}

// GetFilter retrieves a filter.
func (b *InMemoryBackend) GetFilter(detectorID, filterName string) (*Filter, error) {
	b.mu.RLock("GetFilter")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	f, ok := b.filters.Get(detectorKey(detectorID, filterName))
	if !ok {
		return nil, ErrFilterNotFound
	}

	return f, nil
}

// UpdateFilter updates a filter's configuration.
func (b *InMemoryBackend) UpdateFilter(
	detectorID, filterName, description, action string,
	rank int32,
	findingCriteria map[string]any,
) (*Filter, error) {
	b.mu.Lock("UpdateFilter")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	f, ok := b.filters.Get(detectorKey(detectorID, filterName))
	if !ok {
		return nil, ErrFilterNotFound
	}

	if description != "" {
		f.Description = description
	}

	if action != "" {
		f.Action = action
	}

	if rank > 0 {
		f.Rank = rank
	}

	if findingCriteria != nil {
		f.FindingCriteria = findingCriteria
	}

	f.UpdatedAt = time.Now().UTC()

	return f, nil
}

// DeleteFilter removes a filter.
func (b *InMemoryBackend) DeleteFilter(detectorID, filterName string) error {
	b.mu.Lock("DeleteFilter")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	if !b.filters.Delete(detectorKey(detectorID, filterName)) {
		return ErrFilterNotFound
	}

	delete(b.tags, b.filterARN(detectorID, filterName))

	return nil
}

// ListFilters returns filter names for a detector.
func (b *InMemoryBackend) ListFilters(detectorID string) ([]string, error) {
	b.mu.RLock("ListFilters")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	items := b.filtersByDetector.Get(detectorID)
	names := make([]string, len(items))

	for i, f := range items {
		names[i] = f.Name
	}

	slices.Sort(names)

	return names, nil
}

// GetFindings returns findings by IDs.
func (b *InMemoryBackend) GetFindings(detectorID string, findingIDs []string) ([]*Finding, error) {
	b.mu.RLock("GetFindings")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	results := make([]*Finding, 0, len(findingIDs))

	for _, id := range findingIDs {
		f, ok := b.findings.Get(detectorKey(detectorID, id))
		if !ok {
			return nil, ErrFindingNotFound
		}

		results = append(results, f)
	}

	return results, nil
}

// ListFindings returns finding IDs for a detector.
func (b *InMemoryBackend) ListFindings(detectorID string) ([]string, error) {
	b.mu.RLock("ListFindings")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	items := b.findingsByDetector.Get(detectorID)
	ids := make([]string, len(items))

	for i, f := range items {
		ids[i] = f.ID
	}

	slices.Sort(ids)

	return ids, nil
}

// ArchiveFindings marks findings as archived.
func (b *InMemoryBackend) ArchiveFindings(detectorID string, findingIDs []string) error {
	b.mu.Lock("ArchiveFindings")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)

	for _, id := range findingIDs {
		if f, ok := b.findings.Get(detectorKey(detectorID, id)); ok {
			f.Service.Archived = true
			f.UpdatedAt = now
		}
	}

	return nil
}

// UnarchiveFindings marks findings as unarchived.
func (b *InMemoryBackend) UnarchiveFindings(detectorID string, findingIDs []string) error {
	b.mu.Lock("UnarchiveFindings")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)

	for _, id := range findingIDs {
		if f, ok := b.findings.Get(detectorKey(detectorID, id)); ok {
			f.Service.Archived = false
			f.UpdatedAt = now
		}
	}

	return nil
}

// CreateSampleFindings creates sample findings for a detector.
func (b *InMemoryBackend) CreateSampleFindings(detectorID string, findingTypes []string) error {
	b.mu.Lock("CreateSampleFindings")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if len(findingTypes) == 0 {
		findingTypes = []string{"UnauthorizedAccess:IAMUser/ConsoleLoginSuccess.B"}
	}

	for _, ft := range findingTypes {
		id := strings.ReplaceAll(uuid.New().String(), "-", "")
		b.findings.Put(&Finding{
			AccountID:     b.accountID,
			Arn:           b.findingARN(detectorID, id),
			CreatedAt:     now,
			Description:   "Sample finding: " + ft,
			DetectorID:    detectorID,
			ID:            id,
			Region:        b.region,
			Severity:      defaultFindingSeverity,
			Title:         "Sample: " + ft,
			Type:          ft,
			UpdatedAt:     now,
			SchemaVersion: "2.0",
			Service: FindingService{
				Archived:       false,
				Count:          1,
				DetectorID:     detectorID,
				EventFirstSeen: now,
				EventLastSeen:  now,
				ResourceRole:   "TARGET",
				ServiceName:    "guardduty",
			},
			Resource: FindingResource{
				ResourceType: "AccessKey",
			},
		})
	}

	return nil
}

// GetFindingsStatistics returns finding statistics for a detector.
func (b *InMemoryBackend) GetFindingsStatistics(detectorID string) (map[string]any, error) {
	b.mu.RLock("GetFindingsStatistics")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	countBySeverity := map[string]int{}

	for _, f := range b.findingsByDetector.Get(detectorID) {
		key := fmt.Sprintf("%.1f", f.Severity)
		countBySeverity[key]++
	}

	return map[string]any{
		"findingStatistics": map[string]any{
			"countBySeverity": countBySeverity,
		},
	}, nil
}

// UpdateFindingsFeedback updates the feedback for findings.
func (b *InMemoryBackend) UpdateFindingsFeedback(detectorID string, findingIDs []string, feedback string) error {
	b.mu.Lock("UpdateFindingsFeedback")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)

	for _, id := range findingIDs {
		if f, ok := b.findings.Get(detectorKey(detectorID, id)); ok {
			f.Service.UserFeedback = feedback
			f.UpdatedAt = now
		}
	}

	return nil
}

// CreateIPSet creates a new IP set.
//
//nolint:dupl // IPSet and ThreatIntelSet have identical creation patterns
func (b *InMemoryBackend) CreateIPSet(
	detectorID, name, format, location string,
	activate bool,
	tags map[string]string,
) (*IPSet, error) {
	b.mu.Lock("CreateIPSet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	for _, existing := range b.ipSetsByDetector.Get(detectorID) {
		if existing.Name == name {
			return nil, ErrIPSetAlreadyExists
		}
	}

	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	status := statusInactive
	if activate {
		status = statusActive
	}

	now := time.Now().UTC()
	s := &IPSet{
		IPSetID:    id,
		Name:       name,
		Format:     format,
		Location:   location,
		Status:     status,
		Tags:       tags,
		DetectorID: detectorID,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	b.ipSets.Put(s)

	arn := b.ipSetARN(detectorID, id)
	if tags != nil {
		b.tags[arn] = maps.Clone(tags)
	}

	return s, nil
}

// GetIPSet retrieves an IP set.
func (b *InMemoryBackend) GetIPSet(detectorID, ipSetID string) (*IPSet, error) {
	b.mu.RLock("GetIPSet")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	s, ok := b.ipSets.Get(detectorKey(detectorID, ipSetID))
	if !ok {
		return nil, ErrIPSetNotFound
	}

	return s, nil
}

// UpdateIPSet updates an IP set.
func (b *InMemoryBackend) UpdateIPSet(detectorID, ipSetID, name, location string, activate *bool) error {
	b.mu.Lock("UpdateIPSet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	s, ok := b.ipSets.Get(detectorKey(detectorID, ipSetID))
	if !ok {
		return ErrIPSetNotFound
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

// DeleteIPSet removes an IP set.
func (b *InMemoryBackend) DeleteIPSet(detectorID, ipSetID string) error {
	b.mu.Lock("DeleteIPSet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	if !b.ipSets.Delete(detectorKey(detectorID, ipSetID)) {
		return ErrIPSetNotFound
	}

	delete(b.tags, b.ipSetARN(detectorID, ipSetID))

	return nil
}

// ListIPSets returns IP set IDs for a detector.
func (b *InMemoryBackend) ListIPSets(detectorID string) ([]string, error) {
	b.mu.RLock("ListIPSets")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	items := b.ipSetsByDetector.Get(detectorID)
	ids := make([]string, len(items))

	for i, s := range items {
		ids[i] = s.IPSetID
	}

	slices.Sort(ids)

	return ids, nil
}

// CreateThreatIntelSet creates a new threat intelligence set.
//
//nolint:dupl // IPSet and ThreatIntelSet have identical creation patterns
func (b *InMemoryBackend) CreateThreatIntelSet(
	detectorID, name, format, location string,
	activate bool,
	tags map[string]string,
) (*ThreatIntelSet, error) {
	b.mu.Lock("CreateThreatIntelSet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	for _, existing := range b.threatIntelSetsByDetector.Get(detectorID) {
		if existing.Name == name {
			return nil, ErrThreatIntelSetAlreadyExists
		}
	}

	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	status := statusInactive
	if activate {
		status = statusActive
	}

	now := time.Now().UTC()
	s := &ThreatIntelSet{
		ThreatIntelSetID: id,
		Name:             name,
		Format:           format,
		Location:         location,
		Status:           status,
		Tags:             tags,
		DetectorID:       detectorID,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	b.threatIntelSets.Put(s)

	arn := b.threatIntelSetARN(detectorID, id)
	if tags != nil {
		b.tags[arn] = maps.Clone(tags)
	}

	return s, nil
}

// GetThreatIntelSet retrieves a threat intelligence set.
func (b *InMemoryBackend) GetThreatIntelSet(detectorID, setID string) (*ThreatIntelSet, error) {
	b.mu.RLock("GetThreatIntelSet")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	s, ok := b.threatIntelSets.Get(detectorKey(detectorID, setID))
	if !ok {
		return nil, ErrThreatIntelSetNotFound
	}

	return s, nil
}

// UpdateThreatIntelSet updates a threat intelligence set.
func (b *InMemoryBackend) UpdateThreatIntelSet(detectorID, setID, name, location string, activate *bool) error {
	b.mu.Lock("UpdateThreatIntelSet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	s, ok := b.threatIntelSets.Get(detectorKey(detectorID, setID))
	if !ok {
		return ErrThreatIntelSetNotFound
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

// DeleteThreatIntelSet removes a threat intelligence set.
func (b *InMemoryBackend) DeleteThreatIntelSet(detectorID, setID string) error {
	b.mu.Lock("DeleteThreatIntelSet")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	if !b.threatIntelSets.Delete(detectorKey(detectorID, setID)) {
		return ErrThreatIntelSetNotFound
	}

	delete(b.tags, b.threatIntelSetARN(detectorID, setID))

	return nil
}

// ListThreatIntelSets returns threat intel set IDs for a detector.
func (b *InMemoryBackend) ListThreatIntelSets(detectorID string) ([]string, error) {
	b.mu.RLock("ListThreatIntelSets")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	items := b.threatIntelSetsByDetector.Get(detectorID)
	ids := make([]string, len(items))

	for i, s := range items {
		ids[i] = s.ThreatIntelSetID
	}

	slices.Sort(ids)

	return ids, nil
}

// TagResource sets tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t := b.tags[resourceARN]
	if t == nil {
		return map[string]string{}, nil
	}

	return maps.Clone(t), nil
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// registry.ResetAll handles every "clean" table (detectors, findings,
	// members, invitations, orgAdminAccounts, malwareScans,
	// malwareProtectionPlans). The "dirty" tables below were deliberately
	// NOT registered on registry (see store_setup.go), so each is reset
	// individually.
	b.registry.ResetAll()
	b.filters.Reset()
	b.ipSets.Reset()
	b.threatIntelSets.Reset()
	b.publishingDestinations.Reset()
	b.threatEntitySets.Reset()
	b.trustedEntitySets.Reset()
	b.orgConfigs.Reset()
	b.adminAccounts.Reset()
	b.malwareScanSettings.Reset()
	b.tags = make(map[string]map[string]string)
	b.memberSeq = 0
}
