package guardduty

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	orgConfigs             map[string]*OrgConfig
	adminAccounts          map[string]*AdminAccount
	filters                map[string]map[string]*Filter
	findings               map[string]map[string]*Finding
	ipSets                 map[string]map[string]*IPSet
	threatIntelSets        map[string]map[string]*ThreatIntelSet
	tags                   map[string]map[string]string
	members                map[string]map[string]*Member
	invitations            map[string]*Invitation
	orgAdminAccounts       map[string]*OrgAdminAccount
	detectors              map[string]*Detector
	publishingDestinations map[string]map[string]*PublishingDestination
	mu                     *lockmetrics.RWMutex
	malwareScans           map[string]*MalwareScan
	malwareScanSettings    map[string]*MalwareScanSettings
	malwareProtectionPlans map[string]*MalwareProtectionPlan
	threatEntitySets       map[string]map[string]*ThreatEntitySet
	trustedEntitySets      map[string]map[string]*TrustedEntitySet
	accountID              string
	region                 string
	memberSeq              int64
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                     lockmetrics.New("guardduty"),
		accountID:              accountID,
		region:                 region,
		detectors:              make(map[string]*Detector),
		filters:                make(map[string]map[string]*Filter),
		findings:               make(map[string]map[string]*Finding),
		ipSets:                 make(map[string]map[string]*IPSet),
		threatIntelSets:        make(map[string]map[string]*ThreatIntelSet),
		tags:                   make(map[string]map[string]string),
		members:                make(map[string]map[string]*Member),
		invitations:            make(map[string]*Invitation),
		orgAdminAccounts:       make(map[string]*OrgAdminAccount),
		orgConfigs:             make(map[string]*OrgConfig),
		adminAccounts:          make(map[string]*AdminAccount),
		publishingDestinations: make(map[string]map[string]*PublishingDestination),
		malwareScans:           make(map[string]*MalwareScan),
		malwareScanSettings:    make(map[string]*MalwareScanSettings),
		malwareProtectionPlans: make(map[string]*MalwareProtectionPlan),
		threatEntitySets:       make(map[string]map[string]*ThreatEntitySet),
		trustedEntitySets:      make(map[string]map[string]*TrustedEntitySet),
	}
}

func (b *InMemoryBackend) detectorARN(id string) string {
	return fmt.Sprintf("arn:aws:guardduty:%s:%s:detector/%s", b.region, b.accountID, id)
}

func (b *InMemoryBackend) filterARN(detectorID, filterName string) string {
	return fmt.Sprintf("arn:aws:guardduty:%s:%s:detector/%s/filter/%s", b.region, b.accountID, detectorID, filterName)
}

func (b *InMemoryBackend) ipSetARN(detectorID, ipSetID string) string {
	return fmt.Sprintf("arn:aws:guardduty:%s:%s:detector/%s/ipset/%s", b.region, b.accountID, detectorID, ipSetID)
}

func (b *InMemoryBackend) threatIntelSetARN(detectorID, setID string) string {
	return fmt.Sprintf(
		"arn:aws:guardduty:%s:%s:detector/%s/threatintelset/%s",
		b.region,
		b.accountID,
		detectorID,
		setID,
	)
}

func (b *InMemoryBackend) findingARN(detectorID, findingID string) string {
	return fmt.Sprintf("arn:aws:guardduty:%s:%s:detector/%s/finding/%s", b.region, b.accountID, detectorID, findingID)
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

	if len(b.detectors) > 0 {
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
	b.detectors[id] = d
	b.filters[id] = make(map[string]*Filter)
	b.findings[id] = make(map[string]*Finding)
	b.ipSets[id] = make(map[string]*IPSet)
	b.threatIntelSets[id] = make(map[string]*ThreatIntelSet)
	b.members[id] = make(map[string]*Member)
	b.publishingDestinations[id] = make(map[string]*PublishingDestination)
	b.threatEntitySets[id] = make(map[string]*ThreatEntitySet)
	b.trustedEntitySets[id] = make(map[string]*TrustedEntitySet)

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

	d, ok := b.detectors[detectorID]
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

	d, ok := b.detectors[detectorID]
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

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	delete(b.detectors, detectorID)
	delete(b.filters, detectorID)
	delete(b.findings, detectorID)
	delete(b.ipSets, detectorID)
	delete(b.threatIntelSets, detectorID)
	delete(b.members, detectorID)
	delete(b.publishingDestinations, detectorID)
	delete(b.threatEntitySets, detectorID)
	delete(b.trustedEntitySets, detectorID)
	delete(b.tags, b.detectorARN(detectorID))

	return nil
}

// ListDetectors returns all detector IDs.
func (b *InMemoryBackend) ListDetectors() []string {
	b.mu.RLock("ListDetectors")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.detectors))
	for id := range b.detectors {
		ids = append(ids, id)
	}

	sort.Strings(ids)

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

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	if _, ok := b.filters[detectorID][name]; ok {
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
	b.filters[detectorID][name] = f

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

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	f, ok := b.filters[detectorID][filterName]
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

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	f, ok := b.filters[detectorID][filterName]
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

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	if _, ok := b.filters[detectorID][filterName]; !ok {
		return ErrFilterNotFound
	}

	delete(b.filters[detectorID], filterName)
	delete(b.tags, b.filterARN(detectorID, filterName))

	return nil
}

// ListFilters returns filter names for a detector.
func (b *InMemoryBackend) ListFilters(detectorID string) ([]string, error) {
	b.mu.RLock("ListFilters")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	names := make([]string, 0, len(b.filters[detectorID]))
	for name := range b.filters[detectorID] {
		names = append(names, name)
	}

	sort.Strings(names)

	return names, nil
}

// GetFindings returns findings by IDs.
func (b *InMemoryBackend) GetFindings(detectorID string, findingIDs []string) ([]*Finding, error) {
	b.mu.RLock("GetFindings")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	results := make([]*Finding, 0, len(findingIDs))

	for _, id := range findingIDs {
		f, ok := b.findings[detectorID][id]
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

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	ids := make([]string, 0, len(b.findings[detectorID]))
	for id := range b.findings[detectorID] {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids, nil
}

// ArchiveFindings marks findings as archived.
func (b *InMemoryBackend) ArchiveFindings(detectorID string, findingIDs []string) error {
	b.mu.Lock("ArchiveFindings")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	for _, id := range findingIDs {
		if f, ok := b.findings[detectorID][id]; ok {
			f.Service.Archived = true
		}
	}

	return nil
}

// UnarchiveFindings marks findings as unarchived.
func (b *InMemoryBackend) UnarchiveFindings(detectorID string, findingIDs []string) error {
	b.mu.Lock("UnarchiveFindings")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	for _, id := range findingIDs {
		if f, ok := b.findings[detectorID][id]; ok {
			f.Service.Archived = false
		}
	}

	return nil
}

// CreateSampleFindings creates sample findings for a detector.
func (b *InMemoryBackend) CreateSampleFindings(detectorID string, findingTypes []string) error {
	b.mu.Lock("CreateSampleFindings")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	now := time.Now().UTC().Format(time.RFC3339)

	if len(findingTypes) == 0 {
		findingTypes = []string{"UnauthorizedAccess:IAMUser/ConsoleLoginSuccess.B"}
	}

	for _, ft := range findingTypes {
		id := strings.ReplaceAll(uuid.New().String(), "-", "")
		b.findings[detectorID][id] = &Finding{
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
		}
	}

	return nil
}

// GetFindingsStatistics returns finding statistics for a detector.
func (b *InMemoryBackend) GetFindingsStatistics(detectorID string) (map[string]any, error) {
	b.mu.RLock("GetFindingsStatistics")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	countBySeverity := map[string]int{
		"Low":    0,
		"Medium": 0,
		"High":   0,
	}

	for _, f := range b.findings[detectorID] {
		switch {
		case f.Severity < severityLowThreshold:
			countBySeverity["Low"]++
		case f.Severity < severityHighThreshold:
			countBySeverity["Medium"]++
		default:
			countBySeverity["High"]++
		}
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

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	_ = findingIDs
	_ = feedback

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

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	for _, existing := range b.ipSets[detectorID] {
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
	b.ipSets[detectorID][id] = s

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

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	s, ok := b.ipSets[detectorID][ipSetID]
	if !ok {
		return nil, ErrIPSetNotFound
	}

	return s, nil
}

// UpdateIPSet updates an IP set.
func (b *InMemoryBackend) UpdateIPSet(detectorID, ipSetID, name, location string, activate *bool) error {
	b.mu.Lock("UpdateIPSet")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	s, ok := b.ipSets[detectorID][ipSetID]
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

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	if _, ok := b.ipSets[detectorID][ipSetID]; !ok {
		return ErrIPSetNotFound
	}

	delete(b.ipSets[detectorID], ipSetID)
	delete(b.tags, b.ipSetARN(detectorID, ipSetID))

	return nil
}

// ListIPSets returns IP set IDs for a detector.
func (b *InMemoryBackend) ListIPSets(detectorID string) ([]string, error) {
	b.mu.RLock("ListIPSets")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	ids := make([]string, 0, len(b.ipSets[detectorID]))
	for id := range b.ipSets[detectorID] {
		ids = append(ids, id)
	}

	sort.Strings(ids)

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

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	for _, existing := range b.threatIntelSets[detectorID] {
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
	b.threatIntelSets[detectorID][id] = s

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

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	s, ok := b.threatIntelSets[detectorID][setID]
	if !ok {
		return nil, ErrThreatIntelSetNotFound
	}

	return s, nil
}

// UpdateThreatIntelSet updates a threat intelligence set.
func (b *InMemoryBackend) UpdateThreatIntelSet(detectorID, setID, name, location string, activate *bool) error {
	b.mu.Lock("UpdateThreatIntelSet")
	defer b.mu.Unlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	s, ok := b.threatIntelSets[detectorID][setID]
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

	if _, ok := b.detectors[detectorID]; !ok {
		return ErrDetectorNotFound
	}

	if _, ok := b.threatIntelSets[detectorID][setID]; !ok {
		return ErrThreatIntelSetNotFound
	}

	delete(b.threatIntelSets[detectorID], setID)
	delete(b.tags, b.threatIntelSetARN(detectorID, setID))

	return nil
}

// ListThreatIntelSets returns threat intel set IDs for a detector.
func (b *InMemoryBackend) ListThreatIntelSets(detectorID string) ([]string, error) {
	b.mu.RLock("ListThreatIntelSets")
	defer b.mu.RUnlock()

	if _, ok := b.detectors[detectorID]; !ok {
		return nil, ErrDetectorNotFound
	}

	ids := make([]string, 0, len(b.threatIntelSets[detectorID]))
	for id := range b.threatIntelSets[detectorID] {
		ids = append(ids, id)
	}

	sort.Strings(ids)

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

	b.detectors = make(map[string]*Detector)
	b.filters = make(map[string]map[string]*Filter)
	b.findings = make(map[string]map[string]*Finding)
	b.ipSets = make(map[string]map[string]*IPSet)
	b.threatIntelSets = make(map[string]map[string]*ThreatIntelSet)
	b.tags = make(map[string]map[string]string)
	b.members = make(map[string]map[string]*Member)
	b.invitations = make(map[string]*Invitation)
	b.orgAdminAccounts = make(map[string]*OrgAdminAccount)
	b.orgConfigs = make(map[string]*OrgConfig)
	b.adminAccounts = make(map[string]*AdminAccount)
	b.publishingDestinations = make(map[string]map[string]*PublishingDestination)
	b.malwareScans = make(map[string]*MalwareScan)
	b.malwareScanSettings = make(map[string]*MalwareScanSettings)
	b.malwareProtectionPlans = make(map[string]*MalwareProtectionPlan)
	b.threatEntitySets = make(map[string]map[string]*ThreatEntitySet)
	b.trustedEntitySets = make(map[string]map[string]*TrustedEntitySet)
	b.memberSeq = 0
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	type snap struct {
		Detectors              map[string]*Detector                         `json:"detectors"`
		Filters                map[string]map[string]*Filter                `json:"filters"`
		Findings               map[string]map[string]*Finding               `json:"findings"`
		IPSets                 map[string]map[string]*IPSet                 `json:"ipSets"`
		ThreatIntelSets        map[string]map[string]*ThreatIntelSet        `json:"threatIntelSets"`
		Tags                   map[string]map[string]string                 `json:"tags"`
		Members                map[string]map[string]*Member                `json:"members"`
		Invitations            map[string]*Invitation                       `json:"invitations"`
		OrgAdminAccounts       map[string]*OrgAdminAccount                  `json:"orgAdminAccounts"`
		OrgConfigs             map[string]*OrgConfig                        `json:"orgConfigs"`
		AdminAccounts          map[string]*AdminAccount                     `json:"adminAccounts"`
		PublishingDestinations map[string]map[string]*PublishingDestination `json:"publishingDestinations"`
		MalwareScans           map[string]*MalwareScan                      `json:"malwareScans"`
		MalwareScanSettings    map[string]*MalwareScanSettings              `json:"malwareScanSettings"`
		MalwareProtectionPlans map[string]*MalwareProtectionPlan            `json:"malwareProtectionPlans"`
		ThreatEntitySets       map[string]map[string]*ThreatEntitySet       `json:"threatEntitySets"`
		TrustedEntitySets      map[string]map[string]*TrustedEntitySet      `json:"trustedEntitySets"`
		MemberSeq              int64                                        `json:"memberSeq"`
	}

	data, _ := json.Marshal(snap{
		Detectors:              b.detectors,
		Filters:                b.filters,
		Findings:               b.findings,
		IPSets:                 b.ipSets,
		ThreatIntelSets:        b.threatIntelSets,
		Tags:                   b.tags,
		Members:                b.members,
		Invitations:            b.invitations,
		OrgAdminAccounts:       b.orgAdminAccounts,
		OrgConfigs:             b.orgConfigs,
		AdminAccounts:          b.adminAccounts,
		PublishingDestinations: b.publishingDestinations,
		MalwareScans:           b.malwareScans,
		MalwareScanSettings:    b.malwareScanSettings,
		MalwareProtectionPlans: b.malwareProtectionPlans,
		ThreatEntitySets:       b.threatEntitySets,
		TrustedEntitySets:      b.trustedEntitySets,
		MemberSeq:              b.memberSeq,
	})

	return data
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	type snap struct {
		Detectors              map[string]*Detector                         `json:"detectors"`
		Filters                map[string]map[string]*Filter                `json:"filters"`
		Findings               map[string]map[string]*Finding               `json:"findings"`
		IPSets                 map[string]map[string]*IPSet                 `json:"ipSets"`
		ThreatIntelSets        map[string]map[string]*ThreatIntelSet        `json:"threatIntelSets"`
		Tags                   map[string]map[string]string                 `json:"tags"`
		Members                map[string]map[string]*Member                `json:"members"`
		Invitations            map[string]*Invitation                       `json:"invitations"`
		OrgAdminAccounts       map[string]*OrgAdminAccount                  `json:"orgAdminAccounts"`
		OrgConfigs             map[string]*OrgConfig                        `json:"orgConfigs"`
		AdminAccounts          map[string]*AdminAccount                     `json:"adminAccounts"`
		PublishingDestinations map[string]map[string]*PublishingDestination `json:"publishingDestinations"`
		MalwareScans           map[string]*MalwareScan                      `json:"malwareScans"`
		MalwareScanSettings    map[string]*MalwareScanSettings              `json:"malwareScanSettings"`
		MalwareProtectionPlans map[string]*MalwareProtectionPlan            `json:"malwareProtectionPlans"`
		ThreatEntitySets       map[string]map[string]*ThreatEntitySet       `json:"threatEntitySets"`
		TrustedEntitySets      map[string]map[string]*TrustedEntitySet      `json:"trustedEntitySets"`
		MemberSeq              int64                                        `json:"memberSeq"`
	}

	var s snap
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	b.detectors = s.Detectors
	b.filters = s.Filters
	b.findings = s.Findings
	b.ipSets = s.IPSets
	b.threatIntelSets = s.ThreatIntelSets
	b.tags = s.Tags
	b.members = s.Members
	b.invitations = s.Invitations
	b.orgAdminAccounts = s.OrgAdminAccounts
	b.orgConfigs = s.OrgConfigs
	b.adminAccounts = s.AdminAccounts
	b.publishingDestinations = s.PublishingDestinations
	b.malwareScans = s.MalwareScans
	b.malwareScanSettings = s.MalwareScanSettings
	b.malwareProtectionPlans = s.MalwareProtectionPlans
	b.threatEntitySets = s.ThreatEntitySets
	b.trustedEntitySets = s.TrustedEntitySets
	b.memberSeq = s.MemberSeq

	return nil
}
