package inspector2

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	statusEnabled  = "ENABLED"
	statusDisabled = "DISABLED"
	statusEnabling = "ENABLING"

	ec2ScanModeEC2SSMAgentBased = "EC2_SSM_AGENT_BASED"
	ecrRescanDurationLifetime   = "LIFETIME"

	errResourceNotFound = "ResourceNotFoundException"
	errConflict         = "ConflictException"
	errValidation       = "ValidationException"

	inspector2Service = "inspector2"
)

var (
	// ErrFilterNotFound is returned when a filter does not exist.
	ErrFilterNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrFilterAlreadyExists is returned when a filter already exists.
	ErrFilterAlreadyExists = awserr.New(errConflict, awserr.ErrConflict)
	// ErrTagsResourceNotFound is returned when the tagged resource does not exist.
	ErrTagsResourceNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New(errValidation, awserr.ErrInvalidParameter)
)

// Filter represents an Inspector2 findings filter.
type Filter struct { //nolint:govet // fieldalignment: map fields after scalars for readability
	Arn         string            `json:"arn"`
	Name        string            `json:"name"`
	Action      string            `json:"action"`
	Description string            `json:"description,omitempty"`
	Reason      string            `json:"reason,omitempty"`
	OwnerId     string            `json:"ownerId"`
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
	Criteria    map[string]any    `json:"filterCriteria,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// Finding represents an Inspector2 finding (minimal stub for list support).
type Finding struct {
	FindingArn  string `json:"findingArn"`
	AccountId   string `json:"awsAccountId"`
	Type        string `json:"type"`
	Severity    string `json:"severity"`
	Status      string `json:"status"`
	Description string `json:"description"`
}

// Configuration holds Inspector2 scan configuration.
type Configuration struct {
	Ec2ScanMode       string `json:"ec2ScanMode"`
	EcrRescanDuration string `json:"ecrRescanDuration"`
}

// AccountStatusResponse holds Enable/Disable/BatchGetAccountStatus output.
type AccountStatusResponse struct {
	AccountId    string `json:"accountId"`
	Status       string `json:"status"`
	Ec2Status    string `json:"ec2Status"`
	EcrStatus    string `json:"ecrStatus"`
	LambdaStatus string `json:"lambdaStatus"`
}

// InMemoryBackend is the in-memory implementation of Inspector2.
type InMemoryBackend struct {
	mu        *lockmetrics.RWMutex
	filters   map[string]*Filter // arn → filter
	tags      map[string]map[string]string
	config    Configuration
	enabled   bool
	accountID string
	region    string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:      lockmetrics.New("inspector2"),
		filters: make(map[string]*Filter),
		tags:    make(map[string]map[string]string),
		config: Configuration{
			Ec2ScanMode:       ec2ScanModeEC2SSMAgentBased,
			EcrRescanDuration: ecrRescanDurationLifetime,
		},
		accountID: accountID,
		region:    region,
	}
}

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the backend region.
func (b *InMemoryBackend) Region() string { return b.region }

// Enable enables Inspector2 scanning for the given resource types.
func (b *InMemoryBackend) Enable(resourceTypes []string) error {
	b.mu.Lock("Enable")
	defer b.mu.Unlock()

	b.enabled = true
	_ = resourceTypes

	return nil
}

// Disable disables Inspector2 scanning for the given resource types.
func (b *InMemoryBackend) Disable(resourceTypes []string) error {
	b.mu.Lock("Disable")
	defer b.mu.Unlock()

	b.enabled = false
	_ = resourceTypes

	return nil
}

// IsEnabled returns whether Inspector2 is enabled.
func (b *InMemoryBackend) IsEnabled() bool {
	b.mu.RLock("IsEnabled")
	defer b.mu.RUnlock()

	return b.enabled
}

// GetStatus returns account status information.
func (b *InMemoryBackend) GetStatus() *AccountStatusResponse {
	b.mu.RLock("GetStatus")
	defer b.mu.RUnlock()

	status := statusDisabled
	if b.enabled {
		status = statusEnabled
	}

	return &AccountStatusResponse{
		AccountId:    b.accountID,
		Status:       status,
		Ec2Status:    status,
		EcrStatus:    status,
		LambdaStatus: status,
	}
}

func (b *InMemoryBackend) buildFilterARN() string {
	id := uuid.New().String()
	return arn.Build(inspector2Service, b.region, b.accountID, "filter/"+id)
}

// CreateFilter creates a new findings filter.
func (b *InMemoryBackend) CreateFilter(
	name, action, description, reason string,
	criteria map[string]any,
	tags map[string]string,
) (*Filter, error) {
	b.mu.Lock("CreateFilter")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrValidation
	}

	for _, f := range b.filters {
		if f.Name == name {
			return nil, ErrFilterAlreadyExists
		}
	}

	filterARN := b.buildFilterARN()
	now := time.Now().UTC()

	f := &Filter{
		Arn:         filterARN,
		Name:        name,
		Action:      action,
		Description: description,
		Reason:      reason,
		OwnerId:     b.accountID,
		CreatedAt:   now,
		UpdatedAt:   now,
		Criteria:    criteria,
		Tags:        tags,
	}

	b.filters[filterARN] = f
	if len(tags) > 0 {
		b.tags[filterARN] = maps.Clone(tags)
	}

	return f, nil
}

// UpdateFilter updates an existing filter.
func (b *InMemoryBackend) UpdateFilter(
	filterARN, action, description, reason string,
	criteria map[string]any,
) (*Filter, error) {
	b.mu.Lock("UpdateFilter")
	defer b.mu.Unlock()

	f, ok := b.filters[filterARN]
	if !ok {
		return nil, ErrFilterNotFound
	}

	if action != "" {
		f.Action = action
	}

	if description != "" {
		f.Description = description
	}

	if reason != "" {
		f.Reason = reason
	}

	if criteria != nil {
		f.Criteria = criteria
	}

	f.UpdatedAt = time.Now().UTC()

	return f, nil
}

// DeleteFilter deletes a filter by ARN.
func (b *InMemoryBackend) DeleteFilter(filterARN string) error {
	b.mu.Lock("DeleteFilter")
	defer b.mu.Unlock()

	if _, ok := b.filters[filterARN]; !ok {
		return ErrFilterNotFound
	}

	delete(b.filters, filterARN)
	delete(b.tags, filterARN)

	return nil
}

// ListFilters returns all filters, optionally filtered by ARNs and action.
func (b *InMemoryBackend) ListFilters(arns []string, action string) ([]*Filter, error) {
	b.mu.RLock("ListFilters")
	defer b.mu.RUnlock()

	arnSet := make(map[string]bool, len(arns))
	for _, a := range arns {
		arnSet[a] = true
	}

	var result []*Filter

	sortedARNs := make([]string, 0, len(b.filters))
	for a := range b.filters {
		sortedARNs = append(sortedARNs, a)
	}

	sort.Strings(sortedARNs)

	for _, a := range sortedARNs {
		f := b.filters[a]

		if len(arnSet) > 0 && !arnSet[f.Arn] {
			continue
		}

		if action != "" && f.Action != action {
			continue
		}

		result = append(result, f)
	}

	return result, nil
}

// ListFindings returns a page of findings (stub — always empty in this implementation).
func (b *InMemoryBackend) ListFindings(maxResults int32, nextToken string) ([]*Finding, string, error) {
	b.mu.RLock("ListFindings")
	defer b.mu.RUnlock()

	return []*Finding{}, "", nil
}

// GetConfiguration returns the current configuration.
func (b *InMemoryBackend) GetConfiguration() *Configuration {
	b.mu.RLock("GetConfiguration")
	defer b.mu.RUnlock()

	cfg := b.config

	return &cfg
}

// UpdateConfiguration updates the scan configuration.
func (b *InMemoryBackend) UpdateConfiguration(ec2ScanMode, ecrRescanDuration string) error {
	b.mu.Lock("UpdateConfiguration")
	defer b.mu.Unlock()

	if ec2ScanMode != "" {
		b.config.Ec2ScanMode = ec2ScanMode
	}

	if ecrRescanDuration != "" {
		b.config.EcrRescanDuration = ecrRescanDuration
	}

	return nil
}

// TagResource adds or replaces tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return ErrTagsResourceNotFound
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	for k, v := range tags {
		b.tags[resourceARN][k] = v
	}

	// Mirror tags into filter if applicable.
	if f, ok := b.filters[resourceARN]; ok {
		if f.Tags == nil {
			f.Tags = make(map[string]string)
		}

		for k, v := range tags {
			f.Tags[k] = v
		}
	}

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return ErrTagsResourceNotFound
	}

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)

		if f, ok := b.filters[resourceARN]; ok {
			delete(f.Tags, k)
		}
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if !b.resourceExists(resourceARN) {
		return nil, ErrTagsResourceNotFound
	}

	result := make(map[string]string)
	for k, v := range b.tags[resourceARN] {
		result[k] = v
	}

	return result, nil
}

// resourceExists returns true if the ARN corresponds to a known resource.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) resourceExists(resourceARN string) bool {
	if _, ok := b.filters[resourceARN]; ok {
		return true
	}

	// The account itself is always a valid tagging target for Inspector2.
	expected := fmt.Sprintf("arn:aws:%s:%s:%s:owner/%s",
		inspector2Service, b.region, b.accountID, b.accountID)
	if resourceARN == expected {
		if b.tags[resourceARN] == nil {
			return true
		}

		return true
	}

	// Accept any previously tagged ARN.
	_, tagged := b.tags[resourceARN]

	return tagged
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.filters = make(map[string]*Filter)
	b.tags = make(map[string]map[string]string)
	b.config = Configuration{Ec2ScanMode: ec2ScanModeEC2SSMAgentBased, EcrRescanDuration: ecrRescanDurationLifetime}
	b.enabled = false
}

type backendSnapshot struct {
	Filters   map[string]*Filter           `json:"filters"`
	Tags      map[string]map[string]string `json:"tags"`
	Config    Configuration                `json:"config"`
	Enabled   bool                         `json:"enabled"`
	AccountID string                       `json:"accountId"`
	Region    string                       `json:"region"`
}

// Snapshot serializes the backend state.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Filters:   b.filters,
		Tags:      b.tags,
		Config:    b.config,
		Enabled:   b.enabled,
		AccountID: b.accountID,
		Region:    b.region,
	}

	data, _ := json.Marshal(snap)

	return data
}

// Restore deserializes the backend state.
func (b *InMemoryBackend) Restore(data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	var snap backendSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("inspector2: restore: %w", err)
	}

	b.filters = snap.Filters
	b.tags = snap.Tags
	b.config = snap.Config
	b.enabled = snap.Enabled
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}
