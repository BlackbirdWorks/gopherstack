package inspector2

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	statusEnabled  = "ENABLED"
	statusDisabled = "DISABLED"

	ec2ScanModeEC2SSMAgentBased = "EC2_SSM_AGENT_BASED"
	ecrRescanDurationLifetime   = "LIFETIME"

	errResourceNotFound = "ResourceNotFoundException"
	errConflict         = "ConflictException"
	errValidation       = "ValidationException"

	inspector2Service = "inspector2"

	maxTagKeyLen   = 128
	maxTagValueLen = 256
	maxTagCount    = 50
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

// validateTags enforces AWS tag limits: key 1-128 chars, value 0-256 chars, max 50 tags.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: cannot specify more than %d tags", ErrValidation, maxTagCount)
	}

	for k, v := range tags {
		if k == "" || len(k) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key must be between 1 and %d characters",
				ErrValidation,
				maxTagKeyLen,
			)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value must be at most %d characters",
				ErrValidation,
				maxTagValueLen,
			)
		}
	}

	return nil
}

// validateFilterAction returns an error if action is not a valid Inspector2 filter action.
func validateFilterAction(action string) error {
	validActions := map[string]bool{
		"NONE":     true,
		"SUPPRESS": true,
	}
	if action == "" || validActions[action] {
		return nil
	}

	return fmt.Errorf("%w: filter action must be NONE or SUPPRESS, got %q", ErrValidation, action)
}

// Filter represents an Inspector2 findings filter.
type Filter struct {
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
	Criteria    map[string]any    `json:"filterCriteria,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Arn         string            `json:"arn"`
	Name        string            `json:"name"`
	Action      string            `json:"action"`
	Description string            `json:"description,omitempty"`
	Reason      string            `json:"reason,omitempty"`
	OwnerID     string            `json:"ownerId"`
}

// Finding represents an Inspector2 finding.
type Finding struct {
	FindingArn          string         `json:"findingArn"`
	AccountID           string         `json:"awsAccountId"`
	Type                string         `json:"type"`
	Severity            FindingSeverity `json:"severity"`
	Status              string         `json:"status"`
	Description         string         `json:"description"`
	Title               string         `json:"title,omitempty"`
	FirstObservedAt     time.Time      `json:"firstObservedAt"`
	LastObservedAt      time.Time      `json:"lastObservedAt"`
	UpdatedAt           time.Time      `json:"updatedAt"`
	Resources           []FindingResource `json:"resources,omitempty"`
}

// FindingSeverity holds severity details for a finding.
type FindingSeverity struct {
	Label  string  `json:"label"`
	Score  float64 `json:"score,omitempty"`
}

// FindingResource describes a resource associated with a finding.
type FindingResource struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// storedFinding wraps Finding for internal storage.
type storedFinding struct {
	Finding
}

// Configuration holds Inspector2 scan configuration.
type Configuration struct {
	Ec2ScanMode       string `json:"ec2ScanMode"`
	EcrRescanDuration string `json:"ecrRescanDuration"`
}

// AccountStatusResponse holds Enable/Disable/BatchGetAccountStatus output.
type AccountStatusResponse struct {
	AccountID    string `json:"accountId"`
	Status       string `json:"status"`
	Ec2Status    string `json:"ec2Status"`
	EcrStatus    string `json:"ecrStatus"`
	LambdaStatus string `json:"lambdaStatus"`
}

// InMemoryBackend is the in-memory implementation of Inspector2.
type InMemoryBackend struct {
	mu        *lockmetrics.RWMutex
	filters   map[string]*Filter
	findings  map[string]*storedFinding
	tags      map[string]map[string]string
	ax        *appendixAState
	config    Configuration
	accountID string
	region    string
	enabled   bool
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:       lockmetrics.New("inspector2"),
		filters:  make(map[string]*Filter),
		findings: make(map[string]*storedFinding),
		tags:     make(map[string]map[string]string),
		ax:       newAppendixAState(),
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
		AccountID:    b.accountID,
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

	if err := validateFilterAction(action); err != nil {
		return nil, err
	}

	if err := validateTags(tags); err != nil {
		return nil, err
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
		OwnerID:     b.accountID,
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

	if err := validateFilterAction(action); err != nil {
		return nil, err
	}

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

	sortedARNs := make([]string, 0, len(b.filters))
	for a := range b.filters {
		sortedARNs = append(sortedARNs, a)
	}

	sort.Strings(sortedARNs)

	var result []*Filter

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

// AddFinding stores a finding and returns its ARN. Used to seed test state.
func (b *InMemoryBackend) AddFinding(
	findingType, severityLabel, status, title, description string,
	resources []FindingResource,
) string {
	b.mu.Lock("AddFinding")
	defer b.mu.Unlock()

	id := uuid.New().String()
	findingARN := arn.Build(inspector2Service, b.region, b.accountID, "finding/"+id)
	now := time.Now().UTC()

	score := severityScore(severityLabel)

	b.findings[findingARN] = &storedFinding{
		Finding: Finding{
			FindingArn:      findingARN,
			AccountID:       b.accountID,
			Type:            findingType,
			Severity:        FindingSeverity{Label: severityLabel, Score: score},
			Status:          status,
			Description:     description,
			Title:           title,
			FirstObservedAt: now,
			LastObservedAt:  now,
			UpdatedAt:       now,
			Resources:       resources,
		},
	}

	return findingARN
}

// severityScore returns a numeric score for a severity label.
func severityScore(label string) float64 {
	switch label {
	case "CRITICAL":
		return 9.0
	case "HIGH":
		return 7.0
	case "MEDIUM":
		return 5.0
	case "LOW":
		return 3.0
	default:
		return 0.0
	}
}

// ListFindings returns a page of findings, optionally filtered by severity or status.
func (b *InMemoryBackend) ListFindings(filterCriteria map[string]any, maxResults int32, nextToken string) ([]*Finding, string, error) {
	b.mu.RLock("ListFindings")
	defer b.mu.RUnlock()

	all := make([]*Finding, 0, len(b.findings))

	for _, f := range b.findings {
		if !matchesFindingCriteria(&f.Finding, filterCriteria) {
			continue
		}

		cp := f.Finding
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].FindingArn < all[j].FindingArn
	})

	limit := int(maxResults)
	if limit <= 0 {
		limit = 100
	}

	start := decodeFindingToken(nextToken)
	if start >= len(all) {
		return []*Finding{}, "", nil
	}

	end := start + limit
	var next string

	if end < len(all) {
		next = encodeFindingToken(end)
	} else {
		end = len(all)
	}

	return all[start:end], next, nil
}

// matchesFindingCriteria returns true if the finding matches all filter criteria.
// Supports filtering by severityLabel and findingStatus arrays.
func matchesFindingCriteria(f *Finding, criteria map[string]any) bool {
	if len(criteria) == 0 {
		return true
	}

	if severities, ok := extractStringComparisons(criteria, "severity"); ok {
		matched := false

		for _, s := range severities {
			if strings.EqualFold(f.Severity.Label, s) {
				matched = true

				break
			}
		}

		if !matched {
			return false
		}
	}

	if statuses, ok := extractStringComparisons(criteria, "findingStatus"); ok {
		matched := false

		for _, s := range statuses {
			if strings.EqualFold(f.Status, s) {
				matched = true

				break
			}
		}

		if !matched {
			return false
		}
	}

	return true
}

// extractStringComparisons extracts EQUALS comparison values for a filter field.
func extractStringComparisons(criteria map[string]any, key string) ([]string, bool) {
	raw, ok := criteria[key]
	if !ok {
		return nil, false
	}

	items, ok := raw.([]any)
	if !ok {
		return nil, false
	}

	var vals []string

	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		if v, ok := m["value"].(string); ok {
			vals = append(vals, v)
		}
	}

	return vals, len(vals) > 0
}

func encodeFindingToken(idx int) string {
	return fmt.Sprintf("%d", idx)
}

func decodeFindingToken(token string) int {
	if token == "" {
		return 0
	}

	var idx int
	if _, err := fmt.Sscanf(token, "%d", &idx); err != nil || idx < 0 {
		return 0
	}

	return idx
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
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if !b.resourceExists(resourceARN) {
		return ErrTagsResourceNotFound
	}

	existing := b.tags[resourceARN]
	if len(existing)+len(tags) > maxTagCount {
		return fmt.Errorf(
			"%w: resource would exceed maximum of %d tags",
			ErrValidation,
			maxTagCount,
		)
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	if f, ok := b.filters[resourceARN]; ok {
		if f.Tags == nil {
			f.Tags = make(map[string]string)
		}

		maps.Copy(f.Tags, tags)
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

	result := make(map[string]string, len(b.tags[resourceARN]))
	maps.Copy(result, b.tags[resourceARN])

	return result, nil
}

// resourceExists returns true if the ARN corresponds to a known resource.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) resourceExists(resourceARN string) bool {
	if _, ok := b.filters[resourceARN]; ok {
		return true
	}

	// Accept any previously tagged ARN (including account-level ARNs).
	_, tagged := b.tags[resourceARN]

	return tagged
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.filters = make(map[string]*Filter)
	b.findings = make(map[string]*storedFinding)
	b.tags = make(map[string]map[string]string)
	b.config = Configuration{
		Ec2ScanMode:       ec2ScanModeEC2SSMAgentBased,
		EcrRescanDuration: ecrRescanDurationLifetime,
	}
	b.enabled = false
}

type backendSnapshot struct {
	Filters   map[string]*Filter           `json:"filters"`
	Findings  map[string]*storedFinding    `json:"findings"`
	Tags      map[string]map[string]string `json:"tags"`
	Config    Configuration                `json:"config"`
	AccountID string                       `json:"accountId"`
	Region    string                       `json:"region"`
	Enabled   bool                         `json:"enabled"`
}

// Snapshot serializes the backend state.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Filters:   b.filters,
		Findings:  b.findings,
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
	b.findings = snap.Findings
	b.tags = snap.Tags
	b.config = snap.Config
	b.enabled = snap.Enabled
	b.accountID = snap.AccountID
	b.region = snap.Region

	if b.findings == nil {
		b.findings = make(map[string]*storedFinding)
	}

	return nil
}
