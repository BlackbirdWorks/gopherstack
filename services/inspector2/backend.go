package inspector2

import (
	"context"
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

	ec2ScanModeEC2SSMAgentBased = "EC2_SSM_AGENT_BASED"
	ecrRescanDurationLifetime   = "LIFETIME"

	errResourceNotFound = "ResourceNotFoundException"
	errConflict         = "ConflictException"
	errValidation       = "ValidationException"

	inspector2Service = "inspector2"

	maxTagKeyLen   = 128
	maxTagValueLen = 256
	maxTagCount    = 50

	severityScoreCritical = 9.0
	severityScoreHigh     = 7.0
	severityScoreMedium   = 5.0
	severityScoreLow      = 3.0
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

// Finding represents an Inspector2 finding. The store is seedable so callers
// (tests, fixtures, the dashboard) can inject realistic findings that
// ListFindings will then return and filter — behavior that exceeds LocalStack,
// which always returns an empty list.
type Finding struct {
	FirstObservedAt time.Time         `json:"firstObservedAt"`
	LastObservedAt  time.Time         `json:"lastObservedAt"`
	UpdatedAt       time.Time         `json:"updatedAt"`
	Description     string            `json:"description"`
	AccountID       string            `json:"awsAccountId"`
	Type            string            `json:"type"`
	Status          string            `json:"status"`
	Title           string            `json:"title,omitempty"`
	FindingArn      string            `json:"findingArn"`
	FixAvailable    string            `json:"fixAvailable,omitempty"`
	ResourceType    string            `json:"-"`
	ResourceID      string            `json:"-"`
	Severity        FindingSeverity   `json:"severity"`
	Resources       []FindingResource `json:"resources,omitempty"`
}

// FindingSeverity holds severity details for a finding.
type FindingSeverity struct {
	Label string  `json:"label"`
	Score float64 `json:"score,omitempty"`
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

// Inspector2 finding severities and statuses (AWS Inspector2 API).
const (
	severityInformational = "INFORMATIONAL"
	severityLow           = "LOW"
	severityMedium        = "MEDIUM"
	severityHigh          = "HIGH"
	severityCritical      = "CRITICAL"
	severityUntriaged     = "UNTRIAGED"

	findingStatusActive     = "ACTIVE"
	findingStatusSuppressed = "SUPPRESSED"
	findingStatusClosed     = "CLOSED"

	defaultFindingsPageSize = 50
)

// isValidFindingSeverity reports whether s is a recognized Inspector2 severity.
func isValidFindingSeverity(s string) bool {
	switch s {
	case severityInformational, severityLow, severityMedium,
		severityHigh, severityCritical, severityUntriaged:
		return true
	default:
		return false
	}
}

// isValidFindingStatus reports whether s is a recognized Inspector2 status.
func isValidFindingStatus(s string) bool {
	switch s {
	case findingStatusActive, findingStatusSuppressed, findingStatusClosed:
		return true
	default:
		return false
	}
}

// SeedFinding injects a finding into the backend so ListFindings/aggregations
// return realistic data. Unset fields are defaulted to AWS-plausible values. It
// returns the stored finding (with a generated ARN when none was supplied).
//
// This is the additive capability that lets gopherstack exceed LocalStack, whose
// Inspector2 ListFindings is hardwired to return an empty set.
func (b *InMemoryBackend) SeedFinding(f Finding) (*Finding, error) {
	b.mu.Lock("SeedFinding")
	defer b.mu.Unlock()

	stored := f
	if stored.Severity.Label == "" {
		stored.Severity = FindingSeverity{Label: severityMedium, Score: severityScore(severityMedium)}
	}

	if !isValidFindingSeverity(stored.Severity.Label) {
		return nil, fmt.Errorf("%w: invalid finding severity %q", ErrValidation, stored.Severity.Label)
	}

	if stored.Status == "" {
		stored.Status = findingStatusActive
	}

	if !isValidFindingStatus(stored.Status) {
		return nil, fmt.Errorf("%w: invalid finding status %q", ErrValidation, stored.Status)
	}

	if stored.AccountID == "" {
		stored.AccountID = b.accountID
	}

	if stored.Type == "" {
		stored.Type = "PACKAGE_VULNERABILITY"
	}

	now := time.Now().UTC()
	if stored.FirstObservedAt.IsZero() {
		stored.FirstObservedAt = now
	}

	if stored.LastObservedAt.IsZero() {
		stored.LastObservedAt = now
	}

	stored.UpdatedAt = now

	if stored.FindingArn == "" {
		stored.FindingArn = arn.Build(inspector2Service, b.region, stored.AccountID, "finding/"+uuid.NewString())
	}

	clone := stored
	b.findings[stored.FindingArn] = &storedFinding{Finding: clone}

	out := stored

	return &out, nil
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

	b.findings[findingARN] = &storedFinding{
		Finding: Finding{
			FindingArn:      findingARN,
			AccountID:       b.accountID,
			Type:            findingType,
			Severity:        FindingSeverity{Label: severityLabel, Score: severityScore(severityLabel)},
			Status:          status,
			Description:     description,
			Title:           title,
			Resources:       resources,
			FirstObservedAt: now,
			LastObservedAt:  now,
			UpdatedAt:       now,
		},
	}

	return findingARN
}

// severityScore returns a numeric score for a severity label.
func severityScore(label string) float64 {
	switch label {
	case "CRITICAL":
		return severityScoreCritical
	case "HIGH":
		return severityScoreHigh
	case "MEDIUM":
		return severityScoreMedium
	case "LOW":
		return severityScoreLow
	default:
		return 0.0
	}
}

// findingFilterCriteria captures the subset of the Inspector2 filterCriteria
// shape that ListFindings evaluates. Each slice is a set of string filters with
// a comparison and value, matching the AWS StringFilter wire shape.
type findingFilterCriteria struct {
	severities   []stringFilter
	findingTypes []stringFilter
	statuses     []stringFilter
	accountIDs   []stringFilter
}

type stringFilter struct {
	comparison string
	value      string
}

// parseFindingFilterCriteria decodes the AWS filterCriteria map into the subset
// of string filters ListFindings supports. Unknown criteria keys are ignored
// (AWS accepts a large criteria object; unsupported facets simply do not narrow
// the result here rather than erroring).
func parseFindingFilterCriteria(criteria map[string]any) findingFilterCriteria {
	var fc findingFilterCriteria

	fc.severities = extractStringFilters(criteria, "severity")
	fc.findingTypes = extractStringFilters(criteria, "findingType")
	fc.statuses = extractStringFilters(criteria, "findingStatus")
	fc.accountIDs = extractStringFilters(criteria, "awsAccountId")

	return fc
}

func extractStringFilters(criteria map[string]any, key string) []stringFilter {
	raw, ok := criteria[key].([]any)
	if !ok {
		return nil
	}

	filters := make([]stringFilter, 0, len(raw))

	for _, item := range raw {
		m, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		cmp, _ := m["comparison"].(string)
		val, _ := m["value"].(string)

		if val == "" {
			continue
		}

		if cmp == "" {
			cmp = "EQUALS"
		}

		filters = append(filters, stringFilter{comparison: cmp, value: val})
	}

	return filters
}

func matchStringFilters(filters []stringFilter, actual string) bool {
	if len(filters) == 0 {
		return true
	}

	// AWS treats multiple filters on the same field as a logical OR.
	for _, f := range filters {
		switch f.comparison {
		case "PREFIX":
			if len(actual) >= len(f.value) && actual[:len(f.value)] == f.value {
				return true
			}
		case "NOT_EQUALS":
			if actual != f.value {
				return true
			}
		default: // EQUALS and any unrecognized comparison
			if actual == f.value {
				return true
			}
		}
	}

	return false
}

func (fc findingFilterCriteria) matches(f *Finding) bool {
	return matchStringFilters(fc.severities, f.Severity.Label) &&
		matchStringFilters(fc.findingTypes, f.Type) &&
		matchStringFilters(fc.statuses, f.Status) &&
		matchStringFilters(fc.accountIDs, f.AccountID)
}

// ListFindings returns a page of seeded findings filtered by the supplied
// filterCriteria. With no seeded findings it returns an empty page (preserving
// the prior always-empty contract for callers that never seed). Pagination uses
// the finding ARN as a stable cursor over the sorted result set.
func (b *InMemoryBackend) ListFindings(
	maxResults int32, nextToken string, criteria map[string]any,
) ([]*Finding, string, error) {
	b.mu.RLock("ListFindings")
	defer b.mu.RUnlock()

	fc := parseFindingFilterCriteria(criteria)

	matched := make([]*Finding, 0, len(b.findings))

	for _, f := range b.findings {
		if fc.matches(&f.Finding) {
			clone := f.Finding
			matched = append(matched, &clone)
		}
	}

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].FindingArn < matched[j].FindingArn
	})

	pageSize := int(maxResults)
	if pageSize <= 0 {
		pageSize = defaultFindingsPageSize
	}

	start := 0

	if nextToken != "" {
		for i, f := range matched {
			if f.FindingArn == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+pageSize, len(matched))

	page := matched[start:end]

	next := ""
	if end < len(matched) {
		next = matched[end].FindingArn
	}

	return page, next, nil
}

// FindingSeverityCounts returns the number of seeded findings grouped by
// severity, used by ListFindingAggregations.
func (b *InMemoryBackend) FindingSeverityCounts() map[string]int64 {
	b.mu.RLock("FindingSeverityCounts")
	defer b.mu.RUnlock()

	counts := make(map[string]int64, len(b.findings))
	for _, f := range b.findings {
		counts[f.Severity.Label]++
	}

	return counts
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
	b.ax = newAppendixAState()
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
	Appendix  *appendixAState              `json:"appendix"`
	Config    Configuration                `json:"config"`
	AccountID string                       `json:"accountId"`
	Region    string                       `json:"region"`
	Enabled   bool                         `json:"enabled"`
}

// Snapshot serializes the backend state.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Filters:   b.filters,
		Findings:  b.findings,
		Tags:      b.tags,
		Appendix:  b.ax,
		Config:    b.config,
		Enabled:   b.enabled,
		AccountID: b.accountID,
		Region:    b.region,
	}

	data, _ := json.Marshal(snap)

	return data
}

// Restore deserializes the backend state.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
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

	if snap.Appendix != nil {
		b.ax = snap.Appendix
	}

	return nil
}
