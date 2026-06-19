package accessanalyzer

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrAnalyzerNotFound is returned when the named analyzer does not exist.
	ErrAnalyzerNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAnalyzerAlreadyExists is returned when creating a duplicate analyzer.
	ErrAnalyzerAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrArchiveRuleNotFound is returned when the named archive rule does not exist.
	ErrArchiveRuleNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrArchiveRuleAlreadyExists is returned when creating a duplicate archive rule.
	ErrArchiveRuleAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrFindingNotFound is returned when a finding ID is not found.
	ErrFindingNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// AnalyzerType represents the type of an Access Analyzer analyzer.
type AnalyzerType string

const (
	AnalyzerTypeAccount                  AnalyzerType = "ACCOUNT"
	AnalyzerTypeOrganization             AnalyzerType = "ORGANIZATION"
	AnalyzerTypeAccountUnusedAccess      AnalyzerType = "ACCOUNT_UNUSED_ACCESS"
	AnalyzerTypeOrganizationUnusedAccess AnalyzerType = "ORGANIZATION_UNUSED_ACCESS"
)

// AnalyzerStatus represents the status of an analyzer.
type AnalyzerStatus string

const (
	AnalyzerStatusActive   AnalyzerStatus = "ACTIVE"
	AnalyzerStatusCreating AnalyzerStatus = "CREATING"
	AnalyzerStatusDisabled AnalyzerStatus = "DISABLED"
	AnalyzerStatusFailed   AnalyzerStatus = "FAILED"
)

// FindingStatus represents the status of a finding.
type FindingStatus string

const (
	FindingStatusActive   FindingStatus = "ACTIVE"
	FindingStatusArchived FindingStatus = "ARCHIVED"
	FindingStatusResolved FindingStatus = "RESOLVED"
)

// FilterCriterion is a single criterion in a finding filter.
type FilterCriterion struct {
	Contains []string `json:"contains,omitempty"`
	Eq       []string `json:"eq,omitempty"`
	Exists   *bool    `json:"exists,omitempty"`
	Neq      []string `json:"neq,omitempty"`
}

// Analyzer represents an IAM Access Analyzer analyzer.
type Analyzer struct {
	Tags                   map[string]string `json:"tags,omitempty"`
	LastResourceAnalyzedAt *time.Time        `json:"lastResourceAnalyzedAt,omitempty"`
	CreatedAt              time.Time         `json:"createdAt"`
	Arn                    string            `json:"arn"`
	Name                   string            `json:"name"`
	Type                   AnalyzerType      `json:"type"`
	Status                 AnalyzerStatus    `json:"status"`
}

// ArchiveRule represents an archive rule for an analyzer.
type ArchiveRule struct {
	Filter    map[string]FilterCriterion `json:"filter"`
	CreatedAt time.Time                  `json:"createdAt"`
	UpdatedAt time.Time                  `json:"updatedAt"`
	RuleName  string                     `json:"ruleName"`
}

// Finding represents a single IAM Access Analyzer finding.
type Finding struct {
	UpdatedAt    time.Time         `json:"updatedAt"`
	CreatedAt    time.Time         `json:"createdAt"`
	Principal    map[string]string `json:"principal,omitempty"`
	Condition    map[string]string `json:"condition,omitempty"`
	IsPublic     *bool             `json:"isPublic,omitempty"`
	ID           string            `json:"id"`
	AnalyzerArn  string            `json:"analyzerArn"`
	Status       FindingStatus     `json:"status"`
	ResourceType string            `json:"resourceType"`
	ResourceArn  string            `json:"resourceArn"`
	Action       []string          `json:"action,omitempty"`
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu                     *lockmetrics.RWMutex
	analyzers              map[string]*Analyzer               // name → Analyzer
	archiveRules           map[string]map[string]*ArchiveRule // analyzerName → ruleName → Rule
	findings               map[string]map[string]*Finding     // analyzerName → findingID → Finding
	tags                   map[string]map[string]string       // resourceARN → tags
	policyGenerations      map[string]*PolicyGeneration       // jobID → PolicyGeneration
	accessPreviews         map[string]*AccessPreview          // id → AccessPreview
	analyzedResources      map[string]*AnalyzedResource       // analyzerArn|resourceArn → AnalyzedResource
	findingRecommendations map[string]*FindingRecommendation  // findingID → FindingRecommendation
	accountID              string
	region                 string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                     lockmetrics.New("accessanalyzer"),
		accountID:              accountID,
		region:                 region,
		analyzers:              make(map[string]*Analyzer),
		archiveRules:           make(map[string]map[string]*ArchiveRule),
		findings:               make(map[string]map[string]*Finding),
		tags:                   make(map[string]map[string]string),
		policyGenerations:      make(map[string]*PolicyGeneration),
		accessPreviews:         make(map[string]*AccessPreview),
		analyzedResources:      make(map[string]*AnalyzedResource),
		findingRecommendations: make(map[string]*FindingRecommendation),
	}
}

// analyzerARN returns the ARN for an analyzer by name.
func (b *InMemoryBackend) analyzerARN(name string) string {
	return fmt.Sprintf("arn:aws:access-analyzer:%s:%s:analyzer/%s", b.region, b.accountID, name)
}

// CreateAnalyzer creates a new analyzer.
func (b *InMemoryBackend) CreateAnalyzer(
	name string,
	analyzerType AnalyzerType,
	tags map[string]string,
) (*Analyzer, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateAnalyzer")
	defer b.mu.Unlock()

	if _, exists := b.analyzers[name]; exists {
		return nil, ErrAnalyzerAlreadyExists
	}

	now := time.Now().UTC()
	a := &Analyzer{
		Arn:       b.analyzerARN(name),
		Name:      name,
		Type:      analyzerType,
		Status:    AnalyzerStatusActive,
		CreatedAt: now,
		Tags:      cloneTags(tags),
	}

	b.analyzers[name] = a
	b.archiveRules[name] = make(map[string]*ArchiveRule)
	b.findings[name] = make(map[string]*Finding)

	return copyAnalyzer(a), nil
}

// GetAnalyzer returns the named analyzer.
func (b *InMemoryBackend) GetAnalyzer(name string) (*Analyzer, error) {
	b.mu.RLock("GetAnalyzer")
	defer b.mu.RUnlock()

	a, exists := b.analyzers[name]
	if !exists {
		return nil, ErrAnalyzerNotFound
	}

	return copyAnalyzer(a), nil
}

// ListAnalyzers returns all analyzers, optionally filtered by type.
func (b *InMemoryBackend) ListAnalyzers(analyzerType string) ([]*Analyzer, error) {
	b.mu.RLock("ListAnalyzers")
	defer b.mu.RUnlock()

	result := make([]*Analyzer, 0, len(b.analyzers))

	for _, a := range b.analyzers {
		if analyzerType != "" && string(a.Type) != analyzerType {
			continue
		}

		result = append(result, copyAnalyzer(a))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteAnalyzer removes an analyzer and all its findings and archive rules.
func (b *InMemoryBackend) DeleteAnalyzer(name string) error {
	b.mu.Lock("DeleteAnalyzer")
	defer b.mu.Unlock()

	if _, exists := b.analyzers[name]; !exists {
		return ErrAnalyzerNotFound
	}

	delete(b.analyzers, name)
	delete(b.archiveRules, name)
	delete(b.findings, name)

	return nil
}

// CreateArchiveRule adds an archive rule to an analyzer and immediately archives
// all active findings for that analyzer (AWS auto-apply behavior).
func (b *InMemoryBackend) CreateArchiveRule(
	analyzerName, ruleName string,
	filter map[string]FilterCriterion,
) (*ArchiveRule, error) {
	b.mu.Lock("CreateArchiveRule")
	defer b.mu.Unlock()

	if _, exists := b.analyzers[analyzerName]; !exists {
		return nil, ErrAnalyzerNotFound
	}

	rules := b.archiveRules[analyzerName]

	if _, exists := rules[ruleName]; exists {
		return nil, ErrArchiveRuleAlreadyExists
	}

	now := time.Now().UTC()
	rule := &ArchiveRule{
		RuleName:  ruleName,
		Filter:    cloneFilter(filter),
		CreatedAt: now,
		UpdatedAt: now,
	}

	rules[ruleName] = rule

	for _, f := range b.findings[analyzerName] {
		if f.Status == FindingStatusActive {
			f.Status = FindingStatusArchived
			f.UpdatedAt = now
		}
	}

	return copyArchiveRule(rule), nil
}

// GetArchiveRule returns the named archive rule.
func (b *InMemoryBackend) GetArchiveRule(analyzerName, ruleName string) (*ArchiveRule, error) {
	b.mu.RLock("GetArchiveRule")
	defer b.mu.RUnlock()

	if _, exists := b.analyzers[analyzerName]; !exists {
		return nil, ErrAnalyzerNotFound
	}

	rule, exists := b.archiveRules[analyzerName][ruleName]
	if !exists {
		return nil, ErrArchiveRuleNotFound
	}

	return copyArchiveRule(rule), nil
}

// ListArchiveRules returns all archive rules for an analyzer.
func (b *InMemoryBackend) ListArchiveRules(analyzerName string) ([]*ArchiveRule, error) {
	b.mu.RLock("ListArchiveRules")
	defer b.mu.RUnlock()

	if _, exists := b.analyzers[analyzerName]; !exists {
		return nil, ErrAnalyzerNotFound
	}

	rules := b.archiveRules[analyzerName]
	result := make([]*ArchiveRule, 0, len(rules))

	for _, r := range rules {
		result = append(result, copyArchiveRule(r))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RuleName < result[j].RuleName
	})

	return result, nil
}

// DeleteArchiveRule removes an archive rule.
func (b *InMemoryBackend) DeleteArchiveRule(analyzerName, ruleName string) error {
	b.mu.Lock("DeleteArchiveRule")
	defer b.mu.Unlock()

	if _, exists := b.analyzers[analyzerName]; !exists {
		return ErrAnalyzerNotFound
	}

	if _, exists := b.archiveRules[analyzerName][ruleName]; !exists {
		return ErrArchiveRuleNotFound
	}

	delete(b.archiveRules[analyzerName], ruleName)

	return nil
}

// UpdateArchiveRule replaces the filter on an archive rule.
func (b *InMemoryBackend) UpdateArchiveRule(
	analyzerName, ruleName string,
	filter map[string]FilterCriterion,
) (*ArchiveRule, error) {
	b.mu.Lock("UpdateArchiveRule")
	defer b.mu.Unlock()

	if _, exists := b.analyzers[analyzerName]; !exists {
		return nil, ErrAnalyzerNotFound
	}

	rule, exists := b.archiveRules[analyzerName][ruleName]
	if !exists {
		return nil, ErrArchiveRuleNotFound
	}

	rule.Filter = cloneFilter(filter)
	rule.UpdatedAt = time.Now().UTC()

	return copyArchiveRule(rule), nil
}

// AddFinding adds a synthetic finding to an analyzer (for testing / resource scan simulation).
func (b *InMemoryBackend) AddFinding(
	analyzerName, resourceType, resourceArn string,
	action []string,
	principal map[string]string,
	isPublic *bool,
) (*Finding, error) {
	b.mu.Lock("AddFinding")
	defer b.mu.Unlock()

	if _, exists := b.analyzers[analyzerName]; !exists {
		return nil, ErrAnalyzerNotFound
	}

	analyzerARN := b.analyzerARN(analyzerName)
	now := time.Now().UTC()
	f := &Finding{
		ID:           uuid.NewString(),
		AnalyzerArn:  analyzerARN,
		Status:       FindingStatusActive,
		ResourceType: resourceType,
		ResourceArn:  resourceArn,
		Action:       append([]string(nil), action...),
		Principal:    cloneTags(principal),
		IsPublic:     isPublic,
		UpdatedAt:    now,
		CreatedAt:    now,
	}

	b.findings[analyzerName][f.ID] = f

	return copyFinding(f), nil
}

// GetFinding returns a finding by ID.
func (b *InMemoryBackend) GetFinding(analyzerName, findingID string) (*Finding, error) {
	b.mu.RLock("GetFinding")
	defer b.mu.RUnlock()

	if _, exists := b.analyzers[analyzerName]; !exists {
		return nil, ErrAnalyzerNotFound
	}

	f, exists := b.findings[analyzerName][findingID]
	if !exists {
		return nil, ErrFindingNotFound
	}

	return copyFinding(f), nil
}

// ListFindings returns findings for an analyzer, optionally filtered.
func (b *InMemoryBackend) ListFindings(
	analyzerName string,
	_ map[string]FilterCriterion,
	status string,
	maxResults int,
	nextToken string,
) ([]*Finding, string, error) {
	b.mu.RLock("ListFindings")
	defer b.mu.RUnlock()

	if _, exists := b.analyzers[analyzerName]; !exists {
		return nil, "", ErrAnalyzerNotFound
	}

	findings := make([]*Finding, 0, len(b.findings[analyzerName]))

	for _, f := range b.findings[analyzerName] {
		if status != "" && string(f.Status) != status {
			continue
		}

		findings = append(findings, copyFinding(f))
	}

	sort.Slice(findings, func(i, j int) bool {
		return findings[i].ID < findings[j].ID
	})

	// Simple token-based pagination by finding ID prefix.
	start := 0

	if nextToken != "" {
		for i, f := range findings {
			if f.ID == nextToken {
				start = i

				break
			}
		}
	}

	findings = findings[start:]

	if maxResults > 0 && len(findings) > maxResults {
		return findings[:maxResults], findings[maxResults].ID, nil
	}

	return findings, "", nil
}

// UpdateFindings archives or marks active the specified findings.
func (b *InMemoryBackend) UpdateFindings(
	analyzerName string,
	findingIDs []string,
	status FindingStatus,
) error {
	b.mu.Lock("UpdateFindings")
	defer b.mu.Unlock()

	if _, exists := b.analyzers[analyzerName]; !exists {
		return ErrAnalyzerNotFound
	}

	now := time.Now().UTC()

	for _, id := range findingIDs {
		if f, exists := b.findings[analyzerName][id]; exists {
			f.Status = status
			f.UpdatedAt = now
		}
	}

	return nil
}

// StartResourceScan records that a scan was initiated for a resource (no-op in simulation).
func (b *InMemoryBackend) StartResourceScan(analyzerARN string, _ string) error {
	b.mu.RLock("StartResourceScan")
	defer b.mu.RUnlock()

	// Verify the analyzer exists by ARN.
	for _, a := range b.analyzers {
		if a.Arn == analyzerARN {
			return nil
		}
	}

	return ErrAnalyzerNotFound
}

// TagResource sets tags on a resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], kv)

	return nil
}

// UntagResource removes tags from a resource by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	return cloneTags(b.tags[resourceARN]), nil
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.analyzers = make(map[string]*Analyzer)
	b.archiveRules = make(map[string]map[string]*ArchiveRule)
	b.findings = make(map[string]map[string]*Finding)
	b.tags = make(map[string]map[string]string)
	b.policyGenerations = make(map[string]*PolicyGeneration)
	b.accessPreviews = make(map[string]*AccessPreview)
	b.analyzedResources = make(map[string]*AnalyzedResource)
	b.findingRecommendations = make(map[string]*FindingRecommendation)
}

// Region returns the backend's region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the backend's account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	type snap struct {
		Analyzers    map[string]*Analyzer               `json:"analyzers"`
		ArchiveRules map[string]map[string]*ArchiveRule `json:"archiveRules"`
		Findings     map[string]map[string]*Finding     `json:"findings"`
		Tags         map[string]map[string]string       `json:"tags"`
	}

	data, _ := json.Marshal(snap{
		Analyzers:    b.analyzers,
		ArchiveRules: b.archiveRules,
		Findings:     b.findings,
		Tags:         b.tags,
	})

	return data
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	type snap struct {
		Analyzers    map[string]*Analyzer               `json:"analyzers"`
		ArchiveRules map[string]map[string]*ArchiveRule `json:"archiveRules"`
		Findings     map[string]map[string]*Finding     `json:"findings"`
		Tags         map[string]map[string]string       `json:"tags"`
	}

	var s snap
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	b.analyzers = s.Analyzers
	b.archiveRules = s.ArchiveRules
	b.findings = s.Findings
	b.tags = s.Tags

	if b.analyzers == nil {
		b.analyzers = make(map[string]*Analyzer)
	}

	if b.archiveRules == nil {
		b.archiveRules = make(map[string]map[string]*ArchiveRule)
	}

	if b.findings == nil {
		b.findings = make(map[string]map[string]*Finding)
	}

	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	return nil
}

// ---- helpers ----

// cloneTags returns a copy of a string map (nil-safe).
func cloneTags(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}

	out := make(map[string]string, len(m))
	maps.Copy(out, m)

	return out
}

// cloneFilter returns a copy of a filter map (nil-safe).
func cloneFilter(f map[string]FilterCriterion) map[string]FilterCriterion {
	if f == nil {
		return nil
	}

	out := make(map[string]FilterCriterion, len(f))
	maps.Copy(out, f)

	return out
}

func copyAnalyzer(a *Analyzer) *Analyzer {
	cp := *a
	cp.Tags = cloneTags(a.Tags)

	return &cp
}

func copyArchiveRule(r *ArchiveRule) *ArchiveRule {
	cp := *r
	cp.Filter = cloneFilter(r.Filter)

	return &cp
}

func copyFinding(f *Finding) *Finding {
	cp := *f
	cp.Action = append([]string(nil), f.Action...)
	cp.Principal = cloneTags(f.Principal)
	cp.Condition = cloneTags(f.Condition)

	return &cp
}
