package accessanalyzer

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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
//
// AnalyzerName identifies the owning analyzer. It is never part of the wire
// response (archiveRuleToJSON in handler.go builds that by hand), so it is
// tagged json:"-"; it exists purely so archiveRuleKeyFn (store_setup.go) can
// derive the composite "analyzerName|ruleName" store.Table key from the
// value alone, matching the primary key of the map[string]map[string]*ArchiveRule
// this type used to live in.
type ArchiveRule struct {
	Filter       map[string]FilterCriterion `json:"filter"`
	CreatedAt    time.Time                  `json:"createdAt"`
	UpdatedAt    time.Time                  `json:"updatedAt"`
	RuleName     string                     `json:"ruleName"`
	AnalyzerName string                     `json:"-"`
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
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	analyzers *store.Table[Analyzer] // key: name

	// archiveRules was previously nested (analyzerName -> ruleName ->
	// *ArchiveRule); it is now one flat table keyed by the composite
	// "analyzerName|ruleName" string (see archiveRuleKey in store_setup.go),
	// with archiveRulesByAnalyzer grouping entries by analyzer for the "all
	// archive rules of analyzer X" lookups the nested map used to answer
	// directly. "Dirty" table (ArchiveRule.AnalyzerName is json:"-"): NOT on
	// b.registry, persisted via a DTO in persistence.go.
	archiveRules           *store.Table[ArchiveRule]
	archiveRulesByAnalyzer *store.Index[ArchiveRule]

	// findings was previously nested (analyzerName -> findingID -> *Finding).
	// findingID (a UUID minted by AddFinding) is globally unique, so this is
	// a flat table keyed directly by Finding.ID, with findingsByAnalyzer
	// grouping entries by analyzer name -- derived from the already
	// wire-visible AnalyzerArn field, see analyzerNameFromArn -- for the "all
	// findings of analyzer X" lookups the nested map used to answer directly.
	findings           *store.Table[Finding]
	findingsByAnalyzer *store.Index[Finding]

	// tags maps resourceARN -> tags; its value (map[string]string) has no
	// identity of its own, so it is not a store.Table candidate and remains
	// a plain persisted map.
	tags map[string]map[string]string

	policyGenerations *store.Table[PolicyGeneration] // key: jobID

	accessPreviews *store.Table[AccessPreview] // key: id

	// analyzedResources is keyed by the composite "analyzerArn|resourceArn"
	// string (see analyzedResourceKey in store_setup.go) -- both halves are
	// real, already wire-visible fields on AnalyzedResource, so no hidden
	// field was needed; a resource can be observed by more than one
	// analyzer, so ResourceArn alone is not a valid primary key.
	analyzedResources *store.Table[AnalyzedResource]

	findingRecommendations *store.Table[FindingRecommendation] // key: id (== findingID)

	accountID string
	region    string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:        lockmetrics.New("accessanalyzer"),
		registry:  store.NewRegistry(),
		accountID: accountID,
		region:    region,
		tags:      make(map[string]map[string]string),
	}

	registerAllTables(b)

	return b
}

// analyzerARN returns the ARN for an analyzer by name.
func (b *InMemoryBackend) analyzerARN(name string) string {
	return arn.Build("access-analyzer", b.region, b.accountID, fmt.Sprintf("analyzer/%s", name))
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

	if b.analyzers.Has(name) {
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

	b.analyzers.Put(a)

	return copyAnalyzer(a), nil
}

// GetAnalyzer returns the named analyzer.
func (b *InMemoryBackend) GetAnalyzer(name string) (*Analyzer, error) {
	b.mu.RLock("GetAnalyzer")
	defer b.mu.RUnlock()

	a, exists := b.analyzers.Get(name)
	if !exists {
		return nil, ErrAnalyzerNotFound
	}

	return copyAnalyzer(a), nil
}

// ListAnalyzers returns all analyzers, optionally filtered by type.
func (b *InMemoryBackend) ListAnalyzers(analyzerType string) ([]*Analyzer, error) {
	b.mu.RLock("ListAnalyzers")
	defer b.mu.RUnlock()

	all := b.analyzers.All()
	result := make([]*Analyzer, 0, len(all))

	for _, a := range all {
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

	if !b.analyzers.Has(name) {
		return ErrAnalyzerNotFound
	}

	b.analyzers.Delete(name)

	for _, r := range slices.Clone(b.archiveRulesByAnalyzer.Get(name)) {
		b.archiveRules.Delete(archiveRuleKey(r.AnalyzerName, r.RuleName))
	}

	for _, f := range slices.Clone(b.findingsByAnalyzer.Get(name)) {
		b.findings.Delete(f.ID)
	}

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

	if !b.analyzers.Has(analyzerName) {
		return nil, ErrAnalyzerNotFound
	}

	key := archiveRuleKey(analyzerName, ruleName)

	if b.archiveRules.Has(key) {
		return nil, ErrArchiveRuleAlreadyExists
	}

	now := time.Now().UTC()
	rule := &ArchiveRule{
		AnalyzerName: analyzerName,
		RuleName:     ruleName,
		Filter:       cloneFilter(filter),
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	b.archiveRules.Put(rule)

	for _, f := range b.findingsByAnalyzer.Get(analyzerName) {
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

	if !b.analyzers.Has(analyzerName) {
		return nil, ErrAnalyzerNotFound
	}

	rule, exists := b.archiveRules.Get(archiveRuleKey(analyzerName, ruleName))
	if !exists {
		return nil, ErrArchiveRuleNotFound
	}

	return copyArchiveRule(rule), nil
}

// ListArchiveRules returns all archive rules for an analyzer.
func (b *InMemoryBackend) ListArchiveRules(analyzerName string) ([]*ArchiveRule, error) {
	b.mu.RLock("ListArchiveRules")
	defer b.mu.RUnlock()

	if !b.analyzers.Has(analyzerName) {
		return nil, ErrAnalyzerNotFound
	}

	rules := b.archiveRulesByAnalyzer.Get(analyzerName)
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

	if !b.analyzers.Has(analyzerName) {
		return ErrAnalyzerNotFound
	}

	key := archiveRuleKey(analyzerName, ruleName)

	if !b.archiveRules.Has(key) {
		return ErrArchiveRuleNotFound
	}

	b.archiveRules.Delete(key)

	return nil
}

// UpdateArchiveRule replaces the filter on an archive rule.
func (b *InMemoryBackend) UpdateArchiveRule(
	analyzerName, ruleName string,
	filter map[string]FilterCriterion,
) (*ArchiveRule, error) {
	b.mu.Lock("UpdateArchiveRule")
	defer b.mu.Unlock()

	if !b.analyzers.Has(analyzerName) {
		return nil, ErrAnalyzerNotFound
	}

	rule, exists := b.archiveRules.Get(archiveRuleKey(analyzerName, ruleName))
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

	if !b.analyzers.Has(analyzerName) {
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

	b.findings.Put(f)

	return copyFinding(f), nil
}

// GetFinding returns a finding by ID.
func (b *InMemoryBackend) GetFinding(analyzerName, findingID string) (*Finding, error) {
	b.mu.RLock("GetFinding")
	defer b.mu.RUnlock()

	if !b.analyzers.Has(analyzerName) {
		return nil, ErrAnalyzerNotFound
	}

	f, exists := b.findings.Get(findingID)
	if !exists || findingAnalyzerIndexKeyFn(f) != analyzerName {
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

	if !b.analyzers.Has(analyzerName) {
		return nil, "", ErrAnalyzerNotFound
	}

	group := b.findingsByAnalyzer.Get(analyzerName)
	findings := make([]*Finding, 0, len(group))

	for _, f := range group {
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

	if !b.analyzers.Has(analyzerName) {
		return ErrAnalyzerNotFound
	}

	now := time.Now().UTC()

	for _, id := range findingIDs {
		if f, exists := b.findings.Get(id); exists && findingAnalyzerIndexKeyFn(f) == analyzerName {
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
	for _, a := range b.analyzers.All() {
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

	b.registry.ResetAll()
	// archiveRules is a "dirty" table (see store_setup.go), so it is not on
	// b.registry and needs its own Reset() call here.
	b.archiveRules.Reset()
	b.tags = make(map[string]map[string]string)
}

// Region returns the backend's region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the backend's account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Snapshot and Restore (persistence.Persistable) live in persistence.go,
// alongside the archiveRules DTO their registry-table snapshot needs.

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
