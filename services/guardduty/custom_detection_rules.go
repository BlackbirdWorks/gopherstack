package guardduty

import (
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	modeLive   = "LIVE"
	modeDryRun = "DRY_RUN"

	orgConfigStatusActive     = "ACTIVE"
	orgConfigStatusProcessing = "PROCESSING"
	orgConfigStatusFailed     = "FAILED"

	// dryRunLifetime is the documented lifetime of a dry run association or
	// organization configuration (GuardDuty user guide, "Managing Custom
	// Detection Rules").
	dryRunLifetime = 14 * 24 * time.Hour

	customRulePageSize    = 100
	maxOrgConfigAccounts  = 50000
	accountIDLength       = 12
	maxRuleIDLength       = 100
	maxRuleFilters        = 100
	filterConditionEquals = "EQUALS"
	filterConditionHas    = "CONTAINS"
)

// CustomDetectionRule is an entry of GuardDuty's rule catalog. The catalog is
// owned by GuardDuty and no API creates or edits a rule, so the backend only
// holds rules registered through RegisterCustomDetectionRule.
type CustomDetectionRule struct {
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
	RuleID      string    `json:"ruleId"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	DataSource  string    `json:"dataSource"`
	Service     string    `json:"service"`
	Severity    string    `json:"severity"`
	Tactic      string    `json:"tactic"`
	Technique   string    `json:"technique"`
	Language    string    `json:"language,omitempty"`
	Schema      string    `json:"schema,omitempty"`
	Expression  string    `json:"expression"`
}

// CustomDetectionRuleAssociation enables a rule for one account in one mode.
type CustomDetectionRuleAssociation struct {
	CreatedAt     time.Time  `json:"createdAt"`
	UpdatedAt     time.Time  `json:"updatedAt"`
	ExpiresAt     *time.Time `json:"expiresAt,omitempty"`
	AssociationID string     `json:"associationId"`
	RuleID        string     `json:"ruleId"`
	AccountID     string     `json:"accountId"`
	Mode          string     `json:"mode"`
}

// CustomDetectionRuleOrgConfig is the organization-level configuration of a
// rule; a rule holds at most one per mode.
type CustomDetectionRuleOrgConfig struct {
	CreatedAt         time.Time  `json:"createdAt"`
	UpdatedAt         time.Time  `json:"updatedAt"`
	ExpiresAt         *time.Time `json:"expiresAt,omitempty"`
	RuleID            string     `json:"ruleId"`
	Mode              string     `json:"mode"`
	Status            string     `json:"status"`
	StatusReason      string     `json:"statusReason,omitempty"`
	IncludeAccountIDs []string   `json:"includeAccountIds"`
	ExcludeAccountIDs []string   `json:"excludeAccountIds"`
}

// DetectionRuleFilter is one ListCustomDetectionRules filter.
type DetectionRuleFilter struct {
	Name      string
	Condition string
	Values    []string
}

func customRuleKeyFn(v *CustomDetectionRule) string { return v.RuleID }

func customRuleAssociationKeyFn(v *CustomDetectionRuleAssociation) string { return v.AssociationID }

func customRuleOrgConfigKey(ruleID, mode string) string { return ruleID + "|" + mode }

func customRuleOrgConfigKeyFn(v *CustomDetectionRuleOrgConfig) string {
	return customRuleOrgConfigKey(v.RuleID, v.Mode)
}

func (b *InMemoryBackend) customRuleARN(ruleID string) string {
	return arn.Build("guardduty", b.region, b.accountID, "custom-detection-rule/"+ruleID)
}

func (b *InMemoryBackend) customRuleAssociationARN(ruleID, associationID string) string {
	return arn.Build(
		"guardduty", b.region, b.accountID,
		"custom-detection-rule/"+ruleID+"/association/"+associationID,
	)
}

// CustomRuleARN returns the ARN of a catalog rule.
func (b *InMemoryBackend) CustomRuleARN(ruleID string) string { return b.customRuleARN(ruleID) }

// CustomRuleAssociationARN returns the ARN of an association.
func (b *InMemoryBackend) CustomRuleAssociationARN(ruleID, associationID string) string {
	return b.customRuleAssociationARN(ruleID, associationID)
}

func validMode(mode string) bool { return mode == modeLive || mode == modeDryRun }

func expiryFor(mode string, from time.Time) *time.Time {
	if mode != modeDryRun {
		return nil
	}

	t := from.Add(dryRunLifetime)

	return &t
}

func expired(t *time.Time, now time.Time) bool { return t != nil && !now.Before(*t) }

// RegisterCustomDetectionRule adds a rule to the catalog, replacing any rule
// with the same ID.
func (b *InMemoryBackend) RegisterCustomDetectionRule(rule CustomDetectionRule) error {
	if rule.RuleID == "" || len(rule.RuleID) > maxRuleIDLength || !validRuleID(rule.RuleID) {
		return ErrValidation
	}

	b.mu.Lock("RegisterCustomDetectionRule")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	if rule.CreatedAt.IsZero() {
		rule.CreatedAt = now
	}

	if rule.UpdatedAt.IsZero() {
		rule.UpdatedAt = rule.CreatedAt
	}

	b.customDetectionRules.Put(&rule)

	return nil
}

// validRuleID reports whether id matches [a-z0-9]+(-[a-z0-9]+)*.
func validRuleID(id string) bool {
	for seg := range strings.SplitSeq(id, "-") {
		if seg == "" {
			return false
		}

		for _, r := range seg {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') {
				return false
			}
		}
	}

	return true
}

// GetCustomDetectionRule returns one catalog rule.
func (b *InMemoryBackend) GetCustomDetectionRule(ruleID string) (*CustomDetectionRule, error) {
	b.mu.RLock("GetCustomDetectionRule")
	defer b.mu.RUnlock()

	r, ok := b.customDetectionRules.Get(ruleID)
	if !ok {
		return nil, ErrCustomDetectionRuleNotFound
	}

	cp := *r

	return &cp, nil
}

func ruleFieldValue(r *CustomDetectionRule, name string) (string, bool) {
	switch name {
	case "name":
		return r.Name, true
	case keyDescription:
		return r.Description, true
	case "dataSource":
		return r.DataSource, true
	case keySeverity:
		return r.Severity, true
	case "tactic":
		return r.Tactic, true
	case "technique":
		return r.Technique, true
	case "service":
		return r.Service, true
	}

	return "", false
}

func ruleMatchesFilter(r *CustomDetectionRule, f DetectionRuleFilter) bool {
	got, _ := ruleFieldValue(r, f.Name)

	for _, want := range f.Values {
		if f.Condition == filterConditionHas {
			if strings.Contains(got, want) {
				return true
			}
		} else if got == want {
			return true
		}
	}

	return false
}

func validateRuleFilters(filters []DetectionRuleFilter) error {
	if len(filters) > maxRuleFilters {
		return ErrValidation
	}

	for _, f := range filters {
		if _, ok := ruleFieldValue(&CustomDetectionRule{}, f.Name); !ok || len(f.Values) == 0 {
			return ErrValidation
		}

		if f.Condition != "" && f.Condition != filterConditionEquals && f.Condition != filterConditionHas {
			return ErrValidation
		}
	}

	return nil
}

func checkPageParams(maxResults int32, token string) error {
	if maxResults < 0 || maxResults > customRulePageSize {
		return ErrValidation
	}

	return page.ValidateToken(token)
}

// ListCustomDetectionRules lists catalog rules matching every filter.
func (b *InMemoryBackend) ListCustomDetectionRules(
	filters []DetectionRuleFilter, maxResults int32, token string,
) ([]*CustomDetectionRule, string, error) {
	if err := validateRuleFilters(filters); err != nil {
		return nil, "", err
	}

	if err := checkPageParams(maxResults, token); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListCustomDetectionRules")
	defer b.mu.RUnlock()

	all := make([]*CustomDetectionRule, 0, b.customDetectionRules.Len())

	for _, r := range b.customDetectionRules.All() {
		if !slices.ContainsFunc(filters, func(f DetectionRuleFilter) bool { return !ruleMatchesFilter(r, f) }) {
			cp := *r
			all = append(all, &cp)
		}
	}

	slices.SortFunc(all, func(a, c *CustomDetectionRule) int { return strings.Compare(a.RuleID, c.RuleID) })

	p := page.New(all, token, int(maxResults), customRulePageSize)

	return p.Data, p.Next, nil
}

func (b *InMemoryBackend) liveAssociation(
	ruleID, associationID string,
	now time.Time,
) (*CustomDetectionRuleAssociation, bool) {
	a, ok := b.customRuleAssociations.Get(associationID)
	if !ok || a.RuleID != ruleID || a.AccountID != b.accountID || expired(a.ExpiresAt, now) {
		return nil, false
	}

	return a, true
}

// CreateCustomDetectionRuleAssociation enables a rule for the account.
func (b *InMemoryBackend) CreateCustomDetectionRuleAssociation(
	ruleID, mode string, tags map[string]string,
) (*CustomDetectionRuleAssociation, error) {
	if !validMode(mode) || len(ruleID) > maxRuleIDLength || !validRuleID(ruleID) {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateCustomDetectionRuleAssociation")
	defer b.mu.Unlock()

	if !b.customDetectionRules.Has(ruleID) {
		return nil, ErrCustomDetectionRuleNotFound
	}

	now := time.Now().UTC()

	for _, a := range b.customRuleAssociations.All() {
		if a.RuleID == ruleID && a.AccountID == b.accountID && !expired(a.ExpiresAt, now) {
			return nil, ErrCustomDetectionRuleConflict
		}
	}

	a := &CustomDetectionRuleAssociation{
		AssociationID: awsid.New("cdra-"),
		RuleID:        ruleID,
		AccountID:     b.accountID,
		Mode:          mode,
		CreatedAt:     now,
		UpdatedAt:     now,
		ExpiresAt:     expiryFor(mode, now),
	}
	b.customRuleAssociations.Put(a)

	if len(tags) > 0 {
		b.tags[b.customRuleAssociationARN(ruleID, a.AssociationID)] = maps.Clone(tags)
	}

	cp := *a

	return &cp, nil
}

// GetCustomDetectionRuleAssociation returns an association and its tags.
func (b *InMemoryBackend) GetCustomDetectionRuleAssociation(
	ruleID, associationID string,
) (*CustomDetectionRuleAssociation, map[string]string, error) {
	b.mu.RLock("GetCustomDetectionRuleAssociation")
	defer b.mu.RUnlock()

	a, ok := b.liveAssociation(ruleID, associationID, time.Now())
	if !ok {
		return nil, nil, ErrCustomDetectionRuleNotFound
	}

	cp := *a

	return &cp, maps.Clone(b.tags[b.customRuleAssociationARN(ruleID, associationID)]), nil
}

// ListCustomDetectionRuleAssociations lists the account's associations,
// optionally narrowed by rule and mode.
func (b *InMemoryBackend) ListCustomDetectionRuleAssociations(
	ruleID, mode string, maxResults int32, token string,
) ([]*CustomDetectionRuleAssociation, string, error) {
	if (mode != "" && !validMode(mode)) || (ruleID != "" && !validRuleID(ruleID)) {
		return nil, "", ErrValidation
	}

	if err := checkPageParams(maxResults, token); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListCustomDetectionRuleAssociations")
	defer b.mu.RUnlock()

	now := time.Now()
	all := make([]*CustomDetectionRuleAssociation, 0, b.customRuleAssociations.Len())

	for _, a := range b.customRuleAssociations.All() {
		if a.AccountID != b.accountID || expired(a.ExpiresAt, now) ||
			(ruleID != "" && a.RuleID != ruleID) || (mode != "" && a.Mode != mode) {
			continue
		}

		cp := *a
		all = append(all, &cp)
	}

	slices.SortFunc(all, func(a, c *CustomDetectionRuleAssociation) int {
		return strings.Compare(a.AssociationID, c.AssociationID)
	})

	p := page.New(all, token, int(maxResults), customRulePageSize)

	return p.Data, p.Next, nil
}

// UpdateCustomDetectionRuleAssociation changes an association's mode.
func (b *InMemoryBackend) UpdateCustomDetectionRuleAssociation(ruleID, associationID, mode string) error {
	if !validMode(mode) {
		return ErrValidation
	}

	b.mu.Lock("UpdateCustomDetectionRuleAssociation")
	defer b.mu.Unlock()

	now := time.Now().UTC()

	a, ok := b.liveAssociation(ruleID, associationID, now)
	if !ok {
		return ErrCustomDetectionRuleNotFound
	}

	if a.Mode != mode {
		a.ExpiresAt = expiryFor(mode, now)
	}

	a.Mode = mode
	a.UpdatedAt = now

	return nil
}

// DeleteCustomDetectionRuleAssociation disables a rule; deleting an
// association that does not exist succeeds.
func (b *InMemoryBackend) DeleteCustomDetectionRuleAssociation(ruleID, associationID string) error {
	b.mu.Lock("DeleteCustomDetectionRuleAssociation")
	defer b.mu.Unlock()

	if !b.customDetectionRules.Has(ruleID) {
		return ErrCustomDetectionRuleNotFound
	}

	if a, ok := b.customRuleAssociations.Get(associationID); ok && a.RuleID == ruleID {
		b.customRuleAssociations.Delete(associationID)
		delete(b.tags, b.customRuleAssociationARN(ruleID, associationID))
	}

	return nil
}

func validAccountIDs(ids []string) bool {
	if len(ids) > maxOrgConfigAccounts {
		return false
	}

	for _, id := range ids {
		if len(id) != accountIDLength {
			return false
		}
	}

	return true
}

func validateOrgScope(include, exclude []string) error {
	if (len(include) > 0 && len(exclude) > 0) || !validAccountIDs(include) || !validAccountIDs(exclude) {
		return ErrValidation
	}

	return nil
}

// claimsWholeOrg reports whether a configuration targets the whole
// organization: it names no accounts, or names accounts to exclude.
func claimsWholeOrg(c *CustomDetectionRuleOrgConfig) bool { return len(c.IncludeAccountIDs) == 0 }

func scopesOverlap(a, c *CustomDetectionRuleOrgConfig) bool {
	return claimsWholeOrg(a) || claimsWholeOrg(c) ||
		slices.ContainsFunc(
			a.IncludeAccountIDs,
			func(id string) bool { return slices.Contains(c.IncludeAccountIDs, id) },
		)
}

func otherMode(mode string) string {
	if mode == modeLive {
		return modeDryRun
	}

	return modeLive
}

func (b *InMemoryBackend) liveOrgConfig(ruleID, mode string, now time.Time) (*CustomDetectionRuleOrgConfig, bool) {
	c, ok := b.customRuleOrgConfigs.Get(customRuleOrgConfigKey(ruleID, mode))
	if !ok || expired(c.ExpiresAt, now) {
		return nil, false
	}

	return c, true
}

func (b *InMemoryBackend) orgConfigModeConflict(candidate *CustomDetectionRuleOrgConfig, now time.Time) bool {
	other, ok := b.liveOrgConfig(candidate.RuleID, otherMode(candidate.Mode), now)

	return ok && scopesOverlap(candidate, other)
}

// CreateCustomDetectionRuleOrgConfiguration creates the rule's configuration for a mode.
func (b *InMemoryBackend) CreateCustomDetectionRuleOrgConfiguration(
	ruleID, mode string, include, exclude []string,
) error {
	if !validMode(mode) || len(ruleID) > maxRuleIDLength || !validRuleID(ruleID) {
		return ErrValidation
	}

	if err := validateOrgScope(include, exclude); err != nil {
		return err
	}

	b.mu.Lock("CreateCustomDetectionRuleOrgConfiguration")
	defer b.mu.Unlock()

	if !b.customDetectionRules.Has(ruleID) {
		return ErrCustomDetectionRuleNotFound
	}

	now := time.Now().UTC()
	cfg := &CustomDetectionRuleOrgConfig{
		RuleID:            ruleID,
		Mode:              mode,
		Status:            orgConfigStatusActive,
		IncludeAccountIDs: slices.Clone(include),
		ExcludeAccountIDs: slices.Clone(exclude),
		CreatedAt:         now,
		UpdatedAt:         now,
		ExpiresAt:         expiryFor(mode, now),
	}

	if _, exists := b.liveOrgConfig(ruleID, mode, now); exists || b.orgConfigModeConflict(cfg, now) {
		return ErrCustomDetectionRuleConflict
	}

	b.customRuleOrgConfigs.Put(cfg)

	return nil
}

// GetCustomDetectionRuleOrgConfiguration returns the configuration for a rule and mode.
func (b *InMemoryBackend) GetCustomDetectionRuleOrgConfiguration(
	ruleID, mode string,
) (*CustomDetectionRuleOrgConfig, error) {
	if !validMode(mode) {
		return nil, ErrValidation
	}

	b.mu.RLock("GetCustomDetectionRuleOrgConfiguration")
	defer b.mu.RUnlock()

	c, ok := b.liveOrgConfig(ruleID, mode, time.Now())
	if !ok {
		return nil, ErrCustomDetectionRuleNotFound
	}

	return cloneOrgConfig(c), nil
}

func cloneOrgConfig(c *CustomDetectionRuleOrgConfig) *CustomDetectionRuleOrgConfig {
	cp := *c
	cp.IncludeAccountIDs = slices.Clone(c.IncludeAccountIDs)
	cp.ExcludeAccountIDs = slices.Clone(c.ExcludeAccountIDs)

	return &cp
}

// ListCustomDetectionRuleOrgConfigurations lists configurations, optionally by status.
func (b *InMemoryBackend) ListCustomDetectionRuleOrgConfigurations(
	status string, maxResults int32, token string,
) ([]*CustomDetectionRuleOrgConfig, string, error) {
	switch status {
	case "", orgConfigStatusActive, orgConfigStatusProcessing, orgConfigStatusFailed:
	default:
		return nil, "", ErrValidation
	}

	if err := checkPageParams(maxResults, token); err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListCustomDetectionRuleOrgConfigurations")
	defer b.mu.RUnlock()

	now := time.Now()
	all := make([]*CustomDetectionRuleOrgConfig, 0, b.customRuleOrgConfigs.Len())

	for _, c := range b.customRuleOrgConfigs.All() {
		if expired(c.ExpiresAt, now) || (status != "" && c.Status != status) {
			continue
		}

		all = append(all, cloneOrgConfig(c))
	}

	slices.SortFunc(all, func(a, c *CustomDetectionRuleOrgConfig) int {
		return strings.Compare(customRuleOrgConfigKey(a.RuleID, a.Mode), customRuleOrgConfigKey(c.RuleID, c.Mode))
	})

	p := page.New(all, token, int(maxResults), customRulePageSize)

	return p.Data, p.Next, nil
}

// UpdateCustomDetectionRuleOrgConfiguration replaces a configuration's account scope.
func (b *InMemoryBackend) UpdateCustomDetectionRuleOrgConfiguration(
	ruleID, mode string, include, exclude []string,
) error {
	if !validMode(mode) {
		return ErrValidation
	}

	if err := validateOrgScope(include, exclude); err != nil {
		return err
	}

	b.mu.Lock("UpdateCustomDetectionRuleOrgConfiguration")
	defer b.mu.Unlock()

	now := time.Now().UTC()

	c, ok := b.liveOrgConfig(ruleID, mode, now)
	if !ok {
		return ErrCustomDetectionRuleNotFound
	}

	candidate := cloneOrgConfig(c)
	candidate.IncludeAccountIDs = slices.Clone(include)
	candidate.ExcludeAccountIDs = slices.Clone(exclude)

	if b.orgConfigModeConflict(candidate, now) {
		return ErrCustomDetectionRuleConflict
	}

	c.IncludeAccountIDs = candidate.IncludeAccountIDs
	c.ExcludeAccountIDs = candidate.ExcludeAccountIDs
	c.UpdatedAt = now
	c.ExpiresAt = expiryFor(mode, now)

	return nil
}

// DeleteCustomDetectionRuleOrgConfiguration removes the configuration for a rule and mode.
func (b *InMemoryBackend) DeleteCustomDetectionRuleOrgConfiguration(ruleID, mode string) error {
	if !validMode(mode) {
		return ErrValidation
	}

	b.mu.Lock("DeleteCustomDetectionRuleOrgConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.liveOrgConfig(ruleID, mode, time.Now()); !ok {
		return ErrCustomDetectionRuleNotFound
	}

	b.customRuleOrgConfigs.Delete(customRuleOrgConfigKey(ruleID, mode))

	return nil
}
