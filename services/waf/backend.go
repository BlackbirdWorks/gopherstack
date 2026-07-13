package waf

import (
	"fmt"
	"maps"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	changeTokenStatusINSYNC      = "INSYNC"
	changeTokenStatusPROVISIONED = "PROVISIONED"

	maxChangeTokens = 10_000

	updateInsert = "INSERT"
	updateDelete = "DELETE"

	errResourceNotFound = "WAFNonexistentItemException"
	errStaleData        = "WAFStaleDataException"
	errInvalidParameter = "WAFInvalidParameterException"
	errReferencedItem   = "WAFReferencedItemException"
)

var (
	// ErrNotFound is returned when a resource does not exist.
	ErrNotFound = awserr.New(errResourceNotFound, awserr.ErrNotFound)
	// ErrStaleToken is returned when the change token is stale.
	ErrStaleToken = awserr.New(errStaleData, awserr.ErrConflict)
	// ErrInvalidParameter is returned on invalid input.
	ErrInvalidParameter = awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	// ErrReferencedItem is returned when a resource is still referenced.
	ErrReferencedItem = awserr.New(errReferencedItem, awserr.ErrConflict)
)

// WafAction represents the action AWS WAF should take on a matching request.
type WafAction struct { //nolint:revive // AWS SDK naming: waf.WafAction matches SDK
	Type string `json:"Type"`
}

// WafOverrideAction overrides the action in a rule group.
type WafOverrideAction struct { //nolint:revive // AWS SDK naming
	Type string `json:"Type"`
}

// ExcludedRule specifies a rule to exclude from a rule group.
type ExcludedRule struct {
	RuleId string `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
}

// ActivatedRule represents a rule activated in a WebACL.
type ActivatedRule struct {
	Action         *WafAction         `json:"Action,omitempty"`
	OverrideAction *WafOverrideAction `json:"OverrideAction,omitempty"`
	RuleId         string             `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
	Type           string             `json:"Type,omitempty"`
	ExcludedRules  []ExcludedRule     `json:"ExcludedRules,omitempty"`
	Priority       int32              `json:"Priority"`
}

// WebACLUpdate specifies a rule to insert into or delete from a WebACL.
type WebACLUpdate struct {
	Action        string        `json:"Action"`
	ActivatedRule ActivatedRule `json:"ActivatedRule"`
}

// WebACL is a WAF Classic web access control list.
type WebACL struct {
	WebACLId      string          `json:"WebACLId"`
	Name          string          `json:"Name"`
	MetricName    string          `json:"MetricName"`
	DefaultAction WafAction       `json:"DefaultAction"`
	WebACLArn     string          `json:"WebACLArn"`
	Rules         []ActivatedRule `json:"Rules"`
}

// WebACLSummary is a summary of a WebACL.
type WebACLSummary struct {
	WebACLId string `json:"WebACLId"`
	Name     string `json:"Name"`
}

// Predicate represents a condition in a Rule.
type Predicate struct {
	DataId  string `json:"DataId"` //nolint:revive,staticcheck // AWS SDK field name
	Type    string `json:"Type"`
	Negated bool   `json:"Negated"`
}

// RuleUpdate specifies a predicate to insert into or delete from a Rule.
type RuleUpdate struct {
	Action    string    `json:"Action"`
	Predicate Predicate `json:"Predicate"`
}

// Rule is a WAF Classic rule.
type Rule struct {
	RuleId     string      `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
	Name       string      `json:"Name"`
	MetricName string      `json:"MetricName"`
	Predicates []Predicate `json:"Predicates"`
}

// RuleSummary is a summary of a Rule.
type RuleSummary struct {
	RuleId string `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
	Name   string `json:"Name"`
}

// IPSetDescriptor is an IP address type and CIDR range.
type IPSetDescriptor struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

// IPSetUpdate specifies a descriptor to insert into or delete from an IPSet.
type IPSetUpdate struct {
	Action          string          `json:"Action"`
	IPSetDescriptor IPSetDescriptor `json:"IPSetDescriptor"`
}

// IPSet is a WAF Classic IP set.
type IPSet struct {
	IPSetId          string            `json:"IPSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name             string            `json:"Name"`
	IPSetDescriptors []IPSetDescriptor `json:"IPSetDescriptors"`
}

// IPSetSummary is a summary of an IPSet.
type IPSetSummary struct {
	IPSetId string `json:"IPSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name    string `json:"Name"`
}

// FieldToMatch specifies where in a web request to look.
type FieldToMatch struct {
	Type string `json:"Type"`
	Data string `json:"Data,omitempty"`
}

// ByteMatchTuple specifies a match in a byte match set.
type ByteMatchTuple struct {
	FieldToMatch         FieldToMatch `json:"FieldToMatch"`
	PositionalConstraint string       `json:"PositionalConstraint"`
	TargetString         string       `json:"TargetString"` // base64-encoded in AWS, plain string here
	TextTransformation   string       `json:"TextTransformation"`
}

// ByteMatchSetUpdate specifies a tuple to insert into or delete from a ByteMatchSet.
type ByteMatchSetUpdate struct {
	Action         string         `json:"Action"`
	ByteMatchTuple ByteMatchTuple `json:"ByteMatchTuple"`
}

// ByteMatchSet is a WAF Classic byte match set.
type ByteMatchSet struct {
	ByteMatchSetId  string           `json:"ByteMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name            string           `json:"Name"`
	ByteMatchTuples []ByteMatchTuple `json:"ByteMatchTuples"`
}

// ByteMatchSetSummary is a summary of a ByteMatchSet.
type ByteMatchSetSummary struct {
	ByteMatchSetId string `json:"ByteMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name           string `json:"Name"`
}

// SizeConstraint specifies a size constraint.
type SizeConstraint struct {
	FieldToMatch       FieldToMatch `json:"FieldToMatch"`
	ComparisonOperator string       `json:"ComparisonOperator"`
	TextTransformation string       `json:"TextTransformation"`
	Size               int64        `json:"Size"`
}

// SizeConstraintSetUpdate specifies a constraint to insert or delete.
type SizeConstraintSetUpdate struct {
	Action         string         `json:"Action"`
	SizeConstraint SizeConstraint `json:"SizeConstraint"`
}

// SizeConstraintSet is a WAF Classic size constraint set.
type SizeConstraintSet struct {
	SizeConstraintSetId string           `json:"SizeConstraintSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name                string           `json:"Name"`
	SizeConstraints     []SizeConstraint `json:"SizeConstraints"`
}

// SizeConstraintSetSummary is a summary of a SizeConstraintSet.
type SizeConstraintSetSummary struct {
	SizeConstraintSetId string `json:"SizeConstraintSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name                string `json:"Name"`
}

// SqlInjectionMatchTuple specifies a SQL injection match tuple.
type SqlInjectionMatchTuple struct { //nolint:revive,staticcheck // AWS SDK naming
	FieldToMatch       FieldToMatch `json:"FieldToMatch"`
	TextTransformation string       `json:"TextTransformation"`
}

// SqlInjectionMatchSetUpdate specifies a tuple to insert or delete.
//
//nolint:revive,staticcheck // AWS SDK naming
type SqlInjectionMatchSetUpdate struct {
	Action string `json:"Action"`
	//nolint:revive,staticcheck // AWS SDK naming
	SqlInjectionMatchTuple SqlInjectionMatchTuple `json:"SqlInjectionMatchTuple"`
}

// SqlInjectionMatchSet is a WAF Classic SQL injection match set.
//
//nolint:revive,staticcheck // AWS SDK naming
type SqlInjectionMatchSet struct {
	//nolint:revive,staticcheck // AWS SDK naming
	SqlInjectionMatchSetId string `json:"SqlInjectionMatchSetId"`
	Name                   string `json:"Name"`
	//nolint:revive,staticcheck // AWS SDK naming
	SqlInjectionMatchTuples []SqlInjectionMatchTuple `json:"SqlInjectionMatchTuples"`
}

// SqlInjectionMatchSetSummary is a summary of a SqlInjectionMatchSet.
type SqlInjectionMatchSetSummary struct { //nolint:revive,staticcheck // AWS SDK naming
	SqlInjectionMatchSetId string `json:"SqlInjectionMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name                   string `json:"Name"`
}

// XssMatchTuple specifies an XSS match tuple.
type XssMatchTuple struct { //nolint:revive,staticcheck // AWS SDK naming
	FieldToMatch       FieldToMatch `json:"FieldToMatch"`
	TextTransformation string       `json:"TextTransformation"`
}

// XssMatchSetUpdate specifies a tuple to insert or delete.
type XssMatchSetUpdate struct { //nolint:revive,staticcheck // AWS SDK naming
	Action        string        `json:"Action"`
	XssMatchTuple XssMatchTuple `json:"XssMatchTuple"` //nolint:revive,staticcheck // AWS SDK field name
}

// XssMatchSet is a WAF Classic XSS match set.
type XssMatchSet struct { //nolint:revive,staticcheck // AWS SDK naming
	XssMatchSetId  string          `json:"XssMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name           string          `json:"Name"`
	XssMatchTuples []XssMatchTuple `json:"XssMatchTuples"` //nolint:revive,staticcheck // AWS SDK field name
}

// XssMatchSetSummary is a summary of an XssMatchSet.
type XssMatchSetSummary struct { //nolint:revive,staticcheck // AWS SDK naming
	XssMatchSetId string `json:"XssMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name          string `json:"Name"`
}

// GeoMatchConstraint specifies a geo match constraint.
type GeoMatchConstraint struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

// GeoMatchSetUpdate specifies a constraint to insert or delete.
type GeoMatchSetUpdate struct {
	Action             string             `json:"Action"`
	GeoMatchConstraint GeoMatchConstraint `json:"GeoMatchConstraint"`
}

// GeoMatchSet is a WAF Classic geo match set.
type GeoMatchSet struct {
	GeoMatchSetId       string               `json:"GeoMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name                string               `json:"Name"`
	GeoMatchConstraints []GeoMatchConstraint `json:"GeoMatchConstraints"`
}

// GeoMatchSetSummary is a summary of a GeoMatchSet.
type GeoMatchSetSummary struct {
	GeoMatchSetId string `json:"GeoMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name          string `json:"Name"`
}

// RateBasedRule is a WAF Classic rate-based rule.
type RateBasedRule struct {
	RuleId          string      `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
	Name            string      `json:"Name"`
	MetricName      string      `json:"MetricName"`
	RateKey         string      `json:"RateKey"`
	MatchPredicates []Predicate `json:"MatchPredicates"`
	RateLimit       int64       `json:"RateLimit"`
}

// RateBasedRuleSummary is a summary of a RateBasedRule.
type RateBasedRuleSummary struct {
	RuleId string `json:"RuleId"` //nolint:revive,staticcheck // AWS SDK field name
	Name   string `json:"Name"`
}

// RegexPatternSet is a WAF Classic regex pattern set.
type RegexPatternSet struct {
	RegexPatternSetId   string   `json:"RegexPatternSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name                string   `json:"Name"`
	RegexPatternStrings []string `json:"RegexPatternStrings"`
}

// RegexPatternSetSummary is a summary of a RegexPatternSet.
type RegexPatternSetSummary struct {
	RegexPatternSetId string `json:"RegexPatternSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name              string `json:"Name"`
}

// RegexPatternSetUpdate specifies a pattern string to insert or delete.
type RegexPatternSetUpdate struct {
	Action             string `json:"Action"`
	RegexPatternString string `json:"RegexPatternString"`
}

// RegexMatchTuple specifies a regex match tuple.
type RegexMatchTuple struct {
	FieldToMatch       FieldToMatch `json:"FieldToMatch"`
	TextTransformation string       `json:"TextTransformation"`
	RegexPatternSetId  string       `json:"RegexPatternSetId"` //nolint:revive,staticcheck // AWS SDK field name
}

// RegexMatchSetUpdate specifies a tuple to insert or delete.
type RegexMatchSetUpdate struct {
	Action          string          `json:"Action"`
	RegexMatchTuple RegexMatchTuple `json:"RegexMatchTuple"`
}

// RegexMatchSet is a WAF Classic regex match set.
type RegexMatchSet struct {
	RegexMatchSetId  string            `json:"RegexMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name             string            `json:"Name"`
	RegexMatchTuples []RegexMatchTuple `json:"RegexMatchTuples"`
}

// RegexMatchSetSummary is a summary of a RegexMatchSet.
type RegexMatchSetSummary struct {
	RegexMatchSetId string `json:"RegexMatchSetId"` //nolint:revive,staticcheck // AWS SDK field name
	Name            string `json:"Name"`
}

// RuleGroup is a WAF Classic rule group.
type RuleGroup struct {
	RuleGroupId string `json:"RuleGroupId"` //nolint:revive,staticcheck // AWS SDK field name
	Name        string `json:"Name"`
	MetricName  string `json:"MetricName"`
}

// RuleGroupSummary is a summary of a RuleGroup.
type RuleGroupSummary struct {
	RuleGroupId string `json:"RuleGroupId"` //nolint:revive,staticcheck // AWS SDK field name
	Name        string `json:"Name"`
}

// ActivatedRuleUpdate specifies a rule to insert into or delete from a RuleGroup.
type ActivatedRuleUpdate struct {
	Action        string        `json:"Action"`
	ActivatedRule ActivatedRule `json:"ActivatedRule"`
}

// SubscribedRuleGroupSummary is a summary of a subscribed rule group.
type SubscribedRuleGroupSummary struct {
	RuleGroupId string `json:"RuleGroupId"` //nolint:revive,staticcheck // AWS SDK field name
	Name        string `json:"Name"`
	MetricName  string `json:"MetricName"`
}

// LoggingConfiguration is a WAF Classic logging configuration.
type LoggingConfiguration struct {
	ResourceArn           string         `json:"ResourceArn"`
	LogDestinationConfigs []string       `json:"LogDestinationConfigs"`
	RedactedFields        []FieldToMatch `json:"RedactedFields,omitempty"`
}

// Tag is a key-value tag.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// SampledHTTPRequest is a sampled HTTP request.
type SampledHTTPRequest struct {
	RuleId string `json:"RuleWithinRuleGroup,omitempty"` //nolint:revive,staticcheck // AWS SDK field name
	Action string `json:"Action,omitempty"`
	Weight int64  `json:"Weight"`
}

// InMemoryBackend is the in-memory implementation of StorageBackend for WAF Classic.
type InMemoryBackend struct {
	mu                    *lockmetrics.RWMutex
	registry              *store.Registry
	changeTokens          map[string]string // token → status
	webACLs               *store.Table[WebACL]
	rules                 *store.Table[Rule]
	rateBasedRules        *store.Table[RateBasedRule]
	ipSets                *store.Table[IPSet]
	byteMatchSets         *store.Table[ByteMatchSet]
	sizeConstraintSets    *store.Table[SizeConstraintSet]
	sqlInjectionMatchSets *store.Table[SqlInjectionMatchSet]
	xssMatchSets          *store.Table[XssMatchSet]
	geoMatchSets          *store.Table[GeoMatchSet]
	regexPatternSets      *store.Table[RegexPatternSet]
	regexMatchSets        *store.Table[RegexMatchSet]
	ruleGroups            *store.Table[RuleGroup]
	ruleGroupRules        map[string][]ActivatedRule // ruleGroupId → activated rules
	loggingConfigs        *store.Table[LoggingConfiguration]
	permissionPolicies    map[string]string            // resourceArn → policy JSON
	tags                  map[string]map[string]string // arn → tags
	accountID             string
	region                string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:                 lockmetrics.New("waf"),
		registry:           store.NewRegistry(),
		changeTokens:       make(map[string]string),
		ruleGroupRules:     make(map[string][]ActivatedRule),
		permissionPolicies: make(map[string]string),
		tags:               make(map[string]map[string]string),
		accountID:          accountID,
		region:             region,
	}

	registerAllTables(b)

	return b
}

func (b *InMemoryBackend) webACLARN(id string) string {
	return arn.Build("waf", "", b.accountID, fmt.Sprintf("webacl/%s", id))
}

func (b *InMemoryBackend) ruleARN(id string) string {
	return arn.Build("waf", "", b.accountID, fmt.Sprintf("rule/%s", id))
}

func (b *InMemoryBackend) ipSetARN(id string) string {
	return arn.Build("waf", "", b.accountID, fmt.Sprintf("ipset/%s", id))
}

func (b *InMemoryBackend) rateBasedRuleARN(id string) string {
	return arn.Build("waf", "", b.accountID, fmt.Sprintf("ratebasedrule/%s", id))
}

func (b *InMemoryBackend) ruleGroupARN(id string) string {
	return arn.Build("waf", "", b.accountID, fmt.Sprintf("rulegroup/%s", id))
}

// GetChangeToken returns a new change token in PROVISIONED state.
func (b *InMemoryBackend) GetChangeToken() string {
	b.mu.Lock("GetChangeToken")
	defer b.mu.Unlock()

	token := uuid.New().String()
	b.changeTokens[token] = changeTokenStatusPROVISIONED

	if len(b.changeTokens) > maxChangeTokens {
		for k, v := range b.changeTokens {
			if v == changeTokenStatusINSYNC {
				delete(b.changeTokens, k)
			}
		}
	}

	return token
}

// GetChangeTokenStatus returns the status of a change token.
// Unknown tokens return INSYNC, matching real AWS WAF Classic behavior.
func (b *InMemoryBackend) GetChangeTokenStatus(token string) string {
	b.mu.RLock("GetChangeTokenStatus")
	defer b.mu.RUnlock()

	if status, ok := b.changeTokens[token]; ok {
		return status
	}

	return changeTokenStatusINSYNC
}

// MarkChangeTokenUsed transitions a change token from PROVISIONED to INSYNC.
func (b *InMemoryBackend) MarkChangeTokenUsed(token string) {
	b.mu.Lock("MarkChangeTokenUsed")
	defer b.mu.Unlock()

	if _, ok := b.changeTokens[token]; ok {
		b.changeTokens[token] = changeTokenStatusINSYNC
	}
}

// validateChangeToken returns ErrStaleToken unless token was returned by an
// earlier GetChangeToken call and has not yet been consumed by a mutation
// (real AWS WAFStaleDataException: "you tried to create, update, or delete
// an object by using a change token that has already been used"). It does
// not itself consume the token -- the caller (Handler.dispatch) marks it
// INSYNC once the mutation it guards has actually succeeded, via
// MarkChangeTokenUsed. Callers must hold b.mu.
func (b *InMemoryBackend) validateChangeToken(token string) error {
	if status, ok := b.changeTokens[token]; !ok || status != changeTokenStatusPROVISIONED {
		return ErrStaleToken
	}

	return nil
}

// ruleReferenced reports whether id (a Rule, RateBasedRule, or RuleGroup
// identifier) is still activated in some WebACL or contained in some
// RuleGroup -- the two containers that hold ActivatedRule.RuleId references
// (WafRuleType REGULAR/RATE_BASED/GROUP all share the same RuleId shape).
// Callers must hold b.mu.
func (b *InMemoryBackend) ruleReferenced(id string) bool {
	for _, acl := range b.webACLs.All() {
		for _, r := range acl.Rules {
			if r.RuleId == id {
				return true
			}
		}
	}

	for _, rules := range b.ruleGroupRules {
		for _, r := range rules {
			if r.RuleId == id {
				return true
			}
		}
	}

	return false
}

// matchSetReferenced reports whether id (an IPSet, ByteMatchSet,
// SqlInjectionMatchSet, XssMatchSet, SizeConstraintSet, GeoMatchSet, or
// RegexMatchSet identifier) is still used as a Predicate.DataId by any Rule
// or RateBasedRule. Callers must hold b.mu.
func (b *InMemoryBackend) matchSetReferenced(id string) bool {
	for _, r := range b.rules.All() {
		for _, p := range r.Predicates {
			if p.DataId == id {
				return true
			}
		}
	}

	for _, r := range b.rateBasedRules.All() {
		for _, p := range r.MatchPredicates {
			if p.DataId == id {
				return true
			}
		}
	}

	return false
}

// regexPatternSetReferenced reports whether id is still used as a
// RegexMatchTuple.RegexPatternSetId by any RegexMatchSet. Callers must hold
// b.mu.
func (b *InMemoryBackend) regexPatternSetReferenced(id string) bool {
	for _, s := range b.regexMatchSets.All() {
		for _, t := range s.RegexMatchTuples {
			if t.RegexPatternSetId == id {
				return true
			}
		}
	}

	return false
}

// CreateWebACL creates a new WebACL.
func (b *InMemoryBackend) CreateWebACL(
	name, metricName string,
	defaultAction WafAction,
	changeToken string,
	tags map[string]string,
) (*WebACL, error) {
	b.mu.Lock("CreateWebACL")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	acl := &WebACL{
		WebACLId:      id,
		Name:          name,
		MetricName:    metricName,
		DefaultAction: defaultAction,
		Rules:         []ActivatedRule{},
		WebACLArn:     b.webACLARN(id),
	}
	b.webACLs.Put(acl)

	if len(tags) > 0 {
		b.tags[acl.WebACLArn] = maps.Clone(tags)
	}

	return acl, nil
}

// GetWebACL retrieves a WebACL by ID.
func (b *InMemoryBackend) GetWebACL(id string) (*WebACL, error) {
	b.mu.RLock("GetWebACL")
	defer b.mu.RUnlock()

	acl, ok := b.webACLs.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return acl, nil
}

// UpdateWebACL updates a WebACL's default action and rules.
func (b *InMemoryBackend) UpdateWebACL(
	id, changeToken string,
	defaultAction *WafAction,
	updates []WebACLUpdate,
) error {
	b.mu.Lock("UpdateWebACL")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	acl, ok := b.webACLs.Get(id)
	if !ok {
		return ErrNotFound
	}

	if defaultAction != nil {
		acl.DefaultAction = *defaultAction
	}

	for _, u := range updates {
		switch u.Action {
		case updateInsert:
			acl.Rules = append(acl.Rules, u.ActivatedRule)
		case updateDelete:
			filtered := acl.Rules[:0]
			for _, r := range acl.Rules {
				if r.RuleId != u.ActivatedRule.RuleId {
					filtered = append(filtered, r)
				}
			}
			acl.Rules = filtered
		}
	}

	sort.Slice(acl.Rules, func(i, j int) bool {
		return acl.Rules[i].Priority < acl.Rules[j].Priority
	})

	return nil
}

// DeleteWebACL deletes a WebACL.
func (b *InMemoryBackend) DeleteWebACL(id, changeToken string) error {
	b.mu.Lock("DeleteWebACL")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	if !b.webACLs.Has(id) {
		return ErrNotFound
	}

	b.webACLs.Delete(id)

	return nil
}

// ListWebACLs returns summaries of all WebACLs.
func (b *InMemoryBackend) ListWebACLs() []WebACLSummary {
	b.mu.RLock("ListWebACLs")
	defer b.mu.RUnlock()

	all := b.webACLs.All()
	result := make([]WebACLSummary, 0, len(all))
	for _, acl := range all {
		result = append(result, WebACLSummary{WebACLId: acl.WebACLId, Name: acl.Name})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].WebACLId < result[j].WebACLId })

	return result
}

// CreateRule creates a new Rule.
func (b *InMemoryBackend) CreateRule(
	name, metricName, changeToken string,
	tags map[string]string,
) (*Rule, error) {
	b.mu.Lock("CreateRule")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	rule := &Rule{
		RuleId:     id,
		Name:       name,
		MetricName: metricName,
		Predicates: []Predicate{},
	}
	b.rules.Put(rule)

	if len(tags) > 0 {
		b.tags[b.ruleARN(id)] = maps.Clone(tags)
	}

	return rule, nil
}

// GetRule retrieves a Rule by ID.
func (b *InMemoryBackend) GetRule(id string) (*Rule, error) {
	b.mu.RLock("GetRule")
	defer b.mu.RUnlock()

	rule, ok := b.rules.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return rule, nil
}

// UpdateRule updates a Rule's predicates.
func (b *InMemoryBackend) UpdateRule(id, changeToken string, updates []RuleUpdate) error {
	b.mu.Lock("UpdateRule")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	rule, ok := b.rules.Get(id)
	if !ok {
		return ErrNotFound
	}

	for _, u := range updates {
		switch u.Action {
		case updateInsert:
			rule.Predicates = append(rule.Predicates, u.Predicate)
		case updateDelete:
			filtered := rule.Predicates[:0]
			for _, p := range rule.Predicates {
				if p.DataId != u.Predicate.DataId || p.Type != u.Predicate.Type {
					filtered = append(filtered, p)
				}
			}
			rule.Predicates = filtered
		}
	}

	return nil
}

// DeleteRule deletes a Rule.
func (b *InMemoryBackend) DeleteRule(id, changeToken string) error {
	b.mu.Lock("DeleteRule")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	if !b.rules.Has(id) {
		return ErrNotFound
	}

	if b.ruleReferenced(id) {
		return ErrReferencedItem
	}

	b.rules.Delete(id)

	return nil
}

// ListRules returns summaries of all Rules.
func (b *InMemoryBackend) ListRules() []RuleSummary {
	b.mu.RLock("ListRules")
	defer b.mu.RUnlock()

	all := b.rules.All()
	result := make([]RuleSummary, 0, len(all))
	for _, r := range all {
		result = append(result, RuleSummary{RuleId: r.RuleId, Name: r.Name})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].RuleId < result[j].RuleId })

	return result
}

// CreateIPSet creates a new IPSet.
func (b *InMemoryBackend) CreateIPSet(name, changeToken string, tags map[string]string) (*IPSet, error) {
	b.mu.Lock("CreateIPSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	ipSet := &IPSet{
		IPSetId:          id,
		Name:             name,
		IPSetDescriptors: []IPSetDescriptor{},
	}
	b.ipSets.Put(ipSet)

	if len(tags) > 0 {
		b.tags[b.ipSetARN(id)] = maps.Clone(tags)
	}

	return ipSet, nil
}

// GetIPSet retrieves an IPSet by ID.
func (b *InMemoryBackend) GetIPSet(id string) (*IPSet, error) {
	b.mu.RLock("GetIPSet")
	defer b.mu.RUnlock()

	ipSet, ok := b.ipSets.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return ipSet, nil
}

// UpdateIPSet updates an IPSet's descriptors.
func (b *InMemoryBackend) UpdateIPSet(id, changeToken string, updates []IPSetUpdate) error {
	b.mu.Lock("UpdateIPSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	ipSet, ok := b.ipSets.Get(id)
	if !ok {
		return ErrNotFound
	}

	for _, u := range updates {
		switch u.Action {
		case updateInsert:
			ipSet.IPSetDescriptors = append(ipSet.IPSetDescriptors, u.IPSetDescriptor)
		case updateDelete:
			filtered := ipSet.IPSetDescriptors[:0]
			for _, d := range ipSet.IPSetDescriptors {
				if d.Type != u.IPSetDescriptor.Type || d.Value != u.IPSetDescriptor.Value {
					filtered = append(filtered, d)
				}
			}
			ipSet.IPSetDescriptors = filtered
		}
	}

	return nil
}

// DeleteIPSet deletes an IPSet.
func (b *InMemoryBackend) DeleteIPSet(id, changeToken string) error {
	b.mu.Lock("DeleteIPSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	if !b.ipSets.Has(id) {
		return ErrNotFound
	}

	if b.matchSetReferenced(id) {
		return ErrReferencedItem
	}

	b.ipSets.Delete(id)

	return nil
}

// ListIPSets returns summaries of all IPSets.
func (b *InMemoryBackend) ListIPSets() []IPSetSummary {
	b.mu.RLock("ListIPSets")
	defer b.mu.RUnlock()

	all := b.ipSets.All()
	result := make([]IPSetSummary, 0, len(all))
	for _, s := range all {
		result = append(result, IPSetSummary{IPSetId: s.IPSetId, Name: s.Name})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].IPSetId < result[j].IPSetId })

	return result
}

// CreateByteMatchSet creates a new ByteMatchSet.
func (b *InMemoryBackend) CreateByteMatchSet(name, changeToken string) (*ByteMatchSet, error) {
	b.mu.Lock("CreateByteMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	bms := &ByteMatchSet{
		ByteMatchSetId:  id,
		Name:            name,
		ByteMatchTuples: []ByteMatchTuple{},
	}
	b.byteMatchSets.Put(bms)

	return bms, nil
}

// GetByteMatchSet retrieves a ByteMatchSet by ID.
func (b *InMemoryBackend) GetByteMatchSet(id string) (*ByteMatchSet, error) {
	b.mu.RLock("GetByteMatchSet")
	defer b.mu.RUnlock()

	bms, ok := b.byteMatchSets.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return bms, nil
}

// UpdateByteMatchSet updates a ByteMatchSet's tuples.
func (b *InMemoryBackend) UpdateByteMatchSet(id, changeToken string, updates []ByteMatchSetUpdate) error {
	b.mu.Lock("UpdateByteMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	bms, ok := b.byteMatchSets.Get(id)
	if !ok {
		return ErrNotFound
	}

	for _, u := range updates {
		switch u.Action {
		case updateInsert:
			bms.ByteMatchTuples = append(bms.ByteMatchTuples, u.ByteMatchTuple)
		case updateDelete:
			filtered := bms.ByteMatchTuples[:0]
			for _, t := range bms.ByteMatchTuples {
				if t.TargetString != u.ByteMatchTuple.TargetString ||
					t.FieldToMatch.Type != u.ByteMatchTuple.FieldToMatch.Type {
					filtered = append(filtered, t)
				}
			}
			bms.ByteMatchTuples = filtered
		}
	}

	return nil
}

// DeleteByteMatchSet deletes a ByteMatchSet.
func (b *InMemoryBackend) DeleteByteMatchSet(id, changeToken string) error {
	b.mu.Lock("DeleteByteMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	if !b.byteMatchSets.Has(id) {
		return ErrNotFound
	}

	if b.matchSetReferenced(id) {
		return ErrReferencedItem
	}

	b.byteMatchSets.Delete(id)

	return nil
}

// ListByteMatchSets returns summaries of all ByteMatchSets.
func (b *InMemoryBackend) ListByteMatchSets() []ByteMatchSetSummary {
	b.mu.RLock("ListByteMatchSets")
	defer b.mu.RUnlock()

	all := b.byteMatchSets.All()
	result := make([]ByteMatchSetSummary, 0, len(all))
	for _, s := range all {
		result = append(result, ByteMatchSetSummary{ByteMatchSetId: s.ByteMatchSetId, Name: s.Name})
	}

	sort.Slice(
		result,
		func(i, j int) bool { return result[i].ByteMatchSetId < result[j].ByteMatchSetId },
	)

	return result
}

// CreateSizeConstraintSet creates a new SizeConstraintSet.
func (b *InMemoryBackend) CreateSizeConstraintSet(name, changeToken string) (*SizeConstraintSet, error) {
	b.mu.Lock("CreateSizeConstraintSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	scs := &SizeConstraintSet{
		SizeConstraintSetId: id,
		Name:                name,
		SizeConstraints:     []SizeConstraint{},
	}
	b.sizeConstraintSets.Put(scs)

	return scs, nil
}

// GetSizeConstraintSet retrieves a SizeConstraintSet by ID.
func (b *InMemoryBackend) GetSizeConstraintSet(id string) (*SizeConstraintSet, error) {
	b.mu.RLock("GetSizeConstraintSet")
	defer b.mu.RUnlock()

	scs, ok := b.sizeConstraintSets.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return scs, nil
}

// UpdateSizeConstraintSet updates a SizeConstraintSet's constraints.
func (b *InMemoryBackend) UpdateSizeConstraintSet(
	id, changeToken string,
	updates []SizeConstraintSetUpdate,
) error {
	b.mu.Lock("UpdateSizeConstraintSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	scs, ok := b.sizeConstraintSets.Get(id)
	if !ok {
		return ErrNotFound
	}

	for _, u := range updates {
		switch u.Action {
		case updateInsert:
			scs.SizeConstraints = append(scs.SizeConstraints, u.SizeConstraint)
		case updateDelete:
			filtered := scs.SizeConstraints[:0]
			for _, c := range scs.SizeConstraints {
				if c.FieldToMatch.Type != u.SizeConstraint.FieldToMatch.Type ||
					c.Size != u.SizeConstraint.Size {
					filtered = append(filtered, c)
				}
			}
			scs.SizeConstraints = filtered
		}
	}

	return nil
}

// DeleteSizeConstraintSet deletes a SizeConstraintSet.
func (b *InMemoryBackend) DeleteSizeConstraintSet(id, changeToken string) error {
	b.mu.Lock("DeleteSizeConstraintSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	if !b.sizeConstraintSets.Has(id) {
		return ErrNotFound
	}

	if b.matchSetReferenced(id) {
		return ErrReferencedItem
	}

	b.sizeConstraintSets.Delete(id)

	return nil
}

// ListSizeConstraintSets returns summaries of all SizeConstraintSets.
func (b *InMemoryBackend) ListSizeConstraintSets() []SizeConstraintSetSummary {
	b.mu.RLock("ListSizeConstraintSets")
	defer b.mu.RUnlock()

	all := b.sizeConstraintSets.All()
	result := make([]SizeConstraintSetSummary, 0, len(all))
	for _, s := range all {
		result = append(
			result,
			SizeConstraintSetSummary{SizeConstraintSetId: s.SizeConstraintSetId, Name: s.Name},
		)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].SizeConstraintSetId < result[j].SizeConstraintSetId
	})

	return result
}

// CreateSqlInjectionMatchSet creates a new SqlInjectionMatchSet.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) CreateSqlInjectionMatchSet(
	name, changeToken string,
) (*SqlInjectionMatchSet, error) {
	b.mu.Lock("CreateSqlInjectionMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	sims := &SqlInjectionMatchSet{
		SqlInjectionMatchSetId:  id,
		Name:                    name,
		SqlInjectionMatchTuples: []SqlInjectionMatchTuple{},
	}
	b.sqlInjectionMatchSets.Put(sims)

	return sims, nil
}

// GetSqlInjectionMatchSet retrieves a SqlInjectionMatchSet by ID.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) GetSqlInjectionMatchSet(id string) (*SqlInjectionMatchSet, error) {
	b.mu.RLock("GetSqlInjectionMatchSet")
	defer b.mu.RUnlock()

	sims, ok := b.sqlInjectionMatchSets.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return sims, nil
}

// UpdateSqlInjectionMatchSet updates a SqlInjectionMatchSet's tuples.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) UpdateSqlInjectionMatchSet(
	id, changeToken string,
	updates []SqlInjectionMatchSetUpdate,
) error {
	b.mu.Lock("UpdateSqlInjectionMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	sims, ok := b.sqlInjectionMatchSets.Get(id)
	if !ok {
		return ErrNotFound
	}

	for _, u := range updates {
		switch u.Action {
		case updateInsert:
			sims.SqlInjectionMatchTuples = append(
				sims.SqlInjectionMatchTuples,
				u.SqlInjectionMatchTuple,
			)
		case updateDelete:
			filtered := sims.SqlInjectionMatchTuples[:0]
			for _, t := range sims.SqlInjectionMatchTuples {
				if t.FieldToMatch.Type != u.SqlInjectionMatchTuple.FieldToMatch.Type ||
					t.TextTransformation != u.SqlInjectionMatchTuple.TextTransformation {
					filtered = append(filtered, t)
				}
			}
			sims.SqlInjectionMatchTuples = filtered
		}
	}

	return nil
}

// DeleteSqlInjectionMatchSet deletes a SqlInjectionMatchSet.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) DeleteSqlInjectionMatchSet(id, changeToken string) error {
	b.mu.Lock("DeleteSqlInjectionMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	if !b.sqlInjectionMatchSets.Has(id) {
		return ErrNotFound
	}

	if b.matchSetReferenced(id) {
		return ErrReferencedItem
	}

	b.sqlInjectionMatchSets.Delete(id)

	return nil
}

// ListSqlInjectionMatchSets returns summaries of all SqlInjectionMatchSets.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) ListSqlInjectionMatchSets() []SqlInjectionMatchSetSummary {
	b.mu.RLock("ListSqlInjectionMatchSets")
	defer b.mu.RUnlock()

	all := b.sqlInjectionMatchSets.All()
	result := make([]SqlInjectionMatchSetSummary, 0, len(all))
	for _, s := range all {
		result = append(result, SqlInjectionMatchSetSummary{
			SqlInjectionMatchSetId: s.SqlInjectionMatchSetId,
			Name:                   s.Name,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].SqlInjectionMatchSetId < result[j].SqlInjectionMatchSetId
	})

	return result
}

// CreateXssMatchSet creates a new XssMatchSet.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) CreateXssMatchSet(name, changeToken string) (*XssMatchSet, error) {
	b.mu.Lock("CreateXssMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	xms := &XssMatchSet{
		XssMatchSetId:  id,
		Name:           name,
		XssMatchTuples: []XssMatchTuple{},
	}
	b.xssMatchSets.Put(xms)

	return xms, nil
}

// GetXssMatchSet retrieves an XssMatchSet by ID.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) GetXssMatchSet(id string) (*XssMatchSet, error) {
	b.mu.RLock("GetXssMatchSet")
	defer b.mu.RUnlock()

	xms, ok := b.xssMatchSets.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return xms, nil
}

// UpdateXssMatchSet updates an XssMatchSet's tuples.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) UpdateXssMatchSet(id, changeToken string, updates []XssMatchSetUpdate) error {
	b.mu.Lock("UpdateXssMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	xms, ok := b.xssMatchSets.Get(id)
	if !ok {
		return ErrNotFound
	}

	for _, u := range updates {
		switch u.Action {
		case updateInsert:
			xms.XssMatchTuples = append(xms.XssMatchTuples, u.XssMatchTuple)
		case updateDelete:
			filtered := xms.XssMatchTuples[:0]
			for _, t := range xms.XssMatchTuples {
				if t.FieldToMatch.Type != u.XssMatchTuple.FieldToMatch.Type ||
					t.TextTransformation != u.XssMatchTuple.TextTransformation {
					filtered = append(filtered, t)
				}
			}
			xms.XssMatchTuples = filtered
		}
	}

	return nil
}

// DeleteXssMatchSet deletes an XssMatchSet.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) DeleteXssMatchSet(id, changeToken string) error {
	b.mu.Lock("DeleteXssMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	if !b.xssMatchSets.Has(id) {
		return ErrNotFound
	}

	if b.matchSetReferenced(id) {
		return ErrReferencedItem
	}

	b.xssMatchSets.Delete(id)

	return nil
}

// ListXssMatchSets returns summaries of all XssMatchSets.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) ListXssMatchSets() []XssMatchSetSummary {
	b.mu.RLock("ListXssMatchSets")
	defer b.mu.RUnlock()

	all := b.xssMatchSets.All()
	result := make([]XssMatchSetSummary, 0, len(all))
	for _, s := range all {
		result = append(result, XssMatchSetSummary{XssMatchSetId: s.XssMatchSetId, Name: s.Name})
	}

	sort.Slice(
		result,
		func(i, j int) bool { return result[i].XssMatchSetId < result[j].XssMatchSetId },
	)

	return result
}

// CreateGeoMatchSet creates a new GeoMatchSet.
func (b *InMemoryBackend) CreateGeoMatchSet(name, changeToken string) (*GeoMatchSet, error) {
	b.mu.Lock("CreateGeoMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	gms := &GeoMatchSet{
		GeoMatchSetId:       id,
		Name:                name,
		GeoMatchConstraints: []GeoMatchConstraint{},
	}
	b.geoMatchSets.Put(gms)

	return gms, nil
}

// GetGeoMatchSet retrieves a GeoMatchSet by ID.
func (b *InMemoryBackend) GetGeoMatchSet(id string) (*GeoMatchSet, error) {
	b.mu.RLock("GetGeoMatchSet")
	defer b.mu.RUnlock()

	gms, ok := b.geoMatchSets.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return gms, nil
}

// UpdateGeoMatchSet updates a GeoMatchSet's constraints.
func (b *InMemoryBackend) UpdateGeoMatchSet(id, changeToken string, updates []GeoMatchSetUpdate) error {
	b.mu.Lock("UpdateGeoMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	gms, ok := b.geoMatchSets.Get(id)
	if !ok {
		return ErrNotFound
	}

	for _, u := range updates {
		switch u.Action {
		case updateInsert:
			gms.GeoMatchConstraints = append(gms.GeoMatchConstraints, u.GeoMatchConstraint)
		case updateDelete:
			filtered := gms.GeoMatchConstraints[:0]
			for _, c := range gms.GeoMatchConstraints {
				if c.Type != u.GeoMatchConstraint.Type || c.Value != u.GeoMatchConstraint.Value {
					filtered = append(filtered, c)
				}
			}
			gms.GeoMatchConstraints = filtered
		}
	}

	return nil
}

// DeleteGeoMatchSet deletes a GeoMatchSet.
func (b *InMemoryBackend) DeleteGeoMatchSet(id, changeToken string) error {
	b.mu.Lock("DeleteGeoMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	if !b.geoMatchSets.Has(id) {
		return ErrNotFound
	}

	if b.matchSetReferenced(id) {
		return ErrReferencedItem
	}

	b.geoMatchSets.Delete(id)

	return nil
}

// ListGeoMatchSets returns summaries of all GeoMatchSets.
func (b *InMemoryBackend) ListGeoMatchSets() []GeoMatchSetSummary {
	b.mu.RLock("ListGeoMatchSets")
	defer b.mu.RUnlock()

	all := b.geoMatchSets.All()
	result := make([]GeoMatchSetSummary, 0, len(all))
	for _, s := range all {
		result = append(result, GeoMatchSetSummary{GeoMatchSetId: s.GeoMatchSetId, Name: s.Name})
	}

	sort.Slice(
		result,
		func(i, j int) bool { return result[i].GeoMatchSetId < result[j].GeoMatchSetId },
	)

	return result
}

// CreateRateBasedRule creates a new RateBasedRule.
func (b *InMemoryBackend) CreateRateBasedRule(
	name, metricName, rateKey string,
	rateLimit int64,
	changeToken string,
	tags map[string]string,
) (*RateBasedRule, error) {
	b.mu.Lock("CreateRateBasedRule")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	rule := &RateBasedRule{
		RuleId:          id,
		Name:            name,
		MetricName:      metricName,
		RateKey:         rateKey,
		RateLimit:       rateLimit,
		MatchPredicates: []Predicate{},
	}
	b.rateBasedRules.Put(rule)

	if len(tags) > 0 {
		b.tags[b.rateBasedRuleARN(id)] = maps.Clone(tags)
	}

	return rule, nil
}

// GetRateBasedRule retrieves a RateBasedRule by ID.
func (b *InMemoryBackend) GetRateBasedRule(id string) (*RateBasedRule, error) {
	b.mu.RLock("GetRateBasedRule")
	defer b.mu.RUnlock()

	rule, ok := b.rateBasedRules.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return rule, nil
}

// UpdateRateBasedRule updates a RateBasedRule's predicates and rate limit.
func (b *InMemoryBackend) UpdateRateBasedRule(
	id, changeToken string,
	rateLimit int64,
	updates []RuleUpdate,
) error {
	b.mu.Lock("UpdateRateBasedRule")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	rule, ok := b.rateBasedRules.Get(id)
	if !ok {
		return ErrNotFound
	}

	if rateLimit > 0 {
		rule.RateLimit = rateLimit
	}

	for _, u := range updates {
		switch u.Action {
		case updateInsert:
			rule.MatchPredicates = append(rule.MatchPredicates, u.Predicate)
		case updateDelete:
			filtered := rule.MatchPredicates[:0]
			for _, p := range rule.MatchPredicates {
				if p.DataId != u.Predicate.DataId || p.Type != u.Predicate.Type {
					filtered = append(filtered, p)
				}
			}
			rule.MatchPredicates = filtered
		}
	}

	return nil
}

// DeleteRateBasedRule deletes a RateBasedRule.
func (b *InMemoryBackend) DeleteRateBasedRule(id, changeToken string) error {
	b.mu.Lock("DeleteRateBasedRule")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	if !b.rateBasedRules.Has(id) {
		return ErrNotFound
	}

	if b.ruleReferenced(id) {
		return ErrReferencedItem
	}

	b.rateBasedRules.Delete(id)

	return nil
}

// ListRateBasedRules returns summaries of all RateBasedRules.
func (b *InMemoryBackend) ListRateBasedRules() []RateBasedRuleSummary {
	b.mu.RLock("ListRateBasedRules")
	defer b.mu.RUnlock()

	all := b.rateBasedRules.All()
	result := make([]RateBasedRuleSummary, 0, len(all))
	for _, r := range all {
		result = append(result, RateBasedRuleSummary{RuleId: r.RuleId, Name: r.Name})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].RuleId < result[j].RuleId })

	return result
}

// GetRateBasedRuleManagedKeys returns the IP addresses currently blocked by a rate-based rule (stub).
func (b *InMemoryBackend) GetRateBasedRuleManagedKeys(id string) ([]string, error) {
	b.mu.RLock("GetRateBasedRuleManagedKeys")
	defer b.mu.RUnlock()

	if !b.rateBasedRules.Has(id) {
		return nil, ErrNotFound
	}

	return []string{}, nil
}

// CreateRegexPatternSet creates a new RegexPatternSet.
func (b *InMemoryBackend) CreateRegexPatternSet(name, changeToken string) (*RegexPatternSet, error) {
	b.mu.Lock("CreateRegexPatternSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	rps := &RegexPatternSet{
		RegexPatternSetId:   id,
		Name:                name,
		RegexPatternStrings: []string{},
	}
	b.regexPatternSets.Put(rps)

	return rps, nil
}

// GetRegexPatternSet retrieves a RegexPatternSet by ID.
func (b *InMemoryBackend) GetRegexPatternSet(id string) (*RegexPatternSet, error) {
	b.mu.RLock("GetRegexPatternSet")
	defer b.mu.RUnlock()

	rps, ok := b.regexPatternSets.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return rps, nil
}

// UpdateRegexPatternSet updates a RegexPatternSet's pattern strings.
func (b *InMemoryBackend) UpdateRegexPatternSet(id, changeToken string, updates []RegexPatternSetUpdate) error {
	b.mu.Lock("UpdateRegexPatternSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	rps, ok := b.regexPatternSets.Get(id)
	if !ok {
		return ErrNotFound
	}

	for _, u := range updates {
		switch u.Action {
		case updateInsert:
			rps.RegexPatternStrings = append(rps.RegexPatternStrings, u.RegexPatternString)
		case updateDelete:
			filtered := rps.RegexPatternStrings[:0]
			for _, p := range rps.RegexPatternStrings {
				if p != u.RegexPatternString {
					filtered = append(filtered, p)
				}
			}
			rps.RegexPatternStrings = filtered
		}
	}

	return nil
}

// DeleteRegexPatternSet deletes a RegexPatternSet.
func (b *InMemoryBackend) DeleteRegexPatternSet(id, changeToken string) error {
	b.mu.Lock("DeleteRegexPatternSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	if !b.regexPatternSets.Has(id) {
		return ErrNotFound
	}

	if b.regexPatternSetReferenced(id) {
		return ErrReferencedItem
	}

	b.regexPatternSets.Delete(id)

	return nil
}

// ListRegexPatternSets returns summaries of all RegexPatternSets.
func (b *InMemoryBackend) ListRegexPatternSets() []RegexPatternSetSummary {
	b.mu.RLock("ListRegexPatternSets")
	defer b.mu.RUnlock()

	all := b.regexPatternSets.All()
	result := make([]RegexPatternSetSummary, 0, len(all))
	for _, s := range all {
		result = append(result, RegexPatternSetSummary{RegexPatternSetId: s.RegexPatternSetId, Name: s.Name})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RegexPatternSetId < result[j].RegexPatternSetId
	})

	return result
}

// CreateRegexMatchSet creates a new RegexMatchSet.
func (b *InMemoryBackend) CreateRegexMatchSet(name, changeToken string) (*RegexMatchSet, error) {
	b.mu.Lock("CreateRegexMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	rms := &RegexMatchSet{
		RegexMatchSetId:  id,
		Name:             name,
		RegexMatchTuples: []RegexMatchTuple{},
	}
	b.regexMatchSets.Put(rms)

	return rms, nil
}

// GetRegexMatchSet retrieves a RegexMatchSet by ID.
func (b *InMemoryBackend) GetRegexMatchSet(id string) (*RegexMatchSet, error) {
	b.mu.RLock("GetRegexMatchSet")
	defer b.mu.RUnlock()

	rms, ok := b.regexMatchSets.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return rms, nil
}

// UpdateRegexMatchSet updates a RegexMatchSet's tuples.
func (b *InMemoryBackend) UpdateRegexMatchSet(id, changeToken string, updates []RegexMatchSetUpdate) error {
	b.mu.Lock("UpdateRegexMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	rms, ok := b.regexMatchSets.Get(id)
	if !ok {
		return ErrNotFound
	}

	for _, u := range updates {
		switch u.Action {
		case updateInsert:
			rms.RegexMatchTuples = append(rms.RegexMatchTuples, u.RegexMatchTuple)
		case updateDelete:
			filtered := rms.RegexMatchTuples[:0]
			for _, t := range rms.RegexMatchTuples {
				if t.RegexPatternSetId != u.RegexMatchTuple.RegexPatternSetId ||
					t.FieldToMatch.Type != u.RegexMatchTuple.FieldToMatch.Type {
					filtered = append(filtered, t)
				}
			}
			rms.RegexMatchTuples = filtered
		}
	}

	return nil
}

// DeleteRegexMatchSet deletes a RegexMatchSet.
func (b *InMemoryBackend) DeleteRegexMatchSet(id, changeToken string) error {
	b.mu.Lock("DeleteRegexMatchSet")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	if !b.regexMatchSets.Has(id) {
		return ErrNotFound
	}

	if b.matchSetReferenced(id) {
		return ErrReferencedItem
	}

	b.regexMatchSets.Delete(id)

	return nil
}

// ListRegexMatchSets returns summaries of all RegexMatchSets.
func (b *InMemoryBackend) ListRegexMatchSets() []RegexMatchSetSummary {
	b.mu.RLock("ListRegexMatchSets")
	defer b.mu.RUnlock()

	all := b.regexMatchSets.All()
	result := make([]RegexMatchSetSummary, 0, len(all))
	for _, s := range all {
		result = append(result, RegexMatchSetSummary{RegexMatchSetId: s.RegexMatchSetId, Name: s.Name})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RegexMatchSetId < result[j].RegexMatchSetId
	})

	return result
}

// CreateRuleGroup creates a new RuleGroup.
func (b *InMemoryBackend) CreateRuleGroup(
	name, metricName, changeToken string,
	tags map[string]string,
) (*RuleGroup, error) {
	b.mu.Lock("CreateRuleGroup")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return nil, err
	}

	id := uuid.New().String()
	rg := &RuleGroup{
		RuleGroupId: id,
		Name:        name,
		MetricName:  metricName,
	}
	b.ruleGroups.Put(rg)
	b.ruleGroupRules[id] = []ActivatedRule{}

	if len(tags) > 0 {
		b.tags[b.ruleGroupARN(id)] = maps.Clone(tags)
	}

	return rg, nil
}

// GetRuleGroup retrieves a RuleGroup by ID.
func (b *InMemoryBackend) GetRuleGroup(id string) (*RuleGroup, error) {
	b.mu.RLock("GetRuleGroup")
	defer b.mu.RUnlock()

	rg, ok := b.ruleGroups.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	return rg, nil
}

// UpdateRuleGroup updates a RuleGroup's activated rules.
func (b *InMemoryBackend) UpdateRuleGroup(id, changeToken string, updates []ActivatedRuleUpdate) error {
	b.mu.Lock("UpdateRuleGroup")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	if !b.ruleGroups.Has(id) {
		return ErrNotFound
	}

	rules := b.ruleGroupRules[id]
	for _, u := range updates {
		switch u.Action {
		case updateInsert:
			rules = append(rules, u.ActivatedRule)
		case updateDelete:
			filtered := rules[:0]
			for _, r := range rules {
				if r.RuleId != u.ActivatedRule.RuleId {
					filtered = append(filtered, r)
				}
			}
			rules = filtered
		}
	}

	sort.Slice(rules, func(i, j int) bool { return rules[i].Priority < rules[j].Priority })
	b.ruleGroupRules[id] = rules

	return nil
}

// DeleteRuleGroup deletes a RuleGroup.
func (b *InMemoryBackend) DeleteRuleGroup(id, changeToken string) error {
	b.mu.Lock("DeleteRuleGroup")
	defer b.mu.Unlock()

	if err := b.validateChangeToken(changeToken); err != nil {
		return err
	}

	if !b.ruleGroups.Has(id) {
		return ErrNotFound
	}

	if b.ruleReferenced(id) {
		return ErrReferencedItem
	}

	b.ruleGroups.Delete(id)
	delete(b.ruleGroupRules, id)

	return nil
}

// ListRuleGroups returns summaries of all RuleGroups.
func (b *InMemoryBackend) ListRuleGroups() []RuleGroupSummary {
	b.mu.RLock("ListRuleGroups")
	defer b.mu.RUnlock()

	all := b.ruleGroups.All()
	result := make([]RuleGroupSummary, 0, len(all))
	for _, rg := range all {
		result = append(result, RuleGroupSummary{RuleGroupId: rg.RuleGroupId, Name: rg.Name})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].RuleGroupId < result[j].RuleGroupId })

	return result
}

// ListActivatedRulesInRuleGroup returns the activated rules for a RuleGroup.
func (b *InMemoryBackend) ListActivatedRulesInRuleGroup(id string) ([]ActivatedRule, error) {
	b.mu.RLock("ListActivatedRulesInRuleGroup")
	defer b.mu.RUnlock()

	if !b.ruleGroups.Has(id) {
		return nil, ErrNotFound
	}

	rules := b.ruleGroupRules[id]
	result := make([]ActivatedRule, len(rules))
	copy(result, rules)

	return result, nil
}

// ListSubscribedRuleGroups returns subscribed rule groups (always empty in mock).
func (b *InMemoryBackend) ListSubscribedRuleGroups() []SubscribedRuleGroupSummary {
	return []SubscribedRuleGroupSummary{}
}

// PutLoggingConfiguration stores a logging configuration for a WebACL.
func (b *InMemoryBackend) PutLoggingConfiguration(config LoggingConfiguration) (*LoggingConfiguration, error) {
	b.mu.Lock("PutLoggingConfiguration")
	defer b.mu.Unlock()

	stored := config
	b.loggingConfigs.Put(&stored)

	return &stored, nil
}

// GetLoggingConfiguration retrieves the logging configuration for a WebACL ARN.
func (b *InMemoryBackend) GetLoggingConfiguration(resourceArn string) (*LoggingConfiguration, error) {
	b.mu.RLock("GetLoggingConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.loggingConfigs.Get(resourceArn)
	if !ok {
		return nil, ErrNotFound
	}

	return cfg, nil
}

// DeleteLoggingConfiguration removes the logging configuration for a WebACL ARN.
func (b *InMemoryBackend) DeleteLoggingConfiguration(resourceArn string) error {
	b.mu.Lock("DeleteLoggingConfiguration")
	defer b.mu.Unlock()

	if !b.loggingConfigs.Has(resourceArn) {
		return ErrNotFound
	}

	b.loggingConfigs.Delete(resourceArn)

	return nil
}

// ListLoggingConfigurations returns all logging configurations.
func (b *InMemoryBackend) ListLoggingConfigurations() []LoggingConfiguration {
	b.mu.RLock("ListLoggingConfigurations")
	defer b.mu.RUnlock()

	all := b.loggingConfigs.All()
	result := make([]LoggingConfiguration, 0, len(all))
	for _, cfg := range all {
		result = append(result, *cfg)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ResourceArn < result[j].ResourceArn })

	return result
}

// PutPermissionPolicy stores a permission policy for a resource ARN.
func (b *InMemoryBackend) PutPermissionPolicy(resourceArn, policy string) error {
	b.mu.Lock("PutPermissionPolicy")
	defer b.mu.Unlock()

	b.permissionPolicies[resourceArn] = policy

	return nil
}

// GetPermissionPolicy retrieves the permission policy for a resource ARN.
func (b *InMemoryBackend) GetPermissionPolicy(resourceArn string) (string, error) {
	b.mu.RLock("GetPermissionPolicy")
	defer b.mu.RUnlock()

	policy, ok := b.permissionPolicies[resourceArn]
	if !ok {
		return "", ErrNotFound
	}

	return policy, nil
}

// DeletePermissionPolicy removes the permission policy for a resource ARN.
func (b *InMemoryBackend) DeletePermissionPolicy(resourceArn string) error {
	b.mu.Lock("DeletePermissionPolicy")
	defer b.mu.Unlock()

	if _, ok := b.permissionPolicies[resourceArn]; !ok {
		return ErrNotFound
	}

	delete(b.permissionPolicies, resourceArn)

	return nil
}

// TagResource adds tags to a resource identified by ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[arn] == nil {
		b.tags[arn] = make(map[string]string)
	}

	maps.Copy(b.tags[arn], tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, k := range keys {
		delete(b.tags[arn], k)
	}

	return nil
}

// ListTagsForResource returns the tags for a resource ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tagMap := b.tags[arn]
	result := make([]Tag, 0, len(tagMap))

	for k, v := range tagMap {
		result = append(result, Tag{Key: k, Value: v})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })

	return result, nil
}

// GetSampledRequests returns an empty sample (stub).
func (b *InMemoryBackend) GetSampledRequests(_, _ string, _ int64) []SampledHTTPRequest {
	return []SampledHTTPRequest{}
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.changeTokens = make(map[string]string)
	b.ruleGroupRules = make(map[string][]ActivatedRule)
	b.permissionPolicies = make(map[string]string)
	b.tags = make(map[string]map[string]string)
}
