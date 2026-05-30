package waf

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	changeTokenStatusINSYNC      = "INSYNC"
	changeTokenStatusPROVISIONED = "PROVISIONED"

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
	changeTokens          map[string]string // token → status
	webACLs               map[string]*WebACL
	rules                 map[string]*Rule
	ipSets                map[string]*IPSet
	byteMatchSets         map[string]*ByteMatchSet
	sizeConstraintSets    map[string]*SizeConstraintSet
	sqlInjectionMatchSets map[string]*SqlInjectionMatchSet
	xssMatchSets          map[string]*XssMatchSet
	geoMatchSets          map[string]*GeoMatchSet
	tags                  map[string]map[string]string // arn → tags
	accountID             string
	region                string
}

// NewInMemoryBackend constructs a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:                    lockmetrics.New("waf"),
		changeTokens:          make(map[string]string),
		webACLs:               make(map[string]*WebACL),
		rules:                 make(map[string]*Rule),
		ipSets:                make(map[string]*IPSet),
		byteMatchSets:         make(map[string]*ByteMatchSet),
		sizeConstraintSets:    make(map[string]*SizeConstraintSet),
		sqlInjectionMatchSets: make(map[string]*SqlInjectionMatchSet),
		xssMatchSets:          make(map[string]*XssMatchSet),
		geoMatchSets:          make(map[string]*GeoMatchSet),
		tags:                  make(map[string]map[string]string),
		accountID:             accountID,
		region:                region,
	}
}

func (b *InMemoryBackend) webACLARN(id string) string {
	return fmt.Sprintf("arn:aws:waf::%s:webacl/%s", b.accountID, id)
}

func (b *InMemoryBackend) ruleARN(id string) string {
	return fmt.Sprintf("arn:aws:waf::%s:rule/%s", b.accountID, id)
}

func (b *InMemoryBackend) ipSetARN(id string) string {
	return fmt.Sprintf("arn:aws:waf::%s:ipset/%s", b.accountID, id)
}

// GetChangeToken returns a new change token in PROVISIONED state.
func (b *InMemoryBackend) GetChangeToken() string {
	b.mu.Lock("GetChangeToken")
	defer b.mu.Unlock()

	token := uuid.New().String()
	b.changeTokens[token] = changeTokenStatusPROVISIONED

	return token
}

// GetChangeTokenStatus returns the status of a change token.
func (b *InMemoryBackend) GetChangeTokenStatus(token string) string {
	b.mu.RLock("GetChangeTokenStatus")
	defer b.mu.RUnlock()

	if _, ok := b.changeTokens[token]; ok {
		return changeTokenStatusINSYNC
	}

	return changeTokenStatusINSYNC
}

// CreateWebACL creates a new WebACL.
func (b *InMemoryBackend) CreateWebACL(
	name, metricName string,
	defaultAction WafAction,
	tags map[string]string,
) (*WebACL, error) {
	b.mu.Lock("CreateWebACL")
	defer b.mu.Unlock()

	id := uuid.New().String()
	acl := &WebACL{
		WebACLId:      id,
		Name:          name,
		MetricName:    metricName,
		DefaultAction: defaultAction,
		Rules:         []ActivatedRule{},
		WebACLArn:     b.webACLARN(id),
	}
	b.webACLs[id] = acl

	if len(tags) > 0 {
		b.tags[acl.WebACLArn] = maps.Clone(tags)
	}

	return acl, nil
}

// GetWebACL retrieves a WebACL by ID.
func (b *InMemoryBackend) GetWebACL(id string) (*WebACL, error) {
	b.mu.RLock("GetWebACL")
	defer b.mu.RUnlock()

	acl, ok := b.webACLs[id]
	if !ok {
		return nil, ErrNotFound
	}

	return acl, nil
}

// UpdateWebACL updates a WebACL's default action and rules.
func (b *InMemoryBackend) UpdateWebACL(
	id, _ string,
	defaultAction *WafAction,
	updates []WebACLUpdate,
) error {
	b.mu.Lock("UpdateWebACL")
	defer b.mu.Unlock()

	acl, ok := b.webACLs[id]
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
func (b *InMemoryBackend) DeleteWebACL(id, _ string) error {
	b.mu.Lock("DeleteWebACL")
	defer b.mu.Unlock()

	if _, ok := b.webACLs[id]; !ok {
		return ErrNotFound
	}

	delete(b.webACLs, id)

	return nil
}

// ListWebACLs returns summaries of all WebACLs.
func (b *InMemoryBackend) ListWebACLs() []WebACLSummary {
	b.mu.RLock("ListWebACLs")
	defer b.mu.RUnlock()

	result := make([]WebACLSummary, 0, len(b.webACLs))
	for _, acl := range b.webACLs {
		result = append(result, WebACLSummary{WebACLId: acl.WebACLId, Name: acl.Name})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].WebACLId < result[j].WebACLId })

	return result
}

// CreateRule creates a new Rule.
func (b *InMemoryBackend) CreateRule(
	name, metricName, _ string,
	tags map[string]string,
) (*Rule, error) {
	b.mu.Lock("CreateRule")
	defer b.mu.Unlock()

	id := uuid.New().String()
	rule := &Rule{
		RuleId:     id,
		Name:       name,
		MetricName: metricName,
		Predicates: []Predicate{},
	}
	b.rules[id] = rule

	if len(tags) > 0 {
		b.tags[b.ruleARN(id)] = maps.Clone(tags)
	}

	return rule, nil
}

// GetRule retrieves a Rule by ID.
func (b *InMemoryBackend) GetRule(id string) (*Rule, error) {
	b.mu.RLock("GetRule")
	defer b.mu.RUnlock()

	rule, ok := b.rules[id]
	if !ok {
		return nil, ErrNotFound
	}

	return rule, nil
}

// UpdateRule updates a Rule's predicates.
func (b *InMemoryBackend) UpdateRule(id, _ string, updates []RuleUpdate) error {
	b.mu.Lock("UpdateRule")
	defer b.mu.Unlock()

	rule, ok := b.rules[id]
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
func (b *InMemoryBackend) DeleteRule(id, _ string) error {
	b.mu.Lock("DeleteRule")
	defer b.mu.Unlock()

	if _, ok := b.rules[id]; !ok {
		return ErrNotFound
	}

	delete(b.rules, id)

	return nil
}

// ListRules returns summaries of all Rules.
func (b *InMemoryBackend) ListRules() []RuleSummary {
	b.mu.RLock("ListRules")
	defer b.mu.RUnlock()

	result := make([]RuleSummary, 0, len(b.rules))
	for _, r := range b.rules {
		result = append(result, RuleSummary{RuleId: r.RuleId, Name: r.Name})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].RuleId < result[j].RuleId })

	return result
}

// CreateIPSet creates a new IPSet.
func (b *InMemoryBackend) CreateIPSet(name, _ string, tags map[string]string) (*IPSet, error) {
	b.mu.Lock("CreateIPSet")
	defer b.mu.Unlock()

	id := uuid.New().String()
	ipSet := &IPSet{
		IPSetId:          id,
		Name:             name,
		IPSetDescriptors: []IPSetDescriptor{},
	}
	b.ipSets[id] = ipSet

	if len(tags) > 0 {
		b.tags[b.ipSetARN(id)] = maps.Clone(tags)
	}

	return ipSet, nil
}

// GetIPSet retrieves an IPSet by ID.
func (b *InMemoryBackend) GetIPSet(id string) (*IPSet, error) {
	b.mu.RLock("GetIPSet")
	defer b.mu.RUnlock()

	ipSet, ok := b.ipSets[id]
	if !ok {
		return nil, ErrNotFound
	}

	return ipSet, nil
}

// UpdateIPSet updates an IPSet's descriptors.
func (b *InMemoryBackend) UpdateIPSet(id, _ string, updates []IPSetUpdate) error {
	b.mu.Lock("UpdateIPSet")
	defer b.mu.Unlock()

	ipSet, ok := b.ipSets[id]
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
func (b *InMemoryBackend) DeleteIPSet(id, _ string) error {
	b.mu.Lock("DeleteIPSet")
	defer b.mu.Unlock()

	if _, ok := b.ipSets[id]; !ok {
		return ErrNotFound
	}

	delete(b.ipSets, id)

	return nil
}

// ListIPSets returns summaries of all IPSets.
func (b *InMemoryBackend) ListIPSets() []IPSetSummary {
	b.mu.RLock("ListIPSets")
	defer b.mu.RUnlock()

	result := make([]IPSetSummary, 0, len(b.ipSets))
	for _, s := range b.ipSets {
		result = append(result, IPSetSummary{IPSetId: s.IPSetId, Name: s.Name})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].IPSetId < result[j].IPSetId })

	return result
}

// CreateByteMatchSet creates a new ByteMatchSet.
func (b *InMemoryBackend) CreateByteMatchSet(name, _ string) (*ByteMatchSet, error) {
	b.mu.Lock("CreateByteMatchSet")
	defer b.mu.Unlock()

	id := uuid.New().String()
	bms := &ByteMatchSet{
		ByteMatchSetId:  id,
		Name:            name,
		ByteMatchTuples: []ByteMatchTuple{},
	}
	b.byteMatchSets[id] = bms

	return bms, nil
}

// GetByteMatchSet retrieves a ByteMatchSet by ID.
func (b *InMemoryBackend) GetByteMatchSet(id string) (*ByteMatchSet, error) {
	b.mu.RLock("GetByteMatchSet")
	defer b.mu.RUnlock()

	bms, ok := b.byteMatchSets[id]
	if !ok {
		return nil, ErrNotFound
	}

	return bms, nil
}

// UpdateByteMatchSet updates a ByteMatchSet's tuples.
func (b *InMemoryBackend) UpdateByteMatchSet(id, _ string, updates []ByteMatchSetUpdate) error {
	b.mu.Lock("UpdateByteMatchSet")
	defer b.mu.Unlock()

	bms, ok := b.byteMatchSets[id]
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
func (b *InMemoryBackend) DeleteByteMatchSet(id, _ string) error {
	b.mu.Lock("DeleteByteMatchSet")
	defer b.mu.Unlock()

	if _, ok := b.byteMatchSets[id]; !ok {
		return ErrNotFound
	}

	delete(b.byteMatchSets, id)

	return nil
}

// ListByteMatchSets returns summaries of all ByteMatchSets.
func (b *InMemoryBackend) ListByteMatchSets() []ByteMatchSetSummary {
	b.mu.RLock("ListByteMatchSets")
	defer b.mu.RUnlock()

	result := make([]ByteMatchSetSummary, 0, len(b.byteMatchSets))
	for _, s := range b.byteMatchSets {
		result = append(result, ByteMatchSetSummary{ByteMatchSetId: s.ByteMatchSetId, Name: s.Name})
	}

	sort.Slice(
		result,
		func(i, j int) bool { return result[i].ByteMatchSetId < result[j].ByteMatchSetId },
	)

	return result
}

// CreateSizeConstraintSet creates a new SizeConstraintSet.
func (b *InMemoryBackend) CreateSizeConstraintSet(name, _ string) (*SizeConstraintSet, error) {
	b.mu.Lock("CreateSizeConstraintSet")
	defer b.mu.Unlock()

	id := uuid.New().String()
	scs := &SizeConstraintSet{
		SizeConstraintSetId: id,
		Name:                name,
		SizeConstraints:     []SizeConstraint{},
	}
	b.sizeConstraintSets[id] = scs

	return scs, nil
}

// GetSizeConstraintSet retrieves a SizeConstraintSet by ID.
func (b *InMemoryBackend) GetSizeConstraintSet(id string) (*SizeConstraintSet, error) {
	b.mu.RLock("GetSizeConstraintSet")
	defer b.mu.RUnlock()

	scs, ok := b.sizeConstraintSets[id]
	if !ok {
		return nil, ErrNotFound
	}

	return scs, nil
}

// UpdateSizeConstraintSet updates a SizeConstraintSet's constraints.
func (b *InMemoryBackend) UpdateSizeConstraintSet(
	id, _ string,
	updates []SizeConstraintSetUpdate,
) error {
	b.mu.Lock("UpdateSizeConstraintSet")
	defer b.mu.Unlock()

	scs, ok := b.sizeConstraintSets[id]
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
func (b *InMemoryBackend) DeleteSizeConstraintSet(id, _ string) error {
	b.mu.Lock("DeleteSizeConstraintSet")
	defer b.mu.Unlock()

	if _, ok := b.sizeConstraintSets[id]; !ok {
		return ErrNotFound
	}

	delete(b.sizeConstraintSets, id)

	return nil
}

// ListSizeConstraintSets returns summaries of all SizeConstraintSets.
func (b *InMemoryBackend) ListSizeConstraintSets() []SizeConstraintSetSummary {
	b.mu.RLock("ListSizeConstraintSets")
	defer b.mu.RUnlock()

	result := make([]SizeConstraintSetSummary, 0, len(b.sizeConstraintSets))
	for _, s := range b.sizeConstraintSets {
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
	name, _ string,
) (*SqlInjectionMatchSet, error) {
	b.mu.Lock("CreateSqlInjectionMatchSet")
	defer b.mu.Unlock()

	id := uuid.New().String()
	sims := &SqlInjectionMatchSet{
		SqlInjectionMatchSetId:  id,
		Name:                    name,
		SqlInjectionMatchTuples: []SqlInjectionMatchTuple{},
	}
	b.sqlInjectionMatchSets[id] = sims

	return sims, nil
}

// GetSqlInjectionMatchSet retrieves a SqlInjectionMatchSet by ID.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) GetSqlInjectionMatchSet(id string) (*SqlInjectionMatchSet, error) {
	b.mu.RLock("GetSqlInjectionMatchSet")
	defer b.mu.RUnlock()

	sims, ok := b.sqlInjectionMatchSets[id]
	if !ok {
		return nil, ErrNotFound
	}

	return sims, nil
}

// UpdateSqlInjectionMatchSet updates a SqlInjectionMatchSet's tuples.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) UpdateSqlInjectionMatchSet(
	id, _ string,
	updates []SqlInjectionMatchSetUpdate,
) error {
	b.mu.Lock("UpdateSqlInjectionMatchSet")
	defer b.mu.Unlock()

	sims, ok := b.sqlInjectionMatchSets[id]
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
func (b *InMemoryBackend) DeleteSqlInjectionMatchSet(id, _ string) error {
	b.mu.Lock("DeleteSqlInjectionMatchSet")
	defer b.mu.Unlock()

	if _, ok := b.sqlInjectionMatchSets[id]; !ok {
		return ErrNotFound
	}

	delete(b.sqlInjectionMatchSets, id)

	return nil
}

// ListSqlInjectionMatchSets returns summaries of all SqlInjectionMatchSets.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) ListSqlInjectionMatchSets() []SqlInjectionMatchSetSummary {
	b.mu.RLock("ListSqlInjectionMatchSets")
	defer b.mu.RUnlock()

	result := make([]SqlInjectionMatchSetSummary, 0, len(b.sqlInjectionMatchSets))
	for _, s := range b.sqlInjectionMatchSets {
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
func (b *InMemoryBackend) CreateXssMatchSet(name, _ string) (*XssMatchSet, error) {
	b.mu.Lock("CreateXssMatchSet")
	defer b.mu.Unlock()

	id := uuid.New().String()
	xms := &XssMatchSet{
		XssMatchSetId:  id,
		Name:           name,
		XssMatchTuples: []XssMatchTuple{},
	}
	b.xssMatchSets[id] = xms

	return xms, nil
}

// GetXssMatchSet retrieves an XssMatchSet by ID.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) GetXssMatchSet(id string) (*XssMatchSet, error) {
	b.mu.RLock("GetXssMatchSet")
	defer b.mu.RUnlock()

	xms, ok := b.xssMatchSets[id]
	if !ok {
		return nil, ErrNotFound
	}

	return xms, nil
}

// UpdateXssMatchSet updates an XssMatchSet's tuples.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) UpdateXssMatchSet(id, _ string, updates []XssMatchSetUpdate) error {
	b.mu.Lock("UpdateXssMatchSet")
	defer b.mu.Unlock()

	xms, ok := b.xssMatchSets[id]
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
func (b *InMemoryBackend) DeleteXssMatchSet(id, _ string) error {
	b.mu.Lock("DeleteXssMatchSet")
	defer b.mu.Unlock()

	if _, ok := b.xssMatchSets[id]; !ok {
		return ErrNotFound
	}

	delete(b.xssMatchSets, id)

	return nil
}

// ListXssMatchSets returns summaries of all XssMatchSets.
//
//nolint:revive,staticcheck // AWS SDK naming
func (b *InMemoryBackend) ListXssMatchSets() []XssMatchSetSummary {
	b.mu.RLock("ListXssMatchSets")
	defer b.mu.RUnlock()

	result := make([]XssMatchSetSummary, 0, len(b.xssMatchSets))
	for _, s := range b.xssMatchSets {
		result = append(result, XssMatchSetSummary{XssMatchSetId: s.XssMatchSetId, Name: s.Name})
	}

	sort.Slice(
		result,
		func(i, j int) bool { return result[i].XssMatchSetId < result[j].XssMatchSetId },
	)

	return result
}

// CreateGeoMatchSet creates a new GeoMatchSet.
func (b *InMemoryBackend) CreateGeoMatchSet(name, _ string) (*GeoMatchSet, error) {
	b.mu.Lock("CreateGeoMatchSet")
	defer b.mu.Unlock()

	id := uuid.New().String()
	gms := &GeoMatchSet{
		GeoMatchSetId:       id,
		Name:                name,
		GeoMatchConstraints: []GeoMatchConstraint{},
	}
	b.geoMatchSets[id] = gms

	return gms, nil
}

// GetGeoMatchSet retrieves a GeoMatchSet by ID.
func (b *InMemoryBackend) GetGeoMatchSet(id string) (*GeoMatchSet, error) {
	b.mu.RLock("GetGeoMatchSet")
	defer b.mu.RUnlock()

	gms, ok := b.geoMatchSets[id]
	if !ok {
		return nil, ErrNotFound
	}

	return gms, nil
}

// UpdateGeoMatchSet updates a GeoMatchSet's constraints.
func (b *InMemoryBackend) UpdateGeoMatchSet(id, _ string, updates []GeoMatchSetUpdate) error {
	b.mu.Lock("UpdateGeoMatchSet")
	defer b.mu.Unlock()

	gms, ok := b.geoMatchSets[id]
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
func (b *InMemoryBackend) DeleteGeoMatchSet(id, _ string) error {
	b.mu.Lock("DeleteGeoMatchSet")
	defer b.mu.Unlock()

	if _, ok := b.geoMatchSets[id]; !ok {
		return ErrNotFound
	}

	delete(b.geoMatchSets, id)

	return nil
}

// ListGeoMatchSets returns summaries of all GeoMatchSets.
func (b *InMemoryBackend) ListGeoMatchSets() []GeoMatchSetSummary {
	b.mu.RLock("ListGeoMatchSets")
	defer b.mu.RUnlock()

	result := make([]GeoMatchSetSummary, 0, len(b.geoMatchSets))
	for _, s := range b.geoMatchSets {
		result = append(result, GeoMatchSetSummary{GeoMatchSetId: s.GeoMatchSetId, Name: s.Name})
	}

	sort.Slice(
		result,
		func(i, j int) bool { return result[i].GeoMatchSetId < result[j].GeoMatchSetId },
	)

	return result
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

	b.changeTokens = make(map[string]string)
	b.webACLs = make(map[string]*WebACL)
	b.rules = make(map[string]*Rule)
	b.ipSets = make(map[string]*IPSet)
	b.byteMatchSets = make(map[string]*ByteMatchSet)
	b.sizeConstraintSets = make(map[string]*SizeConstraintSet)
	b.sqlInjectionMatchSets = make(map[string]*SqlInjectionMatchSet)
	b.xssMatchSets = make(map[string]*XssMatchSet)
	b.geoMatchSets = make(map[string]*GeoMatchSet)
	b.tags = make(map[string]map[string]string)
}

type backendSnapshot struct {
	ChangeTokens       map[string]string             `json:"changeTokens"`
	WebACLs            map[string]*WebACL            `json:"webACLs"`
	Rules              map[string]*Rule              `json:"rules"`
	IPSets             map[string]*IPSet             `json:"ipSets"`
	ByteMatchSets      map[string]*ByteMatchSet      `json:"byteMatchSets"`
	SizeConstraintSets map[string]*SizeConstraintSet `json:"sizeConstraintSets"`
	//nolint:revive,staticcheck // AWS SDK naming
	SqlInjectionMatchSets map[string]*SqlInjectionMatchSet `json:"sqlInjectionMatchSets"`
	//nolint:revive,staticcheck // AWS SDK naming
	XssMatchSets map[string]*XssMatchSet      `json:"xssMatchSets"`
	GeoMatchSets map[string]*GeoMatchSet      `json:"geoMatchSets"`
	Tags         map[string]map[string]string `json:"tags"`
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	data, _ := json.Marshal(backendSnapshot{
		ChangeTokens:          b.changeTokens,
		WebACLs:               b.webACLs,
		Rules:                 b.rules,
		IPSets:                b.ipSets,
		ByteMatchSets:         b.byteMatchSets,
		SizeConstraintSets:    b.sizeConstraintSets,
		SqlInjectionMatchSets: b.sqlInjectionMatchSets,
		XssMatchSets:          b.xssMatchSets,
		GeoMatchSets:          b.geoMatchSets,
		Tags:                  b.tags,
	})

	return data
}

// Restore deserializes backend state from JSON.
func (b *InMemoryBackend) Restore(data []byte) error {
	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	var s backendSnapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	b.changeTokens = s.ChangeTokens
	b.webACLs = s.WebACLs
	b.rules = s.Rules
	b.ipSets = s.IPSets
	b.byteMatchSets = s.ByteMatchSets
	b.sizeConstraintSets = s.SizeConstraintSets
	b.sqlInjectionMatchSets = s.SqlInjectionMatchSets
	b.xssMatchSets = s.XssMatchSets
	b.geoMatchSets = s.GeoMatchSets
	b.tags = s.Tags

	return nil
}
