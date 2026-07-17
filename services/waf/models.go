package waf

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
