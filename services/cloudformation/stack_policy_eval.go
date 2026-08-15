package cloudformation

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// Stack policy evaluation semantics below are transcribed from AWS's
// documentation, not the SDK: the policy body is an opaque string with no
// wire type in aws-sdk-go-v2, so there is no types/types.go line to cite.
// Source: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/protect-stack-resources.html
//
// Implemented: Effect (Allow/Deny, Deny always overrides Allow), Action
// (Update:Modify / Update:Replace / Update:Delete / Update:*, with "*"
// wildcards), Resource (LogicalResourceId/<id>, with "*" wildcards), and
// Condition (StringEquals/StringLike on ResourceType). Once a policy is set,
// an update action on a resource is denied unless some statement explicitly
// allows it and no statement denies it -- quoting the doc: "After you set a
// stack policy, all of the resources in the stack are protected by default"
// and "When you set a stack policy on your stack, any update not explicitly
// allowed is denied by default." No policy set at all means no restriction
// ("When you create a stack, all update actions are allowed on all
// resources").
//
// NOT implemented, disclosed rather than approximated: NotAction and
// NotResource. AWS's own docs warn these don't reliably protect resources:
// "AWS CloudFormation evaluates stack policies against both the logical
// resource ID and the resource type independently. A default denial blocks
// an update only when both evaluations result in a denied status" -- a
// two-axis evaluation model distinct from ordinary statement matching, which
// AWS itself advises against relying on ("Always use an explicit Deny
// statement to protect resources"). Statements using NotAction/NotResource
// are parsed but never match here, so they contribute neither Allow nor
// Deny -- fail toward "policy behaves as if that statement weren't there"
// rather than fabricating the two-axis semantics.
//
// Principal is required by AWS's syntax but only the wildcard "*" is a valid
// value, so it carries no information for a single-account emulator; it is
// accepted (parsed, ignored) but not evaluated.
type stackPolicyDocument struct {
	Statement []stackPolicyStatement `json:"Statement"`
}

type stackPolicyStatement struct {
	Condition *stackPolicyCondition `json:"Condition"`
	Effect    string                `json:"Effect"`
	Action    stackPolicyStringSet  `json:"Action"`
	Resource  stackPolicyStringSet  `json:"Resource"`
}

type stackPolicyCondition struct {
	StringEquals map[string]stackPolicyStringSet `json:"StringEquals"`
	StringLike   map[string]stackPolicyStringSet `json:"StringLike"`
}

// stackPolicyStringSet unmarshals a JSON value that is either a single
// string or an array of strings, matching how stack policies write
// Action/Resource/Condition values (e.g. "Update:*" vs ["Update:Replace",
// "Update:Delete"]).
type stackPolicyStringSet []string

func (s *stackPolicyStringSet) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = stackPolicyStringSet{single}

		return nil
	}

	var multi []string
	if err := json.Unmarshal(data, &multi); err != nil {
		return err
	}

	*s = multi

	return nil
}

const (
	stackPolicyEffectAllow = "Allow"
	stackPolicyEffectDeny  = "Deny"

	updateActionModify  = "Update:Modify"
	updateActionReplace = "Update:Replace"
	updateActionDelete  = "Update:Delete"

	stackPolicyResourcePrefix = "LogicalResourceId/"
)

// parseStackPolicyDocument parses a stack policy body. Returns an error for
// malformed JSON so a garbage policy is rejected at SetStackPolicy time
// rather than silently never enforcing anything at UpdateStack time.
func parseStackPolicyDocument(policy string) (*stackPolicyDocument, error) {
	var doc stackPolicyDocument
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		return nil, fmt.Errorf("malformed stack policy: %w", err)
	}

	return &doc, nil
}

// evaluateStackPolicy reports whether action is permitted against the
// resource identified by logicalID/resourceType under policy. An empty
// policy (none set) allows everything.
func evaluateStackPolicy(policy, logicalID, resourceType, action string) (bool, error) {
	if policy == "" {
		return true, nil
	}

	doc, err := parseStackPolicyDocument(policy)
	if err != nil {
		return false, err
	}

	resourceTarget := stackPolicyResourcePrefix + logicalID
	allowed := false

	for _, stmt := range doc.Statement {
		if !stmt.appliesTo(resourceTarget, resourceType, action) {
			continue
		}

		switch stmt.Effect {
		case stackPolicyEffectDeny:
			return false, nil
		case stackPolicyEffectAllow:
			allowed = true
		}
	}

	return allowed, nil
}

func (s stackPolicyStatement) appliesTo(resourceTarget, resourceType, action string) bool {
	if !matchesAny(s.Action, action) {
		return false
	}
	if !matchesAny(s.Resource, resourceTarget) {
		return false
	}
	if s.Condition != nil && !s.Condition.matchesResourceType(resourceType) {
		return false
	}

	return true
}

func (c stackPolicyCondition) matchesResourceType(resourceType string) bool {
	if slices.Contains(c.StringEquals["ResourceType"], resourceType) {
		return true
	}
	for _, v := range c.StringLike["ResourceType"] {
		if wildcardMatch(v, resourceType) {
			return true
		}
	}

	return false
}

func matchesAny(patterns stackPolicyStringSet, target string) bool {
	for _, p := range patterns {
		if wildcardMatch(p, target) {
			return true
		}
	}

	return false
}

// wildcardMatch reports whether s matches pattern, where "*" matches any run
// of characters -- the only wildcard AWS documents for stack policy Action,
// Resource and Condition ResourceType values.
func wildcardMatch(pattern, s string) bool {
	if !strings.Contains(pattern, "*") {
		return pattern == s
	}

	parts := strings.Split(pattern, "*")
	if !strings.HasPrefix(s, parts[0]) {
		return false
	}
	s = s[len(parts[0]):]

	for _, part := range parts[1 : len(parts)-1] {
		idx := strings.Index(s, part)
		if idx < 0 {
			return false
		}
		s = s[idx+len(part):]
	}

	return strings.HasSuffix(s, parts[len(parts)-1])
}

// stackPolicyActionForChange maps a computed resource change (from
// diffTemplates/computeChanges) to the stack policy Action it must be
// checked against. Only existing resources being modified or removed are
// gated -- AWS's stack policy model protects existing resources during
// updates; a brand-new resource has no prior state to protect, so Add
// changes are never checked (ok is false).
//
// Replacement == "Conditionally" (see changeset_diff.go's requiresRecreation
// -- this backend can't always tell statically whether a property change
// will force replacement) is treated as Update:Replace here: since the
// change set says the resource MIGHT be replaced, checking the stricter
// action errs toward protecting a resource a Deny:Update:Replace statement
// was meant to guard, rather than silently letting a possible replacement
// through an Update:Modify-only check. This is a deliberate choice, not an
// AWS-documented rule.
func stackPolicyActionForChange(rc ResourceChange) (string, bool) {
	switch rc.Action {
	case "Remove":
		return updateActionDelete, true
	case "Modify":
		if rc.Replacement == replacementTrue || rc.Replacement == replacementConditionally {
			return updateActionReplace, true
		}

		return updateActionModify, true
	default:
		return "", false
	}
}
