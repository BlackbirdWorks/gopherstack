package accessanalyzer

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
)

const (
	effectAllow = "Allow"
	effectDeny  = "Deny"

	checkResultPass = "PASS"
	checkResultFail = "FAIL"

	findingTypeError           = "ERROR"
	findingTypeSecurityWarning = "SECURITY_WARNING"
	findingTypeSuggestion      = "SUGGESTION"

	keyDescription    = "description"
	keyStatementIndex = "statementIndex"
	keyStatementID    = "statementId"
	keyPath           = "path"
	keyValue          = "value"
	keyIndex          = "index"

	learnMoreBase = "https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html"
)

// iamPolicy is a parsed IAM policy document.
type iamPolicy struct {
	Version   string         `json:"Version"`
	Statement []iamStatement `json:"Statement"`
}

// iamStatement is one entry in the Statement array.
type iamStatement struct { //nolint:govet // field order chosen for JSON tag clarity
	Sid         string           `json:"Sid"`
	Effect      string           `json:"Effect"`
	Action      iamStringOrSlice `json:"Action"`
	NotAction   iamStringOrSlice `json:"NotAction"`
	Resource    iamStringOrSlice `json:"Resource"`
	NotResource iamStringOrSlice `json:"NotResource"`
	Principal   iamPrincipal     `json:"Principal"`
	Condition   map[string]any   `json:"Condition"`
}

// iamStringOrSlice deserializes either a JSON string or array of strings.
type iamStringOrSlice []string

func (s *iamStringOrSlice) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		*s = []string{str}

		return nil
	}

	var slice []string
	if err := json.Unmarshal(data, &slice); err != nil {
		return err
	}

	*s = slice

	return nil
}

// iamPrincipal is either the wildcard "*" or a map of principal types.
type iamPrincipal struct { //nolint:govet // field order chosen for readability
	IsWildcard bool
	AWS        iamStringOrSlice
	Service    iamStringOrSlice
	Federated  iamStringOrSlice
}

func (p *iamPrincipal) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		p.IsWildcard = str == "*"

		return nil
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	if v, ok := m["AWS"]; ok {
		_ = json.Unmarshal(v, &p.AWS)
	}

	if v, ok := m["Service"]; ok {
		_ = json.Unmarshal(v, &p.Service)
	}

	if v, ok := m["Federated"]; ok {
		_ = json.Unmarshal(v, &p.Federated)
	}

	return nil
}

// parsePolicy unmarshals a policy document JSON string. Returns empty policy on error.
func parsePolicy(doc string) iamPolicy {
	var p iamPolicy
	_ = json.Unmarshal([]byte(doc), &p)

	return p
}

// iamGlob reports whether value matches pattern (case-insensitive, supports * and ?).
func iamGlob(pattern, value string) bool {
	ok, _ := filepath.Match(strings.ToLower(pattern), strings.ToLower(value))

	return ok
}

// actionAllowed returns true if the statement's Action set covers the given action.
func actionAllowed(stmt iamStatement, action string) bool {
	for _, a := range stmt.Action {
		if iamGlob(a, action) {
			return true
		}
	}

	return false
}

// resourceAllowed returns true if the statement's Resource set covers the given resource.
func resourceAllowed(stmt iamStatement, resource string) bool {
	if len(stmt.Resource) == 0 {
		return false
	}

	for _, r := range stmt.Resource {
		if iamGlob(r, resource) {
			return true
		}
	}

	return false
}

// stmtGrants returns true if the Allow statement grants the action on the resource.
func stmtGrants(stmt iamStatement, action, resource string) bool {
	return stmt.Effect == effectAllow &&
		actionAllowed(stmt, action) &&
		resourceAllowed(stmt, resource)
}

// policyGrants returns true if the policy grants the action on the resource.
func policyGrants(p iamPolicy, action, resource string) bool {
	for _, stmt := range p.Statement {
		if stmtGrants(stmt, action, resource) {
			return true
		}
	}

	return false
}

// isPublicPrincipal returns true if the principal allows public (unauthenticated) access.
func isPublicPrincipal(pr iamPrincipal) bool {
	return pr.IsWildcard || slices.Contains([]string(pr.AWS), "*")
}

// makeReason builds a reason map for check results.
func makeReason(i int, sid, desc string) map[string]any {
	r := map[string]any{
		keyDescription:    desc,
		keyStatementIndex: i,
	}

	if sid != "" {
		r[keyStatementID] = sid
	}

	return r
}

// AccessSpec is the access argument shape for CheckAccessNotGranted.
type AccessSpec struct {
	Actions   []string `json:"actions"`
	Resources []string `json:"resources"`
}

// PolicyCheckResult holds a structured check result returned to callers.
type PolicyCheckResult struct {
	Result  string           `json:"result"`
	Message string           `json:"message"`
	Reasons []map[string]any `json:"reasons,omitempty"`
}

// stmtGrantedReasons returns reasons for all (action, resource) pairs in accesses
// that are granted by the given statement.
func stmtGrantedReasons(stmt iamStatement, i int, accesses []AccessSpec) []map[string]any {
	var reasons []map[string]any

	for _, access := range accesses {
		for _, action := range access.Actions {
			for _, resource := range access.Resources {
				if stmtGrants(stmt, action, resource) {
					desc := fmt.Sprintf("Statement %d grants %s on %s", i, action, resource)
					reasons = append(reasons, makeReason(i, stmt.Sid, desc))
				}
			}
		}
	}

	return reasons
}

// CheckAccessNotGranted returns PASS if the policy does NOT grant any of the specified accesses.
func CheckAccessNotGranted(policyDoc string, accesses []AccessSpec) PolicyCheckResult {
	p := parsePolicy(policyDoc)

	var reasons []map[string]any

	for i, stmt := range p.Statement {
		if stmt.Effect != effectAllow {
			continue
		}

		reasons = append(reasons, stmtGrantedReasons(stmt, i, accesses)...)
	}

	if len(reasons) > 0 {
		return PolicyCheckResult{
			Result:  checkResultFail,
			Message: "The specified policy grants the specified access.",
			Reasons: reasons,
		}
	}

	return PolicyCheckResult{
		Result:  checkResultPass,
		Message: "The specified policy does not grant the specified access.",
	}
}

// CheckNoNewAccess returns PASS if newPolicyDoc does not grant access beyond existingPolicyDoc.
func CheckNoNewAccess(existingDoc, newDoc string) PolicyCheckResult {
	existing := parsePolicy(existingDoc)
	newPol := parsePolicy(newDoc)

	var reasons []map[string]any

	for i, stmt := range newPol.Statement {
		if stmt.Effect != effectAllow {
			continue
		}

		reasons = append(reasons, newAccessReasons(existing, stmt, i)...)
	}

	if len(reasons) > 0 {
		return PolicyCheckResult{
			Result:  checkResultFail,
			Message: "The updated policy grants new access compared to the existing policy.",
			Reasons: reasons,
		}
	}

	return PolicyCheckResult{
		Result:  checkResultPass,
		Message: "The updated policy does not grant new access compared to the existing policy.",
	}
}

// newAccessReasons returns reasons for (action, resource) pairs granted by stmt
// that are not covered by the existing policy.
func newAccessReasons(existing iamPolicy, stmt iamStatement, i int) []map[string]any {
	var reasons []map[string]any

	for _, action := range stmt.Action {
		for _, resource := range stmt.Resource {
			if !policyGrants(existing, action, resource) {
				desc := fmt.Sprintf(
					"New statement %d grants %s on %s not in existing policy",
					i, action, resource,
				)
				reasons = append(reasons, makeReason(i, stmt.Sid, desc))
			}
		}
	}

	return reasons
}

// CheckNoPublicAccess returns PASS if the policy has no public-access Allow statements.
func CheckNoPublicAccess(policyDoc string) PolicyCheckResult {
	p := parsePolicy(policyDoc)

	var reasons []map[string]any

	for i, stmt := range p.Statement {
		if stmt.Effect != effectAllow {
			continue
		}

		if isPublicPrincipal(stmt.Principal) {
			desc := fmt.Sprintf("Statement %d grants public access via wildcard principal", i)
			reasons = append(reasons, makeReason(i, stmt.Sid, desc))
		}
	}

	if len(reasons) > 0 {
		return PolicyCheckResult{
			Result:  checkResultFail,
			Message: "The policy grants public access.",
			Reasons: reasons,
		}
	}

	return PolicyCheckResult{
		Result:  checkResultPass,
		Message: "The policy does not grant public access.",
	}
}

// ValidatePolicyFinding is a single finding from policy validation.
// FindingDetails is a required member of the real types.ValidatePolicyFinding
// ("a localized message that explains the finding and provides guidance on
// how to address it"); it is populated from findingDetailMessages, keyed by
// IssueCode, rather than left empty.
type ValidatePolicyFinding struct {
	FindingType    string           `json:"findingType"`
	IssueCode      string           `json:"issueCode"`
	FindingDetails string           `json:"findingDetails"`
	LearnMoreLink  string           `json:"learnMoreLink"`
	Locations      []map[string]any `json:"locations"`
}

// findingDetailMessages maps every IssueCode this package's validators can
// produce to the human-readable message types.ValidatePolicyFinding.FindingDetails
// requires. Every errFinding/warnFinding call site's code MUST have an entry
// here (see the exhaustive test in handler_policy_validation_test.go).
//
//nolint:gochecknoglobals // static lookup table, same pattern as errCodeLookup elsewhere
var findingDetailMessages = map[string]string{
	"INVALID_POLICY_SYNTAX": "The policy document is not valid JSON.",
	"MISSING_VERSION": "Add a Version element to the policy. Without one, the policy defaults to " +
		"the oldest, most limited version.",
	"INVALID_VERSION":                  "The Version element must be either 2012-10-17 or 2008-10-17.",
	"INVALID_EFFECT":                   "The Effect element of a statement must be either Allow or Deny.",
	"MISSING_ACTION_OR_NOT_ACTION":     "A statement must contain either an Action or a NotAction element.",
	"BOTH_ACTION_AND_NOT_ACTION":       "A statement cannot contain both an Action and a NotAction element.",
	"MISSING_RESOURCE_OR_NOT_RESOURCE": "A statement must contain either a Resource or a NotResource element.",
	"BOTH_RESOURCE_AND_NOT_RESOURCE":   "A statement cannot contain both a Resource and a NotResource element.",
	"PASS_ROLE_WITH_STAR": "Using a wildcard action with a wildcard resource in an Allow statement " +
		"is overly permissive.",
}

func findingDetailsFor(code string) string {
	if msg, ok := findingDetailMessages[code]; ok {
		return msg
	}

	return ""
}

func errFinding(code string, locs []map[string]any) ValidatePolicyFinding {
	return ValidatePolicyFinding{
		FindingType:    findingTypeError,
		IssueCode:      code,
		FindingDetails: findingDetailsFor(code),
		LearnMoreLink:  learnMoreBase,
		Locations:      locs,
	}
}

func warnFinding(findingType, code string, locs []map[string]any) ValidatePolicyFinding {
	return ValidatePolicyFinding{
		FindingType:    findingType,
		IssueCode:      code,
		FindingDetails: findingDetailsFor(code),
		LearnMoreLink:  learnMoreBase,
		Locations:      locs,
	}
}

func rootLoc() []map[string]any {
	return []map[string]any{{keyPath: []any{}}}
}

func fieldLoc(field string) []map[string]any {
	return []map[string]any{{keyPath: []any{map[string]any{keyValue: field}}}}
}

func stmtLoc(stmtPath []any) []map[string]any {
	return []map[string]any{{keyPath: stmtPath}}
}

func stmtFieldLoc(stmtPath []any, field string) []map[string]any {
	return []map[string]any{{keyPath: append(stmtPath, map[string]any{keyValue: field})}}
}

// validateVersion checks the Version field of a policy document.
func validateVersion(raw map[string]json.RawMessage, p iamPolicy) []ValidatePolicyFinding {
	var findings []ValidatePolicyFinding

	if _, ok := raw["Version"]; !ok {
		findings = append(findings, warnFinding(findingTypeSuggestion, "MISSING_VERSION", rootLoc()))
	}

	validVersions := []string{"2012-10-17", "2008-10-17"}

	if p.Version != "" && !slices.Contains(validVersions, p.Version) {
		findings = append(findings, errFinding("INVALID_VERSION", fieldLoc("Version")))
	}

	return findings
}

// validateStatementEffect validates the Effect field of a statement.
func validateStatementEffect(stmtPath []any, stmt iamStatement) *ValidatePolicyFinding {
	if stmt.Effect != effectAllow && stmt.Effect != effectDeny {
		f := errFinding("INVALID_EFFECT", stmtFieldLoc(stmtPath, "Effect"))

		return &f
	}

	return nil
}

// validateStatementActions validates Action/NotAction fields of a statement.
func validateStatementActions(stmtPath []any, stmt iamStatement) []ValidatePolicyFinding {
	var findings []ValidatePolicyFinding

	hasAction := len(stmt.Action) > 0
	hasNotAction := len(stmt.NotAction) > 0

	if !hasAction && !hasNotAction {
		findings = append(findings, errFinding("MISSING_ACTION_OR_NOT_ACTION", stmtLoc(stmtPath)))
	}

	if hasAction && hasNotAction {
		findings = append(findings, errFinding("BOTH_ACTION_AND_NOT_ACTION", stmtLoc(stmtPath)))
	}

	return findings
}

// validateStatementResources validates Resource/NotResource for identity policies.
func validateStatementResources(stmtPath []any, stmt iamStatement, policyType string) []ValidatePolicyFinding {
	if policyType != "IDENTITY_POLICY" {
		return nil
	}

	var findings []ValidatePolicyFinding

	hasResource := len(stmt.Resource) > 0
	hasNotResource := len(stmt.NotResource) > 0

	if !hasResource && !hasNotResource {
		findings = append(findings, errFinding("MISSING_RESOURCE_OR_NOT_RESOURCE", stmtLoc(stmtPath)))
	}

	if hasResource && hasNotResource {
		findings = append(findings, errFinding("BOTH_RESOURCE_AND_NOT_RESOURCE", stmtLoc(stmtPath)))
	}

	return findings
}

// validateStatementPermissiveness warns about overly permissive Allow statements.
func validateStatementPermissiveness(stmtPath []any, stmt iamStatement) *ValidatePolicyFinding {
	if stmt.Effect != effectAllow {
		return nil
	}

	if slices.Contains([]string(stmt.Action), "*") && slices.Contains([]string(stmt.Resource), "*") {
		f := warnFinding(findingTypeSecurityWarning, "PASS_ROLE_WITH_STAR", stmtLoc(stmtPath))

		return &f
	}

	return nil
}

// validateStatement returns all findings for a single policy statement.
func validateStatement(i int, stmt iamStatement, policyType string) []ValidatePolicyFinding {
	stmtPath := []any{
		map[string]any{keyValue: "Statement"},
		map[string]any{keyIndex: i},
	}

	var findings []ValidatePolicyFinding

	if f := validateStatementEffect(stmtPath, stmt); f != nil {
		findings = append(findings, *f)
	}

	findings = append(findings, validateStatementActions(stmtPath, stmt)...)
	findings = append(findings, validateStatementResources(stmtPath, stmt, policyType)...)

	if f := validateStatementPermissiveness(stmtPath, stmt); f != nil {
		findings = append(findings, *f)
	}

	return findings
}

// ValidatePolicy checks a policy document for structural and semantic errors.
func ValidatePolicy(policyDoc, policyType string) []ValidatePolicyFinding {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(policyDoc), &raw); err != nil {
		return []ValidatePolicyFinding{errFinding("INVALID_POLICY_SYNTAX", rootLoc())}
	}

	p := parsePolicy(policyDoc)

	findings := validateVersion(raw, p)

	for i, stmt := range p.Statement {
		findings = append(findings, validateStatement(i, stmt, policyType)...)
	}

	return findings
}
