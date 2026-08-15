package sts

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// Trust-policy actions recognised during AssumeRole* evaluation.
const (
	actionAssumeRole           = "sts:AssumeRole"
	actionAssumeRoleWithSAML   = "sts:AssumeRoleWithSAML"
	actionAssumeRoleWithWebID  = "sts:AssumeRoleWithWebIdentity"
	trustPolicyEffectAllow     = "Allow"
	trustPolicyEffectDeny      = "Deny"
	stsAssumedRoleResourcePart = "assumed-role/"
	iamRoleResourcePart        = "role/"
	arnSegmentAccountIndex     = 4
	arnSegmentResourceIndex    = 5
	arnSegmentMinCount         = 6

	// arnServiceIAM and arnServiceSTS are the service segments of IAM/STS ARNs.
	arnServiceIAM = "iam"
	arnServiceSTS = "sts"

	// condKeyExternalID, condKeyPrincipalArn, and condKeyMFAPresent are the
	// trust-policy condition keys the emulator models (compared case-insensitively).
	condKeyExternalID   = "sts:externalid"
	condKeyPrincipalArn = "aws:principalarn"
	condKeyMFAPresent   = "aws:multifactorauthpresent"

	// Normalized (lowercased, IfExists-stripped, see normalizeConditionOp) forms
	// of the AWS condition operators this evaluator models beyond the String
	// family. ArnEquals and ArnLike are documented by AWS as behaving
	// identically (both wildcard-capable), so both map to the same case.
	condOperatorBool         = "bool"
	condOperatorNull         = "null"
	condOperatorArnEquals    = "arnequals"
	condOperatorArnLike      = "arnlike"
	condOperatorArnNotEquals = "arnnotequals"
	condOperatorArnNotLike   = "arnnotlike"
)

// trustEval carries the caller context evaluated against a role trust policy.
// Either callerArn (an IAM/STS principal ARN) or federatedArn (a SAML provider
// ARN or OIDC issuer) identifies the caller; when both are empty the evaluation
// is skipped (permissive mock behaviour) because there is no principal to match.
type trustEval struct {
	conditionCtx map[string]string
	action       string
	callerArn    string
	federatedArn string
	externalID   string
}

// hasPrincipal reports whether the evaluation carries a caller principal.
func (e trustEval) hasPrincipal() bool {
	return e.callerArn != "" || e.federatedArn != ""
}

// principalLabel returns a human-readable identifier for error messages.
func (e trustEval) principalLabel() string {
	if e.callerArn != "" {
		return e.callerArn
	}

	if e.federatedArn != "" {
		return e.federatedArn
	}

	return "the caller"
}

// conditionValue resolves the request-context value for a condition key that
// the emulator models. The second result reports whether the key is modelled;
// unmodelled keys are treated as satisfied so unfamiliar conditions never cause
// a spurious denial.
func (e trustEval) conditionValue(key string) (string, bool) {
	switch strings.ToLower(key) {
	case condKeyExternalID:
		return e.externalID, true
	case condKeyPrincipalArn:
		return e.callerArn, true
	}

	if v, ok := e.conditionCtx[strings.ToLower(key)]; ok {
		return v, true
	}

	return "", false
}

// trustStatementDoc is a single parsed trust-policy statement.
type trustStatementDoc struct {
	Condition map[string]map[string]json.RawMessage `json:"Condition"`
	Effect    string                                `json:"Effect"`
	Principal json.RawMessage                       `json:"Principal"`
	Action    json.RawMessage                       `json:"Action"`
}

// principalSet is the parsed Principal element of a statement.
type principalSet struct {
	aws       []string
	federated []string
	service   []string
	wildcard  bool
}

// trustPolicy is used to parse the trust policy JSON for ExternalId extraction.
type trustPolicy struct {
	Statement []trustStatement `json:"Statement"`
}

// trustStatement is a single statement in a trust policy.
type trustStatement struct {
	Condition map[string]map[string]json.RawMessage `json:"Condition"`
}

// evaluateAssumeRoleTrust evaluates a role's trust policy against the caller
// context and returns ErrAccessDenied when the caller is not permitted to
// perform the requested action. It is deliberately permissive (returns nil)
// when there is nothing to evaluate: no caller principal, an empty or malformed
// policy, or a policy with no statements. This mirrors the emulator's existing
// behaviour of only enforcing constraints that can be positively determined.
func evaluateAssumeRoleTrust(policyJSON string, ev trustEval) error {
	if !ev.hasPrincipal() {
		return nil
	}

	stmts, ok := parseTrustStatements(policyJSON)
	if !ok || len(stmts) == 0 {
		return nil
	}

	allowed := false

	for i := range stmts {
		st := stmts[i]
		if !actionMatches(st.Action, ev.action) {
			continue
		}

		if !principalMatches(st.Principal, ev) {
			continue
		}

		if !conditionsSatisfied(st.Condition, ev) {
			continue
		}

		if strings.EqualFold(st.Effect, trustPolicyEffectDeny) {
			return fmt.Errorf(
				"%w: trust policy explicitly denies %s for %s",
				ErrAccessDenied, ev.action, ev.principalLabel(),
			)
		}

		if strings.EqualFold(st.Effect, trustPolicyEffectAllow) {
			allowed = true
		}
	}

	if !allowed {
		return fmt.Errorf(
			"%w: %s is not authorized to perform %s on the role's trust policy",
			ErrAccessDenied, ev.principalLabel(), ev.action,
		)
	}

	return nil
}

// parseTrustStatements parses a trust-policy JSON document into its statements.
// The Statement field may be a single object or an array; both are handled.
// The second result is false when the document cannot be parsed at all.
func parseTrustStatements(policyJSON string) ([]trustStatementDoc, bool) {
	if strings.TrimSpace(policyJSON) == "" {
		return nil, false
	}

	var doc struct {
		Statement json.RawMessage `json:"Statement"`
	}

	if err := json.Unmarshal([]byte(policyJSON), &doc); err != nil {
		return nil, false
	}

	raw := trimLeadingSpace(doc.Statement)
	if len(raw) == 0 {
		return nil, true
	}

	if raw[0] == '[' {
		var arr []trustStatementDoc
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, false
		}

		return arr, true
	}

	var single trustStatementDoc
	if err := json.Unmarshal(raw, &single); err != nil {
		return nil, false
	}

	return []trustStatementDoc{single}, true
}

// trimLeadingSpace drops leading ASCII whitespace from a raw JSON message so the
// first significant byte can be inspected without importing bytes helpers.
func trimLeadingSpace(raw json.RawMessage) json.RawMessage {
	i := 0
	for i < len(raw) {
		switch raw[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return raw[i:]
		}
	}

	return raw[i:]
}

// actionMatches reports whether the requested action is granted by the
// statement Action element (a string or array; wildcards supported).
func actionMatches(raw json.RawMessage, requested string) bool {
	if len(raw) == 0 {
		// A statement without an explicit Action is treated as matching so that
		// condition-only test policies (which omit Action) keep working.
		return true
	}

	for _, a := range extractStringValues(raw) {
		if a == "*" || strings.EqualFold(a, requested) {
			return true
		}

		if strings.Contains(a, "*") &&
			wildcardMatch(strings.ToLower(a), strings.ToLower(requested)) {
			return true
		}
	}

	return false
}

// principalMatches reports whether the caller satisfies the statement Principal.
func principalMatches(raw json.RawMessage, ev trustEval) bool {
	if len(raw) == 0 {
		// No Principal specified. Real trust policies always name a principal,
		// but ExternalId-only test fixtures omit it; treat as matching so those
		// remain permitted while conditions still gate the grant.
		return true
	}

	p := parsePrincipal(raw)
	if p.wildcard {
		return true
	}

	for _, awsP := range p.aws {
		if awsPrincipalMatches(awsP, ev.callerArn) {
			return true
		}
	}

	for _, fed := range p.federated {
		if federatedPrincipalMatches(fed, ev.federatedArn) {
			return true
		}
	}

	return false
}

// parsePrincipal parses the Principal element, which may be the string "*" or an
// object keyed by AWS / Federated / Service / CanonicalUser.
func parsePrincipal(raw json.RawMessage) principalSet {
	var out principalSet

	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		if asString == "*" {
			out.wildcard = true
		} else {
			out.aws = []string{asString}
		}

		return out
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return out
	}

	for key, val := range obj {
		values := extractStringValues(val)

		switch strings.ToLower(key) {
		case "aws":
			out.aws = append(out.aws, values...)
		case "federated":
			out.federated = append(out.federated, values...)
		case "service":
			out.service = append(out.service, values...)
		}
	}

	return out
}

// awsPrincipalMatches reports whether an AWS-principal entry from a trust policy
// matches the caller ARN. Supported forms: "*", an exact ARN, a bare 12-digit
// account ID, an account-root ARN, or a role ARN (matched against an
// assumed-role caller ARN derived from that role).
func awsPrincipalMatches(principal, callerArn string) bool {
	if principal == "*" {
		return true
	}

	if callerArn == "" {
		return false
	}

	if strings.EqualFold(principal, callerArn) {
		return true
	}

	callerAccount := arnAccount(callerArn)

	if accountIDRe.MatchString(principal) {
		return principal == callerAccount
	}

	if acct, ok := rootArnAccount(principal); ok {
		return acct == callerAccount
	}

	if roleName, acct, ok := parseRoleArnParts(principal); ok {
		return assumedRoleArnMatches(callerArn, acct, roleName)
	}

	return false
}

// federatedPrincipalMatches reports whether a Federated-principal entry matches
// the caller's federated identifier (SAML provider ARN or OIDC issuer).
func federatedPrincipalMatches(principal, federatedArn string) bool {
	if principal == "*" {
		return true
	}

	if federatedArn == "" {
		return false
	}

	if strings.EqualFold(principal, federatedArn) {
		return true
	}

	// OIDC provider ARNs embed the issuer host after "oidc-provider/". Compare
	// that host against the issuer (with or without an https:// scheme).
	issuerHost := strings.TrimPrefix(strings.TrimPrefix(federatedArn, "https://"), "http://")

	if _, provHost, found := strings.Cut(principal, "oidc-provider/"); found {
		return strings.EqualFold(provHost, issuerHost)
	}

	return strings.EqualFold(principal, issuerHost)
}

// arnAccount returns the account-ID segment of an ARN, or "" when absent.
func arnAccount(arnStr string) string {
	parts := strings.Split(arnStr, ":")
	if len(parts) > arnSegmentAccountIndex {
		return parts[arnSegmentAccountIndex]
	}

	return ""
}

// rootArnAccount reports whether principal is an account-root ARN
// (arn:...:iam::ACCOUNT:root) and returns the account ID when so.
func rootArnAccount(principal string) (string, bool) {
	parts := strings.Split(principal, ":")
	if len(parts) < arnSegmentMinCount {
		return "", false
	}

	if parts[2] == arnServiceIAM && parts[arnSegmentResourceIndex] == "root" {
		return parts[arnSegmentAccountIndex], true
	}

	return "", false
}

// parseRoleArnParts reports whether principal is an IAM role ARN
// (arn:...:iam::ACCOUNT:role/[path/]NAME) and returns its role name and account.
func parseRoleArnParts(principal string) (string, string, bool) {
	parts := strings.SplitN(principal, ":", arnSegmentMinCount)
	if len(parts) < arnSegmentMinCount {
		return "", "", false
	}

	if parts[2] != arnServiceIAM ||
		!strings.HasPrefix(parts[arnSegmentResourceIndex], iamRoleResourcePart) {
		return "", "", false
	}

	return roleNameFromResource(parts[arnSegmentResourceIndex]), parts[arnSegmentAccountIndex], true
}

// assumedRoleArnMatches reports whether callerArn is an STS assumed-role ARN
// (arn:...:sts::ACCOUNT:assumed-role/ROLE/SESSION) derived from the given role.
func assumedRoleArnMatches(callerArn, account, roleName string) bool {
	parts := strings.SplitN(callerArn, ":", arnSegmentMinCount)
	if len(parts) < arnSegmentMinCount {
		return false
	}

	if parts[2] != arnServiceSTS || parts[arnSegmentAccountIndex] != account {
		return false
	}

	resource := strings.TrimPrefix(parts[arnSegmentResourceIndex], stsAssumedRoleResourcePart)
	roleSeg, _, _ := strings.Cut(resource, "/")

	return strings.EqualFold(roleSeg, roleName)
}

// conditionsSatisfied reports whether every condition in a statement holds for
// the caller context. Unmodelled keys and operators are treated as satisfied.
func conditionsSatisfied(cond map[string]map[string]json.RawMessage, ev trustEval) bool {
	for op, kv := range cond {
		for key, raw := range kv {
			if !conditionOperatorHolds(op, key, raw, ev) {
				return false
			}
		}
	}

	return true
}

// conditionOperatorHolds evaluates a single condition operator/key/value tuple.
//
// Fail-open by design (see services/sts/PARITY.md's trust-policy-evaluation
// family entry for the full rationale): an operator this switch does not
// model, or a key conditionValue does not resolve, is treated as satisfied
// rather than denied, so a trust policy carrying a condition gopherstack
// cannot evaluate does not silently break existing callers. Both fallback
// paths log loudly at WARN so the gap is discoverable at runtime instead of
// silent -- that is the fix for gopherstack-yg95, not a change of default.
func conditionOperatorHolds(op, key string, raw json.RawMessage, ev trustEval) bool {
	normOp := normalizeConditionOp(op)

	actual, known := ev.conditionValue(key)

	// Null tests key presence, not value, and AWS documents it as the one
	// operator that does not support an IfExists suffix (presence-testing
	// already is its job). It must run before the generic !known fallback
	// below -- otherwise a Null:"false" (key must be present) condition would
	// always be satisfied by that same fallback, defeating the operator.
	if normOp == condOperatorNull {
		return nullConditionHolds(raw, known)
	}

	if !known {
		warnUnmodeledCondition(key, op, "condition key not modeled by this emulator")

		return true
	}

	want := extractStringValues(raw)

	switch normOp {
	case "stringequals":
		return anyEquals(want, actual, false)
	case "stringequalsignorecase":
		return anyEquals(want, actual, true)
	case "stringnotequals":
		return !anyEquals(want, actual, false)
	case "stringlike":
		return anyWildcard(want, actual)
	case "stringnotlike":
		return !anyWildcard(want, actual)
	case condOperatorBool:
		return anyEquals(want, actual, true)
	case condOperatorArnEquals, condOperatorArnLike:
		// AWS documents ArnEquals/ArnLike as behaving identically, both
		// wildcard-capable. Real AWS matches each of the six colon-delimited
		// ARN segments separately and disallows wildcards spanning segments;
		// this emulator uses the same general-purpose glob matcher as
		// StringLike instead of segment-aware matching, a deliberate
		// simplification since trust-policy ARN conditions in practice
		// wildcard within a single segment (e.g. role/prod-*).
		return anyWildcard(want, actual)
	case condOperatorArnNotEquals, condOperatorArnNotLike:
		return !anyWildcard(want, actual)
	default:
		// Numeric*/Date*/IpAddress/NotIpAddress/BinaryEquals are not modeled:
		// this evaluator has no numeric, timestamp, source-IP, or binary
		// request-context value to compare against for any condition key it
		// carries (structural, not deferred -- see PARITY.md).
		warnUnmodeledCondition(key, op, "condition operator not modeled by this emulator")

		return true
	}
}

// nullConditionHolds evaluates AWS's Null condition operator: "true" requires
// the condition key be absent from the request context, "false" requires it
// be present. Unlike every other operator, Null tests key existence, not the
// key's value, so it is driven by conditionValue's known result rather than a
// string/bool comparison against actual.
func nullConditionHolds(raw json.RawMessage, known bool) bool {
	for _, v := range extractStringValues(raw) {
		wantAbsent, err := strconv.ParseBool(v)
		if err == nil && wantAbsent == !known {
			return true
		}
	}

	return false
}

// warnUnmodeledCondition logs, at WARN, a trust-policy condition this
// evaluator could not enforce and therefore permitted by default. There is no
// request-scoped context available this deep in trust-policy evaluation (see
// PARITY.md), matching the context.Background() logging pattern already used
// elsewhere in the repo for logging outside a request's call chain (e.g.
// services/autoscaling/elbv2_targets.go).
func warnUnmodeledCondition(key, op, reason string) {
	logger.Load(context.Background()).Warn(
		"sts: trust-policy condition not enforced, permitting by default",
		"key", key, "operator", op, "reason", reason,
	)
}

// normalizeConditionOp lowercases a condition operator and strips the AWS
// "...IfExists" suffix, which does not change our matching semantics.
func normalizeConditionOp(op string) string {
	return strings.TrimSuffix(strings.ToLower(op), "ifexists")
}

// anyEquals reports whether actual equals any candidate (optionally case-insensitively).
func anyEquals(candidates []string, actual string, fold bool) bool {
	for _, c := range candidates {
		if fold {
			if strings.EqualFold(c, actual) {
				return true
			}

			continue
		}

		if c == actual {
			return true
		}
	}

	return false
}

// anyWildcard reports whether actual matches any candidate glob pattern.
func anyWildcard(patterns []string, actual string) bool {
	for _, p := range patterns {
		if wildcardMatch(p, actual) {
			return true
		}
	}

	return false
}

// wildcardMatch reports whether s matches the glob pattern, where '*' matches
// any run of characters and '?' matches exactly one character.
func wildcardMatch(pattern, s string) bool {
	// Iterative two-pointer glob matcher with backtracking on '*'.
	var (
		px, sx         int
		starIdx        = -1
		starMatchIndex int
	)

	for sx < len(s) {
		switch {
		case px < len(pattern) && (pattern[px] == s[sx] || pattern[px] == '?'):
			px++
			sx++
		case px < len(pattern) && pattern[px] == '*':
			starIdx = px
			starMatchIndex = sx
			px++
		case starIdx != -1:
			px = starIdx + 1
			starMatchIndex++
			sx = starMatchIndex
		default:
			return false
		}
	}

	for px < len(pattern) && pattern[px] == '*' {
		px++
	}

	return px == len(pattern)
}

// validateExternalID parses a trust policy JSON document and validates that the
// provided externalID satisfies any sts:ExternalId conditions found therein.
// Trust policy statements use OR semantics: if any statement with an ExternalId
// condition matches, access is granted. Only if all statements with ExternalId
// conditions fail is ErrAccessDenied returned.
// If the trust policy requires an ExternalId but none (or the wrong value) is
// supplied, ErrAccessDenied is returned.
func validateExternalID(trustPolicyJSON, externalID string) error {
	if trustPolicyJSON == "" {
		return nil
	}

	var tp trustPolicy

	// Unmarshal errors indicate a malformed policy document. A malformed trust
	// policy leaves tp with a zero value (empty Statements), so no ExternalId
	// condition will be found and the call proceeds without validation — the
	// permissive behaviour is intentional for a mock implementation.
	_ = json.Unmarshal([]byte(trustPolicyJSON), &tp)

	var hasExternalIDCondition bool

	for _, stmt := range tp.Statement {
		required := requiredExternalIDs(stmt.Condition)
		if len(required) == 0 {
			continue
		}

		hasExternalIDCondition = true

		if slices.Contains(required, externalID) {
			return nil
		}
	}

	if hasExternalIDCondition {
		return fmt.Errorf(
			"%w: ExternalId does not match the trust policy condition",
			ErrAccessDenied,
		)
	}

	return nil
}

// requiredExternalIDs extracts all sts:ExternalId values from a trust-statement Condition map.
// Returns nil when no ExternalId condition is present.
func requiredExternalIDs(condition map[string]map[string]json.RawMessage) []string {
	for condOp, condMap := range condition {
		if !strings.EqualFold(condOp, "StringEquals") && !strings.EqualFold(condOp, "StringLike") {
			continue
		}

		for condKey, rawVal := range condMap {
			if strings.EqualFold(condKey, "sts:ExternalId") {
				return extractStringValues(rawVal)
			}
		}
	}

	return nil
}

// validateMFACondition parses a trust policy JSON document and validates that
// mfaPresent (true when the caller supplied a well-formed SerialNumber+TokenCode
// pair) satisfies any aws:MultiFactorAuthPresent Bool condition found therein.
// Mirrors validateExternalID's OR semantics: if any statement's condition is
// satisfied, access is granted; only when every statement carrying the
// condition fails is ErrAccessDenied returned. This is the principal-independent
// half of MFA enforcement — checkAssumeRoleTrust additionally threads the same
// condition through the general Principal-aware evaluator via conditionCtx so
// statements combining Principal and MFA in one clause are evaluated jointly.
func validateMFACondition(trustPolicyJSON string, mfaPresent bool) error {
	if trustPolicyJSON == "" {
		return nil
	}

	var tp trustPolicy

	// Unmarshal errors leave tp with a zero value (empty Statements): a malformed
	// trust policy is treated permissively, same as validateExternalID.
	_ = json.Unmarshal([]byte(trustPolicyJSON), &tp)

	var hasMFACondition bool

	for _, stmt := range tp.Statement {
		required, ok := requiredMFAPresent(stmt.Condition)
		if !ok {
			continue
		}

		hasMFACondition = true

		if required == mfaPresent {
			return nil
		}
	}

	if hasMFACondition {
		return fmt.Errorf(
			"%w: the role's trust policy requires MFA authentication",
			ErrAccessDenied,
		)
	}

	return nil
}

// requiredMFAPresent extracts the required aws:MultiFactorAuthPresent Bool value
// from a trust-statement Condition map. The second result reports whether the
// condition is present at all.
func requiredMFAPresent(condition map[string]map[string]json.RawMessage) (bool, bool) {
	for condOp, condMap := range condition {
		if normalizeConditionOp(condOp) != condOperatorBool {
			continue
		}

		for condKey, rawVal := range condMap {
			if !strings.EqualFold(condKey, "aws:MultiFactorAuthPresent") {
				continue
			}

			for _, v := range extractStringValues(rawVal) {
				if b, err := strconv.ParseBool(v); err == nil {
					return b, true
				}
			}
		}
	}

	return false, false
}

// extractStringValues unmarshals a JSON RawMessage that may be either a string
// or an array of strings and returns the values as a Go string slice.
func extractStringValues(raw json.RawMessage) []string {
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		return []string{single}
	}

	var many []string
	if err := json.Unmarshal(raw, &many); err == nil {
		return many
	}

	return nil
}
