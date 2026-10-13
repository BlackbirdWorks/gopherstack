package glacier

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Vault Lock and vault access policy evaluation is transcribed from AWS's
// documentation, not the SDK (policy bodies are opaque strings with no wire
// type): https://docs.aws.amazon.com/amazonglacier/latest/dev/vault-lock-policy.html
// and https://docs.aws.amazon.com/amazonglacier/latest/dev/glacier-api-permissions-ref.html.
//
// Only Effect=Deny is evaluated: there is no IAM baseline for an Allow to combine
// with. Principal is matched against the caller ARN when the request carries one
// (named principals never match an anonymous caller). Condition supports the
// numeric operators on glacier:ArchiveAgeInDays and the string operators on
// glacier:ResourceTag/<key> (the vault's tags); any other operator or key makes
// the statement not match rather than guess.
type vaultLockPolicyDocument struct {
	Statement []vaultLockPolicyStatement `json:"Statement"`
}

type vaultLockPolicyStatement struct {
	Condition map[string]map[string]glacierPolicyStringSet `json:"Condition"`
	Principal json.RawMessage                              `json:"Principal"`
	Effect    string                                       `json:"Effect"`
	Action    glacierPolicyStringSet                       `json:"Action"`
	Resource  glacierPolicyStringSet                       `json:"Resource"`
}

// glacierPolicyStringSet unmarshals a JSON value that is a string, number, bool or
// an array of them, the shapes policy documents use for Action/Resource/Condition values.
type glacierPolicyStringSet []string

func (s *glacierPolicyStringSet) UnmarshalJSON(data []byte) error {
	var multi []json.RawMessage
	if err := json.Unmarshal(data, &multi); err != nil {
		multi = []json.RawMessage{data}
	}

	out := make(glacierPolicyStringSet, 0, len(multi))

	for _, raw := range multi {
		var str string
		if err := json.Unmarshal(raw, &str); err == nil {
			out = append(out, str)

			continue
		}

		var num json.Number
		if err := json.Unmarshal(raw, &num); err != nil {
			return fmt.Errorf("unsupported policy value %s: %w", raw, err)
		}

		out = append(out, num.String())
	}

	*s = out

	return nil
}

const (
	vaultLockConditionArchiveAge = "glacier:ArchiveAgeInDays"
	vaultLockConditionTagPrefix  = "glacier:ResourceTag/"
)

const (
	glacierActionDeleteArchive = "glacier:DeleteArchive"
	glacierActionDeleteVault   = "glacier:DeleteVault"
)

// parseVaultLockPolicyDocument parses a vault lock policy body. Returns an
// error for malformed JSON so a garbage policy is rejected at
// InitiateVaultLock time rather than silently never enforcing anything.
func parseVaultLockPolicyDocument(policy string) (*vaultLockPolicyDocument, error) {
	var doc vaultLockPolicyDocument
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		return nil, fmt.Errorf("malformed vault lock policy: %w", err)
	}

	return &doc, nil
}

// policyRequest describes the request a policy is evaluated against.
type policyRequest struct {
	tags           map[string]string
	vaultArn       string
	action         string
	caller         string
	archiveAgeDays int
	// checkCaller makes named Principals match against caller; when false only
	// wildcard principals apply (the caller is not known).
	checkCaller bool
}

// evaluateVaultPolicyDeny reports whether a Deny statement in policy applies to req.
// archiveAgeDays is -1 when no archive is in play.
func evaluateVaultPolicyDeny(policy string, req *policyRequest) (bool, error) {
	if policy == "" {
		return false, nil
	}

	doc, err := parseVaultLockPolicyDocument(policy)
	if err != nil {
		return false, err
	}

	for _, stmt := range doc.Statement {
		if stmt.Effect != "Deny" {
			continue
		}

		if !matchesAnyGlacierPolicy(stmt.Action, req.action) ||
			!matchesAnyGlacierPolicy(stmt.Resource, req.vaultArn) {
			continue
		}

		if !principalMatches(stmt.Principal, req) {
			continue
		}

		if stmt.Condition != nil && !conditionMatches(stmt.Condition, req) {
			continue
		}

		return true, nil
	}

	return false, nil
}

// principalMatches reports whether a statement's Principal covers the caller. A
// missing Principal is treated as the wildcard.
func principalMatches(raw json.RawMessage, req *policyRequest) bool {
	if len(raw) == 0 {
		return true
	}

	var set glacierPolicyStringSet

	var single string
	if json.Unmarshal(raw, &single) == nil {
		set = glacierPolicyStringSet{single}
	} else {
		var obj map[string]glacierPolicyStringSet
		if json.Unmarshal(raw, &obj) != nil {
			return false
		}

		set = obj["AWS"]
	}

	for _, p := range set {
		if p == "*" {
			return true
		}

		if req.checkCaller && req.caller != "" && callerIsPrincipal(req.caller, p) {
			return true
		}
	}

	return false
}

// callerIsPrincipal matches a caller ARN against an exact ARN, a bare account ID,
// or the account root ARN.
func callerIsPrincipal(caller, principal string) bool {
	if caller == principal {
		return true
	}

	fields := strings.Split(caller, ":")

	const accountField = 4
	if len(fields) <= accountField {
		return false
	}

	account := fields[accountField]

	return principal == account || principal == "arn:aws:iam::"+account+":root"
}

// conditionMatches requires every operator/key pair to hold; a condition with no
// recognised checks never matches.
func conditionMatches(cond map[string]map[string]glacierPolicyStringSet, req *policyRequest) bool {
	checked := false

	for op, keys := range cond {
		for key, wants := range keys {
			ok, known := conditionHolds(op, key, wants, req)
			if !known || !ok {
				return false
			}

			checked = true
		}
	}

	return checked
}

func conditionHolds(op, key string, wants glacierPolicyStringSet, req *policyRequest) (bool, bool) {
	switch {
	case key == vaultLockConditionArchiveAge:
		return numericConditionHolds(op, wants, req.archiveAgeDays)
	case strings.HasPrefix(key, vaultLockConditionTagPrefix):
		have, present := req.tags[strings.TrimPrefix(key, vaultLockConditionTagPrefix)]

		return stringConditionHolds(op, wants, have, present)
	}

	return false, false
}

func numericConditionHolds(op string, wants glacierPolicyStringSet, have int) (bool, bool) {
	cmp, ok := map[string]func(have, want int) bool{
		"NumericLessThanEquals":    func(have, want int) bool { return have <= want },
		"NumericLessThan":          func(have, want int) bool { return have < want },
		"NumericGreaterThanEquals": func(have, want int) bool { return have >= want },
		"NumericGreaterThan":       func(have, want int) bool { return have > want },
		"NumericEquals":            func(have, want int) bool { return have == want },
		"NumericNotEquals":         func(have, want int) bool { return have != want },
	}[op]
	if !ok {
		return false, false
	}

	if have < 0 {
		return false, true
	}

	negated := op == "NumericNotEquals"
	result := negated

	for _, w := range wants {
		n, err := strconv.Atoi(w)
		if err != nil {
			return false, true
		}

		if negated {
			result = result && cmp(have, n)
		} else {
			result = result || cmp(have, n)
		}
	}

	return result, true
}

func stringConditionHolds(op string, wants glacierPolicyStringSet, have string, present bool) (bool, bool) {
	var match func(want, have string) bool

	negated := false

	switch op {
	case "StringEquals":
		match = func(want, have string) bool { return want == have }
	case "StringNotEquals":
		match, negated = func(want, have string) bool { return want == have }, true
	case "StringLike":
		match = glacierPolicyWildcardMatch
	case "StringNotLike":
		match, negated = glacierPolicyWildcardMatch, true
	default:
		return false, false
	}

	if !present {
		return negated, true
	}

	anyMatch := false

	for _, w := range wants {
		if match(w, have) {
			anyMatch = true

			break
		}
	}

	return anyMatch != negated, true
}

func matchesAnyGlacierPolicy(patterns glacierPolicyStringSet, target string) bool {
	for _, p := range patterns {
		if glacierPolicyWildcardMatch(p, target) {
			return true
		}
	}

	return false
}

// glacierPolicyWildcardMatch reports whether s matches pattern, where "*"
// matches any run of characters -- the wildcard form documented for Glacier
// resource ARNs (vaults/example*, vaults/*) and IAM actions alike.
func glacierPolicyWildcardMatch(pattern, s string) bool {
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
