package glacier

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Vault Lock policy evaluation semantics below are transcribed from AWS's
// documentation, not the SDK: like a CloudFormation stack policy, a Glacier
// vault lock policy body is an opaque string with no wire type in
// aws-sdk-go-v2, so there is no types/types.go line to cite. Sources:
// https://docs.aws.amazon.com/amazonglacier/latest/dev/vault-lock.html
// https://docs.aws.amazon.com/amazonglacier/latest/dev/vault-lock-policy.html
// https://docs.aws.amazon.com/amazonglacier/latest/dev/glacier-api-permissions-ref.html
//
// Implemented: Effect=Deny statements matching Action (glacier:<Verb>, "*"
// wildcards) and Resource (the vault ARN, "*" wildcards per the permissions
// reference's vaults/example* / vaults/* patterns), consulted only from
// DeleteArchive and DeleteVault -- the two operations "deletion protection"
// is about. AWS enforces the policy from the moment InitiateVaultLock puts
// the lock InProgress, not only once Locked: vault-lock.html documents the
// InProgress window as letting you "test your Vault Lock policy before
// locking it down", i.e. requests are evaluated against it during the test
// window too.
//
// The canonical Vault Lock use case (vault-lock-policy.html Example 1: "Deny
// Deletion Permissions for Archives Less Than 365 Days Old") conditions the
// Deny on the Glacier-specific key glacier:ArchiveAgeInDays, evaluated with
// the standard IAM numeric operators. That condition is implemented since
// Archive.CreationDate is already tracked and the age is computable; the
// permissions reference confirms ArchiveAgeInDays is the only condition key
// documented for DeleteArchive besides ResourceTag.
//
// NOT implemented, disclosed rather than approximated:
//   - Effect=Allow is parsed but never grants anything. There is no
//     identity-based/IAM baseline in this emulator for a resource policy to
//     combine with (the KMS-grant precedent: "no IAM layer, stored for wire
//     parity only"), so an Allow statement cannot correctly change
//     behaviour here without fabricating CloudFormation-style
//     default-deny-once-a-policy-exists semantics that AWS does not
//     document for Glacier. Only the well-documented Deny half is
//     implemented.
//   - Principal is required by policy syntax but is not evaluated: this
//     emulator has no per-request caller identity (tracked separately,
//     gopherstack-cu4g), and every AWS-documented Vault Lock example writes
//     Principal "*" -- Vault Lock's whole point is "prevent anyone,
//     including the AWS account owner, from performing prohibited actions"
//     -- so ignoring Principal matches the documented common case rather
//     than under- or over-enforcing a rarer one.
//   - The ResourceTag condition key (used by Example 2's legal-hold
//     pattern) is not implemented: Glacier archives carry no tags in this
//     emulator (only vaults do), so there is no tag state to condition on.
//   - Only DeleteArchive and DeleteVault consult the policy. Other
//     Vault-Lock-governable actions (UploadArchive, InitiateJob, ...) are
//     out of scope for a deletion-protection pass; the policy is stored and
//     available to them but not enforced.
//   - Vault access policies (SetVaultAccessPolicy/GetVaultAccessPolicy) are
//     a separate, still-unenforced feature: they exist specifically to
//     grant cross-account/-principal access, which cannot be evaluated
//     correctly without caller identity (gopherstack-cu4g again). Left
//     untouched here.
type vaultLockPolicyDocument struct {
	Statement []vaultLockPolicyStatement `json:"Statement"`
}

type vaultLockPolicyStatement struct {
	Condition *vaultLockPolicyCondition `json:"Condition"`
	Effect    string                    `json:"Effect"`
	Action    glacierPolicyStringSet    `json:"Action"`
	Resource  glacierPolicyStringSet    `json:"Resource"`
}

// vaultLockPolicyCondition supports the standard IAM numeric operators
// against the glacier:ArchiveAgeInDays condition key -- see the package doc
// above for why this is the only condition key implemented.
type vaultLockPolicyCondition struct {
	NumericLessThanEquals    map[string]string `json:"NumericLessThanEquals"`
	NumericLessThan          map[string]string `json:"NumericLessThan"`
	NumericGreaterThanEquals map[string]string `json:"NumericGreaterThanEquals"`
	NumericGreaterThan       map[string]string `json:"NumericGreaterThan"`
	NumericEquals            map[string]string `json:"NumericEquals"`
}

// glacierPolicyStringSet unmarshals a JSON value that is either a single
// string or an array of strings, matching how policy documents write
// Action/Resource values (e.g. "glacier:*" vs ["glacier:DeleteArchive",
// "glacier:DeleteVault"]).
type glacierPolicyStringSet []string

func (s *glacierPolicyStringSet) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = glacierPolicyStringSet{single}

		return nil
	}

	var multi []string
	if err := json.Unmarshal(data, &multi); err != nil {
		return err
	}

	*s = multi

	return nil
}

const vaultLockConditionArchiveAge = "glacier:ArchiveAgeInDays"

const (
	glacierActionDeleteArchive = "glacier:DeleteArchive"
	glacierActionDeleteVault   = "glacier:DeleteVault"
)

// parseVaultLockPolicyDocument parses a vault lock policy body. Returns an
// error for malformed JSON so a garbage policy is rejected at
// InitiateVaultLock time rather than silently never enforcing anything at
// DeleteArchive/DeleteVault time.
func parseVaultLockPolicyDocument(policy string) (*vaultLockPolicyDocument, error) {
	var doc vaultLockPolicyDocument
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		return nil, fmt.Errorf("malformed vault lock policy: %w", err)
	}

	return &doc, nil
}

// evaluateVaultLockPolicy reports whether action against vaultArn is denied
// by policy. archiveAgeDays is the age in days of the archive the action
// targets, or -1 when the action has no archive in play (DeleteVault).
func evaluateVaultLockPolicy(policy, vaultArn, action string, archiveAgeDays int) (bool, error) {
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

		if !matchesAnyGlacierPolicy(stmt.Action, action) {
			continue
		}

		if !matchesAnyGlacierPolicy(stmt.Resource, vaultArn) {
			continue
		}

		if stmt.Condition != nil && !stmt.Condition.matches(archiveAgeDays) {
			continue
		}

		return true, nil
	}

	return false, nil
}

// matches reports whether the condition is satisfied. A Condition block with
// no recognized operator/key never matches -- an unrecognized condition
// fails toward "the statement behaves as if it weren't there" rather than
// fabricating a match that could wrongly block a permitted action.
func (c *vaultLockPolicyCondition) matches(archiveAgeDays int) bool {
	checks := []struct {
		vals map[string]string
		cmp  func(have, want int) bool
	}{
		{c.NumericLessThanEquals, func(have, want int) bool { return have <= want }},
		{c.NumericLessThan, func(have, want int) bool { return have < want }},
		{c.NumericGreaterThanEquals, func(have, want int) bool { return have >= want }},
		{c.NumericGreaterThan, func(have, want int) bool { return have > want }},
		{c.NumericEquals, func(have, want int) bool { return have == want }},
	}

	matched := false

	for _, chk := range checks {
		want, ok := chk.vals[vaultLockConditionArchiveAge]
		if !ok {
			continue
		}

		matched = true

		if archiveAgeDays < 0 {
			return false
		}

		n, err := strconv.Atoi(want)
		if err != nil || !chk.cmp(archiveAgeDays, n) {
			return false
		}
	}

	return matched
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
