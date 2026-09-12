package glue

import (
	"encoding/json"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// glueAccountIDLen is the number of digits in an AWS account ID.
const glueAccountIDLen = 12

// glueArnAccountIdx is the index of the account-ID segment in a colon-split ARN
// ("arn:partition:service:region:account-id:resource").
const glueArnAccountIdx = 4

// policyDoc is the minimal shape of an IAM-style resource policy document this backend
// needs to detect a cross-account grant: which principals are granted access, and which
// actions they're granted. Mirrors services/accessanalyzer/policy_analysis.go's
// iamPolicy/iamStatement/iamPrincipal parsing (not reused directly -- that package is
// unexported and scoped to its own analysis checks).
type policyDoc struct {
	Statement []policyStatement `json:"Statement"`
}

type policyStatement struct {
	Effect    string          `json:"Effect"`
	Action    stringOrSlice   `json:"Action"`
	Principal policyPrincipal `json:"Principal"`
}

// stringOrSlice deserializes either a JSON string or a JSON array of strings.
type stringOrSlice []string

func (s *stringOrSlice) UnmarshalJSON(data []byte) error {
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

// policyPrincipal is either the wildcard "*" or a map of principal types; only "AWS" is
// relevant here (RAM sharing is inherently an AWS-account concept).
type policyPrincipal struct {
	AWS        stringOrSlice
	IsWildcard bool
}

func (p *policyPrincipal) UnmarshalJSON(data []byte) error {
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

	return nil
}

// principalAccountID extracts the account-ID segment from an ARN principal (e.g.
// "arn:aws:iam::111122223333:role/Dev"), or returns principal unchanged when it is
// already a bare 12-digit account ID. Returns "" for anything else (malformed, a
// service principal, etc.) so callers can treat it as "not a real account".
func principalAccountID(principal string) string {
	if len(principal) == glueAccountIDLen && isAllDigits(principal) {
		return principal
	}

	parts := strings.Split(principal, ":")
	if len(parts) > glueArnAccountIdx && parts[0] == "arn" {
		return parts[glueArnAccountIdx]
	}

	return ""
}

func isAllDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}

	return true
}

// crossAccountGrant parses policyJSON and returns the deduplicated, sorted set of
// principals granted access by an Allow statement whose account differs from
// ownerAccountID, together with the union of actions those statements grant. A
// malformed policy or one with no cross-account grant returns (nil, nil) -- callers
// treat that as "no RAM share needed", matching AWS: RAM policy-based sharing requires
// specific principals, so a public ("*") or same-account-only grant creates no share.
func crossAccountGrant(policyJSON, ownerAccountID string) ([]string, []string) {
	var doc policyDoc
	if err := json.Unmarshal([]byte(policyJSON), &doc); err != nil {
		return nil, nil
	}

	principalSet := make(map[string]struct{})
	actionSet := make(map[string]struct{})

	for _, stmt := range doc.Statement {
		if !strings.EqualFold(stmt.Effect, "Allow") || stmt.Principal.IsWildcard {
			continue
		}

		for _, p := range stmt.Principal.AWS {
			acct := principalAccountID(p)
			if acct == "" || acct == ownerAccountID {
				continue
			}

			principalSet[p] = struct{}{}

			for _, a := range stmt.Action {
				actionSet[a] = struct{}{}
			}
		}
	}

	if len(principalSet) == 0 {
		return nil, nil
	}

	return collections.SortedKeys(principalSet), collections.SortedKeys(actionSet)
}

// syncRAMPolicyShare seams resourceARN's cross-account grant (if any) into a RAM
// CREATED_FROM_POLICY resource share via ramShareCreator. Called outside b.mu -- RAM's
// own lock must never nest inside glue's (services/lambda/lifecycle.go's
// capture/release/call/re-lock pattern) -- and only for a per-resource policy
// (resourceARN != ""); the account-level Data Catalog policy (resourceARN == "") names
// no single RAM-shareable resource. Only EnableHybrid="TRUE" ever creates or updates a
// share, per this backend's documented seam contract; any other value leaves an
// existing share alone.
func (b *InMemoryBackend) syncRAMPolicyShare(resourceARN, enableHybrid, policyJSON string) {
	creator, ownerAccountID := b.resourceShareCreatorLocked()
	if creator == nil || resourceARN == "" || enableHybrid != "TRUE" {
		return
	}

	principals, actions := crossAccountGrant(policyJSON, ownerAccountID)
	if len(principals) == 0 {
		_ = creator.DeletePolicyBasedShare(resourceARN)

		return
	}

	_ = creator.PutPolicyBasedShare(resourceARN, principals, actions)
}

// resourceShareCreatorLocked returns the currently-registered ResourceShareCreator (nil
// if none) and the backend's own account ID, taken under a brief read lock so callers
// never hold b.mu while invoking the seam.
func (b *InMemoryBackend) resourceShareCreatorLocked() (ResourceShareCreator, string) {
	b.mu.RLock("resourceShareCreatorLocked")
	defer b.mu.RUnlock()

	return b.ramShareCreator, b.accountID
}
