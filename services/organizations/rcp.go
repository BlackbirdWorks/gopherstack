package organizations

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

const (
	rcpFullAWSAccessPolicyID          = "p-RCPFullAWSAccess"
	rcpFullAWSAccessPolicyName        = "RCPFullAWSAccess"
	rcpFullAWSAccessPolicyDescription = "Allows access to every operation"
	rcpFullAWSAccessPolicyContent     = `{"Version":"2012-10-17","Statement":` +
		`[{"Effect":"Allow","Principal":"*","Action":"*","Resource":"*"}]}`
	policyVersion = "2012-10-17"
)

type rcpStatement struct {
	Effect       string          `json:"Effect"`
	Principal    json.RawMessage `json:"Principal"`
	NotPrincipal json.RawMessage `json:"NotPrincipal"`
	Action       json.RawMessage `json:"Action"`
	NotAction    json.RawMessage `json:"NotAction"`
	Resource     json.RawMessage `json:"Resource"`
	NotResource  json.RawMessage `json:"NotResource"`
}

type rcpDocument struct {
	Version   string          `json:"Version"`
	Statement json.RawMessage `json:"Statement"`
}

// validateRCPContent applies the RCP syntax rules (User Guide, "RCP syntax"): Effect
// must be Deny, Principal only "*", no NotPrincipal/NotAction, Action names a supported service.
func validateRCPContent(content string) error {
	var doc rcpDocument
	if err := json.Unmarshal([]byte(content), &doc); err != nil || doc.Version != policyVersion {
		return ErrMalformedPolicyDocument
	}

	stmts, ok := decodeRCPStatements(doc.Statement)
	if !ok || len(stmts) == 0 {
		return ErrMalformedPolicyDocument
	}

	for _, s := range stmts {
		if err := validateRCPStatement(s); err != nil {
			return err
		}
	}

	return nil
}

func decodeRCPStatements(raw json.RawMessage) ([]rcpStatement, bool) {
	var many []rcpStatement
	if err := json.Unmarshal(raw, &many); err == nil {
		return many, true
	}

	var one rcpStatement
	if err := json.Unmarshal(raw, &one); err != nil {
		return nil, false
	}

	return []rcpStatement{one}, true
}

func validateRCPStatement(s rcpStatement) error {
	if s.Effect != "Deny" {
		return ErrMalformedPolicyDocument
	}

	if len(s.NotPrincipal) > 0 || string(s.Principal) != `"*"` {
		return ErrInvalidPolicyPrincipal
	}

	if len(s.NotAction) > 0 || len(s.Action) == 0 {
		return ErrMalformedPolicyDocument
	}

	if (len(s.Resource) == 0) == (len(s.NotResource) == 0) {
		return ErrMalformedPolicyDocument
	}

	return validateRCPActions(s.Action)
}

func validateRCPActions(raw json.RawMessage) error {
	var actions []string
	if err := json.Unmarshal(raw, &actions); err != nil {
		var one string
		if err := json.Unmarshal(raw, &one); err != nil {
			return ErrMalformedPolicyDocument
		}

		actions = []string{one}
	}

	if len(actions) == 0 {
		return ErrMalformedPolicyDocument
	}

	for _, a := range actions {
		svc, _, found := strings.Cut(a, ":")
		if !found || !iam.RCPSupportsService(svc) {
			return ErrMalformedPolicyDocument
		}
	}

	return nil
}

// seedRCPFullAWSAccessLocked creates the AWS managed RCPFullAWSAccess policy.
func (b *InMemoryBackend) seedRCPFullAWSAccessLocked() {
	if b.policies.Has(rcpFullAWSAccessPolicyID) {
		return
	}

	b.policies.Put(&Policy{
		PolicySummary: PolicySummary{
			ID:          rcpFullAWSAccessPolicyID,
			ARN:         b.awsManagedPolicyARN(policyTypeRCP, rcpFullAWSAccessPolicyID),
			Name:        rcpFullAWSAccessPolicyName,
			Description: rcpFullAWSAccessPolicyDescription,
			Type:        policyTypeRCP,
			AwsManaged:  true,
		},
		Content: rcpFullAWSAccessPolicyContent,
	})

	if b.policyTargets[rcpFullAWSAccessPolicyID] == nil {
		b.policyTargets[rcpFullAWSAccessPolicyID] = []string{}
	}
}

func (b *InMemoryBackend) rcpEnabledLocked() bool {
	return b.root != nil && slices.ContainsFunc(b.root.PolicyTypes, func(pt PolicyTypeSummary) bool {
		return pt.Type == policyTypeRCP && pt.Status == policyStatusEnabled
	})
}

// attachDefaultRCPLocked attaches RCPFullAWSAccess to targetID while RCPs are enabled.
func (b *InMemoryBackend) attachDefaultRCPLocked(targetID string) {
	if !b.rcpEnabledLocked() {
		return
	}

	b.seedRCPFullAWSAccessLocked()

	if slices.Contains(b.targetPolicies[targetID], rcpFullAWSAccessPolicyID) {
		return
	}

	b.targetPolicies[targetID] = append(b.targetPolicies[targetID], rcpFullAWSAccessPolicyID)
	b.policyTargets[rcpFullAWSAccessPolicyID] = append(b.policyTargets[rcpFullAWSAccessPolicyID], targetID)
}

// detachAllRCPsLocked removes every RCP attachment, including RCPFullAWSAccess, on disable.
func (b *InMemoryBackend) detachAllRCPsLocked() {
	for _, p := range b.policies.All() {
		if p.PolicySummary.Type != policyTypeRCP {
			continue
		}

		for _, target := range b.policyTargets[p.PolicySummary.ID] {
			b.targetPolicies[target] = removeString(b.targetPolicies[target], p.PolicySummary.ID)
		}

		b.policyTargets[p.PolicySummary.ID] = []string{}
	}
}

// attachMissingDefaultRCPsLocked repairs restored state lacking RCPFullAWSAccess.
func (b *InMemoryBackend) attachMissingDefaultRCPsLocked() {
	if !b.rcpEnabledLocked() {
		return
	}

	b.forEachTargetLocked(b.attachDefaultRCPLocked)
}

// RCPsForAccount implements iam.RCPProvider: the RCP levels governing resources
// owned by accountID, root-first. ok is false for the management account,
// accounts outside the organization, or when RCPs are not enabled.
func (b *InMemoryBackend) RCPsForAccount(accountID string) ([]iam.SCPLevel, bool) {
	b.mu.RLock("RCPsForAccount")
	defer b.mu.RUnlock()

	if !b.rcpEnabledLocked() || b.org == nil || accountID == b.org.MasterAccountID {
		return nil, false
	}

	path, ok := b.policyPathLocked(accountID)
	if !ok {
		return nil, false
	}

	return b.levelsForPathLocked(path, policyTypeRCP), true
}

// OrgIDForAccount implements iam.RCPProvider: the organization ID of a member
// or management account, or "" outside any organization.
func (b *InMemoryBackend) OrgIDForAccount(accountID string) string {
	b.mu.RLock("OrgIDForAccount")
	defer b.mu.RUnlock()

	if b.org == nil {
		return ""
	}

	if accountID == b.org.MasterAccountID || b.accounts.Has(accountID) {
		return b.org.ID
	}

	return ""
}
