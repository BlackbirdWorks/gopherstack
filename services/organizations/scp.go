package organizations

import (
	"slices"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// SCPsForAccount implements iam.SCPProvider. Levels are root-first, then each
// OU on the path, then the account itself.
func (b *InMemoryBackend) SCPsForAccount(accountID string) ([]iam.SCPLevel, bool) {
	b.mu.RLock("SCPsForAccount")
	defer b.mu.RUnlock()

	if b.org == nil || b.root == nil || accountID == b.org.MasterAccountID || !b.scpEnabledLocked() {
		return nil, false
	}

	if !b.accounts.Has(accountID) {
		return nil, false
	}

	parent, ok := b.accountParent[accountID]
	if !ok {
		return nil, false
	}

	ous, ok := b.ancestorOUChainLocked(parent)
	if !ok {
		return nil, false
	}

	path := append([]string{b.root.ID}, ous...)
	path = append(path, accountID)

	levels := make([]iam.SCPLevel, 0, len(path))

	for _, target := range path {
		lvl := iam.SCPLevel{TargetID: target}

		for _, pid := range b.targetPolicies[target] {
			if p, found := b.policies.Get(pid); found && p.PolicySummary.Type == policyTypeSCP {
				lvl.Policies = append(lvl.Policies, iam.SCPPolicy{ARN: p.PolicySummary.ARN, Document: p.Content})
			}
		}

		levels = append(levels, lvl)
	}

	return levels, true
}

func (b *InMemoryBackend) scpEnabledLocked() bool {
	return slices.ContainsFunc(b.root.PolicyTypes, func(pt PolicyTypeSummary) bool {
		return pt.Type == policyTypeSCP && pt.Status == policyStatusEnabled
	})
}

// attachDefaultSCPLocked attaches FullAWSAccess to a new OU or account, as
// Organizations does for every entity created while SCPs are enabled.
func (b *InMemoryBackend) attachDefaultSCPLocked(targetID string) {
	if b.root == nil || !b.scpEnabledLocked() || !b.policies.Has(fullAWSAccessPolicyID) {
		return
	}

	if slices.Contains(b.targetPolicies[targetID], fullAWSAccessPolicyID) {
		return
	}

	b.targetPolicies[targetID] = append(b.targetPolicies[targetID], fullAWSAccessPolicyID)
	b.policyTargets[fullAWSAccessPolicyID] = append(b.policyTargets[fullAWSAccessPolicyID], targetID)
}

// attachDefaultSCPEverywhereLocked attaches FullAWSAccess to the root, every OU and every account.
func (b *InMemoryBackend) attachDefaultSCPEverywhereLocked() {
	b.attachDefaultSCPLocked(b.root.ID)

	for _, ou := range b.ous.All() {
		b.attachDefaultSCPLocked(ou.ID)
	}

	for _, a := range b.accounts.All() {
		b.attachDefaultSCPLocked(a.ID)
	}
}

// detachAllSCPsLocked removes every SCP attachment: disabling the SCP policy
// type detaches all SCPs (Organizations User Guide, "SCP effects on permissions").
func (b *InMemoryBackend) detachAllSCPsLocked() {
	for _, p := range b.policies.All() {
		if p.PolicySummary.Type != policyTypeSCP {
			continue
		}

		for _, target := range b.policyTargets[p.PolicySummary.ID] {
			b.targetPolicies[target] = removeString(b.targetPolicies[target], p.PolicySummary.ID)
		}

		b.policyTargets[p.PolicySummary.ID] = []string{}
	}
}
