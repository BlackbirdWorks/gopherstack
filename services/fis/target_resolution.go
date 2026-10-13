package fis

import (
	"context"
	"slices"
	"strings"
)

const (
	emptyTargetResolutionFail = "fail"
	emptyTargetResolutionSkip = "skip"
	accountTargetingMulti     = "multi-account"

	emptyTargetResolutionReason = "Target resolution returned empty set"
	targetResolutionFailedCode  = "TargetResolutionFailed"
)

// TargetResolver resolves a tag-selected target to the ARNs of the matching resources.
type TargetResolver interface {
	ResolveTargets(
		ctx context.Context, resourceType string, tags map[string]string, filters []ExperimentTemplateTargetFilter,
	) []string
}

// SetTargetResolver registers the collaborator that resolves resourceTags and filters to ARNs.
func (b *InMemoryBackend) SetTargetResolver(r TargetResolver) {
	b.mu.Lock("SetTargetResolver")
	defer b.mu.Unlock()

	b.targetResolver = r
}

func arnAccount(arnStr string) string {
	const (
		accountField = 4
		arnFields    = 6
	)

	parts := strings.SplitN(arnStr, ":", arnFields)
	if len(parts) < arnFields {
		return ""
	}

	return parts[accountField]
}

// inTargetAccounts keeps ARNs belonging to one of accounts; ARNs without an account (S3) always match.
func inTargetAccounts(arns, accounts []string) []string {
	out := make([]string, 0, len(arns))

	for _, a := range arns {
		if acct := arnAccount(a); acct == "" || slices.Contains(accounts, acct) {
			out = append(out, a)
		}
	}

	return out
}

func mergeARNs(groups ...[]string) []string {
	seen := make(map[string]struct{})
	out := []string{}

	for _, g := range groups {
		for _, a := range g {
			if _, dup := seen[a]; !dup {
				seen[a] = struct{}{}
				out = append(out, a)
			}
		}
	}

	return out
}

// targetAccounts returns the accounts a template's targets resolve in: its target account
// configurations under multi-account targeting, else the experiment's own account.
func (b *InMemoryBackend) targetAccounts(tpl *ExperimentTemplate) []string {
	if tpl.ExperimentOptions == nil || tpl.ExperimentOptions.AccountTargeting != accountTargetingMulti {
		return []string{b.accountID}
	}

	b.mu.RLock("targetAccounts")
	defer b.mu.RUnlock()

	cfgs := b.targetAccountConfigsByTemplate.Get(tpl.ID)
	accounts := make([]string, 0, len(cfgs))

	for _, c := range cfgs {
		accounts = append(accounts, c.AccountID)
	}

	return accounts
}

// resolveTemplateTargets resolves every tag-selected target of tpl, rewrites tpl's
// ResourceArns with the merged result, mirrors it onto the experiment, and returns the
// names of targets that ended up with no resources.
func (b *InMemoryBackend) resolveTemplateTargets(ctx context.Context, expID string, tpl *ExperimentTemplate) []string {
	b.mu.RLock("resolveTemplateTargets")
	resolver := b.targetResolver
	b.mu.RUnlock()

	if resolver == nil {
		return nil
	}

	accounts := b.targetAccounts(tpl)

	var empty []string

	for name, t := range tpl.Targets {
		if len(t.ResourceTags) == 0 {
			continue
		}

		found := inTargetAccounts(resolver.ResolveTargets(ctx, t.ResourceType, t.ResourceTags, t.Filters), accounts)
		t.ResourceArns = mergeARNs(t.ResourceArns, found)
		tpl.Targets[name] = t

		if len(t.ResourceArns) == 0 {
			empty = append(empty, name)
		}
	}

	slices.Sort(empty)
	b.setResolvedTargetARNs(expID, tpl.Targets)

	return empty
}

func (b *InMemoryBackend) setResolvedTargetARNs(expID string, targets map[string]ExperimentTemplateTarget) {
	b.mu.Lock("setResolvedTargetARNs")
	defer b.mu.Unlock()

	exp, ok := b.experiments.Get(expID)
	if !ok {
		return
	}

	for name, t := range targets {
		if et, found := exp.Targets[name]; found && len(t.ResourceTags) > 0 {
			et.ResourceArns = slices.Clone(t.ResourceArns)
			exp.Targets[name] = et
		}
	}
}

func emptyResolutionMode(tpl *ExperimentTemplate) string {
	if tpl.ExperimentOptions != nil && tpl.ExperimentOptions.EmptyTargetResolutionMode == emptyTargetResolutionSkip {
		return emptyTargetResolutionSkip
	}

	return emptyTargetResolutionFail
}

// skippedActionsFor returns the actions that reference any of the empty targets.
func skippedActionsFor(tpl *ExperimentTemplate, empty []string) map[string]bool {
	skipped := make(map[string]bool)

	for name, action := range tpl.Actions {
		for key, targetName := range action.Targets {
			if slices.Contains(empty, targetName) || slices.Contains(empty, key) {
				skipped[name] = true
			}
		}
	}

	return skipped
}

func (b *InMemoryBackend) markActionsSkipped(expID string, skipped map[string]bool) {
	b.mu.Lock("markActionsSkipped")
	defer b.mu.Unlock()

	exp, ok := b.experiments.Get(expID)
	if !ok {
		return
	}

	for name := range skipped {
		if a, found := exp.Actions[name]; found {
			a.Status = ExperimentActionStatus{Status: actionStatusSkipped, Reason: emptyTargetResolutionReason}
			exp.Actions[name] = a
		}
	}
}
