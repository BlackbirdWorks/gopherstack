package main

import "sort"

// unionOpFuncs is every operation name any resolved SDK module has its own
// deserializeOpError<Op> function for -- the full set of operations this
// scan has SOME per-op ground truth to check against.
func unionOpFuncs(smt *serviceModuleTruth) map[string]bool {
	out := map[string]bool{}

	for _, mgt := range smt.Modules {
		for op := range mgt.OpFuncs {
			out[op] = true
		}
	}

	return out
}

// buildDomainOps groups, across every resolved operation, which domains
// (receiver-type names) resolved at least one root for it -- moduleassign.go's
// input for picking which module governs each domain.
func buildDomainOps(resolved map[string][]opRoot) map[string]map[string]bool {
	out := map[string]map[string]bool{}

	for op, roots := range resolved {
		for domain := range groupRootsByDomain(roots) {
			if out[domain] == nil {
				out[domain] = map[string]bool{}
			}

			out[domain][op] = true
		}
	}

	return out
}

func groupRootsByDomain(roots []opRoot) map[string][]opRoot {
	out := map[string][]opRoot{}

	for _, r := range roots {
		out[r.Domain] = append(out[r.Domain], r)
	}

	return out
}

// effectiveModule resolves which module governs domain: the service's only
// resolved module when there is exactly one (the common case, bypassing
// domain assignment entirely so a service with zero receiver-typed handlers
// still gets checked), or moduleassign.go's data-driven per-domain pick when
// there are several.
func effectiveModule(domain string, domainModule map[string]string, smt *serviceModuleTruth) (string, bool) {
	if len(smt.Modules) == 1 {
		for mod := range smt.Modules {
			return mod, true
		}
	}

	mod, ok := domainModule[domain]

	return mod, ok
}

// siblingsAccepting lists other operations (this module's own PerOp set,
// excluding op itself) whose declared codes DO include code -- the evidence
// that a finding is a real, misplaced code rather than a fabricated one:
// "this shared sentinel is right for these callers, wrong for this one."
// Capped and sorted for stable, readable output.
func siblingsAccepting(mgt *moduleGroundTruth, op, code string) []string {
	const maxSiblings = 5

	var out []string

	for other, codes := range mgt.PerOp {
		if other == op || !codes[code] {
			continue
		}

		out = append(out, other)
	}

	sort.Strings(out)

	if len(out) > maxSiblings {
		out = out[:maxSiblings]
	}

	return out
}
