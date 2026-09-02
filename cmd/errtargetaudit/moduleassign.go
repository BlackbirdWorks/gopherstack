package main

// assignDomainModules picks, for each domain (a receiver-type name, or ""
// for package-level dispatch) that resolved at least one operation, which
// pinned SDK module's per-op ground truth actually governs it -- needed
// only when a service resolves 2+ modules at all (services/bedrock's
// Handler vs AgentsHandler, bedrock vs bedrockagent). The assignment is
// DATA-DRIVEN, not name-matched: each candidate module's own known-operation
// set (moduleGroundTruth.OpFuncs, read straight from its
// deserializeOpError<Op> function names) is intersected against the set of
// operation names THIS domain actually resolved a handler for, and the
// module with the largest overlap wins. This is what correctly separates
// bedrock's two PutResourcePolicy operations -- one op name, genuinely
// different operations in different real APIs -- without ever comparing a
// Go type name to a module name: the "Handler" domain resolves ~108 ops that
// overlap heavily with the "bedrock" module's own op set and barely at all
// with "bedrockagent"'s, and vice versa for "AgentsHandler".
//
// A domain whose best overlap is zero, or tied between two modules, is left
// UNASSIGNED rather than guessed -- scan.go skips ground-truth checking for
// any operation resolved only through an unassigned domain, since which
// module's declared-code set would even apply is genuinely unknown.
func assignDomainModules(domainOps map[string]map[string]bool, smt *serviceModuleTruth) map[string]string {
	out := map[string]string{}

	if len(smt.Modules) <= 1 {
		var only string

		for mod := range smt.Modules {
			only = mod
		}

		if only == "" {
			return out
		}

		for domain := range domainOps {
			out[domain] = only
		}

		return out
	}

	for domain, ops := range domainOps {
		if mod, ok := bestOverlapModule(ops, smt); ok {
			out[domain] = mod
		}
	}

	return out
}

func bestOverlapModule(ops map[string]bool, smt *serviceModuleTruth) (string, bool) {
	best, bestOverlap, tie := "", -1, false

	for mod, mgt := range smt.Modules {
		overlap := 0

		for op := range ops {
			if mgt.OpFuncs[op] {
				overlap++
			}
		}

		switch {
		case overlap > bestOverlap:
			best, bestOverlap, tie = mod, overlap, false
		case overlap == bestOverlap:
			tie = true
		}
	}

	return best, bestOverlap > 0 && !tie
}
