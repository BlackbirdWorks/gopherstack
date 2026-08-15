package main

import (
	"strconv"
	"strings"
)

// resolvePriority turns a MatchPriority() method body's source text into a
// number: a direct "service.PriorityXxx" reference, a local const that
// aliases one (with an optional "+N" bump, see collectPriorityBumpConst), a
// local int const, or a raw integer literal. Returns -1 if none of those
// apply (e.g. the priority is computed via a method call, like macie2's
// h.restRouter().MatchPriority() -- see services/_ROUTE_COLLISIONS.md's
// "known tool limitations" for why that's left unresolved rather than
// guessed at).
func resolvePriority(
	matchPriorityBody string,
	selectorConsts map[string]string,
	intConsts, priorityConsts map[string]int,
) int {
	body := trimMatchPriorityBody(matchPriorityBody)

	if after, hasServicePrefix := strings.CutPrefix(body, "service."); hasServicePrefix {
		if n, ok := priorityConsts[after]; ok {
			return n
		}
	}

	if n, ok := resolveSelectorPriority(body, selectorConsts, priorityConsts); ok {
		return n
	}

	if n, ok := intConsts[body]; ok {
		return n
	}

	if n, err := strconv.Atoi(body); err == nil {
		return n
	}

	return -1
}

func trimMatchPriorityBody(raw string) string {
	body := strings.TrimSpace(raw)
	body = strings.TrimPrefix(body, "{")
	body = strings.TrimSuffix(body, "}")
	body = strings.TrimSpace(body)
	body = strings.TrimPrefix(body, "return")

	return strings.TrimSpace(body)
}

func resolveSelectorPriority(body string, selectorConsts map[string]string, priorityConsts map[string]int) (int, bool) {
	sel, ok := selectorConsts[body]
	if !ok {
		return 0, false
	}

	base, offsetStr, hasOffset := strings.Cut(sel, "+")

	n, ok := priorityConsts[base]
	if !ok {
		return 0, false
	}

	if !hasOffset {
		return n, true
	}

	add, err := strconv.Atoi(offsetStr)
	if err != nil {
		return 0, false
	}

	return n + add, true
}
