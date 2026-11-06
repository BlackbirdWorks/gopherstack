package cloudformation

import (
	"maps"
	"slices"
	"strconv"
	"strings"
)

const (
	samMaxOrConditions     = 10
	samImplicitAPICond     = "ServerlessRestApiCondition"
	samImplicitHTTPAPICond = "ServerlessHttpApiCondition"
	samPathCondSuffix      = "PathCondition"
	samDeletionRetain      = "Retain"
	samDeletionDelete      = "Delete"
	samDeletionSnapshot    = "Snapshot"
	samMethodAny           = "any"
	samKeySchema           = "schema"
	samPathSlash           = "SLASH"
	samPathOpenBrace       = "OB"
	samPathCloseBrace      = "CB"
	samMinCombineConds     = 2
	samCondUnconditional   = ""
)

func samIf(cond string, then any) map[string]any {
	return map[string]any{fnIf: []any{cond, then, samRef(pseudoNoValue)}}
}

// makeCombinedCondition ORs conds into name, spilling into numbered conditions past ten operands.
func makeCombinedCondition(conds []string, name string) map[string]any {
	out := map[string]any{}
	if len(conds) < samMinCombineConds {
		return out
	}
	extra := (len(conds)-samMinCombineConds)/(samMaxOrConditions-1) + 1 - 1
	list := slices.Clone(conds)
	for len(list) > 1 {
		cur := name
		if extra > 0 {
			cur = name + strconv.Itoa(extra)
			extra--
		}
		n := min(samMaxOrConditions, len(list))
		or := make([]any, 0, n)
		for _, c := range list[:n] {
			or = append(or, map[string]any{samKeyCondition: c})
		}
		out[cur] = map[string]any{samFnOr: or}
		list = append(slices.Clone(list[n:]), cur)
	}

	return out
}

// distinctConditions is the sorted condition set of the routes, and whether every route has one.
func distinctConditions(routes []samRoute) ([]string, bool) {
	set := map[string]bool{}
	all := len(routes) > 0
	for _, rt := range routes {
		if rt.condition == samCondUnconditional {
			all = false

			continue
		}
		set[rt.condition] = true
	}

	return slices.Sorted(maps.Keys(set)), all
}

// combinedOrSingle returns the one condition itself, or registers an OR of several under name.
func (t *samTranslator) combinedOrSingle(conds []string, name string) string {
	if len(conds) == 1 {
		return conds[0]
	}
	maps.Copy(t.conditions, makeCombinedCondition(conds, name))

	return name
}

// implicitAttrs are the resource attributes SAM derives for the implicit API from the functions that use it.
func (t *samTranslator) implicitAttrs(a *samAPI) map[string]any {
	out := map[string]any{}
	if conds, all := distinctConditions(a.routes); all {
		out[samKeyCondition] = t.combinedOrSingle(conds, samImplicitAPICond)
	}
	if p := strongestPolicy(a.delPolicies, samDeletionRetain, samDeletionDelete); p != "" {
		out[samKeyDelPolicy] = p
	}
	if p := strongestPolicy(a.updPolicies, samDeletionRetain, samDeletionSnapshot, samDeletionDelete); p != "" {
		out[samKeyUpdPolicy] = p
	}

	return out
}

func strongestPolicy(seen []string, order ...string) string {
	for _, want := range order {
		if slices.Contains(seen, want) {
			return want
		}
	}

	return ""
}

func samPathConditionName(apiID, path string) string {
	p := strings.NewReplacer("/", samPathSlash, "{", samPathOpenBrace, "}", samPathCloseBrace).Replace(path)

	return apiID + p + samPathCondSuffix
}

// applyConditions wraps conditional functions' methods, and the paths whose methods are all conditional.
func (a *samAPI) applyConditions(t *samTranslator, paths map[string]any) {
	for _, rt := range a.routes {
		if rt.condition == samCondUnconditional {
			continue
		}
		item := asMap(paths[rt.path])
		verb := routeVerb(rt.method)
		if op, ok := item[verb]; ok {
			item[verb] = samIf(rt.condition, op)
		}
	}
	if !a.managed() {
		return
	}
	byPath := map[string][]samRoute{}
	for _, rt := range a.routes {
		byPath[rt.path] = append(byPath[rt.path], rt)
	}
	for _, path := range slices.Sorted(maps.Keys(byPath)) {
		conds, all := distinctConditions(byPath[path])
		if !all {
			continue
		}
		paths[path] = samIf(t.combinedOrSingle(conds, samPathConditionName(a.id, path)), paths[path])
	}
}

func routeVerb(method string) string {
	verb := strings.ToLower(method)
	if verb == samMethodAny {
		return samAnyMethodKey
	}

	return verb
}
