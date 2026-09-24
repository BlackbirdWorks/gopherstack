package cloudformation

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// ErrCircularDependency mirrors CloudFormation's real CreateStack/UpdateStack
// validation failure for a dependency cycle among a template's resources,
// whether the cycle is declared via DependsOn or inferred from a
// Ref/Fn::GetAtt/Fn::Sub reference: "Circular dependency between resources:
// [A, B]".
//
//nolint:staticcheck // exact real CloudFormation wording ("Circular dependency between resources: [...]")
var ErrCircularDependency = errors.New("Circular dependency between resources")

// topoSortResources returns the logical resource IDs of resources in an
// order that respects every dependency real CloudFormation infers: explicit
// DependsOn, plus any Ref/Fn::GetAtt/Fn::Sub reference to another resource
// found anywhere in a resource's Properties. Resources with no remaining
// dependency are processed first; within the same dependency level they are
// ordered alphabetically for determinism.
func topoSortResources(resources map[string]TemplateResource) ([]string, error) {
	all := collections.SortedKeys(resources)

	ids := make(map[string]struct{}, len(all))
	for _, id := range all {
		ids[id] = struct{}{}
	}

	revDeps := make(map[string][]string, len(resources))
	inDegree := make(map[string]int, len(all))

	for _, id := range all {
		deps := resourceDependencies(id, resources[id], ids)
		inDegree[id] = len(deps)
		for _, dep := range deps {
			revDeps[dep] = append(revDeps[dep], id)
		}
	}

	queue := make([]string, 0, len(all))
	for _, id := range all {
		if inDegree[id] == 0 {
			queue = append(queue, id)
		}
	}

	result := make([]string, 0, len(all))

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		result = append(result, cur)

		for _, dependent := range revDeps[cur] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				insertSorted(&queue, dependent)
			}
		}
	}

	if len(result) < len(all) {
		return nil, circularDependencyError(all, result)
	}

	return result, nil
}

// insertSorted inserts s into the sorted slice ss, maintaining ascending order.
func insertSorted(ss *[]string, s string) {
	i := sort.SearchStrings(*ss, s)
	*ss = append(*ss, "")
	copy((*ss)[i+1:], (*ss)[i:])
	(*ss)[i] = s
}

// circularDependencyError builds ErrCircularDependency's message from the
// resources topoSortResources could not place: those still holding an
// unresolved dependency once every resolvable resource was processed, i.e.
// the cycle itself plus anything downstream of it. Sorted alphabetically for
// a stable, reproducible message (real AWS's own member order isn't
// documented).
func circularDependencyError(all, placed []string) error {
	done := make(map[string]struct{}, len(placed))
	for _, id := range placed {
		done[id] = struct{}{}
	}

	stuck := make([]string, 0, len(all)-len(placed))
	for _, id := range all {
		if _, ok := done[id]; !ok {
			stuck = append(stuck, id)
		}
	}

	return fmt.Errorf("%w: [%s]", ErrCircularDependency, strings.Join(stuck, ", "))
}

// resourceDependencies returns the logical IDs id's resource depends on: its
// explicit DependsOn plus every other resource referenced anywhere in its
// Properties via Ref, Fn::GetAtt, or Fn::Sub. Only references that resolve to
// another entry in ids (a real resource declared in this template, not a
// parameter or pseudo-parameter such as AWS::Region/AWS::AccountId/
// AWS::StackName/AWS::NoValue) count; id itself is excluded so a resource
// can never depend on itself.
func resourceDependencies(id string, res TemplateResource, ids map[string]struct{}) []string {
	refs := make(map[string]struct{}, len(res.DependsOn))
	for _, dep := range res.DependsOn {
		if _, ok := ids[dep]; ok {
			refs[dep] = struct{}{}
		}
	}

	scanResourceRefs(res.Properties, ids, refs)
	delete(refs, id)

	return collections.SortedKeys(refs)
}

// scanResourceRefs recursively walks v -- a Properties tree, or any nested
// intrinsic-function argument -- recording every logical ID in ids that it
// references via Ref, Fn::GetAtt (list or dotted-string form), or Fn::Sub
// into refs. It recurses through arbitrary nesting (Fn::If/Fn::Join/
// Fn::Select/Fn::FindInMap/plain lists and maps/...) via the generic
// fallback in scanResourceRefsMap.
func scanResourceRefs(v any, ids, refs map[string]struct{}) {
	switch val := v.(type) {
	case map[string]any:
		scanResourceRefsMap(val, ids, refs)
	case []any:
		for _, item := range val {
			scanResourceRefs(item, ids, refs)
		}
	}
}

func scanResourceRefsMap(val map[string]any, ids, refs map[string]struct{}) {
	if ref, ok := val["Ref"].(string); ok {
		addResourceRef(ref, ids, refs)

		return
	}

	if getAttArgs, ok := val[fnGetAtt].([]any); ok && len(getAttArgs) > 0 {
		if logicalID, isStr := getAttArgs[0].(string); isStr {
			addResourceRef(logicalID, ids, refs)
		}

		return
	}

	if getAttStr, ok := val[fnGetAtt].(string); ok {
		logicalID, _, _ := strings.Cut(getAttStr, ".")
		addResourceRef(logicalID, ids, refs)

		return
	}

	if _, isSub := val["Fn::Sub"]; isSub {
		scanSubRefs(val, ids, refs)

		return
	}

	for _, child := range val {
		scanResourceRefs(child, ids, refs)
	}
}

// scanSubRefs records the logical IDs an Fn::Sub node references: every
// ${Id}/${Id.Attr} placeholder in its template string, skipping ${!Literal}
// escapes and any name the two-arg form declares as its own local variable
// (subTemplateAndLocals, shared with validateSubRefs) -- plus any resource
// references inside those local variables' own values.
func scanSubRefs(node map[string]any, ids, refs map[string]struct{}) {
	tmplStr, locals := subTemplateAndLocals(node)

	for _, match := range subVarPattern.FindAllStringSubmatch(tmplStr, -1) {
		expr := match[1]
		if strings.HasPrefix(expr, "!") {
			continue
		}

		logicalID, _, _ := strings.Cut(expr, ".")
		if _, isLocal := locals[logicalID]; isLocal {
			continue
		}

		addResourceRef(logicalID, ids, refs)
	}

	args, isArr := node["Fn::Sub"].([]any)
	if !isArr || len(args) != 2 {
		return
	}

	if varMap, isMap := args[1].(map[string]any); isMap {
		for _, v := range varMap {
			scanResourceRefs(v, ids, refs)
		}
	}
}

func addResourceRef(id string, ids, refs map[string]struct{}) {
	if _, ok := ids[id]; ok {
		refs[id] = struct{}{}
	}
}

// reverseDependencyOrder orders ids -- a subset of the resources declared in
// templateBody -- in reverse dependency order: for a dependency edge between
// two resources, the dependent is ordered before what it depends on. It
// reuses topoSortResources' own creation-order graph over templateBody,
// reverses it, and filters down to ids (a sub-sequence of a topological
// order is itself a valid topological order of the induced subgraph, so
// filtering after reversing is correct). Used for both DeleteStack (ids =
// every currently-live resource) and UpdateStack's stale-resource cleanup
// (ids = only the resources dropped from the new template).
//
// Falls back to plain alphabetical order of ids if templateBody can't be
// parsed or its dependency graph has a cycle -- neither should happen for a
// template that was already successfully created.
func reverseDependencyOrder(ids []string, templateBody string) []string {
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)

	tmpl, err := ParseTemplate(templateBody)
	if err != nil {
		return sorted
	}

	order, err := topoSortResources(tmpl.Resources)
	if err != nil {
		return sorted
	}

	want := make(map[string]struct{}, len(sorted))
	for _, id := range sorted {
		want[id] = struct{}{}
	}

	reversed := make([]string, 0, len(sorted))
	for _, id := range slices.Backward(order) {
		if _, ok := want[id]; ok {
			reversed = append(reversed, id)
		}
	}

	if len(reversed) == len(sorted) {
		return reversed
	}

	// An id absent from templateBody's own Resources (shouldn't happen --
	// every id here was created from it) is appended rather than dropped, so
	// deletion never silently skips a resource.
	found := make(map[string]struct{}, len(reversed))
	for _, id := range reversed {
		found[id] = struct{}{}
	}

	for _, id := range sorted {
		if _, ok := found[id]; !ok {
			reversed = append(reversed, id)
		}
	}

	return reversed
}
