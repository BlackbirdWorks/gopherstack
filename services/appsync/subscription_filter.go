package appsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
)

const (
	extSetSubscriptionFilter   = "setSubscriptionFilter"
	extSetInvalidationFilter   = "setSubscriptionInvalidationFilter"
	extInvalidateSubscriptions = "invalidateSubscriptions"
	opContains                 = "contains"
	opNotContains              = "notContains"
	opBetween                  = "between"
	keyLogs                    = "logs"
)

// ErrInvalidSubscriptionFilter is returned for a malformed enhanced subscription filter.
var ErrInvalidSubscriptionFilter = errors.New("invalid subscription filter")

// subFilterSet is a filterGroup: groups are OR-ed, filters within a group AND-ed.
type subFilterSet [][]subFilter

// plainJSON converts a VTL/JS value into plain JSON-shaped Go values.
func plainJSON(v any) (any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var out any

	err = json.Unmarshal(raw, &out)

	return out, err
}

// parseSubscriptionFilter decodes {"filterGroup":[{"filters":[{fieldName,operator,value}]}]},
// the argument of $extensions.setSubscriptionFilter (AppSync developer guide,
// "Enhanced subscription filtering").
func parseSubscriptionFilter(arg any) (subFilterSet, error) {
	plain, err := plainJSON(arg)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSubscriptionFilter, err)
	}

	root, ok := plain.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: filter must be an object", ErrInvalidSubscriptionFilter)
	}

	groups, ok := root["filterGroup"].([]any)
	if !ok || len(groups) == 0 {
		return nil, fmt.Errorf("%w: filterGroup must be a non-empty list", ErrInvalidSubscriptionFilter)
	}

	out := make(subFilterSet, 0, len(groups))

	for _, g := range groups {
		gm, _ := g.(map[string]any)
		fs, _ := gm["filters"].([]any)

		if len(fs) == 0 {
			return nil, fmt.Errorf("%w: each filter group needs filters", ErrInvalidSubscriptionFilter)
		}

		group := make([]subFilter, 0, len(fs))

		for _, f := range fs {
			fm, _ := f.(map[string]any)
			sf := subFilter{FieldName: vrender(fm["fieldName"]), Operator: vrender(fm["operator"]), Value: fm["value"]}

			if sf.FieldName == "" || !isSubFilterOp(sf.Operator) {
				return nil, fmt.Errorf("%w: bad filter %v", ErrInvalidSubscriptionFilter, f)
			}

			group = append(group, sf)
		}

		out = append(out, group)
	}

	return out, nil
}

// matches reports whether payload satisfies the filter; an empty set matches everything.
func (s subFilterSet) matches(payload any) bool {
	if len(s) == 0 {
		return true
	}

	return slices.ContainsFunc(s, func(g []subFilter) bool {
		return !slices.ContainsFunc(g, func(f subFilter) bool { return !f.matches(payload) })
	})
}

func lookupField(payload any, name string) (any, bool) {
	cur := payload

	for part := range strings.SplitSeq(name, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}

		if cur, ok = m[part]; !ok {
			return nil, false
		}
	}

	return cur, true
}

func (f subFilter) matches(payload any) bool {
	got, ok := lookupField(payload, f.FieldName)
	if !ok {
		return false
	}

	want, _ := plainJSON(f.Value)

	switch f.Operator {
	case "eq":
		return looseEqual(got, want)
	case "ne":
		return !looseEqual(got, want)
	case "le", "lt", "ge", "gt":
		return compareOp(f.Operator, got, want)
	case opBetween:
		b := listOf(want)

		return len(b) == betweenBounds && compareOp("ge", got, b[0]) && compareOp("le", got, b[1])
	}

	return f.matchesCollection(got, want)
}

func (f subFilter) matchesCollection(got, want any) bool {
	inWant := func(g any) bool {
		return slices.ContainsFunc(listOf(want), func(w any) bool { return looseEqual(g, w) })
	}

	switch f.Operator {
	case opContains:
		return containsValue(got, want)
	case opNotContains:
		return !containsValue(got, want)
	case mBeginsWith:
		g, gok := got.(string)
		w, wok := want.(string)

		return gok && wok && strings.HasPrefix(g, w)
	case "in":
		return inWant(got)
	case "notIn":
		return !inWant(got)
	case "containsAny":
		return slices.ContainsFunc(listOf(got), inWant)
	}

	return false
}

func looseEqual(a, b any) bool {
	if af, _, ok := toNum(a); ok {
		if bf, _, ok2 := toNum(b); ok2 {
			return af == bf
		}
	}

	return reflect.DeepEqual(a, b)
}

func compareOp(op string, a, b any) bool {
	var c int

	af, _, aok := toNum(a)
	bf, _, bok := toNum(b)

	switch {
	case aok && bok:
		switch {
		case af < bf:
			c = -1
		case af > bf:
			c = 1
		}
	default:
		as, aStr := a.(string)
		bs, bStr := b.(string)

		if !aStr || !bStr {
			return false
		}

		c = strings.Compare(as, bs)
	}

	switch op {
	case "le":
		return c <= 0
	case "lt":
		return c < 0
	case "ge":
		return c >= 0
	}

	return c > 0
}

func containsValue(got, want any) bool {
	if s, ok := got.(string); ok {
		w, wok := want.(string)

		return wok && strings.Contains(s, w)
	}

	return slices.ContainsFunc(listOf(got), func(g any) bool { return looseEqual(g, want) })
}

// subscriptionFilters runs the Subscription.<field> resolver (if any) and
// collects the $extensions.setSubscriptionFilter /
// setSubscriptionInvalidationFilter calls from its response mapping.
func (b *InMemoryBackend) subscriptionFilters(
	ctx context.Context, apiID string, field *ast.Field, vars map[string]any, rc requestIdentity,
) (subFilterSet, subFilterSet, error) {
	state := b.execSnapshot(apiID)

	resolver := state.resolvers[resolverKey("Subscription", field.Name)]
	if resolver == nil {
		return nil, nil, nil
	}

	env := &execEnv{
		backend: b, resolvers: state.resolvers, datasources: state.datasources,
		functions: state.functions, variables: vars, rc: rc,
	}
	cc := env.newCallCtx("Subscription", field, true)

	if _, err := resolveField(
		ctx, b, resolver, state.datasources, state.functions, extractArguments(field, vars), nil, cc,
	); err != nil {
		return nil, nil, err
	}

	var (
		filter, invalidation subFilterSet
		err                  error
	)

	for _, rec := range cc.extensionCalls() {
		args, _ := rec["args"].([]any)

		switch rec["kind"] {
		case extSetSubscriptionFilter:
			filter, err = parseSubscriptionFilter(argAt(args, 0))
		case extSetInvalidationFilter:
			invalidation, err = parseSubscriptionFilter(argAt(args, 0))
		}

		if err != nil {
			return nil, nil, err
		}
	}

	return filter, invalidation, nil
}

func (c *callCtx) extensionCalls() []map[string]any {
	if c.util == nil {
		return nil
	}

	return c.util.ext
}

// invalidateSubscriptions closes subscriptions whose invalidation filter matches
// a $extensions.invalidateSubscriptions({subscriptionField, payload}) call.
func (b *InMemoryBackend) invalidateSubscriptions(apiID string, calls []map[string]any) {
	for _, rec := range calls {
		if rec["kind"] != extInvalidateSubscriptions {
			continue
		}

		args, _ := rec["args"].([]any)

		plain, err := plainJSON(argAt(args, 0))
		if err != nil {
			continue
		}

		spec, _ := plain.(map[string]any)
		field, _ := spec["subscriptionField"].(string)
		payload := spec["payload"]

		for _, s := range b.invalidatedSubs(apiID, field, payload) {
			b.stopSubscription(s.conn, s.id)
		}
	}
}

func (b *InMemoryBackend) invalidatedSubs(apiID, field string, payload any) []*rtSub {
	b.mu.RLock("InvalidateSubscriptions")
	defer b.mu.RUnlock()

	var out []*rtSub

	for s := range b.rtSubs[apiID] {
		if s.field == field && len(s.invalidation) > 0 && s.invalidation.matches(payload) {
			out = append(out, s)
		}
	}

	return out
}
