package gremlin

import (
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func elemProps(v any) (map[string]any, bool) {
	switch t := v.(type) {
	case *opencypher.Node:
		return t.Props, true
	case *opencypher.Rel:
		return t.Props, true
	}

	return nil, false
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func elemLabel(v any) string {
	switch t := v.(type) {
	case *opencypher.Node:
		return vertexLabel(t)
	case *opencypher.Rel:
		return t.Type
	}

	return ""
}

func labelMatches(v any, label string) bool {
	switch t := v.(type) {
	case *opencypher.Node:
		if vertexLabel(t) == label {
			return true
		}

		if slices.Contains(t.Labels, label) {
			return true
		}
	case *opencypher.Rel:
		return t.Type == label
	}

	return false
}

// propValues lists the values stored under key; a list-valued property is multi-valued.
func propValues(v any, key string) []any {
	props, ok := elemProps(v)
	if !ok {
		return nil
	}

	raw, ok := props[key]
	if !ok || raw == nil {
		return nil
	}

	if l, isList := raw.([]any); isList {
		return l
	}

	return []any{raw}
}

func tokenValue(t Token, v any) (any, bool) {
	switch t.Name {
	case tokID:
		return elementIDOrValue(v), true
	case tokLabel:
		if _, ok := elemProps(v); ok {
			return elemLabel(v), true
		}

		if p, ok := v.(*Prop); ok {
			return p.Key, true
		}
	case tokKey:
		if p, ok := v.(*Prop); ok {
			return p.Key, true
		}
	case tokValue:
		if p, ok := v.(*Prop); ok {
			return p.Val, true
		}
	}

	return nil, false
}

func elementIDOrValue(v any) any {
	if p, ok := v.(*Prop); ok {
		return p.ID()
	}

	if id := elementID(v); id != "" {
		return id
	}

	return nil
}

func matchAny(actual []any, want any) bool {
	for _, a := range actual {
		if matchOne(a, want) {
			return true
		}
	}

	return false
}

func matchOne(actual, want any) bool {
	if p, ok := want.(Pred); ok {
		return p.test(actual)
	}

	return equalValues(actual, want)
}
