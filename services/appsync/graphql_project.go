package appsync

import "github.com/vektah/gqlparser/v2/ast"

// projectSelection shapes a resolver result by the field's selection set:
// only selected members are returned, under their aliases.
func projectSelection(val any, set ast.SelectionSet) any {
	if len(set) == 0 || val == nil {
		return val
	}

	switch v := val.(type) {
	case []any:
		out := make([]any, len(v))
		for i, it := range v {
			out[i] = projectSelection(it, set)
		}

		return out
	case map[string]any:
		out := make(map[string]any, len(set))
		projectInto(out, v, set)

		return out
	default:
		return val
	}
}

func projectInto(out, src map[string]any, set ast.SelectionSet) {
	for _, sel := range set {
		switch s := sel.(type) {
		case *ast.Field:
			if s.Name == "__typename" {
				if s.ObjectDefinition != nil {
					out[s.Alias] = s.ObjectDefinition.Name
				}

				continue
			}

			out[s.Alias] = projectSelection(src[s.Name], s.SelectionSet)
		case *ast.InlineFragment:
			projectInto(out, src, s.SelectionSet)
		case *ast.FragmentSpread:
			if s.Definition != nil {
				projectInto(out, src, s.Definition.SelectionSet)
			}
		}
	}
}
