package appsync

import (
	"context"

	"github.com/vektah/gqlparser/v2/ast"
)

// projectSelection shapes a resolver result by the field's selection set:
// only selected members are returned, under their aliases.
func projectSelection(val any, set ast.SelectionSet) any {
	out, _ := (*execEnv)(nil).project(context.Background(), val, set)

	return out
}

// project shapes val by set. When e is non-nil, fields of a parent object that
// have a resolver are resolved with that object as $ctx.source.
func (e *execEnv) project(ctx context.Context, val any, set ast.SelectionSet) (any, error) {
	if len(set) == 0 || val == nil {
		return val, nil
	}

	switch v := val.(type) {
	case []any:
		out := make([]any, len(v))

		for i, it := range v {
			p, err := e.project(ctx, it, set)
			if err != nil {
				return nil, err
			}

			out[i] = p
		}

		return out, nil
	case map[string]any:
		out := make(map[string]any, len(set))
		if err := e.projectInto(ctx, out, v, set); err != nil {
			return nil, err
		}

		return out, nil
	default:
		return val, nil
	}
}

func (e *execEnv) projectInto(ctx context.Context, out, src map[string]any, set ast.SelectionSet) error {
	for _, sel := range set {
		var err error

		switch s := sel.(type) {
		case *ast.Field:
			err = e.projectField(ctx, out, src, s)
		case *ast.InlineFragment:
			err = e.projectInto(ctx, out, src, s.SelectionSet)
		case *ast.FragmentSpread:
			if s.Definition != nil {
				err = e.projectInto(ctx, out, src, s.Definition.SelectionSet)
			}
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (e *execEnv) projectField(ctx context.Context, out, src map[string]any, s *ast.Field) error {
	if s.Name == "__typename" {
		if s.ObjectDefinition != nil {
			out[s.Alias] = s.ObjectDefinition.Name
		}

		return nil
	}

	var (
		v   any
		err error
	)

	if e != nil && s.ObjectDefinition != nil && e.resolvers[resolverKey(s.ObjectDefinition.Name, s.Name)] != nil {
		v, err = e.resolveFieldValue(ctx, s.ObjectDefinition.Name, s, src)
	} else {
		v, err = e.project(ctx, src[s.Name], s.SelectionSet)
	}

	if err != nil {
		return err
	}

	out[s.Alias] = v

	return nil
}
