package appsync

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/vektah/gqlparser/v2/ast"
)

// errNullBubble marks a null in a non-null position that must propagate to
// the nearest nullable ancestor (GraphQL spec, "Handling Field Errors").
var errNullBubble = errors.New("null bubbled from non-null position")

// projectSelection shapes a resolver result by the field's selection set:
// only selected members are returned, under their aliases.
func projectSelection(val any, set ast.SelectionSet) any {
	out, _ := (*execEnv)(nil).project(context.Background(), val, set)

	return out
}

// project shapes val by set. When e is non-nil, fields of a parent object that
// have a resolver are resolved with that object as $ctx.source.
func (e *execEnv) project(ctx context.Context, val any, set ast.SelectionSet) (any, error) {
	return e.shape(ctx, val, set, nil)
}

func (e *execEnv) shape(ctx context.Context, val any, set ast.SelectionSet, path []any) (any, error) {
	if len(set) == 0 || val == nil {
		return val, nil
	}

	switch v := val.(type) {
	case []any:
		out := make([]any, len(v))

		for i, it := range v {
			p, err := e.shape(ctx, it, set, appendPath(path, i))
			if err != nil {
				return nil, err
			}

			out[i] = p
		}

		return out, nil
	case map[string]any:
		out := make(map[string]any, len(set))
		if err := e.projectInto(ctx, out, v, set, path); err != nil {
			return nil, err
		}

		return out, nil
	default:
		return val, nil
	}
}

func appendPath(path []any, seg any) []any {
	return append(slices.Clone(path), seg)
}

// complete resolves a field value of declared type typ: it shapes lists and
// objects by the field's selection set and applies non-null propagation.
func (e *execEnv) complete(
	ctx context.Context, val any, typ *ast.Type, f *ast.Field, path []any,
) (any, error) {
	if typ == nil {
		return e.shape(ctx, val, f.SelectionSet, path)
	}

	if val == nil {
		if typ.NonNull {
			e.recordNull(typ, f, path)

			return nil, errNullBubble
		}

		return nil, nil
	}

	var (
		out any
		err error
	)

	if items, ok := val.([]any); ok && typ.Elem != nil {
		out, err = e.completeList(ctx, items, typ, f, path)
	} else {
		out, err = e.shape(ctx, val, f.SelectionSet, path)
	}

	return nullable(out, err, typ)
}

func (e *execEnv) completeList(
	ctx context.Context, items []any, typ *ast.Type, f *ast.Field, path []any,
) (any, error) {
	out := make([]any, len(items))

	for i, it := range items {
		v, err := e.complete(ctx, it, typ.Elem, f, appendPath(path, i))
		if err != nil {
			if !errors.Is(err, errNullBubble) || typ.Elem.NonNull {
				return nil, err
			}

			v = nil
		}

		out[i] = v
	}

	return out, nil
}

// nullable absorbs a bubbled null at a nullable position and re-raises it at
// a non-null one.
func nullable(out any, err error, typ *ast.Type) (any, error) {
	if err == nil {
		return out, nil
	}

	if errors.Is(err, errNullBubble) && !typ.NonNull {
		return nil, nil
	}

	return nil, err
}

func (e *execEnv) recordNull(typ *ast.Type, f *ast.Field, path []any) {
	if e == nil {
		return
	}

	parent := ""
	if f.ObjectDefinition != nil {
		parent = f.ObjectDefinition.Name
	}

	e.errs = append(e.errs, graphqlError{
		Message: fmt.Sprintf("Cannot return null for non-nullable type: '%s' within parent '%s' (%s)",
			typ.Name(), parent, pathString(path)),
		Path: slices.Clone(path),
	})
}

func pathString(path []any) string {
	s := ""
	for _, p := range path {
		s += fmt.Sprintf("/%v", p)
	}

	return s
}

func (e *execEnv) projectInto(
	ctx context.Context, out, src map[string]any, set ast.SelectionSet, path []any,
) error {
	for _, sel := range set {
		var err error

		switch s := sel.(type) {
		case *ast.Field:
			err = e.projectField(ctx, out, src, s, path)
		case *ast.InlineFragment:
			err = e.projectInto(ctx, out, src, s.SelectionSet, path)
		case *ast.FragmentSpread:
			if s.Definition != nil {
				err = e.projectInto(ctx, out, src, s.Definition.SelectionSet, path)
			}
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (e *execEnv) projectField(ctx context.Context, out, src map[string]any, s *ast.Field, path []any) error {
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

	fpath := appendPath(path, s.Alias)

	switch {
	case e != nil && s.ObjectDefinition != nil && e.resolvers[resolverKey(s.ObjectDefinition.Name, s.Name)] != nil:
		v, err = e.resolveFieldValue(ctx, s.ObjectDefinition.Name, s, src, fpath)
	case e != nil && s.Definition != nil:
		v, err = e.complete(ctx, src[s.Name], s.Definition.Type, s, fpath)
	default:
		v, err = e.shape(ctx, src[s.Name], s.SelectionSet, fpath)
	}

	if err != nil {
		return err
	}

	out[s.Alias] = v

	return nil
}
