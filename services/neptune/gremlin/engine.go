package gremlin

import (
	"context"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

// Vertex is a detached vertex reference as returned to clients.
type Vertex struct {
	ID    string
	Label string
}

// Edge is a detached edge reference as returned to clients.
type Edge struct {
	ID       string
	Label    string
	OutID    string
	OutLabel string
	InID     string
	InLabel  string
}

// Property is a detached property as returned to clients.
type Property struct {
	Val any
	Key string
	ID  int64
	VP  bool
}

// Engine runs Gremlin against an openCypher graph.
type Engine struct {
	steps map[string]stepFn
}

// NewEngine returns an engine with every supported step registered.
func NewEngine() *Engine { return &Engine{steps: buildSteps()} }

// Eval parses and runs a Gremlin script.
func (e *Engine) Eval(ctx context.Context, g *opencypher.Graph, script string, bindings map[string]any) ([]any, error) {
	stmts, err := ParseScript(script, bindings)
	if err != nil {
		return nil, err
	}

	return e.run(ctx, g, stmts)
}

// Bytecode runs a traversal that arrived as bytecode.
func (e *Engine) Bytecode(ctx context.Context, g *opencypher.Graph, tr *Traversal) ([]any, error) {
	return e.run(ctx, g, []Stmt{{Tr: tr, Term: "toList"}})
}

func (e *Engine) run(ctx context.Context, g *opencypher.Graph, stmts []Stmt) ([]any, error) {
	var result []any

	body := func(st *opencypher.Store) error {
		x := newExec(ctx, st, e.steps)

		for _, s := range stmts {
			out, err := x.run(s.Tr, rootTrav())
			if err != nil {
				return err
			}

			result = detachAll(st, applyTerminal(s.Term, s.N, values(out)))
		}

		return nil
	}

	var err error
	if stmtsMutate(stmts) {
		err = g.Update(ctx, body)
	} else {
		err = g.View(ctx, body)
	}

	if err != nil {
		return nil, mapGraphError(ctx, err)
	}

	return result, nil
}

func stmtsMutate(stmts []Stmt) bool {
	for _, s := range stmts {
		if s.Tr.Mutates() {
			return true
		}
	}

	return false
}

func mapGraphError(ctx context.Context, err error) error {
	if _, ok := AsError(err); ok {
		return err
	}

	if ce, ok := opencypher.AsError(err); ok {
		return &Error{Code: ce.Code, Status: StatusScriptEvalError, Detail: ce.Detail}
	}

	if cerr := ctx.Err(); cerr != nil {
		return ctxError(cerr)
	}

	return &Error{Code: CodeInternal, Status: StatusServerError, Detail: err.Error()}
}

func applyTerminal(term string, n int, vals []any) []any {
	switch term {
	case "iterate":
		return nil
	case "hasNext":
		return []any{len(vals) > 0}
	case "next", "tryNext":
		if n <= 0 {
			n = 1
		}

		return vals[:min(n, len(vals))]
	case "toSet":
		return dedupValues(vals)
	}

	return vals
}

func dedupValues(vals []any) []any {
	seen := map[string]bool{}
	out := make([]any, 0, len(vals))

	for _, v := range vals {
		if k := keyOf(v); !seen[k] {
			seen[k] = true

			out = append(out, v)
		}
	}

	return out
}

func detachAll(st *opencypher.Store, vs []any) []any {
	out := make([]any, len(vs))
	for i, v := range vs {
		out[i] = detach(st, v)
	}

	return out
}

func detach(st *opencypher.Store, v any) any {
	switch t := v.(type) {
	case *opencypher.Node:
		return &Vertex{ID: t.ID, Label: vertexLabel(t)}
	case *opencypher.Rel:
		return detachEdge(st, t)
	case *Prop:
		return &Property{ID: t.ID(), Key: t.Key, Val: detach(st, t.Val), VP: t.VP}
	case []any:
		return detachAll(st, t)
	case Set:
		return Set(detachAll(st, t))
	case *Map:
		return detachMap(st, t)
	case *Path:
		return &Path{Labels: t.Labels, Objects: detachAll(st, t.Objects)}
	}

	return v
}

func detachMap(st *opencypher.Store, m *Map) *Map {
	out := NewMap()

	for i := range m.Len() {
		k, v := m.Entry(i)
		out.Put(detach(st, k), detach(st, v))
	}

	return out
}

func detachEdge(st *opencypher.Store, r *opencypher.Rel) *Edge {
	e := &Edge{ID: r.ID, Label: r.Type, OutID: r.Start, InID: r.End}
	if n := st.Node(r.Start); n != nil {
		e.OutLabel = vertexLabel(n)
	}

	if n := st.Node(r.End); n != nil {
		e.InLabel = vertexLabel(n)
	}

	return e
}
