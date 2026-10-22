package gremlin

import (
	"context"
	"errors"
	"slices"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

const (
	maxTraversers = 500_000
	maxLoops      = 100
)

type pathNode struct {
	parent *pathNode
	obj    any
	labels []string
}

// trav is one traverser: the current object, its path and its loop count.
type trav struct {
	v     any
	path  *pathNode
	loops int
}

func (t *trav) next(v any) *trav {
	return &trav{v: v, path: &pathNode{parent: t.path, obj: v}, loops: t.loops}
}

func (t *trav) keep(v any) *trav {
	return &trav{v: v, path: t.path, loops: t.loops}
}

func (t *trav) labeled(labels []string) *trav {
	if t.path == nil {
		return t
	}

	merged := append(append([]string(nil), t.path.labels...), labels...)

	return &trav{
		v: t.v, loops: t.loops,
		path: &pathNode{parent: t.path.parent, obj: t.path.obj, labels: merged},
	}
}

func (t *trav) lookup(label string) (any, bool) {
	for n := t.path; n != nil; n = n.parent {
		if slices.Contains(n.labels, label) {
			return n.obj, true
		}
	}

	return nil, false
}

func (t *trav) lookupAll(label string) []any {
	var rev []any

	for n := t.path; n != nil; n = n.parent {
		if slices.Contains(n.labels, label) {
			rev = append(rev, n.obj)
		}
	}

	out := make([]any, len(rev))
	for i, v := range rev {
		out[len(rev)-1-i] = v
	}

	return out
}

func (t *trav) toPath() *Path {
	var rev []*pathNode

	for n := t.path; n != nil; n = n.parent {
		rev = append(rev, n)
	}

	p := &Path{}

	for _, r := range slices.Backward(rev) {
		p.Objects = append(p.Objects, r.obj)
		p.Labels = append(p.Labels, r.labels)
	}

	return p
}

type stepFn func(x *exec, s *Step, in []*trav) ([]*trav, error)

type exec struct {
	ctx   context.Context //nolint:containedctx // lives for one query
	st    *opencypher.Store
	steps map[string]stepFn
	side  map[string]any
}

func newExec(ctx context.Context, st *opencypher.Store, steps map[string]stepFn) *exec {
	return &exec{ctx: ctx, st: st, steps: steps, side: map[string]any{}}
}

func (x *exec) run(tr *Traversal, in []*trav) ([]*trav, error) {
	for i := range tr.Steps {
		s := &tr.Steps[i]

		if err := x.ctx.Err(); err != nil {
			return nil, ctxError(err)
		}

		fn, ok := x.steps[s.Name]
		if !ok {
			return nil, malformed("No signature of method: org.apache.tinkerpop.gremlin.process.traversal.dsl."+
				"graph.GraphTraversal.%s() is applicable for argument types: values: %v", s.Name, s.Args)
		}

		out, err := fn(x, s, in)
		if err != nil {
			return nil, err
		}

		if len(out) > maxTraversers {
			return nil, constraint("Traverser limit of %d exceeded", maxTraversers)
		}

		in = out
	}

	return in, nil
}

func ctxError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return timeLimit("A timeout occurred during traversal evaluation")
	}

	return &Error{Code: CodeCancelled, Status: StatusServerError, Detail: "The query was cancelled"}
}

func (x *exec) sub(tr *Traversal, t *trav) ([]*trav, error) {
	return x.run(tr, []*trav{t})
}

func (x *exec) exists(tr *Traversal, t *trav) (bool, error) {
	out, err := x.sub(tr, t)

	return len(out) > 0, err
}

func (x *exec) firstValue(tr *Traversal, t *trav) (any, bool, error) {
	out, err := x.sub(tr, t)
	if err != nil || len(out) == 0 {
		return nil, false, err
	}

	return out[0].v, true, nil
}

func values(ts []*trav) []any {
	out := make([]any, len(ts))
	for i, t := range ts {
		out[i] = t.v
	}

	return out
}

func rootTrav() []*trav { return []*trav{{}} }

func argTraversal(s *Step, i int) (*Traversal, error) {
	if i < len(s.Args) {
		if tr, ok := s.Args[i].(*Traversal); ok {
			return tr, nil
		}
	}

	return nil, invalidArg("%s() expects a traversal argument", s.Name)
}

func argInt(s *Step, i int) (int, error) {
	if i < len(s.Args) && isInt(s.Args[i]) {
		return int(toInt(s.Args[i])), nil
	}

	return 0, invalidArg("%s() expects an integer argument", s.Name)
}

func stringArgs(args []any) []string {
	var out []string

	for _, a := range args {
		switch t := a.(type) {
		case string:
			out = append(out, t)
		case []any:
			out = append(out, stringArgs(t)...)
		}
	}

	return out
}
