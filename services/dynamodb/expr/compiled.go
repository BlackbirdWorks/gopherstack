package expr

import "strings"

// CompiledCondition is a condition bound to one request's attribute maps: placeholder
// values and attribute names are resolved once, then reused across items. Not safe for
// concurrent use.
type CompiledCondition struct {
	eval *Evaluator
	fn   boolFn
}

type boolFn func(item map[string]any) (bool, error)

// CompileCondition binds node to names and values. Evaluation matches Evaluator.Evaluate
// on the same node, including errors for placeholders that are reached but undefined.
func CompileCondition(node Node, names map[string]string, values map[string]any) *CompiledCondition {
	c := &CompiledCondition{eval: &Evaluator{AttrNames: names, AttrValues: values}}
	c.fn = c.eval.compileBool(node)

	return c
}

// Evaluate reports whether item satisfies the condition.
func (c *CompiledCondition) Evaluate(item map[string]any) (bool, error) {
	c.eval.Item = item

	return c.fn(item)
}

type srcKind uint8

const (
	srcNode srcKind = iota
	srcConst
	srcMissing
	srcPath
)

// src is a pre-resolved operand: a constant placeholder value, a missing placeholder,
// a path with attribute names substituted, or any other node evaluated generically.
type src struct {
	raw  any
	node Node
	err  error
	path []PathElement
	str  string
	op   operand
	kind srcKind
}

func (e *Evaluator) compileSrc(n Node) src {
	switch v := n.(type) {
	case *ValuePlaceholder:
		raw, err := e.Evaluate(v)
		if err != nil {
			return src{kind: srcMissing, err: err}
		}

		s := src{kind: srcConst, raw: raw, str: e.toString(raw)}
		s.op.unwrapped = e.unwrapAttributeValue(raw)
		s.op.num, s.op.isNum = e.parseUnwrappedNumeric(s.op.unwrapped)

		return s
	case *PathExpr:
		return src{kind: srcPath, path: e.resolvePathNames(v.Elements)}
	default:
		return src{kind: srcNode, node: n}
	}
}

func (e *Evaluator) resolvePathNames(path []PathElement) []PathElement {
	out := make([]PathElement, len(path))
	for i, el := range path {
		out[i] = el
		if el.Type == ElementKey {
			out[i].Name = e.resolveName(el.Name)
		}
	}

	return out
}

func (s *src) rawValue(e *Evaluator, item map[string]any) (any, error) {
	switch s.kind {
	case srcConst:
		return s.raw, nil
	case srcMissing:
		return nil, s.err
	case srcPath:
		val, _ := e.walkPath(item, s.path)

		return val, nil
	default:
		return e.Evaluate(s.node)
	}
}

func (s *src) operand(e *Evaluator, item map[string]any) (operand, error) {
	switch s.kind {
	case srcConst:
		return s.op, nil
	case srcMissing:
		return operand{}, s.err
	case srcNode, srcPath:
	}

	raw, err := s.rawValue(e, item)
	if err != nil {
		return operand{}, err
	}

	op := operand{unwrapped: e.unwrapAttributeValue(raw)}
	op.num, op.isNum = e.parseUnwrappedNumeric(op.unwrapped)

	return op, nil
}

func (s *src) text(e *Evaluator, item map[string]any) (string, error) {
	if s.kind == srcConst {
		return s.str, nil
	}

	raw, err := s.rawValue(e, item)
	if err != nil {
		return "", err
	}

	return e.toString(raw), nil
}

func (e *Evaluator) compileBool(node Node) boolFn {
	switch n := node.(type) {
	case *LogicalExpr:
		return e.compileLogical(n)
	case *NotExpr:
		inner := e.compileBool(n.Expression)

		return func(item map[string]any) (bool, error) {
			b, err := inner(item)
			if err != nil {
				return false, err
			}

			return !b, nil
		}
	case *ComparisonExpr:
		if n.Operator != TokenPlus && n.Operator != TokenMinus {
			return e.compileComparison(n)
		}
	case *BetweenExpr:
		return e.compileBetween(n)
	case *FunctionExpr:
		if fn := e.compileFunction(n); fn != nil {
			return fn
		}
	}

	return e.compileGeneric(node)
}

func (e *Evaluator) compileGeneric(node Node) boolFn {
	return func(_ map[string]any) (bool, error) {
		v, err := e.Evaluate(node)
		if err != nil {
			return false, err
		}

		b, _ := v.(bool)

		return b, nil
	}
}

func (e *Evaluator) compileLogical(n *LogicalExpr) boolFn {
	left, right, op := e.compileBool(n.Left), e.compileBool(n.Right), n.Operator

	return func(item map[string]any) (bool, error) {
		l, err := left(item)
		if err != nil {
			return false, err
		}

		if op == TokenAND && !l {
			return false, nil
		}

		if op == TokenOR && l {
			return true, nil
		}

		return right(item)
	}
}

func (e *Evaluator) compileComparison(n *ComparisonExpr) boolFn {
	l, r, op := e.compileSrc(n.Left), e.compileSrc(n.Right), n.Operator

	return func(item map[string]any) (bool, error) {
		lo, err := l.operand(e, item)
		if err != nil {
			return false, err
		}

		ro, err := r.operand(e, item)
		if err != nil {
			return false, err
		}

		return e.compareResolved(lo, op, ro), nil
	}
}

func (e *Evaluator) compileBetween(n *BetweenExpr) boolFn {
	v, lo, hi := e.compileSrc(n.Value), e.compileSrc(n.Lower), e.compileSrc(n.Upper)

	return func(item map[string]any) (bool, error) {
		val, err := v.operand(e, item)
		if err != nil {
			return false, err
		}

		lower, err := lo.operand(e, item)
		if err != nil {
			return false, err
		}

		upper, err := hi.operand(e, item)
		if err != nil {
			return false, err
		}

		return e.compareResolved(val, TokenGreaterEqual, lower) &&
			e.compareResolved(val, TokenLessEqual, upper), nil
	}
}

// compileFunction returns nil for functions or argument shapes left to the generic path.
func (e *Evaluator) compileFunction(n *FunctionExpr) boolFn {
	switch n.Name {
	case funcBeginsWith:
		if len(n.Args) != twoArgs {
			return nil
		}

		path, prefix := e.compileSrc(n.Args[0]), e.compileSrc(n.Args[1])

		return func(item map[string]any) (bool, error) {
			s, err := path.text(e, item)
			if err != nil {
				return false, err
			}

			p, err := prefix.text(e, item)
			if err != nil {
				return false, err
			}

			return strings.HasPrefix(s, p), nil
		}
	case funcAttributeExists, funcAttributeNotExists:
		if len(n.Args) != 1 {
			return nil
		}

		p, ok := n.Args[0].(*PathExpr)
		if !ok {
			return nil
		}

		path, want := e.resolvePathNames(p.Elements), n.Name == funcAttributeExists

		return func(item map[string]any) (bool, error) {
			_, exists := e.walkPath(item, path)

			return exists == want, nil
		}
	}

	return nil
}
