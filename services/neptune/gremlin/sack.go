package gremlin

import "slices"

// sackConfig is the traversal-wide sack set by withSack().
type sackConfig struct {
	initial any
	merge   string
	on      bool
}

func regSack(m map[string]stepFn) {
	m["sack"] = stepSack
}

// sackConfigOf reads the withSack() source instructions; a lambda split operator is unsupported.
func sackConfigOf(srcs []Step) (sackConfig, error) {
	var c sackConfig

	for i := range srcs {
		if srcs[i].Name != "withSack" {
			continue
		}

		args := srcs[i].Args
		if len(args) == 0 || len(args) > 2 {
			return c, invalidArg("withSack() takes an initial value and an optional merge Operator")
		}

		c = sackConfig{on: true, initial: normalizeValue(args[0])}

		if len(args) > 1 {
			op, ok := args[1].(Token)
			if !ok || op.Kind != kindOperator {
				return c, unsupported("withSack() merge function must be an Operator; lambdas are not supported")
			}

			c.merge = op.Name
		}
	}

	return c, nil
}

func (x *exec) rootFor(tr *Traversal) ([]*trav, error) {
	c, err := sackConfigOf(tr.Sources)
	if err != nil {
		return nil, err
	}

	x.sack = c
	root := rootTrav()
	root[0].sack = c.initial

	return root, nil
}

func stepSack(x *exec, s *Step, in []*trav) ([]*trav, error) {
	if !x.sack.on {
		return nil, malformed("The provided traverser does not support a sack; use withSack() to enable one")
	}

	if len(s.Args) == 0 {
		return mapStep(func(_ *exec, _ *Step, t *trav) (any, error) { return t.sack, nil })(x, s, in)
	}

	op, ok := s.Args[0].(Token)
	if !ok || op.Kind != kindOperator {
		return nil, invalidArg("sack() takes no arguments or an Operator")
	}

	by := byMods(s)

	return perTrav(func(x *exec, _ *Step, t *trav) ([]*trav, error) {
		v, found, err := x.applyBy(by, 0, t)
		if err != nil {
			return nil, err
		}

		if !found {
			return nil, malformed("The provided traverser does not map to a value")
		}

		ns, err := applyOperator(op.Name, t.sack, normalizeValue(v))
		if err != nil {
			return nil, err
		}

		c := t.keep(t.v)
		c.sack = ns

		return []*trav{c}, nil
	})(x, s, in)
}

// applyOperator evaluates a TinkerPop Operator (a BinaryOperator) on a and b.
func applyOperator(op string, a, b any) (any, error) {
	switch op {
	case "assign":
		return b, nil
	case opAnd, opOr:
		return boolOperator(op, a, b)
	case "addAll":
		return addAll(a, b)
	case opMin, opMax:
		return minMax(op, a, b)
	case "sumLong":
		if isNumber(a) && isNumber(b) {
			return toInt(a) + toInt(b), nil
		}
	}

	if !isNumber(a) || !isNumber(b) {
		return nil, malformed("The Operator %s requires numeric operands", op)
	}

	return arith(op, a, b)
}

func arith(op string, a, b any) (any, error) {
	if isInt(a) && isInt(b) {
		return intArith(op, toInt(a), toInt(b))
	}

	x, y := toFloat(a), toFloat(b)

	switch op {
	case stepSum:
		return x + y, nil
	case opMinus:
		return x - y, nil
	case opMult:
		return x * y, nil
	case opDiv:
		return x / y, nil
	}

	return nil, malformed("Unknown Operator %s", op)
}

func intArith(op string, x, y int64) (any, error) {
	switch op {
	case stepSum:
		return x + y, nil
	case opMinus:
		return x - y, nil
	case opMult:
		return x * y, nil
	case opDiv:
		if y == 0 {
			return nil, malformed("/ by zero")
		}

		return x / y, nil
	}

	return nil, malformed("Unknown Operator %s", op)
}

func boolOperator(op string, a, b any) (any, error) {
	x, xok := a.(bool)
	y, yok := b.(bool)

	if !xok || !yok {
		return nil, malformed("The Operator %s requires boolean operands", op)
	}

	if op == opAnd {
		return x && y, nil
	}

	return x || y, nil
}

func minMax(op string, a, b any) (any, error) {
	c, ok := compareValues(a, b)
	if !ok {
		return nil, malformed("The Operator %s requires comparable operands", op)
	}

	if (op == opMin) == (c <= 0) {
		return a, nil
	}

	return b, nil
}

func addAll(a, b any) (any, error) {
	switch x := a.(type) {
	case []any:
		if y, ok := b.([]any); ok {
			return append(slices.Clone(x), y...), nil
		}
	case Set:
		if y, ok := b.(Set); ok {
			return dedupSet(append(slices.Clone(x), y...)), nil
		}
	case *Map:
		if y, ok := b.(*Map); ok {
			out := NewMap()

			for _, m := range []*Map{x, y} {
				for i := range m.Len() {
					k, v := m.Entry(i)
					out.Put(k, v)
				}
			}

			return out, nil
		}
	}

	return nil, malformed("The Operator addAll requires two collections of the same kind")
}

func dedupSet(vs []any) Set { return Set(dedupValues(vs)) }
