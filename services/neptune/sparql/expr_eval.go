package sparql

import (
	"errors"
	"maps"
	"math"
	"math/big"
	"strings"
	"time"
)

var (
	errType     = errors.New("type error")
	errUnbound  = errors.New("unbound variable")
	errDivZero  = errors.New("division by zero")
	errNoAggCtx = errors.New("aggregate used outside a grouped query")
)

// Binding maps variable names to terms.
type Binding map[string]Term

func (b Binding) clone() Binding {
	out := make(Binding, len(b)+1)
	maps.Copy(out, b)

	return out
}

type env struct {
	b    Binding
	rows []Binding
	gc   Term
	agg  bool
}

func trueTerm() Term { return Typed(lexTrue, XSDBoolean) }

func falseTerm() Term { return Typed(lexFalse, XSDBoolean) }

func boolTerm(v bool) Term {
	if v {
		return trueTerm()
	}

	return falseTerm()
}

func (ev *evaluator) eval(e Expr, en *env) (Term, error) {
	switch v := e.(type) {
	case exprVar:
		t, ok := en.b[v.Name]
		if !ok {
			return Term{}, errUnbound
		}

		return t, nil
	case exprTerm:
		return v.T, nil
	case exprUnary:
		return ev.evalUnary(v, en)
	case exprBinary:
		return ev.evalBinary(v, en)
	case exprIn:
		return ev.evalIn(v, en)
	case exprExists:
		return ev.evalExists(v, en)
	case exprCall:
		return ev.evalCall(v, en)
	default:
		return Term{}, errType
	}
}

func (ev *evaluator) evalUnary(u exprUnary, en *env) (Term, error) {
	t, err := ev.eval(u.E, en)
	if err != nil {
		return Term{}, err
	}

	if u.Op == "!" {
		b, berr := ebv(t)
		if berr != nil {
			return Term{}, berr
		}

		return boolTerm(!b), nil
	}

	n, ok := asNumeric(t)
	if !ok {
		return Term{}, errType
	}

	if u.Op == "+" {
		return t, nil
	}

	return negate(n).term(), nil
}

func negate(n numVal) numVal {
	switch n.k {
	case nInt:
		n.i = -n.i
	case nDec:
		n.r = new(big.Rat).Neg(n.r)
	default:
		n.f = -n.f
	}

	return n
}

func (ev *evaluator) evalBinary(b exprBinary, en *env) (Term, error) {
	if b.Op == "&&" || b.Op == "||" {
		return ev.evalLogical(b, en)
	}

	l, err := ev.eval(b.L, en)
	if err != nil {
		return Term{}, err
	}

	r, err := ev.eval(b.R, en)
	if err != nil {
		return Term{}, err
	}

	switch b.Op {
	case "=", "!=":
		return boolTerm(termEqual(l, r) == (b.Op == "=")), nil
	case "<", ">", "<=", ">=":
		c, cerr := valueCompare(l, r)
		if cerr != nil {
			return Term{}, cerr
		}

		return boolTerm(cmpHolds(b.Op, c)), nil
	default:
		return arith(b.Op, l, r)
	}
}

func cmpHolds(op string, c int) bool {
	switch op {
	case "<":
		return c < 0
	case ">":
		return c > 0
	case "<=":
		return c <= 0
	default:
		return c >= 0
	}
}

func arith(op string, l, r Term) (Term, error) {
	a, ok1 := asNumeric(l)
	b, ok2 := asNumeric(r)

	if !ok1 || !ok2 {
		return Term{}, errType
	}

	n, err := numArith(op, a, b)
	if err != nil {
		return Term{}, err
	}

	return n.term(), nil
}

func (ev *evaluator) evalLogical(b exprBinary, en *env) (Term, error) {
	lt, lerr := ev.eval(b.L, en)
	lv, lerr := ebvOrErr(lt, lerr)
	rt, rerr := ev.eval(b.R, en)
	rv, rerr := ebvOrErr(rt, rerr)

	// three-valued logic: a decisive operand wins over an error on the other side
	decisive := b.Op == "||"

	switch {
	case lerr == nil && lv == decisive, rerr == nil && rv == decisive:
		return boolTerm(decisive), nil
	case lerr != nil || rerr != nil:
		return Term{}, errType
	default:
		return boolTerm(!decisive), nil
	}
}

func ebvOrErr(t Term, err error) (bool, error) {
	if err != nil {
		return false, err
	}

	return ebv(t)
}

func (ev *evaluator) evalIn(in exprIn, en *env) (Term, error) {
	l, err := ev.eval(in.E, en)
	if err != nil {
		return Term{}, err
	}

	sawErr := false

	for _, item := range in.List {
		r, rerr := ev.eval(item, en)
		if rerr != nil {
			sawErr = true

			continue
		}

		if termEqual(l, r) {
			return boolTerm(!in.Not), nil
		}
	}

	if sawErr {
		return Term{}, errType
	}

	return boolTerm(in.Not), nil
}

func (ev *evaluator) evalExists(ex exprExists, en *env) (Term, error) {
	sols, err := ev.evalGroupSeeded(ex.G, en.gc, []Binding{en.b})
	if err != nil {
		return Term{}, err
	}

	return boolTerm((len(sols) > 0) != ex.Not), nil
}

// ebv computes the effective boolean value of t.
func ebv(t Term) (bool, error) {
	if t.Kind != KindLiteral {
		return false, errType
	}

	switch {
	case t.Datatype == XSDBoolean:
		switch t.Value {
		case lexTrue, "1":
			return true, nil
		case lexFalse, "0":
			return false, nil
		}

		return false, errType
	case t.Lang == "" && t.Datatype == "":
		return t.Value != "", nil
	}

	if n, ok := asNumeric(t); ok {
		f := n.float()

		return f != 0 && !math.IsNaN(f), nil
	}

	return false, errType
}

func isStringLit(t Term) bool { return t.Kind == KindLiteral && t.Datatype == "" }

// termEqual implements '=' with Neptune's extension: incomparable literals compare syntactically.
func termEqual(a, b Term) bool {
	if a.Kind != KindLiteral || b.Kind != KindLiteral {
		return a == b
	}

	if c, ok := compareLiterals(a, b); ok {
		return c == 0
	}

	if x, ok := asNumeric(a); ok {
		y, ok2 := asNumeric(b)

		return ok2 && math.IsNaN(x.float()) && math.IsNaN(y.float())
	}

	return a == b
}

// compareLiterals orders two literals of a comparable value space (numeric, boolean or dateTime).
func compareLiterals(a, b Term) (int, bool) {
	if x, ok := asNumeric(a); ok {
		if y, ok2 := asNumeric(b); ok2 {
			return numCompare(x, y)
		}

		return 0, false
	}

	switch {
	case a.Datatype == XSDBoolean && b.Datatype == XSDBoolean:
		x, e1 := ebv(a)
		y, e2 := ebv(b)

		return boolInt(x) - boolInt(y), e1 == nil && e2 == nil
	case a.Datatype == XSDDateTime && b.Datatype == XSDDateTime:
		x, ok1 := parseDateTime(a.Value)
		y, ok2 := parseDateTime(b.Value)

		if !ok1 || !ok2 {
			return 0, false
		}

		return x.Compare(y), true
	default:
		return 0, false
	}
}

// valueCompare orders two terms for <, >, <=, >=; incomparable pairs are a type error.
func valueCompare(a, b Term) (int, error) {
	if isStringLit(a) && isStringLit(b) {
		return strings.Compare(a.Value, b.Value), nil
	}

	if a.Kind == KindLiteral && b.Kind == KindLiteral {
		if c, ok := compareLiterals(a, b); ok {
			return c, nil
		}
	}

	return 0, errType
}

func boolInt(b bool) int {
	if b {
		return 1
	}

	return 0
}

// orderCompare is the total order used by ORDER BY: unbound < blank < IRI < literal.
func orderCompare(a, b Term) int {
	ra, rb := orderRank(a), orderRank(b)
	if ra != rb {
		return ra - rb
	}

	if a.Kind == KindLiteral {
		if c, err := valueCompare(a, b); err == nil {
			return c
		}

		if c := strings.Compare(a.Value, b.Value); c != 0 {
			return c
		}

		if c := strings.Compare(a.DT(), b.DT()); c != 0 {
			return c
		}

		return strings.Compare(a.Lang, b.Lang)
	}

	return strings.Compare(a.Value, b.Value)
}

const (
	rankBlank = iota + 1
	rankIRI
	rankLiteral
)

func orderRank(t Term) int {
	switch t.Kind {
	case KindBlank:
		return rankBlank
	case KindIRI:
		return rankIRI
	case KindLiteral:
		return rankLiteral
	case KindNone:
		return 0
	}

	return 0
}

func (ev *evaluator) now() time.Time { return ev.started }
