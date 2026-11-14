package azurearm

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

const (
	xpathMaxSteps = 2_000_000

	opAnd = "and"
	opOr  = "or"
	opDiv = "div"
	opMod = "mod"

	axisParent       = "parent"
	axisSelf         = "self"
	axisFollowing    = "following"
	axisFollowingSib = "following-sibling"
	axisPrecedingSib = "preceding-sibling"

	axisDescOrSelf = "descendant-or-self"
	axisAncOrSelf  = "ancestor-or-self"
	axisAttribute  = "attribute"
	axisChild      = "child"
	nodeTypeNode   = "node"
)

type xtokKind uint8

const (
	xtNum xtokKind = iota
	xtStr
	xtName
	xtAxis
	xtOp
)

type xtok struct {
	s    string
	n    float64
	kind xtokKind
}

type xpathState struct {
	steps int
}

func (st *xpathState) tick() error {
	st.steps++
	if st.steps > xpathMaxSteps {
		return exprErr("the XPath expression is too expensive to evaluate")
	}

	return nil
}

type xctx struct {
	node *xmlNode
	st   *xpathState
	pos  int
	size int
}

type (
	xval  = any
	xeval func(c *xctx) (xval, error)
)

func scanXName(s string, i int) int {
	for i < len(s) {
		r := rune(s[i])
		if r >= unicode.MaxASCII {
			break
		}

		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '-' && r != '.' {
			break
		}

		i++
	}

	return i
}

func xOperandEnded(toks []xtok) bool {
	if len(toks) == 0 {
		return false
	}

	last := toks[len(toks)-1]

	switch last.kind {
	case xtNum, xtStr, xtName:
		return true
	case xtAxis:
		return false
	case xtOp:
		return last.s == ")" || last.s == "]" || last.s == "." || last.s == ".."
	}

	return false
}

var xpathPunct2 = []string{"//", "..", "::", "!=", "<=", ">="} //nolint:gochecknoglobals // static

func lexXPath(s string) ([]xtok, error) {
	var toks []xtok

	for i := 0; i < len(s); {
		if isXSpace(s[i]) {
			i++

			continue
		}

		t, adv, err := lexXOne(s, i, toks)
		if err != nil {
			return nil, err
		}

		toks = append(toks, t)
		i += adv
	}

	return toks, nil
}

func isXSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

func lexXOne(s string, i int, toks []xtok) (xtok, int, error) {
	c := s[i]

	switch {
	case c >= '0' && c <= '9' || c == '.' && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9':
		return lexXNumber(s, i)
	case c == '"' || c == '\'':
		return lexXString(s, i)
	case c < unicode.MaxASCII && (unicode.IsLetter(rune(c)) || c == '_'):
		t, adv := lexXName(s, i, toks)

		return t, adv, nil
	case c == '$':
		return xtok{}, 0, exprErr("XPath variables are not supported")
	}

	return lexXPunct(s, i, toks)
}

func lexXNumber(s string, i int) (xtok, int, error) {
	j := i
	for j < len(s) && (s[j] >= '0' && s[j] <= '9' || s[j] == '.') {
		j++
	}

	f, err := strconv.ParseFloat(s[i:j], 64)
	if err != nil {
		return xtok{}, 0, exprErr("the XPath number '%s' is not valid", s[i:j])
	}

	return xtok{kind: xtNum, n: f}, j - i, nil
}

func lexXString(s string, i int) (xtok, int, error) {
	end := strings.IndexByte(s[i+1:], s[i])
	if end < 0 {
		return xtok{}, 0, exprErr("the XPath string literal is not terminated")
	}

	return xtok{kind: xtStr, s: s[i+1 : i+1+end]}, end + widthTwo, nil
}

func lexXName(s string, i int, toks []xtok) (xtok, int) {
	j := scanXName(s, i)

	if j+1 < len(s) && s[j] == ':' && s[j+1] != ':' {
		if s[j+1] == '*' {
			j += widthTwo
		} else {
			j = scanXName(s, j+1)
		}
	}

	name := s[i:j]

	if j+1 < len(s) && s[j] == ':' && s[j+1] == ':' {
		return xtok{kind: xtAxis, s: name}, j + widthTwo - i
	}

	if isXWordOp(name) && xOperandEnded(toks) {
		return xtok{kind: xtOp, s: name}, j - i
	}

	return xtok{kind: xtName, s: name}, j - i
}

func lexXPunct(s string, i int, toks []xtok) (xtok, int, error) {
	for _, p := range xpathPunct2 {
		if strings.HasPrefix(s[i:], p) {
			return xtok{kind: xtOp, s: p}, widthTwo, nil
		}
	}

	c := s[i]

	switch c {
	case '*':
		if xOperandEnded(toks) {
			return xtok{kind: xtOp, s: "*"}, 1, nil
		}

		return xtok{kind: xtName, s: "*"}, 1, nil
	case '/', '|', '+', '-', '=', '<', '>', '(', ')', '[', ']', '@', ',', '.':
		return xtok{kind: xtOp, s: string(c)}, 1, nil
	}

	return xtok{}, 0, exprErr("the XPath expression contains the unexpected character '%c'", c)
}

type xparser struct {
	toks []xtok
	i    int
}

func (p *xparser) peek() (xtok, bool) {
	if p.i < len(p.toks) {
		return p.toks[p.i], true
	}

	return xtok{}, false
}

func (p *xparser) isOp(s string) bool {
	t, ok := p.peek()

	return ok && t.kind == xtOp && t.s == s
}

func (p *xparser) accept(s string) bool {
	if p.isOp(s) {
		p.i++

		return true
	}

	return false
}

func (p *xparser) expect(s string) error {
	if !p.accept(s) {
		return exprErr("the XPath expression is not valid: expected '%s'", s)
	}

	return nil
}

func compileXPath(s string) (xeval, error) {
	toks, err := lexXPath(s)
	if err != nil {
		return nil, err
	}

	p := &xparser{toks: toks}

	e, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.i != len(toks) {
		return nil, exprErr("the XPath expression is not valid: unexpected '%s'", toks[p.i].s)
	}

	return e, nil
}

type xbinop func(c *xctx, a, b xval) (xval, error)

func (p *xparser) binary(next func() (xeval, error), ops map[string]xbinop) (xeval, error) {
	left, err := next()
	if err != nil {
		return nil, err
	}

	for {
		t, ok := p.peek()
		if !ok || t.kind != xtOp {
			return left, nil
		}

		op, has := ops[t.s]
		if !has {
			return left, nil
		}

		p.i++

		right, rerr := next()
		if rerr != nil {
			return nil, rerr
		}

		left = joinXBinary(t.s, op, left, right)
	}
}

func joinXBinary(name string, op xbinop, l, r xeval) xeval {
	return func(c *xctx) (xval, error) {
		a, err := l(c)
		if err != nil {
			return nil, err
		}

		if name == "or" && xBool(a) {
			return true, nil
		}

		if name == "and" && !xBool(a) {
			return false, nil
		}

		b, err := r(c)
		if err != nil {
			return nil, err
		}

		return op(c, a, b)
	}
}

func (p *xparser) parseOr() (xeval, error) {
	return p.binary(p.parseAnd, map[string]xbinop{
		opOr: func(_ *xctx, _, b xval) (xval, error) { return xBool(b), nil },
	})
}

func (p *xparser) parseAnd() (xeval, error) {
	return p.binary(p.parseEq, map[string]xbinop{
		opAnd: func(_ *xctx, _, b xval) (xval, error) { return xBool(b), nil },
	})
}

func cmpOp(op string) xbinop {
	return func(_ *xctx, a, b xval) (xval, error) { return xCompare(op, a, b), nil }
}

func (p *xparser) parseEq() (xeval, error) {
	return p.binary(p.parseRel, map[string]xbinop{"=": cmpOp("="), "!=": cmpOp("!=")})
}

func (p *xparser) parseRel() (xeval, error) {
	return p.binary(p.parseAdd, map[string]xbinop{
		"<": cmpOp("<"), "<=": cmpOp("<="), ">": cmpOp(">"), ">=": cmpOp(">="),
	})
}

func arith(f func(x, y float64) float64) xbinop {
	return func(_ *xctx, a, b xval) (xval, error) { return f(xNum(a), xNum(b)), nil }
}

func (p *xparser) parseAdd() (xeval, error) {
	return p.binary(p.parseMul, map[string]xbinop{
		"+": arith(func(x, y float64) float64 { return x + y }),
		"-": arith(func(x, y float64) float64 { return x - y }),
	})
}

func (p *xparser) parseMul() (xeval, error) {
	return p.binary(p.parseUnary, map[string]xbinop{
		"*":   arith(func(x, y float64) float64 { return x * y }),
		opDiv: arith(func(x, y float64) float64 { return x / y }),
		opMod: arith(math.Mod),
	})
}

func (p *xparser) parseUnary() (xeval, error) {
	if p.accept("-") {
		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return func(c *xctx) (xval, error) {
			v, verr := inner(c)
			if verr != nil {
				return nil, verr
			}

			return -xNum(v), nil
		}, nil
	}

	return p.parseUnion()
}

func (p *xparser) parseUnion() (xeval, error) {
	left, err := p.parsePath()
	if err != nil {
		return nil, err
	}

	for p.accept("|") {
		right, rerr := p.parsePath()
		if rerr != nil {
			return nil, rerr
		}

		l, r := left, right
		left = func(c *xctx) (xval, error) {
			a, aerr := l(c)
			if aerr != nil {
				return nil, aerr
			}

			b, berr := r(c)
			if berr != nil {
				return nil, berr
			}

			an, aok := a.([]*xmlNode)
			bn, bok := b.([]*xmlNode)

			if !aok || !bok {
				return nil, exprErr("the XPath union operator needs node sets")
			}

			return xSortUnique(append(append([]*xmlNode{}, an...), bn...)), nil
		}
	}

	return left, nil
}

var xpathNodeTypes = map[string]bool{ //nolint:gochecknoglobals // static
	nodeTypeNode: true, "text": true, "comment": true, "processing-instruction": true,
}

func (p *xparser) startsPrimary() bool {
	t, ok := p.peek()
	if !ok {
		return false
	}

	switch t.kind {
	case xtNum, xtStr:
		return true
	case xtOp:
		return t.s == "("
	case xtName:
		next := p.i + 1

		return next < len(p.toks) && p.toks[next].kind == xtOp && p.toks[next].s == "(" && !xpathNodeTypes[t.s]
	case xtAxis:
		return false
	}

	return false
}

func (p *xparser) parsePath() (xeval, error) {
	if p.startsPrimary() {
		return p.parseFilterPath()
	}

	var absolute, deep bool

	switch {
	case p.accept("/"):
		absolute = true
	case p.accept("//"):
		absolute, deep = true, true
	}

	var steps []xstep

	if deep {
		steps = append(steps, xstep{axis: axisDescOrSelf, test: xtest{kind: xtNode}})
	}

	if absolute && !p.startsStep() {
		return func(c *xctx) (xval, error) { return []*xmlNode{c.node.root()}, nil }, nil
	}

	more, err := p.parseSteps()
	if err != nil {
		return nil, err
	}

	steps = append(steps, more...)

	return pathEval(nil, steps, absolute), nil
}

func (p *xparser) startsStep() bool {
	t, ok := p.peek()
	if !ok {
		return false
	}

	switch t.kind {
	case xtName, xtAxis:
		return true
	case xtOp:
		return t.s == "@" || t.s == "." || t.s == ".."
	case xtNum, xtStr:
		return false
	}

	return false
}

func (p *xparser) parseSteps() ([]xstep, error) {
	var steps []xstep

	for {
		st, err := p.parseStep()
		if err != nil {
			return nil, err
		}

		steps = append(steps, st)

		switch {
		case p.accept("/"):
		case p.accept("//"):
			steps = append(steps, xstep{axis: axisDescOrSelf, test: xtest{kind: xtNode}})
		default:
			return steps, nil
		}
	}
}

func (p *xparser) parseFilterPath() (xeval, error) {
	prim, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	var preds []xeval

	for p.isOp("[") {
		pr, perr := p.parsePredicate()
		if perr != nil {
			return nil, perr
		}

		preds = append(preds, pr)
	}

	var steps []xstep

	switch {
	case p.accept("/"):
	case p.accept("//"):
		steps = append(steps, xstep{axis: axisDescOrSelf, test: xtest{kind: xtNode}})
	default:
		if len(preds) == 0 {
			return prim, nil
		}

		return filterEval(prim, preds), nil
	}

	more, err := p.parseSteps()
	if err != nil {
		return nil, err
	}

	return pathEval(filterEval(prim, preds), append(steps, more...), false), nil
}

func filterEval(prim xeval, preds []xeval) xeval {
	return func(c *xctx) (xval, error) {
		v, err := prim(c)
		if err != nil {
			return nil, err
		}

		ns, ok := v.([]*xmlNode)
		if !ok {
			return nil, exprErr("an XPath predicate can only filter a node set")
		}

		return applyPredicates(c, ns, preds)
	}
}

func (p *xparser) parsePrimary() (xeval, error) {
	t, _ := p.peek()
	p.i++

	switch t.kind {
	case xtNum:
		n := t.n

		return func(*xctx) (xval, error) { return n, nil }, nil
	case xtStr:
		s := t.s

		return func(*xctx) (xval, error) { return s, nil }, nil
	case xtOp:
		e, err := p.parseOr()
		if err != nil {
			return nil, err
		}

		if err = p.expect(")"); err != nil {
			return nil, err
		}

		return e, nil
	case xtName:
		return p.parseCall(t.s)
	case xtAxis:
	}

	return nil, exprErr("the XPath expression is not valid")
}

func (p *xparser) parseCall(name string) (xeval, error) {
	fn, ok := xpathFuncs[name]
	if !ok {
		return nil, exprErr("the XPath function '%s' is not supported", name)
	}

	p.i++ // "("

	var args []xeval

	if !p.isOp(")") {
		for {
			a, err := p.parseOr()
			if err != nil {
				return nil, err
			}

			args = append(args, a)

			if !p.accept(",") {
				break
			}
		}
	}

	if err := p.expect(")"); err != nil {
		return nil, err
	}

	if len(args) < fn.min || fn.max >= 0 && len(args) > fn.max {
		return nil, exprErr("the XPath function '%s' has an invalid number of arguments: %d", name, len(args))
	}

	return func(c *xctx) (xval, error) {
		vals := make([]xval, len(args))

		for i, a := range args {
			v, err := a(c)
			if err != nil {
				return nil, err
			}

			vals[i] = v
		}

		return fn.f(c, vals)
	}, nil
}

func (p *xparser) parsePredicate() (xeval, error) {
	if err := p.expect("["); err != nil {
		return nil, err
	}

	e, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if err = p.expect("]"); err != nil {
		return nil, err
	}

	return e, nil
}

type xtestKind uint8

const (
	xtName0 xtestKind = iota
	xtAny
	xtNode
	xtTextT
	xtCommentT
	xtPI
)

type xtest struct {
	name string
	kind xtestKind
}

type xstep struct {
	axis  string
	test  xtest
	preds []xeval
}

var xpathAxes = map[string]bool{ //nolint:gochecknoglobals // static
	axisChild: true, "descendant": true, axisDescOrSelf: true, axisParent: true, "ancestor": true,
	axisAncOrSelf: true, axisFollowingSib: true, axisPrecedingSib: true, axisFollowing: true,
	"preceding": true, axisSelf: true, axisAttribute: true,
}

func (p *xparser) parseStep() (xstep, error) {
	t, ok := p.peek()
	if !ok {
		return xstep{}, exprErr("the XPath expression ended unexpectedly")
	}

	var st xstep

	switch {
	case t.kind == xtOp && t.s == ".":
		p.i++

		return xstep{axis: axisSelf, test: xtest{kind: xtNode}}, nil
	case t.kind == xtOp && t.s == "..":
		p.i++

		return xstep{axis: axisParent, test: xtest{kind: xtNode}}, nil
	case t.kind == xtOp && t.s == "@":
		p.i++

		st.axis = axisAttribute
	case t.kind == xtAxis:
		if !xpathAxes[t.s] {
			return xstep{}, exprErr("the XPath axis '%s' is not supported", t.s)
		}

		p.i++

		st.axis = t.s
	default:
		st.axis = axisChild
	}

	test, err := p.parseNodeTest()
	if err != nil {
		return xstep{}, err
	}

	st.test = test

	for p.isOp("[") {
		pr, perr := p.parsePredicate()
		if perr != nil {
			return xstep{}, perr
		}

		st.preds = append(st.preds, pr)
	}

	return st, nil
}

func (p *xparser) parseNodeTest() (xtest, error) {
	t, ok := p.peek()
	if !ok || t.kind != xtName {
		return xtest{}, exprErr("the XPath expression is not valid: a node test was expected")
	}

	p.i++

	if next, has := p.peek(); has && next.kind == xtOp && next.s == "(" && xpathNodeTypes[t.s] {
		p.i++

		if nt, lit := p.peek(); lit && nt.kind == xtStr {
			p.i++
		}

		if err := p.expect(")"); err != nil {
			return xtest{}, err
		}

		return xtest{kind: map[string]xtestKind{
			nodeTypeNode: xtNode, "text": xtTextT, "comment": xtCommentT, "processing-instruction": xtPI,
		}[t.s]}, nil
	}

	if t.s == "*" {
		return xtest{kind: xtAny}, nil
	}

	if strings.Contains(t.s, ":") {
		return xtest{}, exprErr(
			"namespace prefixes in XPath name tests are not supported; use local-name() and namespace-uri()",
		)
	}

	return xtest{kind: xtName0, name: t.s}, nil
}

func (t xtest) matches(n *xmlNode, axis string) bool {
	principal := xkElem
	if axis == axisAttribute {
		principal = xkAttr
	}

	switch t.kind {
	case xtNode:
		return true
	case xtTextT:
		return n.kind == xkText
	case xtCommentT:
		return n.kind == xkComment
	case xtPI:
		return false
	case xtAny:
		return n.kind == principal
	case xtName0:
		return n.kind == principal && n.local == t.name && n.space == ""
	}

	return false
}

func pathEval(start xeval, steps []xstep, absolute bool) xeval {
	return func(c *xctx) (xval, error) {
		var cur []*xmlNode

		switch {
		case start != nil:
			v, err := start(c)
			if err != nil {
				return nil, err
			}

			ns, ok := v.([]*xmlNode)
			if !ok {
				return nil, exprErr("an XPath path step needs a node set")
			}

			cur = ns
		case absolute:
			cur = []*xmlNode{c.node.root()}
		default:
			cur = []*xmlNode{c.node}
		}

		for _, st := range steps {
			next, err := evalStep(c, st, cur)
			if err != nil {
				return nil, err
			}

			cur = next
		}

		return cur, nil
	}
}

func evalStep(c *xctx, st xstep, in []*xmlNode) ([]*xmlNode, error) {
	var out []*xmlNode

	for _, n := range in {
		cands, err := axisNodes(c.st, st.axis, n)
		if err != nil {
			return nil, err
		}

		var matched []*xmlNode

		for _, m := range cands {
			if st.test.matches(m, st.axis) {
				matched = append(matched, m)
			}
		}

		matched, err = applyPredicates(&xctx{node: n, st: c.st}, matched, st.preds)
		if err != nil {
			return nil, err
		}

		out = append(out, matched...)
	}

	return xSortUnique(out), nil
}

func applyPredicates(c *xctx, ns []*xmlNode, preds []xeval) ([]*xmlNode, error) {
	for _, pr := range preds {
		var keep []*xmlNode

		for i, n := range ns {
			v, err := pr(&xctx{node: n, st: c.st, pos: i + 1, size: len(ns)})
			if err != nil {
				return nil, err
			}

			if f, isNum := v.(float64); isNum {
				if f == float64(i+1) {
					keep = append(keep, n)
				}
			} else if xBool(v) {
				keep = append(keep, n)
			}
		}

		ns = keep
	}

	return ns, nil
}

func xSortUnique(ns []*xmlNode) []*xmlNode {
	sort.SliceStable(ns, func(i, j int) bool { return ns[i].order < ns[j].order })

	out := ns[:0]

	for i, n := range ns {
		if i == 0 || n != ns[i-1] {
			out = append(out, n)
		}
	}

	return out
}

func axisNodes(st *xpathState, axis string, n *xmlNode) ([]*xmlNode, error) {
	if err := st.tick(); err != nil {
		return nil, err
	}

	switch axis {
	case axisChild:
		return n.children, nil
	case axisAttribute:
		return n.attrs, nil
	case axisSelf:
		return []*xmlNode{n}, nil
	case axisParent:
		if n.parent == nil {
			return nil, nil
		}

		return []*xmlNode{n.parent}, nil
	case "descendant", axisDescOrSelf:
		return descendants(st, n, axis == axisDescOrSelf)
	case "ancestor", axisAncOrSelf:
		return ancestors(n, axis == axisAncOrSelf), nil
	case axisFollowingSib, axisPrecedingSib:
		return siblings(n, axis == axisFollowingSib), nil
	case axisFollowing, "preceding":
		return docOrderAxis(st, n, axis == axisFollowing)
	}

	return nil, exprErr("the XPath axis '%s' is not supported", axis)
}

func descendants(st *xpathState, n *xmlNode, self bool) ([]*xmlNode, error) {
	var out []*xmlNode

	if self {
		out = append(out, n)
	}

	var walk func(x *xmlNode) error

	walk = func(x *xmlNode) error {
		for _, c := range x.children {
			if err := st.tick(); err != nil {
				return err
			}

			out = append(out, c)

			if err := walk(c); err != nil {
				return err
			}
		}

		return nil
	}

	return out, walk(n)
}

func ancestors(n *xmlNode, self bool) []*xmlNode {
	var out []*xmlNode

	if self {
		out = append(out, n)
	}

	for p := n.parent; p != nil; p = p.parent {
		out = append(out, p)
	}

	return out
}

func siblings(n *xmlNode, following bool) []*xmlNode {
	if n.parent == nil || n.kind == xkAttr {
		return nil
	}

	sibs := n.parent.children
	idx := 0

	for i, s := range sibs {
		if s == n {
			idx = i
		}
	}

	if following {
		return sibs[idx+1:]
	}

	out := make([]*xmlNode, 0, idx)
	for i := idx - 1; i >= 0; i-- {
		out = append(out, sibs[i])
	}

	return out
}

func docOrderAxis(st *xpathState, n *xmlNode, following bool) ([]*xmlNode, error) {
	all := n.root().all

	var out []*xmlNode

	if following {
		for _, m := range all[n.end+1:] {
			if m.kind != xkAttr {
				out = append(out, m)
			}
		}

		return out, st.tick()
	}

	anc := map[*xmlNode]bool{}
	for _, a := range ancestors(n, false) {
		anc[a] = true
	}

	for i := n.order - 1; i >= 0; i-- {
		if m := all[i]; m.kind != xkAttr && !anc[m] {
			out = append(out, m)
		}
	}

	return out, st.tick()
}

func xBool(v xval) bool {
	switch t := v.(type) {
	case bool:
		return t
	case float64:
		return t != 0 && !math.IsNaN(t)
	case string:
		return t != ""
	case []*xmlNode:
		return len(t) > 0
	}

	return false
}

func xStr(v xval) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case float64:
		return xFormatNum(t)
	case []*xmlNode:
		if len(t) == 0 {
			return ""
		}

		return t[0].stringValue()
	}

	return ""
}

func xFormatNum(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}

	return strconv.FormatFloat(f, 'f', -1, 64)
}

func xNum(v xval) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case bool:
		if t {
			return 1
		}

		return 0
	}

	f, err := strconv.ParseFloat(strings.TrimSpace(xStr(v)), 64)
	if err != nil {
		return math.NaN()
	}

	return f
}

func xCompare(op string, a, b xval) bool {
	an, aSet := a.([]*xmlNode)
	bn, bSet := b.([]*xmlNode)

	switch {
	case aSet && bSet:
		for _, x := range an {
			for _, y := range bn {
				if xCompareScalar(op, x.stringValue(), y.stringValue()) {
					return true
				}
			}
		}

		return false
	case aSet:
		return xCompareSet(op, an, b, false)
	case bSet:
		return xCompareSet(op, bn, a, true)
	}

	return xCompareScalar(op, a, b)
}

func xCompareSet(op string, ns []*xmlNode, other xval, swapped bool) bool {
	for _, n := range ns {
		var left xval = n.stringValue()
		if _, isBool := other.(bool); isBool {
			left = true
		}

		l, r := left, other
		if swapped {
			l, r = other, left
		}

		if xCompareScalar(op, l, r) {
			return true
		}
	}

	if _, isBool := other.(bool); isBool && len(ns) == 0 {
		if swapped {
			return xCompareScalar(op, other, false)
		}

		return xCompareScalar(op, false, other)
	}

	return false
}

func xCompareScalar(op string, a, b xval) bool {
	if op == "=" || op == "!=" {
		eq := xEqual(a, b)

		return eq == (op == "=")
	}

	x, y := xNum(a), xNum(b)

	switch op {
	case "<":
		return x < y
	case "<=":
		return x <= y
	case ">":
		return x > y
	case ">=":
		return x >= y
	}

	return false
}

func xEqual(a, b xval) bool {
	_, aBool := a.(bool)
	_, bBool := b.(bool)

	if aBool || bBool {
		return xBool(a) == xBool(b)
	}

	_, aNum := a.(float64)
	_, bNum := b.(float64)

	if aNum || bNum {
		return xNum(a) == xNum(b)
	}

	return xStr(a) == xStr(b)
}

// evalXPath runs expr against doc and returns a node set, string, number or boolean.
func evalXPath(doc *xmlNode, expr string) (xval, error) {
	e, err := compileXPath(expr)
	if err != nil {
		return nil, err
	}

	return e(&xctx{node: doc, st: &xpathState{}, pos: 1, size: 1})
}

// logicXPath implements xpath(): one match returns it alone, several return an array.
func logicXPath(a []any) (any, error) {
	doc, err := logicXMLDoc(a[0])
	if err != nil {
		return nil, err
	}

	v, err := evalXPath(doc, logicString(a[1]))
	if err != nil {
		return nil, err
	}

	switch t := v.(type) {
	case []*xmlNode:
		return xpathNodes(t), nil
	case float64:
		if t == math.Trunc(t) && math.Abs(t) < 1<<53 {
			return int64(t), nil
		}

		return t, nil
	}

	return v, nil
}

func xpathNodeValue(n *xmlNode) any {
	switch n.kind {
	case xkText, xkAttr:
		return n.value
	case xkDoc, xkElem, xkComment:
	}

	return n.serialize()
}

func xpathNodes(ns []*xmlNode) any {
	if len(ns) == 1 {
		return xpathNodeValue(ns[0])
	}

	out := make([]any, len(ns))
	for i, n := range ns {
		out[i] = xpathNodeValue(n)
	}

	return out
}

func isXWordOp(name string) bool {
	switch name {
	case opAnd, opOr, opDiv, opMod:
		return true
	}

	return false
}
