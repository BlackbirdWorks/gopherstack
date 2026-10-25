package wfinterp

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	mainRoutine    = "main"
	maxSwitchConds = 50
	minBranches    = 2
	maxBranches    = 10
	nextEnd        = "end"
	nextBreak      = "break"
	keySteps       = "steps"
	keyFor         = "for"
	keyCondition   = "condition"
	keyAssign      = "assign"
	keyCall        = "call"
	keyTry         = "try"
	pairWidth      = 2
	nextContinue   = "continue"
)

// Program is a validated workflow.
type Program struct {
	subs map[string]*routine
}

type routine struct {
	body   *block
	name   string
	params []param
}

type param struct {
	def    any
	name   string
	hasDef bool
}

type block struct {
	index map[string]int
	steps []*step
}

type step struct {
	raise     any
	ret       any
	call      *callSpec
	sw        []*cond
	loop      *forSpec
	par       *parSpec
	try       *trySpec
	nested    *block
	name      string
	next      string
	assigns   []assign
	line      int
	hasRaise  bool
	hasRet    bool
	hasSwitch bool
}

type assign struct {
	val    any
	target *lvalue
}

type accessor struct {
	idx  node
	name string
}

type lvalue struct {
	root string
	path []accessor
}

type callSpec struct {
	args    map[string]any
	result  *lvalue
	fn      string
	hasArgs bool
}

type cond struct {
	test any
	body *step
}

type forSpec struct {
	in       any
	rng      any
	body     *block
	value    string
	index    string
	hasIn    bool
	hasRange bool
}

type parSpec struct {
	limit    any
	loop     *forSpec
	shared   []string
	branches []*branch
	hasLimit bool
}

type branch struct {
	body *block
	name string
}

type trySpec struct {
	retry     any
	body      *block
	except    *block
	exceptAs  string
	hasRetry  bool
	hasExcept bool
}

type compiler struct {
	eng     *Engine
	routine string
	step    string
}

func (c *compiler) fail(n *yaml.Node, format string, a ...any) error {
	line := 0
	if n != nil {
		line = n.Line
	}

	return &SyntaxError{Routine: c.routine, Step: c.step, Line: line, Msg: fmt.Sprintf(format, a...)}
}

// Compile parses and validates workflow source (YAML or JSON).
func (e *Engine) Compile(src string) (*Program, error) {
	if strings.TrimSpace(src) == "" {
		return nil, &SyntaxError{Msg: "the workflow source is empty"}
	}

	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(src), &doc); err != nil {
		return nil, &SyntaxError{Msg: "invalid YAML or JSON: " + strings.TrimPrefix(err.Error(), "yaml: ")}
	}

	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return nil, &SyntaxError{Msg: "the workflow source is empty"}
	}

	c := &compiler{eng: e}
	p := &Program{subs: map[string]*routine{}}

	if err := c.program(p, doc.Content[0]); err != nil {
		return nil, err
	}

	if _, ok := p.subs[mainRoutine]; !ok {
		return nil, &SyntaxError{Msg: "the main subworkflow is missing"}
	}

	for _, r := range p.subs {
		c.routine, c.step = r.name, ""

		if err := c.validateRoutine(p, r); err != nil {
			return nil, err
		}
	}

	return p, nil
}

func resolve(n *yaml.Node) *yaml.Node {
	for n != nil && n.Kind == yaml.AliasNode {
		n = n.Alias
	}

	return n
}

func (c *compiler) program(p *Program, root *yaml.Node) error {
	root = resolve(root)

	switch root.Kind {
	case yaml.SequenceNode:
		r, err := c.routineFrom(mainRoutine, root)
		if err != nil {
			return err
		}

		p.subs[mainRoutine] = r
	case yaml.MappingNode:
		for i := 0; i+1 < len(root.Content); i += 2 {
			name := root.Content[i].Value
			if _, dup := p.subs[name]; dup {
				return c.fail(root.Content[i], "subworkflow %q is defined twice", name)
			}

			r, err := c.routineFrom(name, root.Content[i+1])
			if err != nil {
				return err
			}

			p.subs[name] = r
		}
	default:
		return &SyntaxError{Line: root.Line, Msg: "a workflow must be a map of subworkflows or a list of steps"}
	}

	return nil
}

func (c *compiler) routineFrom(name string, n *yaml.Node) (*routine, error) {
	c.routine, c.step = name, ""
	n = resolve(n)
	r := &routine{name: name}

	if n.Kind == yaml.SequenceNode {
		b, err := c.block(n)
		r.body = b

		return r, err
	}

	if n.Kind != yaml.MappingNode {
		return nil, c.fail(n, "a subworkflow must be a map with steps or a list of steps")
	}

	for i := 0; i+1 < len(n.Content); i += 2 {
		key, val := n.Content[i], resolve(n.Content[i+1])

		switch key.Value {
		case "params":
			ps, err := c.params(val)
			if err != nil {
				return nil, err
			}

			r.params = ps
		case keySteps:
			b, err := c.block(val)
			if err != nil {
				return nil, err
			}

			r.body = b
		default:
			return nil, c.fail(key, "unknown field %q in a subworkflow", key.Value)
		}
	}

	if r.body == nil {
		return nil, c.fail(n, "subworkflow %q has no steps", name)
	}

	return r, nil
}

func (c *compiler) params(n *yaml.Node) ([]param, error) {
	if n.Kind != yaml.SequenceNode {
		return nil, c.fail(n, "params must be a list")
	}

	var out []param

	for _, item := range n.Content {
		item = resolve(item)

		switch {
		case item.Kind == yaml.ScalarNode:
			out = append(out, param{name: item.Value})
		case item.Kind == yaml.MappingNode && len(item.Content) == 2:
			def, err := c.value(item.Content[1])
			if err != nil {
				return nil, err
			}

			out = append(out, param{name: item.Content[0].Value, def: def, hasDef: true})
		default:
			return nil, c.fail(item, "a parameter is a name or a single name: default pair")
		}
	}

	return out, nil
}

func (c *compiler) block(n *yaml.Node) (*block, error) {
	n = resolve(n)
	if n.Kind != yaml.SequenceNode {
		return nil, c.fail(n, "steps must be a list")
	}

	b := &block{index: map[string]int{}}

	for _, item := range n.Content {
		item = resolve(item)
		if item.Kind != yaml.MappingNode || len(item.Content) != 2 {
			return nil, c.fail(item, "each step must be a map with a single step name")
		}

		name := item.Content[0].Value
		if _, dup := b.index[name]; dup {
			return nil, c.fail(item.Content[0], "duplicate step name %q", name)
		}

		c.step = name
		s := &step{name: name, line: item.Content[0].Line}

		if err := c.body(item.Content[1], s, false); err != nil {
			return nil, err
		}

		b.index[name] = len(b.steps)
		b.steps = append(b.steps, s)
	}

	return b, nil
}

func isPrimaryKey(k string) bool {
	switch k {
	case keyAssign, keyCall, "switch", keyFor, "parallel", keyTry, "raise", "return", keySteps:
		return true
	default:
		return false
	}
}

func (c *compiler) body(n *yaml.Node, s *step, isCond bool) error {
	n = resolve(n)
	if n.Kind != yaml.MappingNode {
		return c.fail(n, "a step must be a map")
	}

	primary := 0
	seen := map[string]*yaml.Node{}

	for i := 0; i+1 < len(n.Content); i += 2 {
		key := n.Content[i].Value
		if _, dup := seen[key]; dup {
			return c.fail(n.Content[i], "duplicate field %q", key)
		}

		seen[key] = n.Content[i+1]

		if isPrimaryKey(key) {
			primary++
		}
	}

	if primary > 1 {
		return c.fail(n, "a step can have only one of assign, call, switch, for, parallel, try, raise, return or steps")
	}

	for key, val := range seen {
		if err := c.field(s, key, val, seen, isCond); err != nil {
			return err
		}
	}

	if primary == 0 && s.next == "" && !isCond {
		return c.fail(n, "a step needs one of assign, call, switch, for, parallel, try, raise, return, steps or next")
	}

	return nil
}

func (c *compiler) field(s *step, key string, val *yaml.Node, all map[string]*yaml.Node, isCond bool) error {
	switch key {
	case keyAssign:
		return c.assignField(s, val)
	case keyCall:
		return c.callField(s, val, all)
	case "args", "result":
		return requireSibling(c, key, keyCall, val, all)
	case "switch":
		return c.switchField(s, val)
	case keyFor:
		l, err := c.forField(val)
		s.loop = l

		return err
	case "parallel":
		p, err := c.parallelField(val)
		s.par = p

		return err
	case keyTry:
		return c.tryField(s, val, all)
	case "retry", "except":
		return requireSibling(c, key, keyTry, val, all)
	default:
		return c.plainField(s, key, val, isCond)
	}
}

func requireSibling(c *compiler, key, sibling string, val *yaml.Node, all map[string]*yaml.Node) error {
	if _, ok := all[sibling]; !ok {
		return c.fail(val, "%q is only valid together with %s", key, sibling)
	}

	return nil
}

func (c *compiler) plainField(s *step, key string, val *yaml.Node, isCond bool) error {
	switch key {
	case "raise":
		v, err := c.value(val)
		s.raise, s.hasRaise = v, true

		return err
	case "return":
		v, err := c.value(val)
		s.ret, s.hasRet = v, true

		return err
	case keySteps:
		b, err := c.block(val)
		s.nested = b

		return err
	case "next":
		if val.Kind != yaml.ScalarNode || val.Value == "" {
			return c.fail(val, "next must be a step name")
		}

		s.next = val.Value

		return nil
	case keyCondition:
		if !isCond {
			return c.fail(val, "condition is only valid inside switch")
		}

		return nil
	default:
		return c.fail(val, "unknown step field %q", key)
	}
}

func (c *compiler) assignField(s *step, n *yaml.Node) error {
	n = resolve(n)
	if n.Kind != yaml.SequenceNode {
		return c.fail(n, "assign must be a list of name: value pairs")
	}

	for _, item := range n.Content {
		item = resolve(item)
		if item.Kind != yaml.MappingNode || len(item.Content) != 2 {
			return c.fail(item, "each assignment must be a single name: value pair")
		}

		lv, err := c.lvalue(item.Content[0])
		if err != nil {
			return err
		}

		v, err := c.value(item.Content[1])
		if err != nil {
			return err
		}

		s.assigns = append(s.assigns, assign{target: lv, val: v})
	}

	return nil
}

func (c *compiler) lvalue(n *yaml.Node) (*lvalue, error) {
	exprSrc := n.Value
	toks, err := lex(exprSrc)

	if err != nil || len(toks) < 2 || toks[0].kind != tokIdent {
		return nil, c.fail(n, "%q is not a valid variable name", exprSrc)
	}

	p := &parser{toks: toks}

	root, perr := p.parsePostfix()
	if perr != nil || p.peek().kind != tokEOF {
		return nil, c.fail(n, "%q is not a valid assignment target", exprSrc)
	}

	lv := &lvalue{}

	for cur := root; ; {
		switch x := cur.(type) {
		case *nameNode:
			lv.root = x.name
			reverse(lv.path)

			return lv, nil
		case *memberNode:
			lv.path = append(lv.path, accessor{name: x.name})
			cur = x.x
		case *indexNode:
			lv.path = append(lv.path, accessor{idx: x.i})
			cur = x.x
		default:
			return nil, c.fail(n, "%q is not a valid assignment target", exprSrc)
		}
	}
}

func reverse(p []accessor) {
	for i, j := 0, len(p)-1; i < j; i, j = i+1, j-1 {
		p[i], p[j] = p[j], p[i]
	}
}

func (c *compiler) callField(s *step, val *yaml.Node, all map[string]*yaml.Node) error {
	if val.Kind != yaml.ScalarNode || val.Value == "" {
		return c.fail(val, "call must name a function or subworkflow")
	}

	cs := &callSpec{fn: val.Value}

	if a, ok := all["args"]; ok {
		a = resolve(a)
		if a.Kind != yaml.MappingNode {
			return c.fail(a, "args must be a map")
		}

		v, err := c.value(a)
		if err != nil {
			return err
		}

		cs.args, _ = v.(map[string]any)
		cs.hasArgs = true
	}

	if r, ok := all["result"]; ok {
		lv, err := c.lvalue(r)
		if err != nil {
			return err
		}

		cs.result = lv
	}

	s.call = cs

	return nil
}

func (c *compiler) switchField(s *step, n *yaml.Node) error {
	n = resolve(n)
	if n.Kind != yaml.SequenceNode || len(n.Content) == 0 {
		return c.fail(n, "switch must be a non-empty list of conditions")
	}

	if len(n.Content) > maxSwitchConds {
		return c.fail(n, "a switch can have at most %d conditions", maxSwitchConds)
	}

	s.hasSwitch = true

	for _, item := range n.Content {
		item = resolve(item)
		if item.Kind != yaml.MappingNode {
			return c.fail(item, "each switch condition must be a map")
		}

		var test any

		found := false

		for i := 0; i+1 < len(item.Content); i += 2 {
			if item.Content[i].Value == keyCondition {
				v, err := c.value(item.Content[i+1])
				if err != nil {
					return err
				}

				test, found = v, true
			}
		}

		if !found {
			return c.fail(item, "a switch condition needs a condition field")
		}

		body := &step{name: s.name, line: s.line}
		if err := c.body(item, body, true); err != nil {
			return err
		}

		s.sw = append(s.sw, &cond{test: test, body: body})
	}

	return nil
}

func (c *compiler) forField(n *yaml.Node) (*forSpec, error) {
	n = resolve(n)
	if n.Kind != yaml.MappingNode {
		return nil, c.fail(n, "for must be a map")
	}

	f := &forSpec{}

	for i := 0; i+1 < len(n.Content); i += 2 {
		key, val := n.Content[i], n.Content[i+1]

		var err error

		switch key.Value {
		case "value":
			f.value = val.Value
		case "index":
			f.index = val.Value
		case "in":
			f.in, err = c.value(val)
			f.hasIn = true
		case "range":
			f.rng, err = c.value(val)
			f.hasRange = true
		case keySteps:
			f.body, err = c.block(val)
		default:
			err = c.fail(key, "unknown for field %q", key.Value)
		}

		if err != nil {
			return nil, err
		}
	}

	switch {
	case f.value == "":
		return nil, c.fail(n, "for needs a value variable")
	case f.body == nil:
		return nil, c.fail(n, "for needs steps")
	case f.hasIn == f.hasRange:
		return nil, c.fail(n, "for needs exactly one of in or range")
	}

	return f, nil
}

func (c *compiler) parallelField(n *yaml.Node) (*parSpec, error) {
	n = resolve(n)
	if n.Kind != yaml.MappingNode {
		return nil, c.fail(n, "parallel must be a map")
	}

	p := &parSpec{}

	for i := 0; i+1 < len(n.Content); i += 2 {
		key, val := n.Content[i], resolve(n.Content[i+1])

		var err error

		switch key.Value {
		case "exception_policy":
			if val.Value != "continueAll" {
				err = c.fail(val, "exception_policy %q is not supported: only continueAll", val.Value)
			}
		case "shared":
			for _, s := range val.Content {
				p.shared = append(p.shared, s.Value)
			}
		case "concurrency_limit":
			p.limit, err = c.value(val)
			p.hasLimit = true
		case "branches":
			p.branches, err = c.branches(val)
		case keyFor:
			p.loop, err = c.forField(val)
		default:
			err = c.fail(key, "unknown parallel field %q", key.Value)
		}

		if err != nil {
			return nil, err
		}
	}

	if (p.loop == nil) == (p.branches == nil) {
		return nil, c.fail(n, "parallel needs exactly one of branches or for")
	}

	return p, nil
}

func (c *compiler) branches(n *yaml.Node) ([]*branch, error) {
	if n.Kind != yaml.SequenceNode || len(n.Content) < minBranches || len(n.Content) > maxBranches {
		return nil, c.fail(n, "a parallel step needs %d to %d branches", minBranches, maxBranches)
	}

	var out []*branch

	for _, item := range n.Content {
		item = resolve(item)
		if item.Kind != yaml.MappingNode || len(item.Content) != 2 {
			return nil, c.fail(item, "each branch must be a single branch name: {steps: ...}")
		}

		body := resolve(item.Content[1])
		if body.Kind != yaml.MappingNode || len(body.Content) != 2 ||
			body.Content[0].Value != keySteps {
			return nil, c.fail(body, "a branch must contain only steps")
		}

		b, err := c.block(body.Content[1])
		if err != nil {
			return nil, err
		}

		out = append(out, &branch{name: item.Content[0].Value, body: b})
	}

	return out, nil
}

func (c *compiler) tryField(s *step, n *yaml.Node, all map[string]*yaml.Node) error {
	n = resolve(n)
	t := &trySpec{}

	if n.Kind == yaml.MappingNode && len(n.Content) == 2 && n.Content[0].Value == keySteps {
		b, err := c.block(n.Content[1])
		if err != nil {
			return err
		}

		t.body = b
	} else {
		inner := &step{name: s.name, line: s.line}
		if err := c.body(n, inner, false); err != nil {
			return err
		}

		t.body = &block{index: map[string]int{s.name: 0}, steps: []*step{inner}}
	}

	if r, ok := all["retry"]; ok {
		v, err := c.value(r)
		if err != nil {
			return err
		}

		t.retry, t.hasRetry = v, true
	}

	if ex, ok := all["except"]; ok {
		if err := c.exceptField(t, resolve(ex)); err != nil {
			return err
		}
	}

	if !t.hasRetry && !t.hasExcept {
		return c.fail(n, "try needs a retry or except")
	}

	s.try = t

	return nil
}

func (c *compiler) exceptField(t *trySpec, n *yaml.Node) error {
	if n.Kind != yaml.MappingNode {
		return c.fail(n, "except must be a map")
	}

	for i := 0; i+1 < len(n.Content); i += 2 {
		key, val := n.Content[i], n.Content[i+1]

		switch key.Value {
		case "as":
			t.exceptAs = val.Value
		case keySteps:
			b, err := c.block(val)
			if err != nil {
				return err
			}

			t.except = b
		default:
			return c.fail(key, "unknown except field %q", key.Value)
		}
	}

	if t.except == nil {
		return c.fail(n, "except needs steps")
	}

	t.hasExcept = true

	return nil
}

// value converts a YAML node into a literal value; ${...} strings become *Expr.
func (c *compiler) value(n *yaml.Node) (any, error) {
	n = resolve(n)

	switch n.Kind {
	case yaml.ScalarNode:
		return c.scalar(n)
	case yaml.SequenceNode:
		out := make([]any, len(n.Content))

		for i, it := range n.Content {
			v, err := c.value(it)
			if err != nil {
				return nil, err
			}

			out[i] = v
		}

		return out, nil
	case yaml.MappingNode:
		out := make(map[string]any, len(n.Content)/pairWidth)

		for i := 0; i+1 < len(n.Content); i += 2 {
			k := n.Content[i].Value
			if _, dup := out[k]; dup {
				return nil, c.fail(n.Content[i], "duplicate map key %q", k)
			}

			v, err := c.value(n.Content[i+1])
			if err != nil {
				return nil, err
			}

			out[k] = v
		}

		return out, nil
	default:
		return nil, c.fail(n, "unsupported YAML construct")
	}
}

func (c *compiler) scalar(n *yaml.Node) (any, error) {
	switch n.Tag {
	case "!!null":
		return nil, nil //nolint:nilnil // YAML null
	case "!!bool":
		return n.Value == "true" || n.Value == "True" || n.Value == "TRUE", nil
	case "!!int":
		i, err := strconv.ParseInt(strings.ReplaceAll(n.Value, "_", ""), 0, 64)
		if err != nil {
			return nil, c.fail(n, "invalid integer %q", n.Value)
		}

		return i, nil
	case "!!float":
		f, err := strconv.ParseFloat(n.Value, 64)
		if err != nil {
			return nil, c.fail(n, "invalid number %q", n.Value)
		}

		return f, nil
	}

	s := strings.TrimSpace(n.Value)
	if strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}") {
		return c.expr(n, s[2:len(s)-1])
	}

	return n.Value, nil
}

func (c *compiler) expr(n *yaml.Node, src string) (*Expr, error) {
	e, err := ParseExpr(src)
	if err != nil {
		return nil, c.fail(n, "invalid expression ${%s}: %v", strings.TrimSpace(src), err)
	}

	for _, name := range e.calls() {
		if name == "" || name == fnIf || name == fnDefault {
			continue
		}

		spec, ok := c.eng.funcs[name]

		switch {
		case !ok:
			return nil, c.fail(n, "function %q not found", name)
		case spec.blocking:
			return nil, c.fail(n, "%s cannot be used in an expression; use a call step", name)
		}
	}

	return e, nil
}
