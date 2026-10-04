package iot

import "strings"

type sqlNode interface {
	eval(c *sqlCtx) any
}

// sqlCtx is the evaluation state for one message against one statement.
type sqlCtx struct {
	msg     *ruleMessage
	scope   any
	alias   string
	version string
	v2016   bool
	loaded  bool
}

func newSQLCtx(msg *ruleMessage, version string, v2016 bool) *sqlCtx {
	return &sqlCtx{msg: msg, version: version, v2016: v2016}
}

// root decodes the original payload once; non-JSON payloads are Undefined.
func (c *sqlCtx) root() any {
	if !c.loaded {
		c.loaded = true

		if v, err := decodeJSONValue(c.msg.original, c.v2016); err == nil {
			c.scope = v
		} else {
			c.scope = sqlUndefined{}
		}
	}

	return c.scope
}

func (c *sqlCtx) child(scope any, alias string) *sqlCtx {
	return &sqlCtx{msg: c.msg, scope: scope, alias: alias, version: c.version, v2016: c.v2016, loaded: true}
}

func (c *sqlCtx) num(f float64) any { return normNumber(f, c.v2016) }

type litNode struct{ v any }

func (n litNode) eval(*sqlCtx) any { return n.v }

// starNode is the * expression: the payload as JSON, or raw bytes when it is not JSON.
type starNode struct{ raw bool }

func (n starNode) eval(c *sqlCtx) any {
	if n.raw {
		return sqlRaw(c.msg.original)
	}

	if v := c.root(); !isUndef(v) {
		return v
	}

	return sqlRaw(c.msg.original)
}

type identNode struct{ name string }

func (n identNode) eval(c *sqlCtx) any {
	if strings.HasPrefix(n.name, "@") {
		if v, ok := c.msg.vars[n.name]; ok {
			return v
		}

		return sqlUndefined{}
	}

	if c.alias != "" && n.name == c.alias {
		return c.scope
	}

	return memberOf(c.root(), n.name)
}

func memberOf(base any, name string) any {
	obj, ok := base.(*sqlObject)
	if !ok {
		return sqlUndefined{}
	}

	if v, found := obj.get(name); found {
		return v
	}

	return sqlUndefined{}
}

type memberNode struct {
	base sqlNode
	name string
}

func (n memberNode) eval(c *sqlCtx) any { return memberOf(n.base.eval(c), n.name) }

type indexNode struct{ base, idx sqlNode }

func (n indexNode) eval(c *sqlCtx) any {
	return indexOf(n.base.eval(c), n.idx.eval(c))
}

func indexOf(base, idx any) any {
	arr, ok := base.([]any)
	if !ok {
		return sqlUndefined{}
	}

	i, ok := toIntConv(idx)
	if !ok || i < 0 || i >= int64(len(arr)) {
		return sqlUndefined{}
	}

	return arr[i]
}

type arrayNode struct{ items []sqlNode }

func (n arrayNode) eval(c *sqlCtx) any {
	out := make([]any, 0, len(n.items))
	for _, it := range n.items {
		out = append(out, it.eval(c))
	}

	return out
}

type objectNode struct {
	vals []sqlNode
	keys []string
}

func (n objectNode) eval(c *sqlCtx) any {
	obj := newSQLObject()
	for i, k := range n.keys {
		obj.set(k, n.vals[i].eval(c))
	}

	return obj
}

type caseWhen struct{ test, result sqlNode }

type caseNode struct {
	subject sqlNode
	els     sqlNode
	whens   []caseWhen
}

func (n caseNode) eval(c *sqlCtx) any {
	subj := n.subject.eval(c)
	if isUndef(subj) {
		return sqlUndefined{}
	}

	for _, w := range n.whens {
		if eq, ok := sqlEquals(subj, w.test.eval(c)); ok && eq {
			return w.result.eval(c)
		}
	}

	if n.els != nil {
		return n.els.eval(c)
	}

	return sqlUndefined{}
}

type callNode struct {
	fn   funcImpl
	args []sqlNode
}

func (n callNode) eval(c *sqlCtx) any {
	args := make([]any, len(n.args))
	for i, a := range n.args {
		args[i] = a.eval(c)
	}

	return n.fn(c, args)
}

// existsNode is EXISTS(subquery).
type existsNode struct{ x sqlNode }

func (n existsNode) eval(c *sqlCtx) any {
	arr, ok := n.x.eval(c).([]any)
	if !ok {
		return sqlUndefined{}
	}

	return len(arr) > 0
}
