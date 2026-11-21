package azurearm

import (
	"strconv"
	"strings"
)

const (
	logicKText         = "text"
	logicMaxTraces     = 100
	logicMaxTraceNodes = 200
	logicMaxTraceDepth = 8
	logicMaxTraceValue = 16 << 10
)

// logicTraceNode is one evaluated expression, with the sub-expressions it evaluated.
type logicTraceNode struct {
	Value    any               `json:"value,omitempty"`
	Text     string            `json:"text"`
	Err      string            `json:"err,omitempty"`
	Subs     []*logicTraceNode `json:"subs,omitempty"`
	HasValue bool              `json:"hasValue,omitempty"`
}

// logicTraceRoot is a template string found in an action's inputs and its evaluation.
type logicTraceRoot struct {
	Path string         `json:"path"`
	Node logicTraceNode `json:"node"`
}

type logicTracer struct {
	roots []*logicTraceRoot
	stack [][]*logicTraceNode
	path  []string
	nodes int
}

func newLogicTracer(root string) *logicTracer { return &logicTracer{path: []string{root}} }

func (t *logicTracer) active() bool {
	return len(t.stack) > 0 && len(t.stack) <= logicMaxTraceDepth && t.nodes < logicMaxTraceNodes
}

func (t *logicTracer) pathString() string {
	var b strings.Builder

	for i, s := range t.path {
		if i > 0 && !strings.HasPrefix(s, "[") {
			b.WriteByte('.')
		}

		b.WriteString(s)
	}

	return b.String()
}

func (t *logicTracer) wrap(n *logicExpr, f func() (any, error)) (any, error) {
	t.stack = append(t.stack, nil)
	t.nodes++

	v, err := f()
	top := len(t.stack) - 1
	node := &logicTraceNode{Text: n.text(), Subs: t.stack[top]}
	t.stack = t.stack[:top]
	setTraceResult(node, v, err)

	t.stack[top-1] = append(t.stack[top-1], node)

	return v, err
}

func setTraceResult(n *logicTraceNode, v any, err error) {
	if err != nil {
		n.Err = strings.TrimPrefix(err.Error(), "logic expression error: ")

		return
	}

	if bodySize(v) <= logicMaxTraceValue {
		n.Value, n.HasValue = v, true
	}
}

func (t *logicTracer) beginRoot() { t.stack = append(t.stack, nil) }

func (t *logicTracer) endRoot(text string, v any, err error) {
	top := len(t.stack) - 1
	subs := t.stack[top]
	t.stack = t.stack[:top]

	if len(subs) == 0 && err == nil {
		return
	}

	if len(t.roots) >= logicMaxTraces {
		return
	}

	if len(subs) == 1 && strings.HasPrefix(text, "@") && !strings.HasPrefix(text, "@{") {
		subs = subs[0].Subs
	}

	root := &logicTraceRoot{Path: t.pathString(), Node: logicTraceNode{Text: text, Subs: subs}}
	setTraceResult(&root.Node, v, err)
	t.roots = append(t.roots, root)
}

func (n *logicExpr) text() string {
	switch n.kind {
	case exprLit:
		switch v := n.lit.(type) {
		case nil:
			return "null"
		case string:
			return "'" + strings.ReplaceAll(v, "'", "''") + "'"
		}

		return logicString(n.lit)
	case exprCall:
		args := make([]string, len(n.args))
		for i, a := range n.args {
			args[i] = a.text()
		}

		return n.name + "(" + strings.Join(args, ",") + ")"
	case exprIndex:
		return n.base.text() + safeMark(n.safe) + "[" + n.idx.text() + "]"
	case exprProp:
		return n.base.text() + safeMark(n.safe) + "." + n.name
	}

	return ""
}

func safeMark(safe bool) string {
	if safe {
		return "?"
	}

	return ""
}

func (c *logicEvalCtx) eval(n *logicExpr) (any, error) {
	if c.trace == nil || !c.trace.active() {
		return c.evalNode(n)
	}

	return c.trace.wrap(n, func() (any, error) { return c.evalNode(n) })
}

func (c *logicEvalCtx) pushPath(seg string) {
	if c.trace != nil {
		c.trace.path = append(c.trace.path, seg)
	}
}

func (c *logicEvalCtx) popPath() {
	if c.trace != nil {
		c.trace.path = c.trace.path[:len(c.trace.path)-1]
	}
}

func indexSeg(i int) string { return "[" + strconv.Itoa(i) + "]" }

// evalTraced evaluates v and records the expressions evaluated under root on the named action.
func (rs *logicRunState) evalTraced(env *logicEnv, name, root string, v any) (any, error) {
	c := rs.evalCtx(env)
	c.trace = newLogicTracer(root)

	out, err := evalLogicValue(v, c)

	if len(c.trace.roots) == 0 {
		return out, err
	}

	for _, r := range c.trace.roots {
		if strings.HasPrefix(r.Path, "inputs.authentication") {
			stripTraceValues(&r.Node)
		}
	}

	rs.p.mu.Lock("storeTraces")
	defer rs.p.mu.Unlock()

	if rec := env.local[name]; rec != nil {
		room := max(logicMaxTraces-len(rec.Traces), 0)
		rec.Traces = append(rec.Traces, c.trace.roots[:min(room, len(c.trace.roots))]...)
	}

	return out, err
}

func traceBody(n *logicTraceNode, hideValues bool) map[string]any {
	out := map[string]any{logicKText: n.Text}

	if !hideValues && n.HasValue {
		out["value"] = n.Value
	}

	if n.Err != "" {
		out[logicKError] = map[string]any{logicKCode: logicCodeInvalidTmpl, fieldMessage: n.Err}
	}

	if len(n.Subs) > 0 {
		subs := make([]any, len(n.Subs))
		for i, s := range n.Subs {
			subs[i] = traceBody(s, hideValues)
		}

		out["subexpressions"] = subs
	}

	return out
}

func expressionTraces(a *logicActionRec) map[string]any {
	inputs := make([]any, 0, len(a.Traces))

	for _, r := range a.Traces {
		b := traceBody(&r.Node, a.Inputs.Hidden)
		b["path"] = r.Path
		inputs = append(inputs, b)
	}

	return map[string]any{"inputs": inputs}
}

func (h *Handler) logicExpressionTraces(q logicReq, rep string) error {
	out, err := q.lp.read(q.ref, func(w *logicWorkflow) (any, error) {
		a, _, terr := w.requestTarget(q.rest[1], q.rest[3], rep)
		if terr != nil {
			return nil, terr
		}

		return expressionTraces(a), nil
	})

	return h.logicReply(q, out, err)
}

func stripTraceValues(n *logicTraceNode) {
	n.Value, n.HasValue = nil, false

	for _, s := range n.Subs {
		stripTraceValues(s)
	}
}
