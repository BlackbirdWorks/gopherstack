package bedrockagent

import (
	"fmt"
	"strings"
	"unicode"
)

const (
	flowValidationSeverityError = "Error"
	flowNodeTypeInput           = "Input"
	flowNodeTypeOutput          = "Output"
	flowNodeTypeCondition       = "Condition"
	flowNodeTypeLoop            = "Loop"
	flowNodeTypeLoopInput       = "LoopInput"
	flowNodeTypeLoopController  = "LoopController"
	flowConnectionTypeData      = "Data"
	flowConnectionTypeCond      = "Conditional"
)

const (
	keyNode       = "node"
	keyConnection = "connection"
	keyInput      = "input"
)

// flowNodeConfigKey maps a node type to its FlowNodeConfiguration union key.
func flowNodeConfigKey(nodeType string) (string, bool) {
	switch nodeType {
	case "Input", "Output", "Condition", "Lex", "Prompt", "Storage", "Agent", "Retrieval", "Iterator", "Collector",
		"Loop", "LoopInput", "LoopController", "KnowledgeBase", "LambdaFunction", "InlineCode":
		return strings.ToLower(nodeType[:1]) + nodeType[1:], true
	}

	return "", false
}

// loopIncompatibleNodeType reports whether nodeType is an IncompatibleLoopNodeType.
func loopIncompatibleNodeType(nodeType string) bool {
	switch nodeType {
	case "Input", "Condition", "Iterator", "Collector":
		return true
	}

	return false
}

type flowNodeInfo struct {
	raw       map[string]any
	inputs    map[string]bool
	outputs   map[string]bool
	conds     map[string]bool
	name      string
	typ       string
	inputList []map[string]any
}

type flowConnInfo struct {
	name, source, target, typ string
	sourceOutput, targetInput string
	condition                 string
	hasData, hasCond          bool
}

type flowGraph struct {
	nodes  []*flowNodeInfo
	byName map[string]*flowNodeInfo
	conns  []flowConnInfo
}

func fvIssue(typ, msg string, details map[string]any) FlowValidationError {
	key := strings.ToLower(typ[:1]) + typ[1:]

	return FlowValidationError{
		Severity: flowValidationSeverityError,
		Type:     typ,
		Message:  msg,
		Details:  map[string]any{key: details},
	}
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)

	return m
}

func asString(v any) string {
	s, _ := v.(string)

	return s
}

func nameSet(list any) map[string]bool {
	out := map[string]bool{}

	items, _ := list.([]any)
	for _, it := range items {
		if n := asString(asMap(it)["name"]); n != "" {
			out[n] = true
		}
	}

	return out
}

func parseFlowGraph(definition map[string]any) *flowGraph {
	g := &flowGraph{byName: map[string]*flowNodeInfo{}}

	nodes, _ := definition["nodes"].([]any)
	for _, raw := range nodes {
		m := asMap(raw)
		n := &flowNodeInfo{
			raw:     m,
			name:    asString(m["name"]),
			typ:     asString(m["type"]),
			inputs:  nameSet(m["inputs"]),
			outputs: nameSet(m["outputs"]),
			conds:   map[string]bool{},
		}

		ins, _ := m["inputs"].([]any)
		for _, in := range ins {
			n.inputList = append(n.inputList, asMap(in))
		}

		if n.typ == flowNodeTypeCondition {
			n.conds = nameSet(asMap(asMap(asMap(m["configuration"])["condition"]))["conditions"])
		}

		g.nodes = append(g.nodes, n)
		g.byName[n.name] = n
	}

	conns, _ := definition["connections"].([]any)
	for _, raw := range conns {
		c := asMap(raw)
		cfg := asMap(c["configuration"])
		data, cond := asMap(cfg["data"]), asMap(cfg["conditional"])

		g.conns = append(g.conns, flowConnInfo{
			name:         asString(c["name"]),
			source:       asString(c["source"]),
			target:       asString(c["target"]),
			typ:          asString(c["type"]),
			sourceOutput: asString(data["sourceOutput"]),
			targetInput:  asString(data["targetInput"]),
			condition:    asString(cond["condition"]),
			hasData:      data != nil,
			hasCond:      cond != nil,
		})
	}

	return g
}

// validateFlowGraph returns the validations for a flow definition's node and connection graph.
func validateFlowGraph(definition map[string]any) []FlowValidationError {
	g := parseFlowGraph(definition)
	out := g.endpointIssues()
	out = append(out, g.nodeIssues()...)
	out = append(out, g.connectionIssues()...)
	out = append(out, g.inputIssues()...)
	out = append(out, g.cycleIssues()...)
	out = append(out, g.unreachableIssues(flowNodeTypeInput)...)

	return append(out, g.loopIssues()...)
}

func (g *flowGraph) endpointIssues() []FlowValidationError {
	out := []FlowValidationError{}
	hasInput, hasOutput := false, false

	for _, n := range g.nodes {
		switch n.typ {
		case flowNodeTypeInput:
			hasInput = true
		case flowNodeTypeOutput:
			hasOutput = true
		}
	}

	if !hasInput {
		out = append(out, fvIssue("MissingStartingNodes", "Flow must contain an Input node.", map[string]any{}))
	}

	if !hasOutput {
		out = append(out, fvIssue("MissingEndingNodes", "Flow must contain an Output node.", map[string]any{}))
	}

	return out
}

func (g *flowGraph) nodeIssues() []FlowValidationError {
	var out []FlowValidationError

	for _, n := range g.nodes {
		if key, known := flowNodeConfigKey(n.typ); known && asMap(asMap(n.raw["configuration"])[key]) == nil {
			out = append(out, fvIssue("MissingNodeConfiguration",
				fmt.Sprintf("Node %q of type %s has no %q configuration.", n.name, n.typ, key),
				map[string]any{keyNode: n.name}))
		}

		out = append(out, malformedInputExpressions(n)...)

		if n.typ == flowNodeTypeCondition {
			out = append(out, conditionIssues(n)...)
		}
	}

	return out
}

// malformedInputExpressions checks each input expression is a "$.data" path.
func malformedInputExpressions(n *flowNodeInfo) []FlowValidationError {
	var out []FlowValidationError

	for _, in := range n.inputList {
		expr, present := in["expression"].(string)
		if !present {
			continue
		}

		if cause := inputExpressionCause(expr); cause != "" {
			out = append(out, fvIssue("MalformedNodeInputExpression",
				fmt.Sprintf("Input %q of node %q has a malformed expression: %s.", asString(in["name"]), n.name, cause),
				map[string]any{keyNode: n.name, keyInput: asString(in["name"]), "cause": cause}))
		}
	}

	return out
}

// inputExpressionCause returns why expr is not a "$.data[.field|[index]]*" path, or "".
func inputExpressionCause(expr string) string {
	rest, ok := strings.CutPrefix(expr, "$.data")
	if !ok {
		return "expression must start with $.data"
	}

	for rest != "" {
		switch rest[0] {
		case '.':
			end := strings.IndexFunc(rest[1:], func(r rune) bool { return !isPathRune(r) })
			if end == 0 || len(rest) == 1 {
				return "empty field name after '.'"
			}

			if end < 0 {
				return ""
			}

			rest = rest[1+end:]
		case '[':
			closeIdx := strings.IndexByte(rest, ']')
			if closeIdx < 0 {
				return "unterminated '['"
			}

			rest = rest[closeIdx+1:]
		default:
			return fmt.Sprintf("unexpected character %q", rest[0])
		}
	}

	return ""
}

func isPathRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-'
}

func conditionIssues(n *flowNodeInfo) []FlowValidationError {
	var out []FlowValidationError

	conds, _ := asMap(asMap(n.raw["configuration"])["condition"])["conditions"].([]any)
	seen := map[string]bool{}
	hasDefault := false

	for _, raw := range conds {
		c := asMap(raw)
		name, expr := asString(c["name"]), asString(c["expression"])

		if expr == "" {
			hasDefault = true

			continue
		}

		if unbalanced(expr) {
			out = append(out, fvIssue("MalformedConditionExpression",
				fmt.Sprintf("Condition %q of node %q has unbalanced parentheses.", name, n.name),
				map[string]any{keyNode: n.name, "condition": name, "cause": "unbalanced parentheses"}))
		}

		if seen[expr] {
			out = append(out, fvIssue("DuplicateConditionExpression",
				fmt.Sprintf("Node %q repeats condition expression %q.", n.name, expr),
				map[string]any{keyNode: n.name, "expression": expr}))
		}

		seen[expr] = true
	}

	if !hasDefault {
		out = append(out, fvIssue("MissingDefaultCondition",
			fmt.Sprintf("Condition node %q has no default condition.", n.name),
			map[string]any{keyNode: n.name}))
	}

	return out
}

func unbalanced(expr string) bool {
	depth := 0

	for _, r := range expr {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return true
			}
		}
	}

	return depth != 0
}
