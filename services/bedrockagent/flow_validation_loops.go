package bedrockagent

import "fmt"

// loopIssues validates each Loop node's nested definition.
func (g *flowGraph) loopIssues() []FlowValidationError {
	var out []FlowValidationError

	for _, n := range g.nodes {
		if n.typ != flowNodeTypeLoop {
			continue
		}

		body := asMap(asMap(asMap(n.raw["configuration"])["loop"])["definition"])
		if body == nil {
			continue
		}

		out = append(out, validateLoopBody(n.name, body)...)
	}

	return out
}

func validateLoopBody(loopNode string, body map[string]any) []FlowValidationError {
	g := parseFlowGraph(body)

	var out []FlowValidationError

	inputs, controllers := 0, 0

	for _, n := range g.nodes {
		switch {
		case n.typ == flowNodeTypeLoopInput:
			inputs++
		case n.typ == flowNodeTypeLoopController:
			controllers++
		case loopIncompatibleNodeType(n.typ):
			out = append(out, fvIssue("LoopIncompatibleNodeType",
				fmt.Sprintf("Node %q of type %s is not allowed inside loop %q.", n.name, n.typ, loopNode),
				map[string]any{keyNode: loopNode, "incompatibleNodeName": n.name, "incompatibleNodeType": n.typ}))
		}
	}

	out = append(out, loopCountIssues(loopNode, "LoopInput", inputs)...)
	out = append(out, loopCountIssues(loopNode, "LoopController", controllers)...)
	out = append(out, g.nodeIssues()...)
	out = append(out, g.connectionIssues()...)
	out = append(out, g.cycleIssues()...)
	out = append(out, g.unreachableIssues(flowNodeTypeLoopInput)...)

	return append(out, g.loopIssues()...)
}

func loopCountIssues(loopNode, kind string, n int) []FlowValidationError {
	details := map[string]any{"loopNode": loopNode}

	switch {
	case n == 0:
		return []FlowValidationError{fvIssue("Missing"+kind+"Node",
			fmt.Sprintf("Loop %q has no %s node.", loopNode, kind), details)}
	case n > 1:
		return []FlowValidationError{fvIssue("Multiple"+kind+"Nodes",
			fmt.Sprintf("Loop %q has more than one %s node.", loopNode, kind), details)}
	}

	return nil
}
