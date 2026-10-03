package bedrockagent

import "fmt"

const (
	flowValidationSeverityError = "Error"
	flowNodeTypeInput           = "Input"
	flowNodeTypeOutput          = "Output"
)

// validateFlowGraph checks connections against nodes and requires an Input and an Output node.
func validateFlowGraph(definition map[string]any) []FlowValidationError {
	nodes, _ := definition["nodes"].([]any)
	conns, _ := definition["connections"].([]any)

	names := make(map[string]bool, len(nodes))
	hasInput, hasOutput := false, false

	for _, raw := range nodes {
		n, _ := raw.(map[string]any)
		name, _ := n["name"].(string)
		names[name] = true

		switch typ, _ := n["type"].(string); typ {
		case flowNodeTypeInput:
			hasInput = true
		case flowNodeTypeOutput:
			hasOutput = true
		}
	}

	out := []FlowValidationError{}

	if !hasInput {
		out = append(out, FlowValidationError{
			Severity: flowValidationSeverityError,
			Type:     "MissingStartingNodes",
			Message:  "Flow must contain an Input node.",
			Details:  map[string]any{"missingStartingNodes": map[string]any{}},
		})
	}

	if !hasOutput {
		out = append(out, FlowValidationError{
			Severity: flowValidationSeverityError,
			Type:     "MissingEndingNodes",
			Message:  "Flow must contain an Output node.",
			Details:  map[string]any{"missingEndingNodes": map[string]any{}},
		})
	}

	return append(out, validateFlowConnections(conns, names)...)
}

func validateFlowConnections(conns []any, names map[string]bool) []FlowValidationError {
	var out []FlowValidationError

	seen := make(map[string]bool, len(conns))

	for _, raw := range conns {
		c, _ := raw.(map[string]any)
		name, _ := c["name"].(string)
		source, _ := c["source"].(string)
		target, _ := c["target"].(string)
		typ, _ := c["type"].(string)

		if !names[source] {
			out = append(out, FlowValidationError{
				Severity: flowValidationSeverityError,
				Type:     "UnknownConnectionSource",
				Message:  fmt.Sprintf("Connection %q references unknown source node %q.", name, source),
				Details:  map[string]any{"unknownConnectionSource": map[string]any{"connection": name}},
			})
		}

		if !names[target] {
			out = append(out, FlowValidationError{
				Severity: flowValidationSeverityError,
				Type:     "UnknownConnectionTarget",
				Message:  fmt.Sprintf("Connection %q references unknown target node %q.", name, target),
				Details:  map[string]any{"unknownConnectionTarget": map[string]any{"connection": name}},
			})
		}

		key := source + "\x00" + target + "\x00" + typ + "\x00" + conditionOf(c)
		if seen[key] {
			out = append(out, FlowValidationError{
				Severity: flowValidationSeverityError,
				Type:     "DuplicateConnections",
				Message: fmt.Sprintf(
					"Connection %q duplicates an existing connection from %q to %q.",
					name,
					source,
					target,
				),
				Details: map[string]any{
					"duplicateConnections": map[string]any{"source": source, "target": target},
				},
			})
		}

		seen[key] = true
	}

	return out
}

func conditionOf(c map[string]any) string {
	cfg, _ := c["configuration"].(map[string]any)
	cond, _ := cfg["conditional"].(map[string]any)
	s, _ := cond["condition"].(string)

	return s
}
