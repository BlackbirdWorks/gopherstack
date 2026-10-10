package bedrockagent

import "fmt"

func (g *flowGraph) connectionIssues() []FlowValidationError {
	var out []FlowValidationError

	seen := make(map[string]bool, len(g.conns))

	for _, c := range g.conns {
		src, tgt := g.byName[c.source], g.byName[c.target]

		if src == nil {
			out = append(out, fvIssue("UnknownConnectionSource",
				fmt.Sprintf("Connection %q references unknown source node %q.", c.name, c.source),
				map[string]any{keyConnection: c.name}))
		}

		if tgt == nil {
			out = append(out, fvIssue("UnknownConnectionTarget",
				fmt.Sprintf("Connection %q references unknown target node %q.", c.name, c.target),
				map[string]any{keyConnection: c.name}))
		}

		out = append(out, g.connectionConfigIssues(c, src, tgt)...)

		key := c.source + "\x00" + c.target + "\x00" + c.typ + "\x00" + c.condition
		if seen[key] {
			out = append(out, fvIssue(
				"DuplicateConnections",
				fmt.Sprintf(
					"Connection %q duplicates an existing connection from %q to %q.",
					c.name,
					c.source,
					c.target,
				),
				map[string]any{"source": c.source, "target": c.target},
			))
		}

		seen[key] = true
	}

	return out
}

func (g *flowGraph) connectionConfigIssues(c flowConnInfo, src, tgt *flowNodeInfo) []FlowValidationError {
	var out []FlowValidationError

	missing := func() {
		out = append(out, fvIssue("MissingConnectionConfiguration",
			fmt.Sprintf("Connection %q of type %s has no matching configuration.", c.name, c.typ),
			map[string]any{keyConnection: c.name}))
	}

	switch c.typ {
	case flowConnectionTypeData:
		if !c.hasData {
			missing()

			break
		}

		if src != nil && !src.outputs[c.sourceOutput] {
			out = append(out, fvIssue("UnknownConnectionSourceOutput",
				fmt.Sprintf("Connection %q references unknown output %q of node %q.", c.name, c.sourceOutput, src.name),
				map[string]any{keyConnection: c.name}))
		}

		if tgt != nil && !tgt.inputs[c.targetInput] {
			out = append(out, fvIssue("UnknownConnectionTargetInput",
				fmt.Sprintf("Connection %q references unknown input %q of node %q.", c.name, c.targetInput, tgt.name),
				map[string]any{keyConnection: c.name}))
		}
	case flowConnectionTypeCond:
		if !c.hasCond {
			missing()

			break
		}

		if src != nil && !src.conds[c.condition] {
			out = append(out, fvIssue("UnknownConnectionCondition",
				fmt.Sprintf("Connection %q references unknown condition %q.", c.name, c.condition),
				map[string]any{keyConnection: c.name}))
		}
	}

	return out
}

// inputIssues reports target inputs with several data connections, and declared inputs with none.
func (g *flowGraph) inputIssues() []FlowValidationError {
	var out []FlowValidationError

	counts := map[string]int{}

	for _, c := range g.conns {
		if c.typ == flowConnectionTypeData && g.byName[c.target] != nil && g.byName[c.target].inputs[c.targetInput] {
			counts[c.target+"\x00"+c.targetInput]++
		}
	}

	for _, n := range g.nodes {
		for _, in := range n.inputList {
			name := asString(in["name"])
			key := n.name + "\x00" + name

			switch {
			case counts[key] > 1:
				out = append(out, fvIssue("MultipleNodeInputConnections",
					fmt.Sprintf("Input %q of node %q has %d connections.", name, n.name, counts[key]),
					map[string]any{keyNode: n.name, keyInput: name}))
			case counts[key] == 0 && n.typ != flowNodeTypeInput && asString(in["category"]) == "":
				out = append(out, fvIssue("UnfulfilledNodeInput",
					fmt.Sprintf("Input %q of node %q has no data connection.", name, n.name),
					map[string]any{keyNode: n.name, keyInput: name}))
			}
		}
	}

	return out
}

// cycleIssues reports each connection that closes a cycle.
func (g *flowGraph) cycleIssues() []FlowValidationError {
	adj := map[string][]flowConnInfo{}

	for _, c := range g.conns {
		if g.byName[c.source] != nil && g.byName[c.target] != nil {
			adj[c.source] = append(adj[c.source], c)
		}
	}

	const (
		onStack = 1
		done    = 2
	)

	state := map[string]int{}

	var (
		out []FlowValidationError
		dfs func(string)
	)

	dfs = func(name string) {
		state[name] = onStack

		for _, c := range adj[name] {
			switch state[c.target] {
			case onStack:
				out = append(out, fvIssue("CyclicConnection",
					fmt.Sprintf("Connection %q creates a cycle.", c.name), map[string]any{keyConnection: c.name}))
			case 0:
				dfs(c.target)
			}
		}

		state[name] = done
	}

	for _, n := range g.nodes {
		if state[n.name] == 0 {
			dfs(n.name)
		}
	}

	return out
}

// unreachableIssues reports nodes with no path from any startType node.
func (g *flowGraph) unreachableIssues(startType string) []FlowValidationError {
	reached := map[string]bool{}

	var queue []string

	for _, n := range g.nodes {
		if n.typ == startType {
			reached[n.name] = true
			queue = append(queue, n.name)
		}
	}

	if len(queue) == 0 {
		return nil
	}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		for _, c := range g.conns {
			if c.source == cur && g.byName[c.target] != nil && !reached[c.target] {
				reached[c.target] = true
				queue = append(queue, c.target)
			}
		}
	}

	var out []FlowValidationError

	for _, n := range g.nodes {
		if !reached[n.name] {
			out = append(out, fvIssue(
				"UnreachableNode",
				fmt.Sprintf(
					"Node %q cannot be reached from a starting node.",
					n.name,
				),
				map[string]any{keyNode: n.name},
			))
		}
	}

	return out
}
