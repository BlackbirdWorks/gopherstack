package glue

import "sort"

const (
	nodeTypeTrigger = "TRIGGER"
	nodeTypeJob     = "JOB"
	nodeTypeCrawler = "CRAWLER"
)

// workflowGraphLocked builds the DAG of triggers/jobs/crawlers belonging to
// the named workflow, derived entirely from real state: which triggers have
// WorkflowName == name, each trigger's TriggerAction (downstream nodes) and
// TriggerPredicate.Conditions (upstream nodes that must complete to fire the
// trigger). Node UniqueId is "<kind>/<name>" -- AWS's real ID-generation
// algorithm isn't discoverable from the public SDK shapes, same simplification
// already accepted in this file for FormType.Id. Caller must hold b.mu.
func (b *InMemoryBackend) workflowGraphLocked(name string) *WorkflowGraph {
	var triggers []*Trigger

	for _, t := range b.triggers.All() {
		if t.WorkflowName == name {
			triggers = append(triggers, t)
		}
	}

	nodes := make(map[string]WorkflowNode, len(triggers))

	var edges []WorkflowEdge

	for _, t := range triggers {
		triggerID := nodeTypeTrigger + "/" + t.Name
		nodes[triggerID] = WorkflowNode{
			UniqueID:       triggerID,
			Type:           nodeTypeTrigger,
			Name:           t.Name,
			TriggerDetails: &WorkflowTriggerNodeDetails{Trigger: cloneTrigger(t)},
		}

		edges = appendConditionEdges(edges, nodes, triggerID, t)
		edges = appendActionEdges(edges, nodes, triggerID, t)
	}

	return sortedWorkflowGraph(nodes, edges)
}

// appendConditionEdges adds job/crawler nodes and edges from a trigger's
// predicate conditions (nodes whose completion fires the trigger).
func appendConditionEdges(
	edges []WorkflowEdge,
	nodes map[string]WorkflowNode,
	triggerID string,
	t *Trigger,
) []WorkflowEdge {
	if t.Predicate == nil {
		return edges
	}

	for _, c := range t.Predicate.Conditions {
		if id := addComponentNode(nodes, nodeTypeJob, c.JobName); id != "" {
			edges = append(edges, WorkflowEdge{SourceID: id, DestinationID: triggerID})
		}

		if id := addComponentNode(nodes, nodeTypeCrawler, c.CrawlerName); id != "" {
			edges = append(edges, WorkflowEdge{SourceID: id, DestinationID: triggerID})
		}
	}

	return edges
}

// appendActionEdges adds job/crawler nodes and edges for a trigger's actions
// (nodes the trigger runs).
func appendActionEdges(
	edges []WorkflowEdge,
	nodes map[string]WorkflowNode,
	triggerID string,
	t *Trigger,
) []WorkflowEdge {
	for _, a := range t.Actions {
		if id := addComponentNode(nodes, nodeTypeJob, a.JobName); id != "" {
			edges = append(edges, WorkflowEdge{SourceID: triggerID, DestinationID: id})
		}

		if id := addComponentNode(nodes, nodeTypeCrawler, a.CrawlerName); id != "" {
			edges = append(edges, WorkflowEdge{SourceID: triggerID, DestinationID: id})
		}
	}

	return edges
}

// addComponentNode inserts a JOB/CRAWLER node keyed by name if absent and
// returns its UniqueId, or "" if name is empty.
func addComponentNode(nodes map[string]WorkflowNode, kind, name string) string {
	if name == "" {
		return ""
	}

	id := kind + "/" + name
	if _, ok := nodes[id]; !ok {
		nodes[id] = WorkflowNode{UniqueID: id, Type: kind, Name: name}
	}

	return id
}

// sortedWorkflowGraph flattens nodes/edges into a WorkflowGraph with a
// deterministic (sorted) order, since map iteration order is not stable.
func sortedWorkflowGraph(nodes map[string]WorkflowNode, edges []WorkflowEdge) *WorkflowGraph {
	out := make([]WorkflowNode, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].UniqueID < out[j].UniqueID })

	sort.Slice(edges, func(i, j int) bool {
		if edges[i].SourceID != edges[j].SourceID {
			return edges[i].SourceID < edges[j].SourceID
		}

		return edges[i].DestinationID < edges[j].DestinationID
	})

	return &WorkflowGraph{Nodes: out, Edges: edges}
}
