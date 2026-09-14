package glue

// Crawler, Job, Connection, Trigger, Workflow and Database all carry ARN
// tagged json:"Arn,omitempty" so 865a0c12f can key tag persistence by it, but
// none of the real Get*/List*/BatchGet* outputs carries an Arn member
// (aws-sdk-go-v2/service/glue@v1.157.0 types/types.go: Crawler:2841,
// Job:6832, Connection:2117, Trigger:12584, Workflow:13103, Database:3337).
// Retagging ARN json:"-" would recreate the persistence bug 865a0c12f fixed
// (gopherstack-6vwds), so each type gets a read-path wire twin instead: embed
// the model and redeclare ARN at depth 0 with the same tag, left at its zero
// value. encoding/json always prefers the shallower depth over a promoted
// field with the same JSON name regardless of value, and the zero string is
// dropped by omitempty -- so the twin marshals every real field unmodified
// and never emits Arn. Mirror image of dc4d95c5a's persistence twin, which
// shadows an embedded json:"-" field with a depth-zero real tag to put a
// field ON the wire; here it shadows an embedded real tag with a depth-zero
// zero-value copy to keep one OFF. A depth-zero json:"-" field does NOT work
// for this: an excluded field drops out of promotion entirely instead of
// shadowing, so the embedded Arn field would still win (verified directly
// against encoding/json before relying on this).

type crawlerWire struct {
	*Crawler
	ARN string `json:"Arn,omitempty"`
}

func toCrawlerWire(c *Crawler) *crawlerWire {
	if c == nil {
		return nil
	}

	return &crawlerWire{Crawler: c}
}

func toCrawlerWireList(cs []*Crawler) []*crawlerWire {
	out := make([]*crawlerWire, 0, len(cs))
	for _, c := range cs {
		out = append(out, toCrawlerWire(c))
	}

	return out
}

type jobWire struct {
	*Job
	ARN string `json:"Arn,omitempty"`
}

func toJobWire(j *Job) *jobWire {
	if j == nil {
		return nil
	}

	return &jobWire{Job: j}
}

func toJobWireList(js []*Job) []*jobWire {
	out := make([]*jobWire, 0, len(js))
	for _, j := range js {
		out = append(out, toJobWire(j))
	}

	return out
}

type connectionWire struct {
	*Connection
	ARN string `json:"Arn,omitempty"`
}

func toConnectionWire(c *Connection) *connectionWire {
	if c == nil {
		return nil
	}

	return &connectionWire{Connection: c}
}

func toConnectionWireList(cs []*Connection) []*connectionWire {
	out := make([]*connectionWire, 0, len(cs))
	for _, c := range cs {
		out = append(out, toConnectionWire(c))
	}

	return out
}

type triggerWire struct {
	*Trigger
	ARN string `json:"Arn,omitempty"`
}

func toTriggerWire(t *Trigger) *triggerWire {
	if t == nil {
		return nil
	}

	return &triggerWire{Trigger: t}
}

func toTriggerWireList(ts []*Trigger) []*triggerWire {
	out := make([]*triggerWire, 0, len(ts))
	for _, t := range ts {
		out = append(out, toTriggerWire(t))
	}

	return out
}

type databaseWire struct {
	*Database
	ARN string `json:"Arn,omitempty"`
}

func toDatabaseWire(db *Database) *databaseWire {
	if db == nil {
		return nil
	}

	return &databaseWire{Database: db}
}

func toDatabaseWireList(dbs []*Database) []*databaseWire {
	out := make([]*databaseWire, 0, len(dbs))
	for _, db := range dbs {
		out = append(out, toDatabaseWire(db))
	}

	return out
}

// workflowWire also shadows Graph: workflowGraphLocked embeds real *Trigger
// values (via cloneTrigger) three levels down inside
// TriggerDetails.Trigger, and the depth-shadow rule above only resolves
// conflicts within one struct's own field set, so it cannot reach that deep.
// toWorkflowGraphWire rebuilds the graph with triggerWire in place of each
// nested Trigger instead.
type workflowWire struct {
	*Workflow
	Graph   *workflowGraphWire `json:"Graph,omitempty"`
	LastRun *workflowRunWire   `json:"LastRun,omitempty"`
	ARN     string             `json:"Arn,omitempty"`
}

func toWorkflowWire(w *Workflow) *workflowWire {
	if w == nil {
		return nil
	}

	return &workflowWire{Workflow: w, Graph: toWorkflowGraphWire(w.Graph), LastRun: toWorkflowRunWire(w.LastRun)}
}

func toWorkflowWireList(ws []*Workflow) []*workflowWire {
	out := make([]*workflowWire, 0, len(ws))
	for _, w := range ws {
		out = append(out, toWorkflowWire(w))
	}

	return out
}

// workflowRunWire is the wire shape for a WorkflowRun response. It does NOT
// embed *WorkflowRun: encoding/json's field-shadow rule matches on JSON name
// (the tag-resolved key), not Go identifier, so an outer field with a
// different json tag than the promoted one does not suppress it -- both
// would be emitted. WorkflowRun's own "WorkflowName" tag is kept only for
// snapshot/persistence compatibility (see its doc comment, models.go); this
// struct is the real HTTP response shape, with the real "Name" key
// (deserializers.go's awsAwsjson11_deserializeDocumentWorkflowRun).
type workflowRunWire struct {
	Properties    map[string]string      `json:"WorkflowRunProperties,omitempty"`
	Statistics    *WorkflowRunStatistics `json:"Statistics,omitempty"`
	Name          string                 `json:"Name"`
	RunID         string                 `json:"WorkflowRunId"`
	Status        string                 `json:"Status"`
	PreviousRunID string                 `json:"PreviousRunId,omitempty"`
	StartedOn     float64                `json:"StartedOn,omitempty"`
	CompletedOn   float64                `json:"CompletedOn,omitempty"`
}

func toWorkflowRunWire(r *WorkflowRun) *workflowRunWire {
	if r == nil {
		return nil
	}

	return &workflowRunWire{
		Properties:    r.Properties,
		Statistics:    r.Statistics,
		Name:          r.WorkflowName,
		RunID:         r.RunID,
		Status:        r.Status,
		PreviousRunID: r.PreviousRunID,
		StartedOn:     r.StartedOn,
		CompletedOn:   r.CompletedOn,
	}
}

func toWorkflowRunWireList(rs []*WorkflowRun) []*workflowRunWire {
	out := make([]*workflowRunWire, 0, len(rs))
	for _, r := range rs {
		out = append(out, toWorkflowRunWire(r))
	}

	return out
}

type workflowGraphWire struct {
	Nodes []workflowNodeWire `json:"Nodes,omitempty"`
	Edges []WorkflowEdge     `json:"Edges,omitempty"`
}

type workflowNodeWire struct {
	TriggerDetails *workflowTriggerNodeDetailsWire `json:"TriggerDetails,omitempty"`
	Name           string                          `json:"Name,omitempty"`
	Type           string                          `json:"Type,omitempty"`
	UniqueID       string                          `json:"UniqueId,omitempty"`
}

type workflowTriggerNodeDetailsWire struct {
	Trigger *triggerWire `json:"Trigger,omitempty"`
}

func toWorkflowGraphWire(g *WorkflowGraph) *workflowGraphWire {
	if g == nil {
		return nil
	}

	nodes := make([]workflowNodeWire, 0, len(g.Nodes))
	for _, n := range g.Nodes {
		nodes = append(nodes, workflowNodeWire{
			UniqueID:       n.UniqueID,
			Type:           n.Type,
			Name:           n.Name,
			TriggerDetails: toWorkflowTriggerNodeDetailsWire(n.TriggerDetails),
		})
	}

	return &workflowGraphWire{Nodes: nodes, Edges: g.Edges}
}

func toWorkflowTriggerNodeDetailsWire(d *WorkflowTriggerNodeDetails) *workflowTriggerNodeDetailsWire {
	if d == nil {
		return nil
	}

	return &workflowTriggerNodeDetailsWire{Trigger: toTriggerWire(d.Trigger)}
}
