package gcpworkflows

import (
	"context"
	"maps"
	"slices"
	"strings"

	"cloud.google.com/go/workflows/apiv1/workflowspb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

const (
	defaultExecPage = 100
	maxBasicPage    = 1000
	maxFullPage     = 100
)

func (b *InMemoryBackend) dropExecutionsLocked(prefix string) {
	for name, rec := range b.execs {
		if strings.HasPrefix(name, prefix) {
			if rec.cancel != nil {
				rec.cancel()
			}

			delete(b.execs, name)
		}
	}

	for url, cb := range b.callbacks {
		if strings.HasPrefix(cb.exec, prefix) {
			delete(b.callbacks, url)
		}
	}
}

func parseArgument(arg string) (any, error) {
	if len(arg) > maxArgument {
		return nil, errInvalid("argument is larger than %d bytes.", maxArgument)
	}

	if strings.TrimSpace(arg) == "" {
		return nil, nil //nolint:nilnil // no argument
	}

	v, err := wfinterp.ParseJSON([]byte(arg))
	if err != nil {
		return nil, errInvalid("argument must be valid JSON: %v", err)
	}

	return v, nil
}

// CreateExecution starts an execution of the latest revision of a workflow.
func (b *InMemoryBackend) CreateExecution(in *executionspb.CreateExecutionRequest) (*executionspb.Execution, error) {
	if _, _, _, err := parseWorkflow(in.GetParent()); err != nil {
		return nil, err
	}

	ex := in.GetExecution()
	if ex == nil {
		ex = &executionspb.Execution{}
	}

	arg, err := parseArgument(ex.GetArgument())
	if err != nil {
		return nil, err
	}

	if err = validateLabels(ex.GetLabels()); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateExecution")
	defer b.mu.Unlock()

	rec, ok := b.workflows[in.GetParent()]
	if !ok {
		return nil, errNotFound("Workflow %q not found.", in.GetParent())
	}

	wf := rec.latest()
	prog := b.programLocked(rec, wf)

	if prog == nil {
		return nil, errPrecondition("Workflow %q has no runnable revision.", in.GetParent())
	}

	name := in.GetParent() + "/executions/" + uuid.NewString()
	pb := &executionspb.Execution{
		Name:               name,
		StartTime:          b.ts(),
		State:              executionspb.Execution_ACTIVE,
		Argument:           ex.GetArgument(),
		WorkflowRevisionId: wf.GetRevisionId(),
		CallLogLevel:       callLogLevel(ex, wf),
		Labels:             mergeLabels(wf.GetLabels(), ex.GetLabels()),
		Status:             &executionspb.Execution_Status{},
	}

	er := &execRec{pb: pb}
	b.execs[name] = er
	b.launchLocked(er, wf, prog, arg)

	return clone(pb), nil
}

func callLogLevel(ex *executionspb.Execution, wf *workflowspb.Workflow) executionspb.Execution_CallLogLevel {
	if l := ex.GetCallLogLevel(); l != executionspb.Execution_CALL_LOG_LEVEL_UNSPECIFIED {
		return l
	}

	return executionspb.Execution_CallLogLevel(wf.GetCallLogLevel())
}

func mergeLabels(base, over map[string]string) map[string]string {
	if len(base)+len(over) == 0 {
		return nil
	}

	out := make(map[string]string, len(base)+len(over))
	maps.Copy(out, base)
	maps.Copy(out, over)

	return out
}

// programLocked returns the compiled program of a revision, compiling again after a restore.
func (b *InMemoryBackend) programLocked(rec *workflowRec, wf *workflowspb.Workflow) *wfinterp.Program {
	if p, ok := rec.progs[wf.GetRevisionId()]; ok {
		return p
	}

	p, err := b.engine.Compile(wf.GetSourceContents())
	if err != nil {
		return nil
	}

	rec.progs[wf.GetRevisionId()] = p

	return p
}

func viewed(pb *executionspb.Execution, view executionspb.ExecutionView) *executionspb.Execution {
	out := clone(pb)

	if view == executionspb.ExecutionView_BASIC {
		return &executionspb.Execution{
			Name: out.GetName(), StartTime: out.GetStartTime(), EndTime: out.GetEndTime(),
			Duration: out.GetDuration(), State: out.GetState(), WorkflowRevisionId: out.GetWorkflowRevisionId(),
		}
	}

	if out.GetState() != executionspb.Execution_ACTIVE {
		out.Status = nil
	}

	return out
}

// GetExecution returns an execution; the view defaults to FULL.
func (b *InMemoryBackend) GetExecution(in *executionspb.GetExecutionRequest) (*executionspb.Execution, error) {
	if err := validateExecutionName(in.GetName()); err != nil {
		return nil, err
	}

	b.mu.RLock("GetExecution")
	defer b.mu.RUnlock()

	rec, ok := b.execs[in.GetName()]
	if !ok {
		return nil, errNotFound("Execution %q not found.", in.GetName())
	}

	return viewed(rec.pb, in.GetView()), nil
}

// ListExecutions lists the executions of a workflow, newest first by default; the view defaults to BASIC.
func (b *InMemoryBackend) ListExecutions(
	in *executionspb.ListExecutionsRequest,
) (*executionspb.ListExecutionsResponse, error) {
	if _, _, _, err := parseWorkflow(in.GetParent()); err != nil {
		return nil, err
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	pred, err := parseExecFilter(in.GetFilter())
	if err != nil {
		return nil, err
	}

	b.mu.RLock("ListExecutions")

	if _, ok := b.workflows[in.GetParent()]; !ok {
		b.mu.RUnlock()

		return nil, errNotFound("Workflow %q not found.", in.GetParent())
	}

	var all []*executionspb.Execution

	for name, rec := range b.execs {
		if strings.HasPrefix(name, in.GetParent()+"/") && pred(rec.pb) {
			all = append(all, clone(rec.pb))
		}
	}

	b.mu.RUnlock()

	if err = orderExecutions(all, in.GetOrderBy()); err != nil {
		return nil, err
	}

	view := in.GetView()
	if view == executionspb.ExecutionView_EXECUTION_VIEW_UNSPECIFIED {
		view = executionspb.ExecutionView_BASIC
	}

	limit := maxBasicPage
	if view == executionspb.ExecutionView_FULL {
		limit = maxFullPage
	}

	pg := page.New(all, in.GetPageToken(), min(int(in.GetPageSize()), limit), defaultExecPage)

	out := make([]*executionspb.Execution, len(pg.Data))
	for i, e := range pg.Data {
		out[i] = viewed(e, view)
	}

	return &executionspb.ListExecutionsResponse{Executions: out, NextPageToken: pg.Next}, nil
}

func execKey(field string, e *executionspb.Execution) (string, bool) {
	switch field {
	case fieldExecID:
		return e.GetName(), true
	case fieldStart:
		return e.GetStartTime().AsTime().Format("2006-01-02T15:04:05.000000000Z"), true
	case fieldEnd:
		return e.GetEndTime().AsTime().Format("2006-01-02T15:04:05.000000000Z"), true
	case fieldDuration:
		return e.GetDuration().AsDuration().String(), true
	case fieldState:
		return e.GetState().String(), true
	case fieldRevision:
		return e.GetWorkflowRevisionId(), true
	default:
		return "", false
	}
}

func orderExecutions(all []*executionspb.Execution, orderBy string) error {
	if strings.TrimSpace(orderBy) == "" {
		orderBy = fieldStart + " " + dirDescending
	}

	type term struct {
		field string
		desc  bool
	}

	var terms []term

	for part := range strings.SplitSeq(orderBy, ",") {
		f := strings.Fields(part)
		if len(f) == 0 || len(f) > 2 {
			return errInvalid("order_by %q is malformed.", orderBy)
		}

		if _, ok := execKey(f[0], &executionspb.Execution{}); !ok {
			return errInvalid("order_by field %q is not supported.", f[0])
		}

		terms = append(terms, term{field: f[0], desc: len(f) == 2 && f[1] == dirDescending})
	}

	slices.SortStableFunc(all, func(a, b *executionspb.Execution) int {
		for _, t := range terms {
			ka, _ := execKey(t.field, a)
			kb, _ := execKey(t.field, b)

			c := strings.Compare(ka, kb)
			if t.desc {
				c = -c
			}

			if c != 0 {
				return c
			}
		}

		return 0
	})

	return nil
}

// CancelExecution cancels an active execution.
func (b *InMemoryBackend) CancelExecution(in *executionspb.CancelExecutionRequest) (*executionspb.Execution, error) {
	if err := validateExecutionName(in.GetName()); err != nil {
		return nil, err
	}

	b.mu.Lock("CancelExecution")
	defer b.mu.Unlock()

	rec, ok := b.execs[in.GetName()]
	if !ok {
		return nil, errNotFound("Execution %q not found.", in.GetName())
	}

	if rec.pb.GetState() != executionspb.Execution_ACTIVE {
		return nil, errPrecondition("Execution %q is %s and cannot be cancelled.", in.GetName(), rec.pb.GetState())
	}

	if rec.cancel != nil {
		rec.cancel()
	}

	b.endLocked(in.GetName(), rec, executionspb.Execution_CANCELLED, "", cancelledError())

	return viewed(rec.pb, executionspb.ExecutionView_FULL), nil
}

func cancelledError() *executionspb.Execution_Error {
	return &executionspb.Execution_Error{
		Payload: `"Execution cancelled by user"`,
		Context: "Execution cancelled by user",
	}
}

// endLocked moves an active execution to a terminal state.
func (b *InMemoryBackend) endLocked(
	name string, rec *execRec, state executionspb.Execution_State, result string, errPB *executionspb.Execution_Error,
) {
	end := b.ts()
	rec.pb.State, rec.pb.EndTime = state, end
	rec.pb.Duration = durationpb.New(end.AsTime().Sub(rec.pb.GetStartTime().AsTime()))
	rec.pb.Status = nil

	if state == executionspb.Execution_SUCCEEDED {
		rec.pb.Result = result
	} else {
		rec.pb.Error = errPB
	}

	for url, cb := range b.callbacks {
		if cb.exec == name {
			delete(b.callbacks, url)
		}
	}
}

// StartExecution runs a workflow with a JSON argument; Eventarc uses it for workflow destinations.
func (b *InMemoryBackend) StartExecution(_ context.Context, workflow string, argument []byte) error {
	_, err := b.CreateExecution(&executionspb.CreateExecutionRequest{
		Parent:    workflow,
		Execution: &executionspb.Execution{Argument: string(argument)},
	})

	return err
}

// WorkflowExists reports whether a workflow exists.
func (b *InMemoryBackend) WorkflowExists(name string) bool {
	b.mu.RLock("WorkflowExists")
	defer b.mu.RUnlock()

	_, ok := b.workflows[name]

	return ok
}
