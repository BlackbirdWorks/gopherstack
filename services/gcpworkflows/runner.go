package gcpworkflows

import (
	"context"
	"errors"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/workflows/apiv1/workflowspb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpdispatch"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

// launchLocked starts the runner goroutine of a new execution; the caller holds b.mu.
func (b *InMemoryBackend) launchLocked(er *execRec, wf *workflowspb.Workflow, prog *wfinterp.Program, arg any) {
	ctx, cancel := context.WithCancel(b.runCtx)
	er.cancel = cancel
	name := er.pb.GetName()
	host := b.hostFor(name, wf)

	b.wg.Go(func() {
		defer cancel()

		res, err := b.engine.Run(logger.WithWorker(ctx, "gcpworkflows", "execution"), prog, arg, host)
		b.complete(name, res, err)
	})
}

func (b *InMemoryBackend) complete(name string, res any, runErr error) {
	b.mu.Lock("complete")
	defer b.mu.Unlock()

	rec, ok := b.execs[name]
	if !ok || rec.pb.GetState() != executionspb.Execution_ACTIVE {
		return
	}

	if runErr == nil {
		raw, err := wfinterp.ToJSON(res)
		if err != nil {
			b.endLocked(name, rec, executionspb.Execution_FAILED, "", &executionspb.Execution_Error{
				Payload: `{"message":"the result cannot be encoded as JSON","tags":["ValueError"]}`,
				Context: err.Error(),
			})

			return
		}

		b.endLocked(name, rec, executionspb.Execution_SUCCEEDED, string(raw), nil)

		return
	}

	var werr *wfinterp.Error
	if !errors.As(runErr, &werr) {
		raw, _ := wfinterp.ToJSON(map[string]any{"message": runErr.Error()})
		b.endLocked(name, rec, executionspb.Execution_FAILED, "", &executionspb.Execution_Error{
			Payload: string(raw), Context: runErr.Error(),
		})

		return
	}

	if werr.Cancelled() {
		b.endLocked(name, rec, executionspb.Execution_CANCELLED, "", cancelledError())

		return
	}

	b.endLocked(name, rec, executionspb.Execution_FAILED, "", failure(werr))
}

func failure(werr *wfinterp.Error) *executionspb.Execution_Error {
	out := &executionspb.Execution_Error{Payload: werr.Payload(), Context: werr.Context()}

	if len(werr.Stack) > 0 {
		st := &executionspb.Execution_StackTrace{}
		for _, f := range werr.Stack {
			st.Elements = append(st.Elements, &executionspb.Execution_StackTraceElement{
				Step: f.Step, Routine: f.Routine,
				Position: &executionspb.Execution_StackTraceElement_Position{Line: int64(f.Line)},
			})
		}

		out.StackTrace = st
	}

	return out
}

type runInfo struct {
	userEnv  map[string]string
	exec     string
	project  string
	location string
	workflow string
	revision string
	account  string
}

func (r runInfo) env(name string) (string, bool) {
	switch name {
	case "GOOGLE_CLOUD_PROJECT_ID":
		return r.project, true
	case "GOOGLE_CLOUD_PROJECT_NUMBER":
		return projectNumber(r.project), true
	case "GOOGLE_CLOUD_LOCATION":
		return r.location, true
	case "GOOGLE_CLOUD_WORKFLOW_ID":
		return r.workflow, true
	case "GOOGLE_CLOUD_WORKFLOW_REVISION_ID":
		return r.revision, true
	case "GOOGLE_CLOUD_WORKFLOW_EXECUTION_ID":
		return lastSegment(r.exec), true
	case "GOOGLE_CLOUD_SERVICE_ACCOUNT_NAME":
		return accountEmail(r.account), true
	}

	v, ok := r.userEnv[name]

	return v, ok
}

// hostFor builds the wfinterp.Host of one execution; the caller holds b.mu.
func (b *InMemoryBackend) hostFor(execName string, wf *workflowspb.Workflow) *wfinterp.Host {
	project, location, id, _ := parseWorkflow(wf.GetName())
	info := runInfo{
		userEnv: wf.GetUserEnvVars(), exec: execName, project: project, location: location, workflow: id,
		revision: wf.GetRevisionId(), account: wf.GetServiceAccount(),
	}
	clock, client := b.clock, b.client

	return &wfinterp.Host{
		Clock: clock,
		HTTP:  client,
		Env:   info.env,
		Log:   func(e wfinterp.LogEntry) { b.emitLog(info, e) },
		Token: func(kind, audience, scope, target string) string {
			email := accountEmail(info.account)
			if kind == "OIDC" {
				return gcpdispatch.OIDCToken(email, audience, target, clock.Now())
			}

			return gcpdispatch.OAuthToken(email, scope)
		},
		Connector: func(ctx context.Context, name string, args map[string]any) (any, bool, error) {
			return b.callConnector(ctx, info, name, args)
		},
		CreateCallback: func(_ context.Context, method string) (string, error) {
			return b.createCallback(execName, method)
		},
		AwaitCallback: b.awaitCallback,
		Progress:      func(frames []wfinterp.Frame) { b.setSteps(execName, frames) },
	}
}

func (b *InMemoryBackend) setSteps(name string, frames []wfinterp.Frame) {
	b.mu.Lock("setSteps")
	defer b.mu.Unlock()

	rec, ok := b.execs[name]
	if !ok || rec.pb.GetState() != executionspb.Execution_ACTIVE {
		return
	}

	steps := make([]*executionspb.Execution_Status_Step, len(frames))
	for i, f := range frames {
		steps[i] = &executionspb.Execution_Status_Step{Routine: f.Routine, Step: f.Step}
	}

	rec.pb.Status = &executionspb.Execution_Status{CurrentSteps: steps}
}

func (b *InMemoryBackend) emitLog(info runInfo, e wfinterp.LogEntry) {
	ctx := context.Background()
	log := logger.Load(ctx)
	log.InfoContext(ctx, "gcpworkflows: sys.log", "execution", info.exec, "severity", e.Severity, "text", e.Text)

	b.mu.RLock("emitLog")
	sink := b.logs
	b.mu.RUnlock()

	if sink == nil {
		return
	}

	entry := &loggingpb.LogEntry{
		LogName: gcpserve.LogName(info.project, "Workflows"),
		Resource: &monitoredres.MonitoredResource{
			Type: "workflows.googleapis.com/Workflow",
			Labels: map[string]string{
				"project_id": info.project, "location": info.location,
				"workflow_id": info.workflow, "revision_id": info.revision,
			},
		},
		Labels: map[string]string{
			"workflows.googleapis.com/execution_id": lastSegment(info.exec),
		},
		Severity: severityOf(e.Severity),
	}

	if e.JSON != nil {
		if s, err := structpb.NewStruct(e.JSON); err == nil {
			entry.Payload = &loggingpb.LogEntry_JsonPayload{JsonPayload: s}
		}
	}

	if entry.GetPayload() == nil {
		entry.Payload = &loggingpb.LogEntry_TextPayload{TextPayload: e.Text}
	}

	_ = sink.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{Entries: []*loggingpb.LogEntry{entry}})
}

func severityOf(s string) ltype.LogSeverity {
	if v, ok := ltype.LogSeverity_value[s]; ok {
		return ltype.LogSeverity(v)
	}

	return ltype.LogSeverity_DEFAULT
}
