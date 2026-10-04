package stepfunctions

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

const (
	sdkSyncPollInterval = 25 * time.Millisecond
	sdkSyncPatternV2    = "sync:2"
	stateSucceeded      = "SUCCEEDED"
	statusTimedOut      = "TIMED_OUT"
)

// syncSpec describes how a ".sync" optimized integration polls its job.
type syncSpec struct {
	pollParams func(started map[string]any) any
	state      func(poll map[string]any) string
	result     func(poll map[string]any, pattern string) any
	pollAction string
	failed     []string
	running    []string
}

type syncRequest struct {
	client  any
	started any
	prefix  string
	spec    syncSpec
	call    asl.SDKCall
}

func syncSpecFor(svc, action string) (syncSpec, bool) {
	switch svc + "." + action {
	case "sfn.startExecution":
		return syncSpec{
			pollAction: "describeExecution",
			pollParams: func(s map[string]any) any { return map[string]any{"ExecutionArn": s["ExecutionArn"]} },
			state:      func(p map[string]any) string { return str(p["Status"]) },
			running:    []string{statusRunning, "PENDING_REDRIVE"},
			failed:     []string{statusFailed, statusTimedOut, "ABORTED"},
			result:     executionSyncResult,
		}, true
	case "batch.submitJob":
		return syncSpec{
			pollAction: "describeJobs",
			pollParams: func(s map[string]any) any { return map[string]any{"Jobs": []any{s["JobId"]}} },
			state:      func(p map[string]any) string { return str(first(p["Jobs"])["Status"]) },
			running:    []string{"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", statusRunning},
			failed:     []string{statusFailed},
			result:     func(p map[string]any, _ string) any { return first(p["Jobs"]) },
		}, true
	case "athena.startQueryExecution":
		return syncSpec{
			pollAction: "getQueryExecution",
			pollParams: func(s map[string]any) any { return map[string]any{"QueryExecutionId": s["QueryExecutionId"]} },
			state: func(p map[string]any) string {
				return str(child(child(p, "QueryExecution"), "Status")["State"])
			},
			running: []string{"QUEUED", "RUNNING"},
			failed:  []string{statusFailed, "CANCELLED"},
			result:  func(p map[string]any, _ string) any { return p },
		}, true
	case "codebuild.startBuild":
		return syncSpec{
			pollAction: "batchGetBuilds",
			pollParams: func(s map[string]any) any { return map[string]any{"Ids": []any{child(s, "Build")["Id"]}} },
			state:      func(p map[string]any) string { return str(first(p["Builds"])["BuildStatus"]) },
			running:    []string{"IN_PROGRESS"},
			failed:     []string{statusFailed, "FAULT", ecsTaskStatusStopped, statusTimedOut},
			result:     func(p map[string]any, _ string) any { return map[string]any{"Build": first(p["Builds"])} },
		}, true
	case "ecs.runTask":
		return ecsRunTaskSyncSpec(), true
	case "glue.startJobRun":
		return glueStartJobRunSyncSpec(), true
	default:
		return syncSpec{}, false
	}
}

func ecsRunTaskSyncSpec() syncSpec {
	return syncSpec{
		pollAction: "describeTasks",
		pollParams: func(s map[string]any) any {
			tasks, _ := s["Tasks"].([]any)
			arns := make([]any, 0, len(tasks))
			cluster := ""

			for _, t := range tasks {
				m, _ := t.(map[string]any)
				arns = append(arns, m["TaskArn"])
				cluster = str(m["ClusterArn"])
			}

			return map[string]any{"Cluster": cluster, "Tasks": arns}
		},
		state:   ecsSyncState,
		running: []string{statusRunning},
		failed:  []string{statusFailed},
		result:  func(p map[string]any, _ string) any { return p },
	}
}

// ecsSyncState is RUNNING until every task is STOPPED, FAILED on a missing
// task or a container that exited non-zero.
func ecsSyncState(p map[string]any) string {
	tasks, _ := p["Tasks"].([]any)
	if failures, _ := p["Failures"].([]any); len(failures) > 0 || len(tasks) == 0 {
		return statusFailed
	}

	state := ecsTaskStatusStopped

	for _, t := range tasks {
		m, _ := t.(map[string]any)
		if str(m["LastStatus"]) != ecsTaskStatusStopped {
			return statusRunning
		}

		containers, _ := m["Containers"].([]any)
		for _, c := range containers {
			cm, _ := c.(map[string]any)
			if code, ok := cm["ExitCode"].(float64); ok && code != 0 {
				state = statusFailed
			}
		}
	}

	return state
}

func glueStartJobRunSyncSpec() syncSpec {
	return syncSpec{
		pollAction: "getJobRun",
		pollParams: func(s map[string]any) any {
			return map[string]any{"JobName": s["JobName"], "RunId": s["JobRunId"]}
		},
		state:   func(p map[string]any) string { return str(child(p, "JobRun")["JobRunState"]) },
		running: []string{"STARTING", statusRunning, "STOPPING", "WAITING"},
		failed:  []string{ecsTaskStatusStopped, statusFailed, "TIMEOUT", "ERROR", "EXPIRED"},
		result:  func(p map[string]any, _ string) any { return p },
	}
}

// withStartedFields adds the request's JobName to Glue StartJobRun's response, as the optimized integration does.
func withStartedFields(call asl.SDKCall, out any) any {
	m, ok := out.(map[string]any)
	params, _ := call.Params.(map[string]any)

	if !ok || call.Service+"."+call.Action != "glue.startJobRun" || params["JobName"] == nil {
		return out
	}

	m["JobName"] = params["JobName"]

	return m
}

// runTaskFailures fails a .sync run of ECS RunTask that returned Failures, as AmazonECS.Unknown.
func runTaskFailures(call asl.SDKCall, out any) error {
	m, _ := out.(map[string]any)
	failures, _ := m["Failures"].([]any)

	if call.Service+"."+call.Action != "ecs.runTask" || call.Pattern == "" || len(failures) == 0 {
		return nil
	}

	cause, _ := json.Marshal(failures)

	return &asl.FailError{ErrCode: "AmazonECS.Unknown", Cause: string(cause)}
}

// executionSyncResult shapes DescribeExecution: .sync keeps Output an escaped
// JSON string, .sync:2 parses it.
func executionSyncResult(p map[string]any, pattern string) any {
	if pattern != sdkSyncPatternV2 {
		return p
	}

	if s, ok := p["Output"].(string); ok {
		var parsed any
		if json.Unmarshal([]byte(s), &parsed) == nil {
			p["Output"] = parsed
		}
	}

	return p
}

func str(v any) string {
	s, _ := v.(string)

	return s
}

func child(m map[string]any, key string) map[string]any {
	c, _ := m[key].(map[string]any)

	return c
}

func first(v any) map[string]any {
	items, _ := v.([]any)
	if len(items) == 0 {
		return nil
	}

	m, _ := items[0].(map[string]any)

	return m
}

// waitSync polls the started job until it leaves its running states or ctx ends.
func (a *sdkAdapter) waitSync(ctx context.Context, req syncRequest) (any, error) {
	started, _ := req.started.(map[string]any)
	params := req.spec.pollParams(started)

	ticker := time.NewTicker(sdkSyncPollInterval)
	defer ticker.Stop()

	for {
		out, err := invokeSDKMethod(ctx, req.client, req.call.Service, req.spec.pollAction, params)
		if err != nil {
			return nil, sdkFailure(req.prefix, err)
		}

		poll, _ := out.(map[string]any)
		state := req.spec.state(poll)

		if !slices.Contains(req.spec.running, state) {
			return finishSync(req, poll, state)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func finishSync(req syncRequest, poll map[string]any, state string) (any, error) {
	result := req.spec.result(poll, req.call.Pattern)

	if state == stateSucceeded || !slices.Contains(req.spec.failed, state) && state != "" {
		return result, nil
	}

	cause, _ := json.Marshal(result)

	return nil, &asl.FailError{ErrCode: errCodeTaskFailed, Cause: string(cause)}
}
