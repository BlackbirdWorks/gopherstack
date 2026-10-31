package mcpserver

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	defaultTraceLimit = 20
	maxTraceLimit     = 100
	defaultDenyLimit  = 50
	maxDenyLimit      = 500
	traceBufferFetch  = 100
	redactedValue     = "REDACTED"
)

func readOnly(title string) *mcp.ToolAnnotations {
	return &mcp.ToolAnnotations{Title: title, ReadOnlyHint: true, OpenWorldHint: new(false)}
}

func destructive(title string) *mcp.ToolAnnotations {
	return &mcp.ToolAnnotations{Title: title, DestructiveHint: new(true), OpenWorldHint: new(false)}
}

func additive(title string) *mcp.ToolAnnotations {
	return &mcp.ToolAnnotations{Title: title, DestructiveHint: new(false), OpenWorldHint: new(false)}
}

// HealthInput is the (empty) input of gopherstack_health.
type HealthInput struct{}

// HealthOutput reports the running instance's identity and enabled services.
type HealthOutput struct {
	Status        string   `json:"status"`
	Version       string   `json:"version"`
	Services      []string `json:"services"`
	Disabled      []string `json:"disabled,omitempty"`
	UptimeSeconds int64    `json:"uptime_seconds"`
	Goroutines    int      `json:"goroutines"`
}

// SnapshotSaveInput names a snapshot to save.
type SnapshotSaveInput struct {
	Name     string   `json:"name"               jsonschema:"snapshot name (a new version is created if it exists)"`
	Message  string   `json:"message,omitempty"  jsonschema:"optional version message"`
	Services []string `json:"services,omitempty" jsonschema:"only save these services (default: all)"`
}

// SnapshotLoadInput selects a snapshot version to restore.
type SnapshotLoadInput struct {
	Name     string   `json:"name"               jsonschema:"snapshot name"`
	Version  string   `json:"version,omitempty"  jsonschema:"version number or 'latest' (default latest)"`
	Strategy string   `json:"strategy,omitempty" jsonschema:"overwrite (default) or service-merge"`
	Services []string `json:"services,omitempty" jsonschema:"only load these services (default: all in the snapshot)"`
	Merge    bool     `json:"merge,omitempty"    jsonschema:"merge into existing state instead of replacing it"`
}

// SnapshotDeleteInput selects a snapshot (version) to delete.
type SnapshotDeleteInput struct {
	Name    string `json:"name"              jsonschema:"snapshot name"`
	Version string `json:"version,omitempty" jsonschema:"version to delete (default: all versions)"`
}

// SnapshotListInput is the (empty) input of snapshot_list.
type SnapshotListInput struct{}

// DataOutput wraps a JSON document returned by the admin API.
type DataOutput struct {
	Data any `json:"data"`
}

// ResetInput scopes a state reset.
type ResetInput struct {
	Service string `json:"service,omitempty" jsonschema:"only reset this service (default: every service)"`
}

// FaultsListInput is the (empty) input of chaos_list_faults.
type FaultsListInput struct{}

// FaultError is the error a fault rule injects.
type FaultError struct {
	Code       string `json:"code"       jsonschema:"AWS error code, e.g. ThrottlingException"`
	StatusCode int    `json:"statusCode" jsonschema:"HTTP status code, e.g. 429"`
}

// FaultRule is one chaos fault; omitted match fields match everything.
type FaultRule struct {
	Error       *FaultError `json:"error,omitempty"       jsonschema:"injected error (default 503 ServiceUnavailable)"`
	Probability *float64    `json:"probability,omitempty" jsonschema:"firing probability 0..1 (default 1)"`
	Service     string      `json:"service,omitempty"     jsonschema:"service name to match, e.g. dynamodb"`
	Region      string      `json:"region,omitempty"      jsonschema:"region to match"`
	Operation   string      `json:"operation,omitempty"   jsonschema:"operation name to match, e.g. PutItem"`
}

// FaultsSetInput sets chaos fault rules.
type FaultsSetInput struct {
	Rules  []FaultRule `json:"rules"            jsonschema:"fault rules; evaluated in order, first match wins"`
	Append bool        `json:"append,omitempty" jsonschema:"append to existing rules instead of replacing them"`
}

// FaultsOutput lists the active chaos fault rules.
type FaultsOutput struct {
	Rules []FaultRule `json:"rules"`
}

// FaultsClearInput is the (empty) input of chaos_clear_faults.
type FaultsClearInput struct{}

// StatusOutput confirms a state-changing operation.
type StatusOutput struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// TracesInput filters recent request traces.
type TracesInput struct {
	Service   string `json:"service,omitempty"   jsonschema:"only this service, e.g. s3"`
	Operation string `json:"operation,omitempty" jsonschema:"only this operation name, e.g. PutObject (case-insensitive)"`
	Status    string `json:"status,omitempty"    jsonschema:"status code like 500, class like 4xx, or 'failed' (>=400)"`
	Limit     int    `json:"limit,omitempty"     jsonschema:"max traces to return, 1-100 (default 20)"`
}

// Trace is a redacted summary of one captured request (no headers or bodies).
type Trace struct {
	Timestamp    time.Time `json:"timestamp"`
	ID           string    `json:"id"`
	Method       string    `json:"method"`
	Path         string    `json:"path"`
	Query        string    `json:"query,omitempty"`
	Service      string    `json:"service,omitempty"`
	Operation    string    `json:"operation,omitempty"`
	ErrorCode    string    `json:"error_code,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
	Status       int       `json:"status"`
	DurationMS   int64     `json:"duration_ms"`
}

// TracesOutput lists traces, newest first.
type TracesOutput struct {
	Traces []Trace `json:"traces"`
}

// IAMDenialsInput filters IAM policy-evaluation denials.
type IAMDenialsInput struct {
	Service   string `json:"service,omitempty"   jsonschema:"only this service"`
	Principal string `json:"principal,omitempty" jsonschema:"only this principal ARN"`
	Since     string `json:"since,omitempty"     jsonschema:"RFC3339 time or a duration such as 5m"`
	Limit     int    `json:"limit,omitempty"     jsonschema:"max events, 1-500 (default 50)"`
}

// IAMDenialsOutput lists denied evaluations from the IAM policy stream.
type IAMDenialsOutput struct {
	Events  []map[string]any `json:"events"`
	Dropped int64            `json:"dropped"`
}

// InitHooksInput is the (empty) input of init_hooks_list.
type InitHooksInput struct{}

func (b *backend) health(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	_ HealthInput,
) (*mcp.CallToolResult, HealthOutput, error) {
	var out HealthOutput
	if err := b.call(ctx, http.MethodGet, "/_gopherstack/health", nil, nil, &out); err != nil {
		return nil, out, err
	}

	return nil, out, nil
}

func (b *backend) snapshotSave(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in SnapshotSaveInput,
) (*mcp.CallToolResult, DataOutput, error) {
	body := map[string]any{"message": in.Message, "services": in.Services}

	return b.data(ctx, http.MethodPost, "/_gopherstack/pods/"+url.PathEscape(in.Name), nil, body)
}

func (b *backend) snapshotList(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	_ SnapshotListInput,
) (*mcp.CallToolResult, DataOutput, error) {
	return b.data(ctx, http.MethodGet, "/_gopherstack/pods", nil, nil)
}

func (b *backend) snapshotLoad(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in SnapshotLoadInput,
) (*mcp.CallToolResult, DataOutput, error) {
	q := url.Values{}
	setIf(q, "version", in.Version)
	setIf(q, "strategy", in.Strategy)
	setIf(q, "services", strings.Join(in.Services, ","))

	if in.Merge {
		q.Set("merge", "true")
	}

	return b.data(ctx, http.MethodPost, "/_gopherstack/pods/"+url.PathEscape(in.Name)+"/load", q, nil)
}

func (b *backend) snapshotDelete(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in SnapshotDeleteInput,
) (*mcp.CallToolResult, DataOutput, error) {
	q := url.Values{}
	setIf(q, "version", in.Version)

	return b.data(ctx, http.MethodDelete, "/_gopherstack/pods/"+url.PathEscape(in.Name), q, nil)
}

func (b *backend) reset(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in ResetInput,
) (*mcp.CallToolResult, DataOutput, error) {
	q := url.Values{}
	setIf(q, "service", in.Service)

	return b.data(ctx, http.MethodPost, "/_gopherstack/reset", q, nil)
}

func (b *backend) faultsList(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	_ FaultsListInput,
) (*mcp.CallToolResult, FaultsOutput, error) {
	return b.faults(ctx, http.MethodGet, "/_gopherstack/chaos/faults", nil)
}

func (b *backend) faultsSet(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in FaultsSetInput,
) (*mcp.CallToolResult, FaultsOutput, error) {
	method := http.MethodPost
	if in.Append {
		method = http.MethodPatch
	}

	rules := in.Rules
	if rules == nil {
		rules = []FaultRule{}
	}

	return b.faults(ctx, method, "/_gopherstack/chaos/faults", rules)
}

func (b *backend) faultsClear(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	_ FaultsClearInput,
) (*mcp.CallToolResult, StatusOutput, error) {
	if err := b.call(ctx, http.MethodPost, "/_gopherstack/chaos/faults/clear", nil, nil, nil); err != nil {
		return nil, StatusOutput{}, err
	}

	return nil, StatusOutput{Status: "ok", Message: "all fault rules cleared"}, nil
}

func (b *backend) faults(ctx context.Context, method, path string, in any) (*mcp.CallToolResult, FaultsOutput, error) {
	rules := []FaultRule{}
	if err := b.call(ctx, method, path, nil, in, &rules); err != nil {
		return nil, FaultsOutput{}, err
	}

	return nil, FaultsOutput{Rules: rules}, nil
}

func (b *backend) traces(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in TracesInput,
) (*mcp.CallToolResult, TracesOutput, error) {
	limit := clamp(in.Limit, defaultTraceLimit, maxTraceLimit)

	q := url.Values{"n": {strconv.Itoa(traceBufferFetch)}}
	setIf(q, "service", in.Service)
	setIf(q, "status", in.Status)

	var raw []capturedRequest
	if err := b.call(ctx, http.MethodGet, "/_gopherstack/requests", q, nil, &raw); err != nil {
		return nil, TracesOutput{}, err
	}

	out := TracesOutput{Traces: make([]Trace, 0, min(limit, len(raw)))}

	for i := range raw {
		t := raw[i].trace()
		if in.Operation != "" && !strings.EqualFold(t.Operation, in.Operation) {
			continue
		}

		out.Traces = append(out.Traces, t)
		if len(out.Traces) == limit {
			break
		}
	}

	return nil, out, nil
}

func (b *backend) iamDenials(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in IAMDenialsInput,
) (*mcp.CallToolResult, IAMDenialsOutput, error) {
	q := url.Values{"decision": {"deny"}, "n": {strconv.Itoa(clamp(in.Limit, defaultDenyLimit, maxDenyLimit))}}
	setIf(q, "service", in.Service)
	setIf(q, "principal", in.Principal)
	setIf(q, "since", in.Since)

	out := IAMDenialsOutput{Events: []map[string]any{}}
	if err := b.call(ctx, http.MethodGet, "/_gopherstack/iam/policy-stream", q, nil, &out); err != nil {
		return nil, out, err
	}

	if out.Events == nil {
		out.Events = []map[string]any{}
	}

	return nil, out, nil
}

// InitHooksRunInput selects registered init hook scripts to run.
type InitHooksRunInput struct {
	Stage  string `json:"stage"            jsonschema:"BOOT, START, READY or SHUTDOWN"`
	Script string `json:"script,omitempty" jsonschema:"file name of one script as listed by init_hooks_list (default: every script in the stage)"`
}

func (b *backend) initHooksRun(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	in InitHooksRunInput,
) (*mcp.CallToolResult, DataOutput, error) {
	if strings.ContainsAny(in.Script, `/\`) || in.Script == ".." || in.Script == "." {
		return nil, DataOutput{}, fmt.Errorf("%w: script must be a registered file name", ErrBackend)
	}

	return b.data(ctx, http.MethodPost, "/_gopherstack/init/run", nil, in)
}

func (b *backend) initHooks(
	ctx context.Context,
	_ *mcp.CallToolRequest,
	_ InitHooksInput,
) (*mcp.CallToolResult, DataOutput, error) {
	return b.data(ctx, http.MethodGet, "/_localstack/init", nil, nil)
}

func (b *backend) data(
	ctx context.Context,
	method, path string,
	q url.Values,
	in any,
) (*mcp.CallToolResult, DataOutput, error) {
	var out any
	if err := b.call(ctx, method, path, q, in, &out); err != nil {
		return nil, DataOutput{}, err
	}

	if out == nil {
		out = map[string]any{"status": "ok"}
	}

	return nil, DataOutput{Data: out}, nil
}

func setIf(q url.Values, key, val string) {
	if val != "" {
		q.Set(key, val)
	}
}

func clamp(v, def, maxV int) int {
	if v <= 0 {
		return def
	}

	return min(v, maxV)
}

// addTool registers t unless the server is read-only and t can change state.
func addTool[In, Out any](s *mcp.Server, ro bool, t *mcp.Tool, h mcp.ToolHandlerFor[In, Out]) {
	if ro && !t.Annotations.ReadOnlyHint {
		return
	}

	mcp.AddTool(s, t, h)
}

func addTools(s *mcp.Server, b *backend, ro bool) {
	addTool(s, ro, &mcp.Tool{
		Name:        "gopherstack_health",
		Description: "Report the running Gopherstack's version, uptime and the list of enabled and disabled services.",
		Annotations: readOnly("Health and enabled services"),
	}, b.health)
	addTool(s, ro, &mcp.Tool{
		Name:        "snapshot_list",
		Description: "List saved state snapshots (pods) and their versions.",
		Annotations: readOnly("List state snapshots"),
	}, b.snapshotList)
	addTool(s, ro, &mcp.Tool{
		Name:        "snapshot_save",
		Description: "Save the current in-memory state of all (or selected) services as a new snapshot version.",
		Annotations: additive("Save state snapshot"),
	}, b.snapshotSave)
	addTool(s, ro, &mcp.Tool{
		Name: "snapshot_load",
		Description: "Restore a saved snapshot into the running server. " +
			"Replaces current state of the loaded services unless merge is set.",
		Annotations: destructive("Load state snapshot"),
	}, b.snapshotLoad)
	addTool(s, ro, &mcp.Tool{
		Name:        "snapshot_delete",
		Description: "Delete a saved snapshot version, or every version when no version is given.",
		Annotations: destructive("Delete state snapshot"),
	}, b.snapshotDelete)
	addTool(s, ro, &mcp.Tool{
		Name: "reset_state",
		Description: "Erase in-memory state of every service, or only the named service. " +
			"Cannot be undone unless a snapshot was saved.",
		Annotations: destructive("Reset state"),
	}, b.reset)
	addTool(s, ro, &mcp.Tool{
		Name:        "chaos_list_faults",
		Description: "List the active chaos fault-injection rules.",
		Annotations: readOnly("List chaos faults"),
	}, b.faultsList)
	addTool(s, ro, &mcp.Tool{
		Name: "chaos_set_faults",
		Description: "Replace (default) or append chaos fault rules that make matching AWS requests " +
			"fail with a chosen error.",
		Annotations: destructive("Set chaos faults"),
	}, b.faultsSet)
	addTool(s, ro, &mcp.Tool{
		Name:        "chaos_clear_faults",
		Description: "Remove every chaos fault rule.",
		Annotations: destructive("Clear chaos faults"),
	}, b.faultsClear)
	addTool(s, ro, &mcp.Tool{
		Name: "query_traces",
		Description: "Query recent request traces (newest first, at most 100 retained) " +
			"filtered by service, operation and HTTP status. " +
			"Headers and bodies are never returned.",
		Annotations: readOnly("Query request traces"),
	}, b.traces)
	addTool(s, ro, &mcp.Tool{
		Name: "iam_denials",
		Description: "Query IAM policy-evaluation denials (requires --enforce-iam or --iam-audit), " +
			"including the deciding policy and a suggested fix.",
		Annotations: readOnly("IAM policy denials"),
	}, b.iamDenials)
	addTool(s, ro, &mcp.Tool{
		Name: "init_hooks_list",
		Description: "List init hook scripts per stage (BOOT, START, READY, SHUTDOWN) with completion state " +
			"and results. Hooks run at startup only.",
		Annotations: readOnly("List init hooks"),
	}, b.initHooks)

	addTool(s, ro, &mcp.Tool{
		Name: "init_hooks_run",
		Description: "Run init hook scripts already present in the configured stage directories, " +
			"one script by name (from init_hooks_list) or a whole stage. " +
			"Returns exit code and bounded output; arbitrary paths or inline scripts are rejected.",
		Annotations: destructive("Run init hooks"),
	}, b.initHooksRun)
}
