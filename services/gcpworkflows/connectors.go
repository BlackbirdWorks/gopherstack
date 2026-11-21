package gcpworkflows

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpdispatch"
	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

const (
	svcPubSub        = "pubsub"
	svcSecretManager = "secretmanager"
	svcFirestore     = "firestore"
	svcStorage       = "storage"
	svcCloudTasks    = "cloudtasks"
	svcKMS           = "kms"
	svcCloudBuild    = "cloudbuild"
	svcLogging       = "logging"
	svcWorkflowExec  = "workflowexecutions"
	maxConnectorBody = 8 << 20
	bodyArg          = "body"
)

// connDef maps one connector onto a REST method of an emulator. {+name} placeholders take a resource name
// whose slashes are kept, {name} placeholders are escaped as one path segment; every other argument is a
// query parameter, and body is the JSON request body.
type connDef struct {
	method  string
	service string
	path    string
	lro     bool
}

const (
	pathName   = "/v1/{+name}"
	pathColl   = "/v1/{+parent}/{collectionId}"
	pathBucket = "/storage/v1/b/{bucket}"
	pathObject = pathBucket + "/o/{object}"
	smPrefix   = "googleapis.secretmanager.v1.projects.secrets."
	psPrefix   = "googleapis.pubsub.v1.projects.topics."
	fsPrefix   = "googleapis.firestore.v1.projects.databases.documents."
	storageV1  = "googleapis.storage.v1."
	ctPrefix   = "googleapis.cloudtasks.v2.projects.locations.queues."
	cbPrefix   = "googleapis.cloudbuild.v1.projects.builds."
	wePrefix   = "googleapis.workflowexecutions.v1.projects.locations.workflows.executions."
	pathV2Name = "/v2/{+name}"
	pathBuild  = "/v1/projects/{projectId}/builds"
)

func connectors() map[string]connDef {
	const (
		get   = http.MethodGet
		post  = http.MethodPost
		del   = http.MethodDelete
		patch = http.MethodPatch
	)

	return map[string]connDef{
		psPrefix + "publish": {post, svcPubSub, "/v1/{+topic}:publish", false},
		psPrefix + "create":  {http.MethodPut, svcPubSub, pathName, false},
		psPrefix + "get":     {get, svcPubSub, "/v1/{+topic}", false},
		psPrefix + "delete":  {del, svcPubSub, "/v1/{+topic}", false},

		smPrefix + "create":          {post, svcSecretManager, "/v1/{+parent}/secrets", false},
		smPrefix + "get":             {get, svcSecretManager, pathName, false},
		smPrefix + "delete":          {del, svcSecretManager, pathName, false},
		smPrefix + "addVersion":      {post, svcSecretManager, "/v1/{+parent}:addVersion", false},
		smPrefix + "versions.access": {get, svcSecretManager, "/v1/{+name}:access", false},
		smPrefix + "versions.get":    {get, svcSecretManager, pathName, false},
		fsPrefix + "get":             {get, svcFirestore, pathName, false},
		fsPrefix + "list":            {get, svcFirestore, pathColl, false},
		fsPrefix + "patch":           {patch, svcFirestore, pathName, false},
		fsPrefix + "delete":          {del, svcFirestore, pathName, false},
		fsPrefix + "createDocument":  {post, svcFirestore, pathColl, false},
		storageV1 + "buckets.get":    {get, svcStorage, pathBucket, false},
		storageV1 + "buckets.insert": {post, svcStorage, "/storage/v1/b", false},
		storageV1 + "objects.get":    {get, svcStorage, pathObject, false},
		storageV1 + "objects.list":   {get, svcStorage, pathBucket + "/o", false},
		storageV1 + "objects.delete": {del, svcStorage, pathObject, false},
		storageV1 + "objects.insert": {post, svcStorage, "/upload/storage/v1/b/{bucket}/o", false},

		ctPrefix + "create":       {post, svcCloudTasks, "/v2/{+parent}/queues", false},
		ctPrefix + "get":          {get, svcCloudTasks, pathV2Name, false},
		ctPrefix + "list":         {get, svcCloudTasks, "/v2/{+parent}/queues", false},
		ctPrefix + "delete":       {del, svcCloudTasks, pathV2Name, false},
		ctPrefix + "pause":        {post, svcCloudTasks, "/v2/{+name}:pause", false},
		ctPrefix + "resume":       {post, svcCloudTasks, "/v2/{+name}:resume", false},
		ctPrefix + "purge":        {post, svcCloudTasks, "/v2/{+name}:purge", false},
		ctPrefix + "tasks.create": {post, svcCloudTasks, "/v2/{+parent}/tasks", false},
		ctPrefix + "tasks.get":    {get, svcCloudTasks, pathV2Name, false},
		ctPrefix + "tasks.list":   {get, svcCloudTasks, "/v2/{+parent}/tasks", false},
		ctPrefix + "tasks.delete": {del, svcCloudTasks, pathV2Name, false},
		ctPrefix + "tasks.run":    {post, svcCloudTasks, "/v2/{+name}:run", false},

		cbPrefix + "create": {post, svcCloudBuild, pathBuild, true},
		cbPrefix + "get":    {get, svcCloudBuild, pathBuild + "/{id}", false},
		cbPrefix + "list":   {get, svcCloudBuild, pathBuild, false},
		cbPrefix + "cancel": {post, svcCloudBuild, pathBuild + "/{id}:cancel", false},

		wePrefix + "create": {post, svcWorkflowExec, "/v1/{+parent}/executions", false},
		wePrefix + "get":    {get, svcWorkflowExec, pathName, false},
		wePrefix + "list":   {get, svcWorkflowExec, "/v1/{+parent}/executions", false},
		wePrefix + "cancel": {post, svcWorkflowExec, "/v1/{+name}:cancel", false},
	}
}

func isConnector(name string) bool {
	_, ok := connectors()[name]

	return ok
}

var errMissingArg = errors.New("missing string argument")

var placeholderRe = regexp.MustCompile(`\{(\+?)([A-Za-z]+)\}`)

func expandPath(tmpl string, args map[string]any) (string, map[string]any, error) {
	rest := maps.Clone(args)

	var perr error

	out := placeholderRe.ReplaceAllStringFunc(tmpl, func(m string) string {
		sub := placeholderRe.FindStringSubmatch(m)
		v, ok := rest[sub[2]].(string)

		if !ok || v == "" {
			perr = fmt.Errorf("%w %q", errMissingArg, sub[2])

			return ""
		}

		delete(rest, sub[2])

		if sub[1] == "+" {
			segs := strings.Split(v, "/")
			for i, s := range segs {
				segs[i] = url.PathEscape(s)
			}

			return strings.Join(segs, "/")
		}

		return url.PathEscape(v)
	})

	if perr != nil {
		return "", nil, perr
	}

	return out, rest, nil
}

// serviceBase resolves the base URL of an emulated service; Workflow Executions is this emulator's own listener.
func (b *InMemoryBackend) serviceBase(svc string) string {
	b.mu.RLock("serviceBase")
	defer b.mu.RUnlock()

	if svc == svcWorkflowExec {
		return b.pushBase
	}

	if b.endpoints == nil {
		return ""
	}

	return b.endpoints.BaseURL(svc)
}

const (
	connectorParams    = "connector_params"
	defaultLROTimeout  = 1800 * time.Second
	maxLROTimeout      = 365 * 24 * time.Hour
	defaultPollInitial = time.Second
	defaultPollMax     = 60 * time.Second
	defaultPollFactor  = 1.25
)

// pollPolicy is connector_params.polling_policy; the defaults are the documented ones.
type pollPolicy struct {
	initial, max time.Duration
	factor       float64
	timeout      time.Duration
	skip         bool
}

func secondsArg(m map[string]any, key string, def time.Duration) time.Duration {
	f, ok := wfinterpNumber(m[key])
	if !ok || f <= 0 {
		return def
	}

	return time.Duration(f * float64(time.Second))
}

func wfinterpNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func parsePollPolicy(args map[string]any) (pollPolicy, map[string]any) {
	rest := maps.Clone(args)
	cp, _ := rest[connectorParams].(map[string]any)
	delete(rest, connectorParams)

	pp := pollPolicy{
		initial: defaultPollInitial, max: defaultPollMax, factor: defaultPollFactor,
		timeout: min(secondsArg(cp, "timeout", defaultLROTimeout), maxLROTimeout),
	}
	pp.skip, _ = cp["skip_polling"].(bool)

	if pol, ok := cp["polling_policy"].(map[string]any); ok {
		pp.initial = secondsArg(pol, "initial_delay", pp.initial)
		pp.max = secondsArg(pol, "max_delay", pp.max)

		if f, fok := wfinterpNumber(pol["multiplier"]); fok && f >= 1 {
			pp.factor = f
		}
	}

	return pp, rest
}

func (b *InMemoryBackend) callConnector(
	ctx context.Context, info runInfo, name string, args map[string]any,
) (any, bool, error) {
	def, ok := connectors()[name]
	if !ok {
		return nil, false, nil
	}

	base := b.serviceBase(def.service)
	if base == "" {
		return nil, true, wfinterp.NewError(
			fmt.Sprintf("%s is not available: no %s emulator is connected", name, def.service),
			wfinterp.TagOperationError,
		)
	}

	pp, args := parsePollPolicy(args)

	path, rest, err := expandPath(def.path, args)
	if err != nil {
		return nil, true, wfinterp.NewError(fmt.Sprintf("%s: %v", name, err), wfinterp.TagTypeError)
	}

	b.mu.RLock("callConnector")
	client, clock := b.client, b.clock
	b.mu.RUnlock()

	token := "Bearer " + gcpdispatch.OAuthToken(accountEmail(info.account), "")

	req, err := buildConnectorRequest(ctx, def, base+path, name, rest)
	if err != nil {
		return nil, true, err
	}

	req.Header.Set("Authorization", token)

	res, err := doConnector(client, req)
	if err != nil || !def.lro || pp.skip {
		return res, true, err
	}

	res, err = pollOperation(ctx, client, clock, base, token, res, pp)

	return res, true, err
}

// pollOperation waits for a long-running operation with exponential backoff on the workflow clock and returns its
// response, or raises its error; an operation still running after the timeout raises TimeoutError.
func pollOperation(
	ctx context.Context, client *http.Client, clock wfinterp.Clock, base, token string, first any, pp pollPolicy,
) (any, error) {
	op, _ := first.(map[string]any)
	name, _ := op["name"].(string)
	delay, waited := pp.initial, time.Duration(0)

	for {
		if done, _ := op["done"].(bool); done {
			return operationResult(op)
		}

		if name == "" {
			return nil, wfinterp.NewError(
				"connector returned an operation without a name", wfinterp.TagResponseTypeError)
		}

		if waited >= pp.timeout {
			return nil, wfinterp.NewError("timed out waiting for the operation "+name, wfinterp.TagTimeoutError)
		}

		step := min(delay, pp.timeout-waited)
		if err := clock.Sleep(ctx, step); err != nil {
			return nil, err //nolint:wrapcheck // cancellation passes through
		}

		waited += step
		delay = min(time.Duration(float64(delay)*pp.factor), pp.max)

		path, _, perr := expandPath("/v1/{+name}", map[string]any{"name": name})
		if perr != nil {
			return nil, wfinterp.NewError(perr.Error(), wfinterp.TagTypeError)
		}

		req, rerr := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
		if rerr != nil {
			return nil, wfinterp.NewError(rerr.Error(), wfinterp.TagValueError)
		}

		req.Header.Set("Authorization", token)

		got, err := doConnector(client, req)
		if err != nil {
			return nil, err
		}

		op, _ = got.(map[string]any)
	}
}

func operationResult(op map[string]any) (any, error) {
	if e, ok := op["error"].(map[string]any); ok {
		msg, _ := e["message"].(string)
		werr := wfinterp.NewError(msg, wfinterp.TagOperationError)

		return nil, werr
	}

	if resp, ok := op["response"]; ok {
		return resp, nil
	}

	return map[string]any{}, nil
}

func buildConnectorRequest(
	ctx context.Context,
	def connDef,
	target, name string,
	rest map[string]any,
) (*http.Request, error) {
	q := url.Values{}
	if def.service == svcStorage && strings.HasPrefix(def.path, "/upload/") {
		q.Set("uploadType", "media")
	}

	var payload any

	for k, v := range rest {
		if k == bodyArg {
			payload = v

			continue
		}

		if l, isList := v.([]any); isList {
			for _, e := range l {
				q.Add(k, fmt.Sprint(e))
			}

			continue
		}

		q.Set(k, fmt.Sprint(v))
	}

	var (
		body  io.Reader
		ctype string
	)

	switch p := payload.(type) {
	case nil:
	case string:
		body, ctype = strings.NewReader(p), "text/plain; charset=utf-8"
	case []byte:
		body, ctype = bytes.NewReader(p), "application/octet-stream"
	default:
		raw, err := wfinterp.ToJSON(p)
		if err != nil {
			return nil, wfinterp.NewError(fmt.Sprintf("%s: body: %v", name, err), wfinterp.TagValueError)
		}

		body, ctype = bytes.NewReader(raw), "application/json"
	}

	if len(q) > 0 {
		target += "?" + q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, def.method, target, body)
	if err != nil {
		return nil, wfinterp.NewError(fmt.Sprintf("%s: %v", name, err), wfinterp.TagValueError)
	}

	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}

	return req, nil
}

func doConnector(client *http.Client, req *http.Request) (any, error) {
	resp, err := client.Do(req)
	if err != nil {
		if req.Context().Err() != nil {
			return nil, req.Context().Err()
		}

		return nil, wfinterp.NewError("connector connection failed: "+err.Error(), wfinterp.TagConnectionFailed)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxConnectorBody))
	if err != nil {
		return nil, wfinterp.NewError("connector read failed: "+err.Error(), wfinterp.TagConnectionError)
	}

	var body any

	switch ct := resp.Header.Get("Content-Type"); {
	case len(raw) == 0:
		body = map[string]any{}
	case strings.Contains(ct, "json"):
		if body, err = wfinterp.ParseJSON(raw); err != nil {
			return nil, wfinterp.NewError("connector returned invalid JSON", wfinterp.TagResponseTypeError)
		}
	case strings.HasPrefix(ct, "text/"):
		body = string(raw)
	default:
		body = raw
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return nil, wfinterp.NewHTTPError(resp.StatusCode, resp.Header, body, errorMessage(body))
	}

	return body, nil
}

func errorMessage(body any) string {
	m, _ := body.(map[string]any)
	e, _ := m["error"].(map[string]any)
	msg, _ := e["message"].(string)

	return msg
}
