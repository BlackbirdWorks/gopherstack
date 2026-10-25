package wfinterp_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

func TestSyntaxErrors(t *testing.T) {
	t.Parallel()

	const retOne = "    - r:\n        return: 1\n"

	tests := []struct {
		name string
		src  string
		want string
	}{
		{"empty", "", "empty"},
		{"not yaml", "main: [", "invalid YAML"},
		{"no main", "other:\n  steps:\n" + retOne, "main subworkflow is missing"},
		{"duplicate step", "main:\n  steps:\n" + retOne + retOne, "duplicate step name"},
		{"unknown field", "main:\n  steps:\n    - a:\n        bogus: 1\n", "unknown step field"},
		{"two primaries", "main:\n  steps:\n    - a:\n        return: 1\n        assign: [{x: 1}]\n", "only one of"},
		{"bad expression", "main:\n  steps:\n    - a:\n        return: '${1 +}'\n", "invalid expression"},
		{"unknown function", "main:\n  steps:\n    - a:\n        return: '${nope(1)}'\n", "not found"},
		{"blocking in expression", "main:\n  steps:\n    - a:\n        return: '${sys.sleep(1)}'\n", "call step"},
		{"next to nowhere", "main:\n  steps:\n    - a:\n        next: ghost\n", "does not exist"},
		{"break outside loop", "main:\n  steps:\n    - a:\n        next: break\n", "only valid inside a for loop"},
		{"call unknown", "main:\n  steps:\n    - a:\n        call: ghost\n", "not found"},
		{
			"subworkflow missing arg",
			"main:\n  steps:\n    - a:\n        call: s\ns:\n  params: [x]\n  steps:\n" + retOne,
			"missing the argument",
		},
		{
			"subworkflow unknown arg",
			"main:\n  steps:\n    - a:\n        call: s\n        args: {y: 1}\ns:\n  steps:\n" + retOne,
			"unknown argument",
		},
		{
			"stdlib missing arg",
			"main:\n  steps:\n    - a:\n        call: text.split\n        args: {source: x}\n",
			"missing required",
		},
		{
			"return in parallel",
			`
main:
  steps:
    - p:
        parallel:
          branches:
            - a:
                steps:
                  - r:
                      return: 1
            - b:
                steps:
                  - r:
                      return: 2
`,
			"not allowed inside a parallel",
		},
		{
			"one branch",
			`
main:
  steps:
    - p:
        parallel:
          branches:
            - a:
                steps:
                  - r:
                      assign: [{x: 1}]
`,
			"branches",
		},
		{
			"exception policy",
			"main:\n  steps:\n    - p:\n        parallel:\n          exception_policy: killAll\n          branches: []\n",
			"continueAll",
		},
		{
			"for without in",
			`
main:
  steps:
    - f:
        for:
          value: v
          steps:
            - a:
                assign: [{x: 1}]
`,
			"exactly one of in or range",
		},
		{"unknown connector", "main:\n  steps:\n    - a:\n        call: googleapis.nope.v1.things.get\n", "not found"},
	}

	eng := wfinterp.NewEngine()
	eng.KnownConnector = func(string) bool { return false }

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := eng.Compile(tt.src)
			require.Error(t, err)

			var se *wfinterp.SyntaxError
			require.ErrorAs(t, err, &se)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestSyntaxErrorPosition(t *testing.T) {
	t.Parallel()

	_, err := wfinterp.NewEngine().
		Compile("main:\n  steps:\n    - ok:\n        assign: [{x: 1}]\n    - broken:\n        bogus: 1\n")

	var se *wfinterp.SyntaxError
	require.ErrorAs(t, err, &se)
	assert.Equal(t, "main", se.Routine)
	assert.Equal(t, "broken", se.Step)
	assert.Equal(t, 6, se.Line)
}

func TestParallel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want any
		name string
		src  string
	}{
		{
			name: "branches write shared variables",
			src: `
main:
  steps:
    - init:
        assign: [{a: 0}, {b: 0}]
    - p:
        parallel:
          shared: [a, b]
          branches:
            - one:
                steps:
                  - s1:
                      assign: [{a: 1}]
            - two:
                steps:
                  - s2:
                      assign: [{b: 2}]
    - r:
        return: ${a * 10 + b}`,
			want: int64(12),
		},
		{
			name: "parallel for accumulates a shared total",
			src: `
main:
  steps:
    - init:
        assign: [{total: 0}]
    - p:
        parallel:
          shared: [total]
          concurrency_limit: 2
          for:
            value: v
            in: [1, 2, 3, 4, 5]
            steps:
              - add:
                  assign: [{total: "${total + v}"}]
    - r:
        return: ${total}`,
			want: int64(15),
		},
		{
			name: "branch locals stay local",
			src: `
main:
  steps:
    - p:
        parallel:
          branches:
            - one:
                steps:
                  - s:
                      assign: [{tmp: 1}]
            - two:
                steps:
                  - s:
                      assign: [{tmp: 2}]
    - c:
        try:
          assign: [{x: "${tmp}"}]
        except:
          as: e
          steps:
            - r:
                return: ${e.tags[0]}`,
			want: "KeyError",
		},
		{
			name: "unshared parent variable cannot be written",
			src: `
main:
  steps:
    - init:
        assign: [{v: 0}]
    - c:
        try:
          steps:
            - p:
                parallel:
                  branches:
                    - one:
                        steps:
                          - s:
                              assign: [{v: 1}]
                    - two:
                        steps:
                          - s:
                              assign: [{w: 1}]
        except:
          as: e
          steps:
            - r:
                return: ${e.tags}`,
			want: []any{"UnhandledBranchError", "RuntimeError"},
		},
		{
			name: "unhandled branch error lists failures and others still run",
			src: `
main:
  steps:
    - init:
        assign: [{ran: 0}]
    - c:
        try:
          steps:
            - p:
                parallel:
                  shared: [ran]
                  for:
                    value: n
                    range: [0, 3]
                    steps:
                      - chk:
                          switch:
                            - condition: ${n % 2 == 1}
                              raise: "odd"
                      - mark:
                          assign: [{ran: "${ran + 1}"}]
        except:
          as: e
          steps:
            - r:
                return: {failed: "${len(e.branches)}", ran: "${ran}", payload: "${e.branches[0].error.payload}"}`,
			want: map[string]any{"failed": int64(2), "ran": int64(2), "payload": "odd"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := run(t, tt.src, nil, nil)
			require.NoError(t, r.err)
			assert.Equal(t, tt.want, r.value)
		})
	}
}

func TestSysCalls(t *testing.T) {
	t.Parallel()

	r := run(t, `
main:
  steps:
    - l1:
        call: sys.log
        args: {text: "hello", severity: "INFO"}
    - l2:
        call: sys.log
        args: {json: {k: 1}}
    - l3:
        call: sys.log
        args: {data: 5}
    - nap:
        call: sys.sleep
        args: {seconds: 30}
    - until:
        call: sys.sleep_until
        args: {time: "2026-01-02T03:05:05Z"}
    - r:
        return: "${sys.now()}"`, nil, nil)
	require.NoError(t, r.err)

	require.Len(t, r.logs.entries, 3)
	assert.Equal(t, "hello", r.logs.entries[0].Text)
	assert.Equal(t, "INFO", r.logs.entries[0].Severity)
	assert.Equal(t, map[string]any{"k": int64(1)}, r.logs.entries[1].JSON)
	assert.Equal(t, "5", r.logs.entries[2].Text)
	assert.Equal(t, []time.Duration{30 * time.Second, 30 * time.Second}, r.clock.slept())
}

func TestUncaughtErrorReportsPosition(t *testing.T) {
	t.Parallel()

	r := run(t, `
main:
  steps:
    - fine:
        assign: [{x: 1}]
    - boom:
        raise: {message: "bad", code: 9}`, nil, nil)

	var werr *wfinterp.Error
	require.ErrorAs(t, r.err, &werr)
	assert.JSONEq(t, `{"message":"bad","code":9}`, werr.Payload())
	assert.Contains(t, werr.Context(), `in step "boom", routine "main"`)
	assert.Equal(t, "boom", werr.Stack[0].Step)
	assert.False(t, werr.Cancelled())
}

func TestStepAndDepthLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "infinite loop hits the step limit",
			src:  "main:\n  steps:\n    - a:\n        assign: [{x: 1}]\n        next: a\n",
			want: "ResourceLimitError",
		},
		{
			name: "infinite recursion hits the depth limit",
			src: "main:\n  steps:\n    - a:\n        call: sub\n" +
				"sub:\n  steps:\n    - a:\n        call: sub\n",
			want: "RecursionError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := run(t, tt.src, nil, nil)

			var werr *wfinterp.Error
			require.ErrorAs(t, r.err, &werr)
			assert.Contains(t, werr.Payload(), tt.want)
		})
	}
}

type blockingClock struct{ fakeClock }

func (*blockingClock) Sleep(ctx context.Context, _ time.Duration) error {
	<-ctx.Done()

	return ctx.Err()
}

func TestCancellationInterruptsSleep(t *testing.T) {
	t.Parallel()

	eng := wfinterp.NewEngine()
	prog, err := eng.Compile(
		`
main:
  steps:
    - c:
        try:
          call: sys.sleep
          args: {seconds: 100}
        except:
          as: e
          steps:
            - r:
                return: caught
`,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	host := &wfinterp.Host{
		Clock: &blockingClock{},
		Progress: func(f []wfinterp.Frame) {
			select {
			case started <- struct{}{}:
			default:
			}
			_ = f
		},
	}

	done := make(chan error, 1)

	go func() {
		_, runErr := eng.Run(ctx, prog, nil, host)
		done <- runErr
	}()

	<-started
	cancel()

	var werr *wfinterp.Error
	require.ErrorAs(t, <-done, &werr)
	assert.True(t, werr.Cancelled(), "a cancelled execution is not catchable")
}

func TestHTTPCalls(t *testing.T) {
	t.Parallel()

	var flaky atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(
				[]byte(`{"n":5,"q":"` + r.URL.Query().Get("a") + `","auth":"` + r.Header.Get("Authorization") + `"}`),
			)
		case "/echo":
			buf := make([]byte, 64)
			n, _ := r.Body.Read(buf)
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(r.Method + ":" + r.Header.Get("Content-Type") + ":" + string(buf[:n])))
		case "/flaky":
			if flaky.Add(1) < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)

				return
			}

			_, _ = w.Write([]byte("fine"))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("nope"))
		}
	}))
	t.Cleanup(srv.Close)

	host := func(h *wfinterp.Host) {
		h.HTTP = srv.Client()
		h.Token = func(kind, aud, scope, _ string) string { return kind + "|" + aud + "|" + scope }
	}

	tests := []struct {
		want any
		name string
		step string
	}{
		{
			name: "get json with query and auth",
			step: `
    - c:
        call: http.get
        args:
          url: URL/json
          query: {a: x}
          auth: {type: OIDC, audience: aud1}
        result: res
    - r:
        return: {code: "${res.code}", n: "${res.body.n}", q: "${res.body.q}", auth: "${res.body.auth}"}`,
			want: map[string]any{"code": int64(200), "n": int64(5), "q": "x", "auth": "Bearer OIDC|aud1|"},
		},
		{
			name: "post a map as json",
			step: `
    - c:
        call: http.post
        args: {url: URL/echo, body: {k: 1}}
        result: res
    - r:
        return: ${res.body}`,
			want: `POST:application/json:{"k":1}`,
		},
		{
			name: "request with method and string body",
			step: `
    - c:
        call: http.request
        args: {method: PUT, url: URL/echo, body: "raw"}
        result: res
    - r:
        return: ${res.body}`,
			want: "PUT:text/plain; charset=utf-8:raw",
		},
		{
			name: "http error is catchable with code and tags",
			step: `
    - c:
        try:
          call: http.get
          args: {url: URL/missing}
        except:
          as: e
          steps:
            - r:
                return: {code: "${e.code}", tags: "${e.tags}", body: "${e.body}"}`,
			want: map[string]any{"code": int64(404), "tags": []any{"HttpError"}, "body": "nope"},
		},
		{
			name: "default retry rides out 503s",
			step: `
    - c:
        try:
          call: http.get
          args: {url: URL/flaky}
          result: res
        retry: ${http.default_retry}
    - r:
        return: ${res.body}`,
			want: "fine",
		},
		{
			name: "connection failure is tagged",
			step: `
    - c:
        try:
          call: http.get
          args: {url: "http://127.0.0.1:1/x"}
        except:
          as: e
          steps:
            - r:
                return: ${e.tags[0]}`,
			want: "ConnectionFailedError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := "main:\n  steps:\n" + replaceAll(tt.step, "URL", srv.URL)
			r := run(t, src, nil, host)
			require.NoError(t, r.err)
			assert.Equal(t, tt.want, r.value)
		})
	}
}

func TestCallbacks(t *testing.T) {
	t.Parallel()

	src := `
main:
  steps:
    - mk:
        call: events.create_callback_endpoint
        args: {http_callback_method: POST}
        result: cb
    - wait:
        call: events.await_callback
        args: {callback: "${cb}", timeout: 30}
        result: got
    - r:
        return: {url: "${cb.url}", body: "${got.http_request.body}"}`

	r := run(t, src, nil, func(h *wfinterp.Host) {
		h.CreateCallback = func(context.Context, string) (string, error) { return "http://cb.test/1", nil }
		h.AwaitCallback = func(context.Context, string) (map[string]any, error) {
			return map[string]any{"body": map[string]any{"ok": true}}, nil
		}
	})
	require.NoError(t, r.err)
	assert.Equal(t, map[string]any{"url": "http://cb.test/1", "body": map[string]any{"ok": true}}, r.value)
}

func TestConnectorDispatch(t *testing.T) {
	t.Parallel()

	r := run(t, `
main:
  steps:
    - c:
        call: googleapis.fake.v1.things.get
        args: {name: "n1"}
        result: out
    - r:
        return: ${out}`, nil, func(h *wfinterp.Host) {
		h.Connector = func(_ context.Context, name string, args map[string]any) (any, bool, error) {
			return map[string]any{"name": name, "arg": args["name"]}, true, nil
		}
	})
	require.NoError(t, r.err)
	assert.Equal(t, map[string]any{"name": "googleapis.fake.v1.things.get", "arg": "n1"}, r.value)
}
