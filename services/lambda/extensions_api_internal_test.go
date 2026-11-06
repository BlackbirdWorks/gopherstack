package lambda

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

const extTestFnARN = "arn:aws:lambda:us-east-1:000000000000:function:ext-fn"

type extHarness struct {
	srv    *runtimeServer
	client *http.Client
	base   string
}

func newExtHarness(t *testing.T) *extHarness {
	t.Helper()

	srv := newRuntimeServer(0)
	srv.exts.setMeta(extensionMeta{
		functionName: "ext-fn", version: "$LATEST", handler: "index.handler",
		functionARN: extTestFnARN, accountID: "000000000000",
	})

	ts := httptest.NewServer(srv.routes())
	t.Cleanup(ts.Close)
	t.Cleanup(func() { srv.stop(context.Background()) })

	return &extHarness{srv: srv, client: testclient.New(t), base: ts.URL}
}

func (h *extHarness) doRaw(
	ctx context.Context, method, path string, hdr map[string]string, body string,
) (int, http.Header, string, error) {
	req, err := http.NewRequestWithContext(ctx, method, h.base+path, strings.NewReader(body))
	if err != nil {
		return 0, nil, "", err
	}

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return 0, nil, "", err
	}

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)

	return resp.StatusCode, resp.Header, string(raw), err
}

func (h *extHarness) do(
	t *testing.T,
	method, path string,
	hdr map[string]string,
	body string,
) (int, http.Header, string) {
	t.Helper()

	status, respHdr, raw, err := h.doRaw(t.Context(), method, path, hdr, body)
	require.NoError(t, err)

	return status, respHdr, raw
}

func (h *extHarness) register(t *testing.T, name string, events ...string) string {
	t.Helper()

	b, err := json.Marshal(map[string]any{"events": events})
	require.NoError(t, err)

	status, hdr, body := h.do(t, http.MethodPost, "/2020-01-01/extension/register",
		map[string]string{headerExtensionName: name}, string(b))
	require.Equal(t, http.StatusOK, status, body)

	return hdr.Get(headerExtensionID)
}

func (h *extHarness) nextAsync(t *testing.T, id string) <-chan string {
	t.Helper()

	out := make(chan string, 1)

	go func() {
		_, _, body, _ := h.doRaw(t.Context(), http.MethodGet, "/2020-01-01/extension/event/next",
			map[string]string{headerExtensionID: id}, "")
		out <- body
	}()

	return out
}

func TestExtensionRegister(t *testing.T) {
	t.Parallel()

	tests := []struct {
		prepare    func(*testing.T, *extHarness)
		name       string
		extName    string
		body       string
		feature    string
		wantType   string
		wantStatus int
	}{
		{name: "ok", extName: "e1", body: `{"events":["INVOKE","SHUTDOWN"]}`, wantStatus: 200},
		{name: "no_body", extName: "e1", wantStatus: 200},
		{name: "account_id", extName: "e1", feature: "accountId", wantStatus: 200},
		{name: "missing_name", body: `{}`, wantStatus: 400, wantType: "Extension.InvalidExtensionName"},
		{
			name:       "bad_event",
			extName:    "e1",
			body:       `{"events":["FOO"]}`,
			wantStatus: 400,
			wantType:   "Extension.InvalidEventType",
		},
		{name: "bad_json", extName: "e1", body: `{`, wantStatus: 400, wantType: "Extension.InvalidRequestBody"},
		{
			name: "duplicate", extName: "dup", wantStatus: 403, wantType: "Extension.AlreadyRegistered",
			prepare: func(t *testing.T, h *extHarness) {
				t.Helper()
				h.register(t, "dup")
			},
		},
		{
			name: "too_many", extName: "eleven", wantStatus: 403, wantType: "Extension.TooManyExtensions",
			prepare: func(t *testing.T, h *extHarness) {
				t.Helper()

				for i := range maxExtensions {
					h.register(t, "e"+string(rune('a'+i)))
				}
			},
		},
		{
			name: "after_init", extName: "late", wantStatus: 403, wantType: "Extension.RegistrationClosed",
			prepare: func(_ *testing.T, h *extHarness) { h.srv.exts.awaitInit(context.Background()) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newExtHarness(t)
			if tt.prepare != nil {
				tt.prepare(t, h)
			}

			hdr := map[string]string{}
			if tt.extName != "" {
				hdr[headerExtensionName] = tt.extName
			}

			if tt.feature != "" {
				hdr[headerExtensionFeature] = tt.feature
			}

			status, respHdr, body := h.do(t, http.MethodPost, "/2020-01-01/extension/register", hdr, tt.body)
			require.Equal(t, tt.wantStatus, status, body)

			if tt.wantStatus != http.StatusOK {
				var e extensionError

				require.NoError(t, json.Unmarshal([]byte(body), &e))
				assert.Equal(t, tt.wantType, e.Type)

				return
			}

			assert.NotEmpty(t, respHdr.Get(headerExtensionID))

			var resp map[string]string

			require.NoError(t, json.Unmarshal([]byte(body), &resp))
			assert.Equal(t, "ext-fn", resp["functionName"])
			assert.Equal(t, "$LATEST", resp["functionVersion"])
			assert.Equal(t, "index.handler", resp["handler"])

			if tt.feature != "" {
				assert.Equal(t, "000000000000", resp["accountId"])
			} else {
				assert.NotContains(t, resp, "accountId")
			}
		})
	}
}

func TestExtensionInvokeFanout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		events []string
		trace  bool
		want   bool
	}{
		{name: "subscribed", events: []string{"INVOKE"}, want: true},
		{name: "subscribed_with_trace", events: []string{"INVOKE", "SHUTDOWN"}, trace: true, want: true},
		{name: "shutdown_only", events: []string{"SHUTDOWN"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newExtHarness(t)
			h.srv.traceActive.Store(tt.trace)
			id := h.register(t, "ext", tt.events...)
			other := h.register(t, "other", "INVOKE")

			type result struct {
				err     error
				payload []byte
			}

			done := make(chan result, 1)

			go func() {
				out, _, _, err := h.srv.invoke(t.Context(), []byte(`{"a":1}`), "", time.Minute)
				done <- result{payload: out, err: err}
			}()

			otherEv := h.nextAsync(t, other)
			var extEv <-chan string
			if tt.want {
				extEv = h.nextAsync(t, id)
			}

			status, hdr, _ := h.do(t, http.MethodGet, "/2018-06-01/runtime/invocation/next", nil, "")
			require.Equal(t, http.StatusOK, status)

			reqID := hdr.Get("Lambda-Runtime-Aws-Request-Id")
			assert.Equal(t, extTestFnARN, hdr.Get("Lambda-Runtime-Invoked-Function-Arn"))

			var ev map[string]any

			require.NoError(t, json.Unmarshal([]byte(<-otherEv), &ev))
			assert.Equal(t, "INVOKE", ev["eventType"])
			assert.Equal(t, reqID, ev["requestId"])
			assert.Equal(t, extTestFnARN, ev["invokedFunctionArn"])
			deadline, _ := ev["deadlineMs"].(float64)
			assert.Equal(t, hdr.Get("Lambda-Runtime-Deadline-Ms"), strconv.FormatInt(int64(deadline), 10))

			if tt.trace {
				tr, ok := ev["tracing"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "X-Amzn-Trace-Id", tr["type"])
				assert.Equal(t, hdr.Get("Lambda-Runtime-Trace-Id"), tr["value"])
			} else {
				assert.NotContains(t, ev, "tracing")
			}

			if tt.want {
				assert.Contains(t, <-extEv, reqID)
			}

			status, _, _ = h.do(t, http.MethodPost, "/2018-06-01/runtime/invocation/"+reqID+"/response", nil, `"ok"`)
			require.Equal(t, http.StatusAccepted, status)

			res := <-done
			require.NoError(t, res.err)
			assert.JSONEq(t, `"ok"`, string(res.payload))

			// The response returned before the extensions finished; the Invoke phase
			// stays open until every subscribed extension calls Next again.
			h.srv.exts.mu.RLock("test")
			open := len(h.srv.exts.waits)
			h.srv.exts.mu.RUnlock()
			assert.Equal(t, 1, open)

			if tt.want {
				_ = h.nextAsync(t, id)
			}

			_ = h.nextAsync(t, other)

			idle := make(chan struct{})

			go func() {
				h.srv.exts.awaitIdle(t.Context())
				close(idle)
			}()

			<-idle

			h.srv.exts.mu.RLock("test")
			assert.Empty(t, h.srv.exts.waits)
			h.srv.exts.mu.RUnlock()
		})
	}
}

func TestExtensionShutdown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reason     string
		wantReason string
	}{
		{name: "default_spindown", wantReason: "spindown"},
		{name: "timeout", reason: shutdownReasonTimeout, wantReason: "timeout"},
		{name: "failure", reason: shutdownReasonFailure, wantReason: "failure"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newExtHarness(t)
			sub := h.register(t, "sub", "INVOKE", "SHUTDOWN")
			h.register(t, "invoke-only", "INVOKE")

			ev := h.nextAsync(t, sub)

			if tt.reason != "" {
				h.srv.setShutdownReason(tt.reason)
			}

			h.srv.stop(t.Context())

			var got map[string]any

			require.NoError(t, json.Unmarshal([]byte(<-ev), &got))
			assert.Equal(t, "SHUTDOWN", got["eventType"])
			assert.Equal(t, tt.wantReason, got["shutdownReason"])
			assert.Positive(t, got["deadlineMs"])
		})
	}
}

func TestExtensionErrorsAndAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		method     string
		errType    string
		wantStatus int
		withID     bool
	}{
		{name: "next_unknown_id", path: "event/next", method: http.MethodGet, wantStatus: 403},
		{
			name:       "init_error_unknown_id",
			path:       "init/error",
			method:     http.MethodPost,
			errType:    "Extension.X",
			wantStatus: 403,
		},
		{name: "init_error_missing_type", path: "init/error", method: http.MethodPost, withID: true, wantStatus: 400},
		{
			name:       "init_error_ok",
			path:       "init/error",
			method:     http.MethodPost,
			withID:     true,
			errType:    "Extension.X",
			wantStatus: 202,
		},
		{
			name:       "exit_error_ok",
			path:       "exit/error",
			method:     http.MethodPost,
			withID:     true,
			errType:    "Extension.X",
			wantStatus: 202,
		},
		{name: "exit_error_missing_type", path: "exit/error", method: http.MethodPost, withID: true, wantStatus: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newExtHarness(t)
			id := h.register(t, "ext", "INVOKE")

			hdr := map[string]string{}
			if tt.withID {
				hdr[headerExtensionID] = id
			} else {
				hdr[headerExtensionID] = "nope"
			}

			if tt.errType != "" {
				hdr[headerExtensionErrorType] = tt.errType
			}

			status, _, body := h.do(t, tt.method, "/2020-01-01/extension/"+tt.path, hdr,
				`{"errorMessage":"boom","errorType":"X","stackTrace":[]}`)
			require.Equal(t, tt.wantStatus, status, body)

			if tt.wantStatus != http.StatusAccepted {
				return
			}

			assert.JSONEq(t, `{"status":"OK"}`, body)

			status, _, _ = h.do(t, http.MethodGet, "/2020-01-01/extension/event/next",
				map[string]string{headerExtensionID: id}, "")
			assert.Equal(t, http.StatusForbidden, status, "an extension that reported an error is closed")
		})
	}
}

func TestExtensionInitGate(t *testing.T) {
	t.Parallel()

	h := newExtHarness(t)
	h.srv.exts.setExternal([]string{"a", "b"})

	released := make(chan struct{})

	go func() {
		h.srv.exts.awaitInit(t.Context())
		close(released)
	}()

	for _, name := range []string{"a", "b"} {
		id := h.register(t, name)

		go func() {
			_, _, _, _ = h.doRaw(t.Context(), http.MethodGet, "/2020-01-01/extension/event/next",
				map[string]string{headerExtensionID: id}, "")
		}()
	}

	<-released

	status, _, _ := h.do(t, http.MethodPost, "/2020-01-01/extension/register",
		map[string]string{headerExtensionName: "c"}, "")
	assert.Equal(t, http.StatusForbidden, status)
}

func TestExtensionInitErrorFailsInvocationAndOpensGate(t *testing.T) {
	t.Parallel()

	h := newExtHarness(t)
	h.srv.exts.setExternal([]string{"bad"})
	id := h.register(t, "bad", "INVOKE")

	go func() {
		_, _, _, _ = h.doRaw(t.Context(), http.MethodGet, "/2020-01-01/extension/event/next",
			map[string]string{headerExtensionID: id}, "")
	}()

	type res struct {
		err     error
		payload []byte
		isErr   bool
	}

	done := make(chan res, 1)

	go func() {
		out, isErr, _, err := h.srv.invoke(t.Context(), []byte(`{}`), "", time.Minute)
		done <- res{payload: out, isErr: isErr, err: err}
	}()

	go func() {
		_, _, _, _ = h.doRaw(t.Context(), http.MethodGet, "/2018-06-01/runtime/invocation/next", nil, "")
	}()

	// Wait for the runtime to pick the invocation up.
	require.Eventually(t, func() bool {
		n := 0
		h.srv.pending.Range(func(_, _ any) bool {
			n++

			return true
		})

		return n == 1
	}, 5*time.Second, time.Millisecond)

	status, _, _ := h.do(t, http.MethodPost, "/2020-01-01/extension/init/error",
		map[string]string{headerExtensionID: id, headerExtensionErrorType: "Extension.ConfigInvalid"},
		`{"errorMessage":"bad config"}`)
	require.Equal(t, http.StatusAccepted, status)

	r := <-done
	require.NoError(t, r.err)
	assert.True(t, r.isErr)
	assert.Contains(t, string(r.payload), "bad config")
}
