package extensions

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hookManager(t *testing.T, failOpen *bool, hook RequestHook, handler http.HandlerFunc) *Manager {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	m := newBare(t, Spec{Name: "h", Command: []string{"x"}})
	m.cfg.FailOpen = failOpen
	m.cfg.HookTimeout = 2e9

	hook.Services = []string{"s3"}
	reg := Registration{Endpoint: srv.URL, Hooks: Hooks{Request: []RequestHook{hook}}}
	require.NoError(t, m.register(m.exts[0], reg))

	return m
}

//go:fix inline
func TestPreHook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		handler    http.HandlerFunc
		failOpen   *bool
		name       string
		wantHeader string
		hook       RequestHook
		wantStatus int
	}{
		{
			name: "short circuit",
			hook: RequestHook{Pre: "/pre"},
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, `{"respond":{"status":418,"headers":{"X-A":"1"},"body":"dGVhcG90"}}`)
			},
			wantStatus: 418,
		},
		{
			name: "set headers continues",
			hook: RequestHook{Pre: "/pre"},
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, `{"set_headers":{"X-Injected":"yes"}}`)
			},
			wantHeader: "yes",
		},
		{
			name:    "error fails open",
			hook:    RequestHook{Pre: "/pre"},
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
		},
		{
			name:       "error fails closed",
			hook:       RequestHook{Pre: "/pre", FailOpen: new(false)},
			handler:    func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "global fail closed",
			hook:       RequestHook{Pre: "/pre"},
			failOpen:   new(false),
			handler:    func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) },
			wantStatus: http.StatusBadGateway,
		},
		{
			name: "timeout fails closed",
			hook: RequestHook{Pre: "/pre", TimeoutMs: 30, FailOpen: new(false)},
			handler: func(_ http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				<-r.Context().Done()
			},
			wantStatus: http.StatusBadGateway,
		},
		{
			name: "timeout fails open",
			hook: RequestHook{Pre: "/pre", TimeoutMs: 30},
			handler: func(_ http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				<-r.Context().Done()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := hookManager(t, tt.failOpen, tt.hook, tt.handler)
			hs := m.Hooks("S3")
			require.NotNil(t, hs)
			assert.Nil(t, m.Hooks("sqs"))

			req := httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader("payload"))
			resp := hs.Pre(req)

			if tt.wantStatus != 0 {
				require.NotNil(t, resp)
				assert.Equal(t, tt.wantStatus, resp.Status)

				return
			}

			assert.Nil(t, resp)
			assert.Equal(t, tt.wantHeader, req.Header.Get("X-Injected"))

			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			assert.Equal(t, "payload", string(body))
		})
	}
}

func TestPreHookPayload(t *testing.T) {
	t.Parallel()

	var got hookRequest

	m := hookManager(t, nil, RequestHook{Pre: "/pre"}, func(_ http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
	})

	req := httptest.NewRequest(http.MethodPut, "/bkt/key?x=1", strings.NewReader("data"))
	req.Header.Set("X-Custom", "v")
	require.Nil(t, m.Hooks("s3").Pre(req))

	assert.Equal(t, "pre", got.Phase)
	assert.Equal(t, "h", got.Extension)
	assert.Equal(t, "PUT", got.Method)
	assert.Equal(t, "/bkt/key", got.Path)
	assert.Equal(t, "x=1", got.Query)
	assert.Equal(t, "data", string(got.Body))
	assert.Equal(t, "v", got.Headers.Get("X-Custom"))
}

func TestPostHook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reply      string
		wantBody   string
		wantHeader string
		wantStatus int
	}{
		{name: "observe only", reply: ``, wantStatus: 200, wantBody: "orig"},
		{
			name:       "rewrite",
			reply:      `{"status":201,"body":"bmV3","set_headers":{"X-P":"1"}}`,
			wantStatus: 201,
			wantBody:   "new",
			wantHeader: "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var seen hookRequest

			m := hookManager(t, nil, RequestHook{Post: "/post"}, func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&seen)
				_, _ = io.WriteString(w, tt.reply)
			})

			hs := m.Hooks("s3")
			require.True(t, hs.HasPost())

			out := httptest.NewRecorder()
			rec := NewRecorder(out)
			rec.Header().Set("Content-Type", "text/plain")
			_, _ = rec.Write([]byte("orig"))

			require.Nil(t, hs.Post(httptest.NewRequest(http.MethodGet, "/b/k", nil), rec))
			rec.Commit()

			assert.Equal(t, "post", seen.Phase)
			assert.Equal(t, 200, seen.Status)
			assert.Equal(t, "orig", string(seen.ResponseBody))
			assert.Equal(t, tt.wantStatus, out.Code)
			assert.Equal(t, tt.wantBody, out.Body.String())
			assert.Equal(t, tt.wantHeader, out.Header().Get("X-P"))
			assert.Equal(t, "text/plain", out.Header().Get("Content-Type"))
		})
	}
}
