package app

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// kvPersistSvc is a minimal persistable service: POST /<name>/<key> appends the body to key.
type kvPersistSvc struct {
	vals map[string]string
	extTestService
	mu sync.Mutex
}

func newKV(name string) *kvPersistSvc {
	kv := &kvPersistSvc{vals: map[string]string{}}
	kv.name, kv.path = name, "/"+name+"/"

	return kv
}

func (k *kvPersistSvc) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		body, _ := io.ReadAll(c.Request().Body)

		k.mu.Lock()
		k.vals[strings.TrimPrefix(c.Request().URL.Path, k.path)] += string(body)
		k.mu.Unlock()

		return c.String(http.StatusOK, "ok")
	}
}

func (k *kvPersistSvc) Snapshot(context.Context) []byte {
	k.mu.Lock()
	defer k.mu.Unlock()

	b, _ := json.Marshal(k.vals)

	return b
}

func (k *kvPersistSvc) Restore(_ context.Context, data []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	return json.Unmarshal(data, &k.vals)
}

func (k *kvPersistSvc) getA() string {
	k.mu.Lock()
	defer k.mu.Unlock()

	return k.vals["a"]
}

type persistHarness struct {
	rt  *persistRuntime
	m   *persistence.Manager
	e   *echo.Echo
	kvs []*kvPersistSvc
}

func bootPersist(t *testing.T, dir string, s PersistModeSettings, names ...string) *persistHarness {
	t.Helper()

	cli := &CLI{Persist: true, DataDir: dir, PersistModes: s}
	store, err := cli.createPersistenceStore()
	require.NoError(t, err)

	m := persistence.NewManager(t.Context(), store)
	rt, err := setupPersistModes(t.Context(), cli, m)
	require.NoError(t, err)

	h := &persistHarness{rt: rt, m: m, e: echo.New()}

	regs := make([]service.Registerable, 0, len(names))

	for _, n := range names {
		kv := newKV(n)
		h.kvs = append(h.kvs, kv)
		regs = append(regs, kv)
		m.Register(n, kv)
	}

	m.RestoreAll(t.Context())

	h.e.Use(persistenceMiddleware(m, regs))
	h.e.Any("/*", func(c *echo.Context) error {
		for _, kv := range h.kvs {
			if kv.RouteMatcher()(c) {
				return kv.Handler()(c)
			}
		}

		return c.NoContent(http.StatusNotFound)
	})

	return h
}

func (h *persistHarness) post(t *testing.T, path, body string) {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(body))
	h.e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

// crash stops background loops without the clean-shutdown checkpoint.
func (h *persistHarness) crash() {
	for _, s := range h.rt.stops {
		s()
	}
}

func TestPersistModesCrashRecovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		before func(h *persistHarness)
		name   string
		mode   string
	}{
		{
			name:   "hybrid",
			mode:   "hybrid",
			before: func(h *persistHarness) { h.m.FlushDirty(context.Background()) },
		},
		{
			name:   "snapshot",
			mode:   "snapshot",
			before: func(h *persistHarness) { h.m.SaveAll(context.Background()) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			s := PersistModeSettings{
				PersistMode:          tt.mode,
				PersistFlushInterval: 1 << 40,
			}

			h := bootPersist(t, dir, s, "kv")
			h.post(t, "/kv/a", "1")
			h.post(t, "/kv/a", "2")
			tt.before(h)
			h.crash()

			h2 := bootPersist(t, dir, s, "kv")
			assert.Equal(t, "12", h2.kvs[0].getA())
			h2.crash()
		})
	}
}

func TestPersistModesServiceFilter(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s := PersistModeSettings{
		PersistMode:     "snapshot",
		PersistServices: []string{"KV1"},
		PersistExclude:  []string{"kv3"},
	}

	h := bootPersist(t, dir, s, "kv1", "kv2", "kv3")
	for _, n := range []string{"kv1", "kv2", "kv3"} {
		h.post(t, "/"+n+"/a", "x")
	}

	h.m.SaveAll(t.Context())
	h.crash()

	h2 := bootPersist(t, dir, s, "kv1", "kv2", "kv3")
	assert.Equal(t, "x", h2.kvs[0].getA())
	assert.Empty(t, h2.kvs[1].getA())
	assert.Empty(t, h2.kvs[2].getA())
}

func TestPersistModesPerServiceOverride(t *testing.T) {
	t.Parallel()

	s := PersistModeSettings{PersistMode: "snapshot", PersistServiceMode: []string{"KV=hybrid"}}
	pol, err := s.policy()
	require.NoError(t, err)
	assert.True(t, pol.Uses(persistence.ModeHybrid))

	mode, on := pol.ModeFor("kv")
	assert.True(t, on)
	assert.Equal(t, persistence.ModeHybrid, mode)

	_, err = (PersistModeSettings{PersistMode: "bogus"}).policy()
	require.Error(t, err)
}

func TestIsReadOp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		query  string
		want   bool
	}{
		{name: "ddb query", target: "DynamoDB_20120810.Query", want: true},
		{name: "ddb put", target: "DynamoDB_20120810.PutItem"},
		{name: "ddb batch get", target: "DynamoDB_20120810.BatchGetItem", want: true},
		{name: "query action list", query: "Action=ListQueues", want: true},
		{name: "query action create", query: "Action=CreateQueue"},
		{name: "no op", target: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/?"+tt.query, nil)
			if tt.target != "" {
				r.Header.Set("X-Amz-Target", tt.target)
			}

			assert.Equal(t, tt.want, isReadOp(r))
		})
	}
}
