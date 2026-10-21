package client_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	compatcontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/client"
)

type fakeDaemon struct {
	createBody map[string]any
	mu         sync.Mutex
}

func (f *fakeDaemon) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch {
	case strings.HasSuffix(r.URL.Path, "/containers/create"):
		f.mu.Lock()
		_ = json.NewDecoder(r.Body).Decode(&f.createBody)
		f.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"Id":"abc","Warnings":[]}`))
	case strings.HasSuffix(r.URL.Path, "/info"):
		_, _ = w.Write([]byte(`{"Runtimes":{"runc":{"path":"runc"},"runsc":{"path":"/usr/bin/runsc"}}}`))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func newFakeClient(t *testing.T) (*client.Client, *fakeDaemon) {
	t.Helper()

	d := &fakeDaemon{}
	srv := httptest.NewServer(d)
	t.Cleanup(srv.Close)

	c, err := client.NewClientWithOpts(client.WithHost("tcp://" + strings.TrimPrefix(srv.URL, "http://")))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c, d
}

func TestContainerCreate_SendsHostConfigRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want    any
		name    string
		runtime string
	}{
		{name: "runsc", runtime: "runsc", want: "runsc"},
		{name: "default", runtime: "", want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, d := newFakeClient(t)
			cfg := &compatcontainer.Config{Image: "x"}
			hc := &compatcontainer.HostConfig{Runtime: tt.runtime}

			_, err := c.ContainerCreate(t.Context(), cfg, hc, nil, nil, "n")
			require.NoError(t, err)

			d.mu.Lock()
			defer d.mu.Unlock()

			host, ok := d.createBody["HostConfig"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.want, host["Runtime"])
		})
	}
}

func TestRuntimes_ListsRegistered(t *testing.T) {
	t.Parallel()

	c, _ := newFakeClient(t)

	got, err := c.Runtimes(t.Context())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"runc", "runsc"}, got)
}
