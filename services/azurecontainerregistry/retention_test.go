package azurecontainerregistry_test

import (
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetentionDeletesUntaggedManifests(t *testing.T) {
	t.Parallel()

	const day = 24 * time.Hour

	tests := []struct {
		name      string
		wait      time.Duration
		days      int
		enabled   bool
		lock      bool
		retag     bool
		disableAt bool
		gone      bool
	}{
		{name: "disabled keeps", days: 0, enabled: false, wait: 400 * day, gone: false},
		{name: "zero days deletes at once", days: 0, enabled: true, wait: 0, gone: true},
		{name: "before period keeps", days: 7, enabled: true, wait: 7*day - time.Second, gone: false},
		{name: "after period deletes", days: 7, enabled: true, wait: 7 * day, gone: true},
		{name: "delete locked keeps", days: 0, enabled: true, lock: true, wait: day, gone: false},
		{name: "retagged keeps", days: 7, enabled: true, retag: true, wait: 8 * day, gone: false},
		{name: "disabled before period keeps", days: 7, enabled: true, disableAt: true, wait: 8 * day, gone: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			creds := e.registry(t, "retreg", true)
			c := e.client(t, "retreg", basic(creds.Username, creds.Password1))

			var offset atomic.Int64

			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			e.h.Backend.SetClock(func() time.Time { return start.Add(time.Duration(offset.Load())) })

			var enabled atomic.Bool

			enabled.Store(tt.enabled)
			e.h.Backend.SetRetentionSource(func(string) (int, bool) { return tt.days, enabled.Load() })

			img := c.PushImage("app", "v1")

			if tt.lock {
				body := []byte(`{"value":{"deleteEnabled":false}}`)
				require.Equal(t, http.StatusOK,
					c.Do(http.MethodPatch, "/acr/v1/app/_manifests/"+img.Digest, body, nil).Status)
			}

			if tt.retag {
				c.PushImage("app", "v1b")
			}

			require.Equal(t, http.StatusOK, c.Do(http.MethodDelete, "/acr/v1/app/_tags/v1", nil, nil).Status)

			if tt.retag {
				put := c.Do(http.MethodPut, "/v2/app/manifests/v1", img.Body,
					map[string]string{"Content-Type": "application/vnd.docker.distribution.manifest.v2+json"})
				require.Equal(t, http.StatusCreated, put.Status)
			}

			if tt.disableAt {
				enabled.Store(false)
			}

			offset.Store(int64(tt.wait))

			got := c.Do(http.MethodGet, "/v2/app/manifests/"+img.Digest, nil, nil)
			if tt.gone {
				assert.Equal(t, http.StatusNotFound, got.Status)
			} else {
				assert.Equal(t, http.StatusOK, got.Status)
			}
		})
	}
}

func TestRetentionSkipsIndexChildrenAndOCI(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "retidx", true)
	c := e.client(t, "retidx", basic(creds.Username, creds.Password1))
	e.h.Backend.SetRetentionSource(func(string) (int, bool) { return 0, true })

	child := c.PushImage("multi", "child")
	index := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json",` +
		`"manifests":[{"mediaType":"application/vnd.docker.distribution.manifest.v2+json","size":` +
		strconv.Itoa(len(child.Body)) + `,"digest":"` + child.Digest +
		`","platform":{"architecture":"amd64","os":"linux"}}]}`)

	put := c.Do(http.MethodPut, "/v2/multi/manifests/latest", index,
		map[string]string{"Content-Type": "application/vnd.docker.distribution.manifest.list.v2+json"})
	require.Equal(t, http.StatusCreated, put.Status, string(put.Body))
	require.Equal(t, http.StatusOK, c.Do(http.MethodDelete, "/acr/v1/multi/_tags/child", nil, nil).Status)

	got := c.Do(http.MethodGet, "/v2/multi/manifests/"+child.Digest, nil, nil)
	assert.Equal(t, http.StatusOK, got.Status)
}

func TestRetentionScheduleSurvivesRestore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	policy := func(string) (int, bool) { return 7, true }

	first := newEnv(t, dir)
	creds := first.registry(t, "retsnap", true)
	first.h.Backend.SetClock(func() time.Time { return start })
	first.h.Backend.SetRetentionSource(policy)

	c := first.client(t, "retsnap", basic(creds.Username, creds.Password1))
	img := c.PushImage("app", "v1")
	require.Equal(t, http.StatusOK, c.Do(http.MethodDelete, "/acr/v1/app/_tags/v1", nil, nil).Status)

	snap := first.h.Backend.Snapshot(t.Context())
	first.h.Backend.Close()

	second := newEnv(t, dir)
	require.NoError(t, second.h.Backend.Restore(t.Context(), snap))
	second.h.Backend.SetClock(func() time.Time { return start.Add(8 * 24 * time.Hour) })
	second.h.Backend.SetRetentionSource(policy)

	c = second.client(t, "retsnap", basic(creds.Username, creds.Password1))
	assert.Equal(t, http.StatusNotFound, c.Do(http.MethodGet, "/v2/app/manifests/"+img.Digest, nil, nil).Status)
}
