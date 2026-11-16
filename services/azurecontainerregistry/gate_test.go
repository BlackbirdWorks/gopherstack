package azurecontainerregistry_test

import (
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
)

// duringPlan runs push on its own goroutine from inside a planned delete and waits until it either finishes or
// queues on the registry gate, reporting whether it finished while the plan was held.
func duringPlan(e *env, reg string, push func() int) (func() (int, bool), func()) {
	status := make(chan int, 1)

	var once atomic.Bool

	var finished, finishedWhileHeld atomic.Bool

	hook := func() {
		if !once.CompareAndSwap(false, true) {
			return
		}

		go func() {
			status <- push()

			finished.Store(true)
		}()

		for !finished.Load() && e.h.Backend.GateWaiters(reg) == 0 {
			runtime.Gosched()
		}

		finishedWhileHeld.Store(finished.Load())
	}

	collect := func() (int, bool) { return <-status, finishedWhileHeld.Load() }

	return collect, hook
}

func TestRetentionSweepIsAtomicWithTagWrites(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "gatereg", true)
	c := e.client(t, "gatereg", basic(creds.Username, creds.Password1))
	e.h.Backend.SetRetentionSource(func(string) (int, bool) { return 0, true })

	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e.h.Backend.SetClock(func() time.Time { return start })

	img := c.PushImage("app", "v1")

	src := e.registry(t, "gatesrc", true)
	require.Equal(t, img.Digest, e.client(t, "gatesrc", basic(src.Username, src.Password1)).
		PushImage("app", "v1").Digest)
	require.Equal(t, http.StatusOK, c.Do(http.MethodDelete, "/acr/v1/app/_tags/v1", nil, nil).Status)

	retag := func() int {
		err := e.h.Backend.ImportImage(t.Context(), "gatereg", acr.ImportRequest{
			Source: acr.ImportSource{
				Registry: "gatesrc", Image: "app@" + img.Digest, Username: src.Username, Password: src.Password1,
			},
			TargetTags: []string{"app:v1"}, Force: true,
		})
		if err != nil {
			return http.StatusInternalServerError
		}

		return http.StatusCreated
	}

	collect, hook := duringPlan(e, "gatereg", retag)
	e.h.Backend.SetPlannedHook(hook)

	require.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/", nil, nil).Status)

	status, finishedEarly := collect()
	assert.Equal(t, http.StatusCreated, status)
	assert.False(t, finishedEarly, "tag write must wait for the sweep")
	assert.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/app/manifests/v1", nil, nil).Status,
		"the re-created tag survives the sweep")
}

func TestManifestDeleteCollectionIsAtomicWithManifestPush(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "gcgate", true)
	c := e.client(t, "gcgate", basic(creds.Username, creds.Password1))

	a := pushLayered(t, c, "app", "a", "a", []byte("shared layer"))

	push := func() int {
		return c.Do(http.MethodPut, "/v2/app/manifests/again", a.manifest,
			map[string]string{"Content-Type": ocitest.ManifestMediaType}).Status
	}

	collect, hook := duringPlan(e, "gcgate", push)
	e.h.Backend.SetPlannedHook(hook)

	require.Equal(t, http.StatusAccepted,
		c.Do(http.MethodDelete, "/v2/app/manifests/"+a.digest, nil, nil).Status)

	status, finishedEarly := collect()
	assert.False(t, finishedEarly, "manifest push must wait for the collection")

	if status == http.StatusCreated {
		for _, d := range append([]string{a.config}, a.layers...) {
			assert.Equal(t, http.StatusOK,
				c.Do(http.MethodHead, "/v2/app/blobs/"+d, nil, nil).Status, "pushed manifest references %s", d)
		}
	}
}

func TestManifestWriteMalformedInput(t *testing.T) {
	t.Parallel()

	manifest := map[string]string{"Content-Type": ocitest.ManifestMediaType}

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{name: "garbage manifest", method: http.MethodPut, path: "/v2/app/manifests/t", body: "not json", want: 400},
		{name: "empty manifest", method: http.MethodPut, path: "/v2/app/manifests/t", body: "", want: 400},
		{name: "oversize manifest", method: http.MethodPut, path: "/v2/app/manifests/t",
			body: strings.Repeat("a", 4<<20+1), want: 400},
		{name: "delete traversal digest", method: http.MethodDelete,
			path: "/v2/app/manifests/sha256:../../x", want: 404},
		{name: "delete short digest", method: http.MethodDelete, path: "/v2/app/manifests/sha256:a", want: 404},
		{name: "delete unknown manifest", method: http.MethodDelete,
			path: "/v2/app/manifests/sha256:" + strings.Repeat("0", 64), want: 404},
		{name: "acr delete bad digest", method: http.MethodDelete,
			path: "/acr/v1/app/_manifests/sha256:../x", want: 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			creds := e.registry(t, "malreg", true)
			c := e.client(t, "malreg", basic(creds.Username, creds.Password1))

			got := c.Do(tt.method, tt.path, []byte(tt.body), manifest)
			assert.Contains(t, []int{tt.want, http.StatusBadRequest}, got.Status, string(got.Body))
			assert.Less(t, got.Status, 500)
			assert.Empty(t, e.h.Backend.Repositories("malreg"))
		})
	}
}
