package azurearm_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIMGateway_CacheStoreDropsPerClientHeaders(t *testing.T) {
	t.Parallel()

	var hits atomic.Int64

	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Set-Cookie", "session=abc; HttpOnly")
		w.Header().Set("Proxy-Authenticate", "Basic")
		w.Header().Set("X-Shared", "yes")
		_, _ = w.Write([]byte("body"))
	}))
	t.Cleanup(be.Close)

	e := newGWEnv(t)
	e.putAPI(t, "data", "data", be.URL)
	e.putOp(t, "data", "get", "get", "/{id}")
	e.policy(t, "/apis/data", `<policies><inbound><base /><cache-lookup vary-by-developer="false"
		vary-by-developer-groups="false" /></inbound><backend><base /></backend>
		<outbound><base /><cache-store duration="60" /></outbound></policies>`, http.StatusCreated)

	_, _, first := e.call(t, "GET", "/data/a", "", nil)
	assert.Equal(t, "session=abc; HttpOnly", first.Get("Set-Cookie"), "the first client gets its own cookie")

	status, body, second := e.call(t, "GET", "/data/a", "", nil)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "body", body)
	assert.Equal(t, int64(1), hits.Load(), "the second call was served from the cache")
	assert.Equal(t, "yes", second.Get("X-Shared"))
	assert.Empty(t, second.Values("Set-Cookie"), "a cached response never replays another client's cookie")
	assert.Empty(t, second.Values("Proxy-Authenticate"))
}
