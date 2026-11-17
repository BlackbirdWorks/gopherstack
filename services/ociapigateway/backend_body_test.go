package ociapigateway_test

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func httpRoute(backendURL, extra string) string {
	return `{"routes":[{"path":"/x","methods":["GET"],"backend":{"type":"HTTP_BACKEND","url":"` + backendURL + `"` +
		extra + `}}]}`
}

func (e *env) tryStream(t *testing.T, gwID, path string) (*http.Response, error) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, e.base+"/_apigw/"+gwID+path, nil)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	if err == nil {
		t.Cleanup(func() { _ = resp.Body.Close() })
	}

	return resp, err
}

func (e *env) openStream(t *testing.T, gwID, path string) *http.Response {
	t.Helper()

	resp, err := e.tryStream(t, gwID, path)
	require.NoError(t, err)

	return resp
}

func TestHTTPBackendStreamsEachChunk(t *testing.T) {
	t.Parallel()

	proceed := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("one\n"))
		w.(http.Flusher).Flush()

		select {
		case <-proceed:
			_, _ = w.Write([]byte("two\n"))
		case <-t.Context().Done():
		}
	}))
	t.Cleanup(backend.Close)

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "stream")
	e.mkDeployment(t, *gw.Id, "/s", httpRoute(backend.URL, ""))

	resp := e.openStream(t, *gw.Id, "/s/x")
	rd := bufio.NewReader(resp.Body)

	line, err := rd.ReadString('\n')
	require.NoError(t, err)
	assert.Equal(t, "one\n", line, "the first chunk arrives before the back end finishes")

	close(proceed)

	line, err = rd.ReadString('\n')
	require.NoError(t, err)
	assert.Equal(t, "two\n", line)
}

func TestHTTPBackendBodyStallTimesOut(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("partial"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(backend.Close)

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "stall")
	e.mkDeployment(t, *gw.Id, "/s", httpRoute(backend.URL, `,"readTimeoutInSeconds":0.1`))

	resp := e.openStream(t, *gw.Id, "/s/x")
	got, err := io.ReadAll(resp.Body)

	require.Error(t, err, "the stalled body is cut off instead of hanging")
	assert.Equal(t, "partial", string(got))
}

func TestHTTPBackendResponseCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		size    int
		limit   int64
		wantErr bool
	}{
		{name: "under", size: 1000, limit: 2000},
		{name: "exact", size: 2000, limit: 2000},
		{name: "over", size: 5000, limit: 2000, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(strings.Repeat("a", tt.size)))
			}))
			t.Cleanup(backend.Close)

			e := newEnv(t, envOpts{})
			e.backend.SetBackendResponseLimit(tt.limit)

			gw := e.mkGateway(t, "cap")
			e.mkDeployment(t, *gw.Id, "/s", httpRoute(backend.URL, ""))

			resp, err := e.tryStream(t, *gw.Id, "/s/x")
			if tt.wantErr {
				if err == nil {
					got, rerr := io.ReadAll(resp.Body)
					require.Error(t, rerr)
					assert.Less(t, len(got), tt.size)
				}

				return
			}

			require.NoError(t, err)

			got, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Len(t, got, tt.size)
		})
	}
}

func TestHTTPBackendHopHeadersDropped(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		h := w.Header()
		h.Set("Keep-Alive", "timeout=5")
		h.Set("Proxy-Authenticate", "Basic")
		h.Set("Upgrade", "h2c")
		h.Set("X-Kept", "yes")
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(backend.Close)

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "hop")
	e.mkDeployment(t, *gw.Id, "/s", httpRoute(backend.URL, ""))

	resp := e.openStream(t, *gw.Id, "/s/x")
	assert.Equal(t, "yes", resp.Header.Get("X-Kept"))

	for _, h := range []string{"Keep-Alive", "Proxy-Authenticate", "Upgrade"} {
		assert.Empty(t, resp.Header.Get(h), h)
	}
}

func TestHTTPBackendBodylessStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		status int
	}{
		{name: "no content", method: http.MethodGet, status: http.StatusNoContent},
		{name: "not modified", method: http.MethodGet, status: http.StatusNotModified},
		{name: "head", method: http.MethodHead, status: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
			}))
			t.Cleanup(backend.Close)

			e := newEnv(t, envOpts{})
			gw := e.mkGateway(t, "nobody")
			spec := `{"routes":[{"path":"/x","methods":["GET","HEAD"],"backend":{"type":"HTTP_BACKEND","url":"` +
				backend.URL + `"}}]}`
			e.mkDeployment(t, *gw.Id, "/s", spec)

			req, err := http.NewRequestWithContext(
				t.Context(), tt.method, e.base+"/_apigw/"+*gw.Id+"/s/x", nil,
			)
			require.NoError(t, err)

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.status, resp.StatusCode)
			assert.Empty(t, body)
		})
	}
}
