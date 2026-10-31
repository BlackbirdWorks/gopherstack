package neptune_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// upstreamRuntime stands in for Gremlin Server: it listens on the published inner port and echoes the path.
type upstreamRuntime struct {
	*fakeEngineRuntime
	servers []*httptest.Server
}

func (u *upstreamRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	parts := strings.Split(spec.Ports[0], ":")

	ln, err := net.Listen("tcp", net.JoinHostPort(parts[0], parts[1]))
	if err != nil {
		return "", err
	}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "upstream:"+r.URL.Path)
	}))
	srv.Listener = ln
	srv.Start()
	u.servers = append(u.servers, srv)

	return u.fakeEngineRuntime.CreateAndStart(ctx, spec)
}

func (u *upstreamRuntime) Close() error {
	for _, s := range u.servers {
		s.Close()
	}

	return nil
}

func cypherDo(t *testing.T, method, target, contentType, body string) (int, map[string]any) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body))
	require.NoError(t, err)

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var out map[string]any
	require.NoError(t, json.Unmarshal(raw, &out), string(raw))

	return resp.StatusCode, out
}

func TestOpenCypherEndpoint(t *testing.T) {
	t.Parallel()

	rt := &upstreamRuntime{fakeEngineRuntime: &fakeEngineRuntime{}}
	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(neptune.EngineConfig{
		Runtime: rt, Probe: func(context.Context, string) error { return nil },
		StartTimeout: time.Minute, OpenCypher: true,
	})
	t.Cleanup(b.Close)

	createEngineCluster(t, b, "1.3.0.0")
	c := waitCluster(t, b, "c1", "available")
	base := "http://" + net.JoinHostPort(c.Endpoint, strconv.Itoa(c.Port))

	require.Len(t, rt.specs, 1)
	assert.True(t, strings.HasPrefix(rt.specs[0].Ports[0], "127.0.0.1:"))
	assert.NotEqual(t, c.Port, publishedPort(t, rt.specs[0]))

	form := "application/x-www-form-urlencoded"

	code, out := cypherDo(t, http.MethodPost, base+"/openCypher", form,
		url.Values{"query": {"CREATE (n:Person {name:$n}) RETURN n"}, "parameters": {`{"n":"Alice"}`}}.Encode())
	require.Equal(t, http.StatusOK, code)

	rows, ok := out["results"].([]any)
	require.True(t, ok)
	require.Len(t, rows, 1)

	n, ok := rows[0].(map[string]any)["n"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "node", n["~entityType"])
	assert.Equal(t, []any{"Person"}, n["~labels"])
	assert.Equal(t, map[string]any{"name": "Alice"}, n["~properties"])

	code, out = cypherDo(t, http.MethodGet, base+"/openCypher?"+url.Values{
		"query": {"MATCH (n:Person) WHERE n.name = $n RETURN count(n) AS c"}, "parameters": {`{"n":"Alice"}`},
	}.Encode(), "", "")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, []any{map[string]any{"c": float64(1)}}, out["results"])

	code, out = cypherDo(t, http.MethodPost, base+"/opencypher", "application/json",
		`{"query":"MATCH (n) RETURN n.name AS name","parameters":{}}`)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, []any{map[string]any{"name": "Alice"}}, out["results"])

	code, out = cypherDo(
		t,
		http.MethodPost,
		base+"/openCypher",
		form,
		url.Values{"query": {"MATCH (n RETURN n"}}.Encode(),
	)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "MalformedQueryException", out["code"])
	assert.NotEmpty(t, out["requestId"])
	assert.NotEmpty(t, out["detailedMessage"])

	code, out = cypherDo(t, http.MethodPost, base+"/openCypher", form, "")
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "MissingParameterException", out["code"])

	resp, err := cypherGet(t, base+"/gremlin")
	require.NoError(t, err)

	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	assert.Equal(t, "upstream:/gremlin", string(body))

	_, err = b.StopDBCluster(t.Context(), "c1")
	require.NoError(t, err)
	waitCluster(t, b, "c1", "stopped")

	resp, err = cypherGet(t, base+"/openCypher?query=RETURN+1")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func cypherGet(t *testing.T, target string) (*http.Response, error) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
	require.NoError(t, err)

	return testclient.New(t).Do(req)
}
