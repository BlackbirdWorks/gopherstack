package cosmosdb_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

// liveServer is a real listener with master-key auth enforced; raw requests are signed with the handler's key.
type liveServer struct {
	t       *testing.T
	h       *cosmosdb.Handler
	backend *cosmosdb.InMemoryBackend
	base    string
}

type rawResponse struct {
	header http.Header
	body   []byte
	status int
}

func (r rawResponse) object(t *testing.T) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(r.body, &m), string(r.body))

	return m
}

func newLiveServer(t *testing.T) *liveServer {
	t.Helper()

	backend := cosmosdb.NewInMemoryBackend()
	h := cosmosdb.NewHandler(backend)
	h.Port = 0
	h.ValidateAuth = true

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	s := &liveServer{t: t, h: h, backend: backend, base: fmt.Sprintf("http://127.0.0.1:%d", h.Port)}
	require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs", nil, `{"id":"d"}`).status)
	require.Equal(t, http.StatusCreated, s.do(http.MethodPost, "/dbs/d/colls", nil,
		`{"id":"c","partitionKey":{"paths":["/pk"],"kind":"Hash"}}`).status)

	return s
}

func (s *liveServer) do(method, path string, headers map[string]string, body string) rawResponse {
	s.t.Helper()

	req, err := http.NewRequestWithContext(s.t.Context(), method, s.base+path, strings.NewReader(body))
	require.NoError(s.t, err)

	req.Header.Set("Content-Type", "application/json")

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	require.NoError(s.t, s.h.SignRequest(req))

	resp, err := testclient.New(s.t).Do(req)
	require.NoError(s.t, err)

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(s.t, err)

	return rawResponse{status: resp.StatusCode, header: resp.Header, body: raw}
}

func (s *liveServer) container() *azcosmos.ContainerClient {
	s.t.Helper()

	cred, err := azcosmos.NewKeyCredential(cosmosdb.DefaultMasterKey)
	require.NoError(s.t, err)

	client, err := azcosmos.NewClientWithKey(s.base+"/", cred, &azcosmos.ClientOptions{
		EnableContentResponseOnWrite: true,
		Retry:                        policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(s.t, err)

	c, err := client.NewContainer("d", "c")
	require.NoError(s.t, err)

	return c
}

func (s *liveServer) create(doc string) rawResponse {
	s.t.Helper()

	return s.do(http.MethodPost, "/dbs/d/colls/c/docs", map[string]string{
		"X-Ms-Documentdb-Partitionkey": `["a"]`,
	}, doc)
}

func (s *liveServer) get(id string) rawResponse {
	s.t.Helper()

	return s.do(http.MethodGet, "/dbs/d/colls/c/docs/"+id, map[string]string{
		"X-Ms-Documentdb-Partitionkey": `["a"]`,
	}, "")
}

func responseStatus(t *testing.T, err error) int {
	t.Helper()

	re, ok := err.(*azcore.ResponseError) //nolint:errorlint // the SDK returns it unwrapped
	require.True(t, ok, "not a ResponseError: %v", err)

	return re.StatusCode
}

func joinComma(parts []string) string { return strings.Join(parts, ",") }

// stripSystem removes server-managed properties from a document body.
func stripSystem[T ~string | ~[]byte](t *testing.T, raw T) string {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &m))

	for _, k := range []string{"_rid", "_self", "_etag", "_ts", "_attachments", "_lsn"} {
		delete(m, k)
	}

	out, err := json.Marshal(m)
	require.NoError(t, err)

	return string(out)
}
