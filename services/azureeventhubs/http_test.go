package azureeventhubs_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/azureeventhubs"
)

func newHTTP(t *testing.T, validate bool) (*httptest.Server, *azureeventhubs.InMemoryBackend) {
	t.Helper()

	b := azureeventhubs.NewInMemoryBackend()
	h := azureeventhubs.NewHandler(b)
	h.ValidateSAS = validate

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv, b
}

func do(t *testing.T, method, url, body string, hdr map[string]string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	require.NoError(t, err)

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out)
}

const hubXML = `<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml">` +
	`<EventHubDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect">` +
	`<MessageRetentionInDays>3</MessageRetentionInDays><PartitionCount>3</PartitionCount>` +
	`</EventHubDescription></content></entry>`

func TestHTTP_AtomManagement(t *testing.T) {
	t.Parallel()

	srv, b := newHTTP(t, false)

	status, body := do(t, http.MethodPut, srv.URL+"/rest-hub", hubXML, nil)
	require.Equal(t, http.StatusCreated, status)
	assert.Contains(t, body, "<PartitionCount>3</PartitionCount>")
	assert.Contains(t, body, "<MessageRetentionInDays>3</MessageRetentionInDays>")

	status, _ = do(t, http.MethodPut, srv.URL+"/rest-hub", hubXML, nil)
	assert.Equal(t, http.StatusOK, status, "idempotent re-create")

	status, body = do(t, http.MethodGet, srv.URL+"/rest-hub", "", nil)
	require.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "<string>2</string>")

	status, body = do(t, http.MethodGet, srv.URL+"/$Resources/EventHubs", "", nil)
	require.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "rest-hub")

	status, _ = do(t, http.MethodPut, srv.URL+"/rest-hub/consumergroups/cg1", "", nil)
	require.Equal(t, http.StatusCreated, status)

	status, body = do(t, http.MethodGet, srv.URL+"/rest-hub/consumergroups", "", nil)
	require.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "cg1")
	assert.Contains(t, body, "$Default")

	status, _ = do(t, http.MethodDelete, srv.URL+"/rest-hub/consumergroups/$Default", "", nil)
	assert.Equal(t, http.StatusBadRequest, status)

	status, _ = do(t, http.MethodDelete, srv.URL+"/rest-hub/consumergroups/cg1", "", nil)
	assert.Equal(t, http.StatusOK, status)

	status, _ = do(
		t,
		http.MethodPut,
		srv.URL+"/bad-hub",
		strings.Replace(hubXML, ">3</PartitionCount>", ">5000</PartitionCount>", 1),
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, status)

	status, _ = do(t, http.MethodPut, srv.URL+"/broken", "<entry", nil)
	assert.Equal(t, http.StatusBadRequest, status)

	status, _ = do(t, http.MethodDelete, srv.URL+"/rest-hub", "", nil)
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, 0, b.HubCount())

	status, _ = do(t, http.MethodGet, srv.URL+"/rest-hub", "", nil)
	assert.Equal(t, http.StatusNotFound, status)
}

func TestHTTP_Send(t *testing.T) {
	t.Parallel()

	srv, b := newHTTP(t, false)

	status, _ := do(t, http.MethodPut, srv.URL+"/send-hub", hubXML, nil)
	require.Equal(t, http.StatusCreated, status)

	href := azureeventhubs.HubRef{Namespace: azureeventhubs.DefaultNamespace, Hub: "send-hub"}

	tests := []struct {
		hdr       map[string]string
		name      string
		path      string
		body      string
		wantCode  int
		wantTotal int64
	}{
		{name: "single", path: "/send-hub/messages", body: "hello", wantCode: http.StatusCreated, wantTotal: 1},
		{
			name:      "single with key",
			path:      "/send-hub/messages",
			body:      "keyed",
			wantCode:  http.StatusCreated,
			wantTotal: 2,
			hdr:       map[string]string{"BrokerProperties": `{"PartitionKey":"k"}`},
		},
		{
			name:      "partition",
			path:      "/send-hub/partitions/2/messages",
			body:      "p2",
			wantCode:  http.StatusCreated,
			wantTotal: 3,
		},
		{
			name: "json batch", path: "/send-hub/messages", wantCode: http.StatusCreated, wantTotal: 5,
			body: `[{"Body":"b1"},{"Body":"b2","UserProperties":{"n":1},"BrokerProperties":{"PartitionKey":"z"}}]`,
			hdr:  map[string]string{"Content-Type": "application/vnd.microsoft.servicebus.json"},
		},
		{name: "missing hub", path: "/ghost/messages", body: "x", wantCode: http.StatusNotFound, wantTotal: 5},
		{
			name:      "missing partition",
			path:      "/send-hub/partitions/9/messages",
			body:      "x",
			wantCode:  http.StatusNotFound,
			wantTotal: 5,
		},
		{name: "bad json batch", path: "/send-hub/messages", body: "{", wantCode: http.StatusBadRequest, wantTotal: 5,
			hdr: map[string]string{"Content-Type": "application/vnd.microsoft.servicebus.json"}},
		{name: "bad broker props", path: "/send-hub/messages", body: "x", wantCode: http.StatusBadRequest, wantTotal: 5,
			hdr: map[string]string{"BrokerProperties": "{"}},
		{
			name:      "bad path",
			path:      "/send-hub/nope/nope/messages",
			body:      "x",
			wantCode:  http.StatusBadRequest,
			wantTotal: 5,
		},
	}

	// Cases run sequentially: wantTotal is cumulative.
	for _, tt := range tests {
		code, _ := do(t, http.MethodPost, srv.URL+tt.path, tt.body, tt.hdr)
		assert.Equal(t, tt.wantCode, code, tt.name)

		var total int64

		for pid := range 3 {
			info, err := b.PartitionInfo(href, pid)
			require.NoError(t, err)

			total += info.LastSequence + 1
		}

		assert.Equal(t, tt.wantTotal, total, tt.name)
	}

	status, _ = do(t, http.MethodGet, srv.URL+"/send-hub/messages", "", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, status)

	big := strings.Repeat("x", azureeventhubs.MaxEventBytes+10)
	status, _ = do(t, http.MethodPost, srv.URL+"/send-hub/messages", big, nil)
	assert.Equal(t, http.StatusRequestEntityTooLarge, status)
}

func TestHTTP_SASAuthorization(t *testing.T) {
	t.Parallel()

	srv, b := newHTTP(t, true)
	require.NoError(t, b.EnsureNamespace(testNS))
	_, err := b.CreateHub(hubRef(), azureeventhubs.HubConfig{})
	require.NoError(t, err)

	sendOnly, err := b.PutRule(testNS, "", "sender", []string{azureeventhubs.RightSend})
	require.NoError(t, err)

	token := func(name, key string, expiry time.Time) map[string]string {
		exp := expiry.Unix()
		sig := azureeventhubs.SignSAS("res", exp, key)

		return map[string]string{"Authorization": "SharedAccessSignature sr=res&sig=" + urlEscape(sig) +
			"&se=" + strconv.FormatInt(exp, 10) + "&skn=" + name}
	}

	future := time.Now().Add(time.Hour)

	tests := []struct {
		hdr    map[string]string
		name   string
		method string
		path   string
		want   int
	}{
		{name: "no auth", method: http.MethodGet, path: "/" + testHub, want: http.StatusUnauthorized},
		{
			name:   "root manage",
			method: http.MethodGet,
			path:   "/" + testHub,
			hdr:    token(azureeventhubs.DefaultKeyName, azureeventhubs.DefaultKeyValue, future),
			want:   http.StatusOK,
		},
		{
			name:   "wrong key",
			method: http.MethodGet,
			path:   "/" + testHub,
			hdr:    token(azureeventhubs.DefaultKeyName, sendOnly.PrimaryKey, future),
			want:   http.StatusUnauthorized,
		},
		{
			name:   "expired",
			method: http.MethodGet,
			path:   "/" + testHub,
			hdr:    token(azureeventhubs.DefaultKeyName, azureeventhubs.DefaultKeyValue, time.Now().Add(-time.Hour)),
			want:   http.StatusUnauthorized,
		},
		{
			name:   "send rule can send",
			method: http.MethodPost,
			path:   "/" + testHub + "/messages",
			hdr:    token("sender", sendOnly.PrimaryKey, future),
			want:   http.StatusCreated,
		},
		{
			name:   "send rule cannot manage",
			method: http.MethodDelete,
			path:   "/" + testHub,
			hdr:    token("sender", sendOnly.PrimaryKey, future),
			want:   http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, _ := do(t, tt.method, srv.URL+tt.path, "payload", tt.hdr)
			assert.Equal(t, tt.want, code)
		})
	}
}

func TestHandler_Metadata(t *testing.T) {
	t.Parallel()

	h := azureeventhubs.NewHandler(azureeventhubs.NewInMemoryBackend())
	assert.Equal(t, "AzureEventHubs", h.Name())
	assert.NotEmpty(t, h.GetSupportedOperations())
	assert.Equal(t, 0, h.MatchPriority())
	assert.False(t, h.RouteMatcher()(nil))

	p := &azureeventhubs.Provider{}
	assert.Equal(t, "AzureEventHubs", p.Name())

	_, err := p.Init(nil)
	require.ErrorIs(t, err, azureeventhubs.ErrNilAppContext)
}
