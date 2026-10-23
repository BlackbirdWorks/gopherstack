package azureeventgrid_test

import (
	"bytes"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eg "github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

const egBody = `[{"id":"e1","subject":"/s","eventType":"App.Thing",` +
	`"eventTime":"2026-10-20T12:00:00Z","dataVersion":"1","data":{}}]`

func post(t *testing.T, c *http.Client, url, host string, hdr map[string]string, body string) *http.Response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, bytes.NewBufferString(body))
	require.NoError(t, err)

	if host != "" {
		req.Host = host
	}

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	resp, err := c.Do(req)
	require.NoError(t, err)

	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}

func TestPublishTopicResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		host    string
		keyOf   string
		want    string
		status  int
		useKey  bool
		useNone bool
	}{
		{name: "host label wins", host: "t2.localhost", want: "t2", status: http.StatusOK},
		{name: "key picks topic", host: "", keyOf: "t2", useKey: true, want: "t2", status: http.StatusOK},
		{name: "no credential is ambiguous", host: "", useNone: true, status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			_, err := e.backend.PutTopic("t2", topicRID, "")
			require.NoError(t, err)

			for _, n := range []string{"t1", "t2"} {
				dest := map[string]any{
					"endpointType": "StorageQueue",
					"properties":   map[string]any{"resourceId": storageID, "queueName": "q"},
				}
				_, err = e.backend.PutSubscription(t.Context(), n, "s", map[string]any{"destination": dest})
				require.NoError(t, err)
			}

			hdr := map[string]string{}

			if tt.useKey {
				k, _ := e.backend.TopicKeys(tt.keyOf)
				hdr["aeg-sas-key"] = k.Key2
			}

			resp := post(t, e.srv.Client(), e.srv.URL+"/api/events", tt.host, hdr, egBody)
			assert.Equal(t, tt.status, resp.StatusCode)

			for _, n := range []string{"t1", "t2"} {
				want := 0
				if n == tt.want {
					want = 1
				}

				assert.Equal(t, want, eg.PendingCount(e.backend, n, "s"), "topic %s", n)
			}
		})
	}
}

func TestHandlerErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		status int
	}{
		{name: "empty body", method: http.MethodPost, path: "/api/events", body: "", status: http.StatusBadRequest},
		{name: "not json", method: http.MethodPost, path: "/api/events", body: "{", status: http.StatusBadRequest},
		{name: "empty array", method: http.MethodPost, path: "/api/events", body: "[]", status: http.StatusBadRequest},
		{name: "unknown path", method: http.MethodPost, path: "/nope", body: "{}", status: http.StatusNotFound},
		{name: "get publish", method: http.MethodGet, path: "/api/events", status: http.StatusMethodNotAllowed},
		{
			name:   "unknown validation code",
			method: http.MethodGet,
			path:   "/validate?id=zzz",
			status: http.StatusNotFound,
		},
		{
			name:   "unknown pull action",
			method: http.MethodPost,
			path:   "/topics/nt/eventsubscriptions/ps:explode",
			body:   "{}",
			status: http.StatusNotFound,
		},
		{
			name:   "ack without tokens",
			method: http.MethodPost,
			path:   "/topics/nt/eventsubscriptions/ps:acknowledge",
			body:   `{"lockTokens":[]}`,
			status: http.StatusBadRequest,
		},
		{
			name:   "ns publish unknown topic",
			method: http.MethodPost,
			path:   "/topics/zzz:publish",
			body:   "{}",
			status: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			e.nsSetup(t)

			req, err := http.NewRequestWithContext(
				t.Context(),
				tt.method,
				e.srv.URL+tt.path,
				bytes.NewBufferString(tt.body),
			)
			require.NoError(t, err)

			resp, err := e.srv.Client().Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, tt.status, resp.StatusCode)
		})
	}
}

func TestOversizedRequestIsRejected(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	big := bytes.Repeat([]byte("a"), eg.MaxRequestBytes+10)

	resp := post(t, e.srv.Client(), e.srv.URL+"/api/events", "", nil, string(big))
	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
}

func TestOperationExtraction(t *testing.T) {
	t.Parallel()

	h := eg.NewHandler(eg.NewInMemoryBackend(), eg.Settings{})
	e := echo.New()

	tests := []struct {
		name, method, path, ctype, want string
	}{
		{name: "publish", method: http.MethodPost, path: "/api/events", want: "PublishEvents"},
		{
			name:   "publish cloud",
			method: http.MethodPost,
			path:   "/api/events",
			ctype:  "application/cloudevents-batch+json",
			want:   "PublishCloudEvents",
		},
		{name: "send", method: http.MethodPost, path: "/topics/t:publish", want: "SendEvents"},
		{
			name:   "receive",
			method: http.MethodPost,
			path:   "/topics/t/eventsubscriptions/s:receive",
			want:   "ReceiveEvents",
		},
		{
			name:   "renew",
			method: http.MethodPost,
			path:   "/topics/t/eventsubscriptions/s:renewLock",
			want:   "RenewEventLocks",
		},
		{name: "validate", method: http.MethodGet, path: "/validate", want: "ValidateSubscription"},
		{name: "unknown", method: http.MethodGet, path: "/x", want: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, http.NoBody)
			req.Header.Set("Content-Type", tt.ctype)

			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractOperation(c))
			assert.Equal(t, tt.path, h.ExtractResource(c))
		})
	}

	assert.Equal(t, "AzureEventGrid", h.Name())
	assert.Len(t, h.GetSupportedOperations(), 9)
	assert.False(
		t,
		h.RouteMatcher()(e.NewContext(httptest.NewRequest(http.MethodGet, "/", http.NoBody), httptest.NewRecorder())),
	)
}

func freePort(t *testing.T) int {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer ln.Close()

	return ln.Addr().(*net.TCPAddr).Port
}

func TestWorkerServesHTTPSAndDelivers(t *testing.T) {
	t.Parallel()

	b := eg.NewInMemoryBackend()
	rec := &recorder{}
	b.SetTargets(rec)

	_, err := b.PutTopic("t1", topicRID, "")
	require.NoError(t, err)

	dest := map[string]any{
		"endpointType": "StorageQueue",
		"properties":   map[string]any{"resourceId": storageID, "queueName": "q"},
	}
	_, err = b.PutSubscription(t.Context(), "t1", "s", map[string]any{"destination": dest})
	require.NoError(t, err)

	port := freePort(t)
	h := eg.NewHandler(b, eg.Settings{Port: port})
	require.NoError(t, h.StartWorker(t.Context()))

	t.Cleanup(func() { h.Shutdown(t.Context()) })

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	t.Cleanup(client.CloseIdleConnections)

	resp := post(t, client, "https://127.0.0.1:"+strconv.Itoa(port)+"/api/events", "", nil, egBody)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.Eventually(t, func() bool { return len(rec.all()) == 1 }, 10*time.Second, 20*time.Millisecond,
		"the delivery worker is kicked by a publish")
}

func TestStartWorkerRejectsHalfConfiguredTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		settings eg.Settings
	}{
		{name: "cert only", settings: eg.Settings{TLSCertFile: "/nonexistent.pem"}},
		{name: "key only", settings: eg.Settings{TLSKeyFile: "/nonexistent.key"}},
		{
			name:     "unreadable pair",
			settings: eg.Settings{TLSCertFile: "/nonexistent.pem", TLSKeyFile: "/nonexistent.key"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.settings.Port = freePort(t)
			h := eg.NewHandler(eg.NewInMemoryBackend(), tt.settings)
			require.Error(t, h.StartWorker(t.Context()))
		})
	}
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	_, err := (&eg.Provider{}).Init(nil)
	require.ErrorIs(t, err, eg.ErrNilAppContext)
	assert.Equal(t, "AzureEventGrid", (&eg.Provider{}).Name())
}

func TestResetDropsState(t *testing.T) {
	t.Parallel()

	b, _, _ := newBackend(t)
	require.NoError(t, b.PutNamespace("ns"))
	b.Reset()

	_, err := b.Topic("t1")
	require.ErrorIs(t, err, eg.ErrTopicNotFound)
	assert.False(t, b.NamespaceExists("ns"))
}
