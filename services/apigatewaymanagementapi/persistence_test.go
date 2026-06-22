package apigatewaymanagementapi_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *apigatewaymanagementapi.InMemoryBackend) string
		verify func(t *testing.T, b *apigatewaymanagementapi.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_connection",
			setup: func(b *apigatewaymanagementapi.InMemoryBackend) string {
				conn, err := b.CreateConnection("conn-abc", "1.2.3.4", "test-agent", nil)
				if err != nil {
					return ""
				}

				return conn.ConnectionID
			},
			verify: func(t *testing.T, b *apigatewaymanagementapi.InMemoryBackend, id string) {
				t.Helper()

				conn, err := b.GetConnection(id)
				require.NoError(t, err)
				assert.Equal(t, id, conn.ConnectionID)
				assert.Equal(t, "1.2.3.4", conn.SourceIP)
				assert.Equal(t, "test-agent", conn.UserAgent)
			},
		},
		{
			name: "round_trip_preserves_messages",
			setup: func(b *apigatewaymanagementapi.InMemoryBackend) string {
				_, err := b.CreateConnection("conn-msg", "5.6.7.8", "msg-agent", nil)
				if err != nil {
					return ""
				}

				if postErr := b.PostToConnection("conn-msg", []byte("hello world")); postErr != nil {
					return ""
				}

				return "conn-msg"
			},
			verify: func(t *testing.T, b *apigatewaymanagementapi.InMemoryBackend, id string) {
				t.Helper()

				msgs := b.GetMessages(id)
				require.Len(t, msgs, 1)
				assert.Equal(t, []byte("hello world"), msgs[0].Data)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *apigatewaymanagementapi.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *apigatewaymanagementapi.InMemoryBackend, _ string) {
				t.Helper()

				assert.Empty(t, b.ListConnections())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := apigatewaymanagementapi.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := apigatewaymanagementapi.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	b := apigatewaymanagementapi.NewInMemoryBackend()

	_, err := b.CreateConnection("conn-reset", "1.1.1.1", "reset-agent", nil)
	require.NoError(t, err)

	require.NoError(t, b.PostToConnection("conn-reset", []byte("data")))

	b.Reset()

	assert.Empty(t, b.ListConnections())
	assert.Empty(t, b.GetMessages("conn-reset"))
}

func TestAPIGatewayMgmtAPIHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := apigatewaymanagementapi.NewInMemoryBackend()
	h := apigatewaymanagementapi.NewHandler(backend)

	_, err := backend.CreateConnection("snap-conn", "10.0.0.1", "snap-agent", nil)
	require.NoError(t, err)

	snap := h.Snapshot()
	require.NotNil(t, snap)

	fresh := apigatewaymanagementapi.NewInMemoryBackend()
	freshH := apigatewaymanagementapi.NewHandler(fresh)
	require.NoError(t, freshH.Restore(snap))

	conns := fresh.ListConnections()
	assert.Len(t, conns, 1)
	assert.Equal(t, "snap-conn", conns[0].ConnectionID)
}

func TestAPIGatewayMgmtAPIHandler_Routing(t *testing.T) {
	t.Parallel()

	h := apigatewaymanagementapi.NewHandler(apigatewaymanagementapi.NewInMemoryBackend())

	assert.Equal(t, "APIGatewayManagementAPI", h.Name())
	assert.Positive(t, h.MatchPriority())

	e := echo.New()

	tests := []struct {
		name      string
		path      string
		wantMatch bool
	}{
		{"connections path", "/@connections/abc123", true},
		{"no match", "/other", false},
		{"root no match", "/", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}
