package redshiftdata_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSessionKeepAlive_Expiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantStatus   string
		advance      time.Duration
		keepAlive    int
		wantReuseOK  bool
		wantAliveSet bool
	}{
		{
			name: "within_keepalive", keepAlive: 60, advance: 30 * time.Second,
			wantStatus: "AVAILABLE", wantReuseOK: true, wantAliveSet: true,
		},
		{
			name: "after_keepalive", keepAlive: 60, advance: 61 * time.Second,
			wantStatus: "CLOSED", wantAliveSet: true,
		},
		{
			name: "forced_close_after_24h", keepAlive: 86400, advance: 25 * time.Hour,
			wantStatus: "CLOSED", wantAliveSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h := newTestHandler(t)
				rec := doRequest(t, h, "ExecuteStatement", map[string]any{
					"Sql":                     "SELECT 1",
					"ClusterIdentifier":       "c",
					"Database":                "db",
					"SessionKeepAliveSeconds": tt.keepAlive,
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				sessionID, _ := decodeBody(t, rec.Body)["SessionId"].(string)
				require.NotEmpty(t, sessionID)

				time.Sleep(tt.advance)

				rec = doRequest(t, h, "ListSessions", map[string]any{"SessionId": sessionID})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				sessions, _ := decodeBody(t, rec.Body)["Sessions"].([]any)
				require.Len(t, sessions, 1)
				sess, _ := sessions[0].(map[string]any)
				assert.Equal(t, tt.wantStatus, sess["Status"])
				assert.InDelta(t, float64(tt.keepAlive), sess["SessionAliveSeconds"], 0)
				assert.NotNil(t, sess["SessionTtl"])

				rec = doRequest(t, h, "ExecuteStatement", map[string]any{
					"Sql": "SELECT 1", "ClusterIdentifier": "c", "SessionId": sessionID,
				})
				if tt.wantReuseOK {
					assert.Equal(t, http.StatusOK, rec.Code)

					return
				}
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "ValidationException")
			})
		})
	}
}

func decodeBody(t *testing.T, rec interface{ Bytes() []byte }) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Bytes(), &out))

	return out
}
