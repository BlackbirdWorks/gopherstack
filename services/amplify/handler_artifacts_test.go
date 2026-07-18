package amplify_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_GenerateAccessLogs verifies POST /apps/{appId}/accesslogs returns a log URL.
func TestHandler_GenerateAccessLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "returns_log_url", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "LogApp")

			rec := doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/accesslogs",
				map[string]any{"domainName": "example.com"})
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotNil(t, resp["logUrl"])
		})
	}
}
