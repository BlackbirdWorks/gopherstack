package mcpserver_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/internal/mcpserver"
)

func TestAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		token      string
		remote     string
		header     string
		wantStatus int
	}{
		{name: "loopback_v4_no_token", remote: "127.0.0.1:5000", wantStatus: http.StatusOK},
		{name: "loopback_v6_no_token", remote: "[::1]:5000", wantStatus: http.StatusOK},
		{name: "remote_no_token", remote: "10.1.2.3:5000", wantStatus: http.StatusForbidden},
		{name: "remote_token_missing", token: "tok", remote: "10.1.2.3:5000", wantStatus: http.StatusUnauthorized},
		{
			name:       "remote_token_wrong",
			token:      "tok",
			remote:     "10.1.2.3:5000",
			header:     "Bearer x",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "remote_basic_scheme",
			token:      "tok",
			remote:     "10.1.2.3:5000",
			header:     "Basic tok",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "remote_token_ok",
			token:      "tok",
			remote:     "10.1.2.3:5000",
			header:     "Bearer tok",
			wantStatus: http.StatusOK,
		},
		{name: "loopback_token_missing", token: "tok", remote: "127.0.0.1:5000", wantStatus: http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := mcpserver.Auth(tt.token, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", http.NoBody)
			req.RemoteAddr = tt.remote

			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
