package azuregraph_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/azuregraph"
)

func TestInProcessToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		validate bool
	}{
		{"unvalidated", false},
		{"validated", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := azuregraph.DefaultSettings()
			s.ValidateTokens = tt.validate
			h := azuregraph.NewHandler(azuregraph.NewBackend(s), s)

			tok, err := h.InProcessToken()
			require.NoError(t, err)

			e := echo.New()
			e.Any("/*", h.Handler())

			srv := httptest.NewServer(e)
			t.Cleanup(srv.Close)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/v1.0/applications", nil)
			require.NoError(t, err)

			req.Header.Set("Authorization", "Bearer "+tok)

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)
		})
	}
}
