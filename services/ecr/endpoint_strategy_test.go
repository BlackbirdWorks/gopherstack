package ecr_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/endpointstrategy"
	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestRegistryRoutesOnStrategyHosts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
		region   string
		host     string
	}{
		{"domain", "domain", "eu-west-1", "000000000000.dkr.ecr.eu-west-1.localhost.localstack.cloud:4566"},
		{"off", "off", "us-east-1", "localhost.localstack.cloud:4566"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := ecr.NewInMemoryBackend("000000000000", tc.region, "")
			backend.SetEndpointStrategy(endpointstrategy.Config{ECR: tc.strategy, HostPort: "localhost:4566"})

			registry := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("registry:" + r.URL.Path))
			})
			h := ecr.NewHandler(backend, registry)

			repo, err := backend.CreateRepository(t.Context(), "repo1", "", false, "", "")
			require.NoError(t, err)
			assert.Equal(t, tc.host+"/repo1", repo.RepositoryURI)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+tc.host+"/v2/repo1/manifests/latest", nil)
			rec := httptest.NewRecorder()
			c := echo.New().NewContext(req, rec)

			assert.True(t, h.RouteMatcher()(c))
			require.NoError(t, h.Handler()(c))
			assert.Equal(t, "registry:/v2/repo1/manifests/latest", rec.Body.String())
		})
	}
}
