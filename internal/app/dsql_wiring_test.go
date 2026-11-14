package app

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dsql"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

type dsqlFakeInstance struct{}

func (dsqlFakeInstance) Addr() (string, int) { return "127.0.0.1", 41234 }

func (dsqlFakeInstance) Close(context.Context) {}

type dsqlFakeLauncher struct{}

func (dsqlFakeLauncher) Launch(context.Context, dsql.ClusterRef) (dsql.Instance, error) {
	return dsqlFakeInstance{}, nil
}

func TestWireDSQLAuth(t *testing.T) {
	t.Parallel()

	b := dsql.NewInMemoryBackend()
	b.EnableEngine(dsqlFakeLauncher{})
	t.Cleanup(b.Close)

	c, err := b.CreateCluster("123456789012", "us-east-1", dsql.CreateClusterInput{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, _, ok := b.ConnectAddr(c.Identifier)

		return ok
	}, 10*time.Second, time.Millisecond)

	e := echo.New()
	services := []service.Registerable{
		dsql.NewHandler(b), iambackend.NewHandler(iambackend.NewInMemoryBackend()),
		stsbackend.NewHandler(stsbackend.NewInMemoryBackend()),
	}
	wireDSQLAuth(e, services, true)

	tests := []struct {
		name string
		path string
		code int
	}{
		{name: "cluster address", path: "/_gopherstack/dsql/clusters/" + c.Identifier, code: http.StatusOK},
		{name: "unknown cluster", path: "/_gopherstack/dsql/clusters/nope", code: http.StatusNotFound},
		{name: "no ca without docker engine", path: dsqlCAPath, code: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.path, nil))
			assert.Equal(t, tt.code, rec.Code)

			if tt.code != http.StatusOK {
				return
			}

			var got struct {
				Host     string `json:"host"`
				Endpoint string `json:"endpoint"`
				Port     int    `json:"port"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got, jsonx.V1()))
			assert.Equal(t, "127.0.0.1", got.Host)
			assert.Equal(t, 41234, got.Port)
			assert.Equal(t, c.Endpoint, got.Endpoint)
		})
	}
}

func TestDSQLIAMAuthConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		withIAM bool
		enforce bool
	}{
		{name: "iam and sts present", withIAM: true, enforce: true},
		{name: "services absent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				iamH *iambackend.Handler
				stsH *stsbackend.Handler
			)

			if tt.withIAM {
				iamH = iambackend.NewHandler(iambackend.NewInMemoryBackend())
				stsH = stsbackend.NewHandler(stsbackend.NewInMemoryBackend())
			}

			cfg := dsqlIAMAuthConfig(iamH, stsH, tt.enforce)
			assert.Equal(t, tt.enforce, cfg.Enforce)
			assert.Equal(t, tt.withIAM, cfg.Identities != nil)
			assert.Equal(t, tt.withIAM, cfg.Sessions != nil)
		})
	}
}
