package app

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
)

func TestTransparentFlagDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantHTTPS string
		args      []string
		wantOn    bool
	}{
		{name: "default off", args: nil, wantOn: false, wantHTTPS: ":443"},
		{name: "explicit false", args: []string{"--transparent-dns=false"}, wantOn: false, wantHTTPS: ":443"},
		{
			name:      "enabled",
			args:      []string{"--transparent-dns", "--transparent-https-addr=:8443"},
			wantOn:    true,
			wantHTTPS: ":8443",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var root rootCLI

			parser, err := kong.New(&root, kong.Name("gopherstack"), kong.Writers(nil, nil))
			require.NoError(t, err)

			_, err = parser.Parse(tt.args)
			require.NoError(t, err)

			assert.Equal(t, tt.wantOn, root.Serve.Transparent.Enabled)
			assert.Equal(t, tt.wantHTTPS, root.Serve.Transparent.HTTPSAddr)
			assert.Equal(t, "127.0.0.1", root.Serve.DNSResolveIP)
		})
	}
}

func TestSetupTransparentServesCA(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "ca pem", path: "/_gopherstack/ca.pem"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			t.Cleanup(func() {
				_ = os.Unsetenv(container.EnvInjectDNS)
				_ = os.Unsetenv(container.EnvInjectCAFile)
			})

			settings := TransparentSettings{Enabled: true, HTTPSAddr: "127.0.0.1:0"}

			tr, dnsSrv, err := setupTransparent(ctx, settings, "127.0.0.1:0", "127.0.0.1")
			require.NoError(t, err)
			require.NotNil(t, dnsSrv)

			require.NoError(t, tr.serveHTTPS(ctx, echo.New()))

			addr := tr.srv.Addr().String()

			client := &http.Client{Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}}
			defer client.CloseIdleConnections()

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr+tt.path, nil)
			require.NoError(t, err)

			resp, err := client.Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Contains(t, string(body), "BEGIN CERTIFICATE")
		})
	}
}

func TestContainerCAFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		containered bool
		useHost     bool
		wantSkip    bool
	}{
		{name: "host temp", containered: false},
		{name: "containerised skips", containered: true, wantSkip: true},
		{name: "containerised with host path", containered: true, useHost: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ca, err := devtls.NewCA()
			require.NoError(t, err)

			hostPath := ""
			if tt.useHost {
				hostPath = filepath.Join(t.TempDir(), "ca.pem")
			}

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			got, err := containerCAFile(ctx, hostPath, tt.containered, ca)
			require.NoError(t, err)

			if tt.wantSkip {
				assert.Empty(t, got)

				return
			}

			assert.FileExists(t, got)

			if tt.useHost {
				assert.Equal(t, hostPath, got)
			}
		})
	}
}
