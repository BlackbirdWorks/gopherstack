package app

import (
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func portOf(t *testing.T, addr net.Addr) int {
	t.Helper()

	return addr.(*net.TCPAddr).Port //nolint:forcetypeassert // httptest listens on TCP
}

func TestRegistryExchanger(t *testing.T) {
	t.Parallel()

	ok := func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()

		if r.URL.Path != "/oauth2/exchange" || r.Method != http.MethodPost ||
			r.Form.Get("grant_type") != "access_token" || r.Form.Get("access_token") != "secret-access-token" ||
			r.Form.Get("tenant") != "t1" || r.Form.Get("service") != "reg.azurecr.localhost" ||
			r.Host != "reg.azurecr.localhost:"+r.URL.Port() && !strings.HasPrefix(r.Host, "reg.azurecr.localhost:") {
			http.Error(w, "bad request", http.StatusBadRequest)

			return
		}

		_, _ = w.Write([]byte(`{"refresh_token":"rt-1"}`))
	}

	tests := []struct {
		handler http.HandlerFunc
		name    string
		host    string
		want    string
		wantErr string
	}{
		{ok, "exchanges", "reg.azurecr.localhost", "rt-1", ""},
		{ok, "other port refused", "reg.azurecr.localhost:1", "", "not the emulated"},
		{ok, "no port refused", "reg.azurecr.localhost", "", ""},
		{func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", http.StatusUnauthorized) },
			"registry denies", "reg.azurecr.localhost", "", "answered 401"},
		{func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) },
			"no token in answer", "reg.azurecr.localhost", "", "no refresh token"},
		{func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "http://127.0.0.1:1/x", http.StatusFound)
		},
			"redirect not followed", "reg.azurecr.localhost", "", "answered 302"},
		{func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"refresh_token":"` + strings.Repeat("a", 200<<10) + `"}`))
		}, "oversized answer", "reg.azurecr.localhost", "", "no refresh token"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(tt.handler)
			t.Cleanup(srv.Close)

			port := portOf(t, srv.Listener.Addr())
			host := tt.host

			switch tt.name {
			case "other port refused":
			case "no port refused":
			default:
				host += ":" + strconv.Itoa(port)
			}

			got, err := newRegistryExchanger(port, "")(t.Context(), host, "t1", "secret-access-token")

			if tt.want == "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.NotContains(t, err.Error(), "secret-access-token")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRegistryExchangerOverTLS(t *testing.T) {
	t.Parallel()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"refresh_token":"rt-tls"}`))
	}))
	t.Cleanup(srv.Close)

	port := portOf(t, srv.Listener.Addr())
	certPath := filepath.Join(t.TempDir(), "acr.pem")
	require.NoError(t, os.WriteFile(certPath,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600))

	other := filepath.Join(t.TempDir(), "other.pem")
	require.NoError(t, os.WriteFile(other, []byte("not pem"), 0o600))

	tests := []struct {
		name, cert, host, want string
		wantErr                bool
	}{
		{"trusted operator certificate", certPath, "example.com:" + strconv.Itoa(port), "rt-tls", false},
		{"name outside the certificate", certPath, "elsewhere.test:" + strconv.Itoa(port), "", true},
		{"no certificate means plain http", "", "example.com:" + strconv.Itoa(port), "", true},
		{
			"unreadable certificate",
			filepath.Join(t.TempDir(), "missing.pem"),
			"example.com:" + strconv.Itoa(port),
			"",
			true,
		},
		{"certificate file is not pem", other, "example.com:" + strconv.Itoa(port), "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newRegistryExchanger(port, tt.cert)(t.Context(), tt.host, "t1", "secret-access-token")
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
