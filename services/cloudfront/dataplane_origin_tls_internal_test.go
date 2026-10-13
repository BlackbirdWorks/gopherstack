package cloudfront

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOriginClientVerifiesCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		trust   bool
		wantErr bool
	}{
		{name: "untrusted chain rejected", trust: false, wantErr: true},
		{name: "trusted chain accepted", trust: true, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("ok"))
			}))
			t.Cleanup(srv.Close)

			client := newOriginClient()
			if tt.trust {
				pool := x509.NewCertPool()
				pool.AddCert(srv.Certificate())

				tr, ok := client.Transport.(*http.Transport)
				require.True(t, ok)

				tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
			}

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
			require.NoError(t, err)

			resp, err := client.Do(req)
			if tt.wantErr {
				require.Error(t, err)

				var certErr *tls.CertificateVerificationError
				assert.ErrorAs(t, err, &certErr)

				return
			}

			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		})
	}
}
