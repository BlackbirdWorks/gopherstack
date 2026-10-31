package dmsengine

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testPKI struct {
	server tls.Certificate
	caPEM  string
}

func newTestPKI(t *testing.T) testPKI {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"}, IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(
			2,
		),
		Subject:     pkix.Name{CommonName: "db"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	require.NoError(t, err)

	return testPKI{
		caPEM:  string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})),
		server: tls.Certificate{Certificate: [][]byte{srvDER}, PrivateKey: srvKey},
	}
}

// serveTLSAfterSSLRequest answers PostgreSQL's SSLRequest and completes a TLS handshake, then hangs up.
func serveTLSAfterSSLRequest(t *testing.T, cert tls.Certificate) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			go func() {
				defer c.Close()

				buf := make([]byte, 8)
				if _, rerr := c.Read(buf); rerr != nil {
					return
				}

				_, _ = c.Write([]byte("S"))
				_ = tls.Server(c, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}).
					Handshake()
			}()
		}
	}()

	port, err := strconv.Atoi(ln.Addr().(*net.TCPAddr).AddrPort().String()[len("127.0.0.1:"):])
	require.NoError(t, err)

	return port
}

func TestTLSConfigFor(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)

	tests := []struct {
		wantErr error
		name    string
		ep      EndpointConfig
		wantNil bool
		ownRoot bool
	}{
		{name: "none is plaintext", ep: EndpointConfig{SSLMode: "none", Server: "db"}, wantNil: true},
		{name: "unset is plaintext", ep: EndpointConfig{Server: "db"}, wantNil: true},
		{
			name:    "require with certificate trusts it",
			ep:      EndpointConfig{SSLMode: "require", Server: "db", CertificatePEM: pki.caPEM},
			ownRoot: true,
		},
		{
			name:    "verify-full with certificate trusts it",
			ep:      EndpointConfig{SSLMode: "verify-full", Server: "db", CertificatePEM: pki.caPEM},
			ownRoot: true,
		},
		{name: "require without certificate uses system roots", ep: EndpointConfig{SSLMode: "require", Server: "db"}},
		{
			name:    "garbage certificate",
			ep:      EndpointConfig{SSLMode: "require", Server: "db", CertificatePEM: "nope"},
			wantErr: ErrCertificate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := tlsConfigFor(tt.ep)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			if tt.wantNil {
				assert.Nil(t, cfg)

				return
			}

			require.NotNil(t, cfg)
			assert.False(t, cfg.InsecureSkipVerify)
			assert.Equal(t, "db", cfg.ServerName)
			assert.Equal(t, tt.ownRoot, cfg.RootCAs != nil)
		})
	}
}

func TestPostgresTLSVerification(t *testing.T) {
	t.Parallel()

	pki := newTestPKI(t)

	tests := []struct {
		name       string
		pem        string
		wantVerify bool
	}{
		{name: "require with the signing certificate verifies", pem: pki.caPEM, wantVerify: true},
		{name: "require without a certificate fails verification clearly", pem: "", wantVerify: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			port := serveTLSAfterSSLRequest(t, pki.server)
			ep := EndpointConfig{
				Engine: "postgres", Server: "127.0.0.1", Port: port, Database: "d", User: "u", Password: "p",
				SSLMode: "require", CertificatePEM: tt.pem,
			}

			src, err := openPGSource(t.Context(), ep)
			require.NoError(t, err)
			t.Cleanup(func() { _ = src.Close() })

			err = src.Ping(t.Context())
			require.Error(t, err)

			if tt.wantVerify {
				assert.NotContains(t, err.Error(), "x509:")

				return
			}

			assert.Contains(t, err.Error(), "SslMode require needs a trusted server certificate")
			assert.Contains(t, err.Error(), "CertificateArn")
		})
	}
}
