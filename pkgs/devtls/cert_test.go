package devtls_test

import (
	"crypto/x509"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
)

func TestGenerateSelfSignedCert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		extraHosts  []string
		wantDNS     []string
		wantIPCount int
	}{
		{
			name:        "no extra hosts still covers localhost/127.0.0.1/::1",
			extraHosts:  nil,
			wantDNS:     []string{"localhost"},
			wantIPCount: 2,
		},
		{
			name:        "extra DNS name and IP are added",
			extraHosts:  []string{"arm.gopherstack.local", "10.0.0.5"},
			wantDNS:     []string{"localhost", "arm.gopherstack.local"},
			wantIPCount: 3,
		},
		{
			name:        "duplicate extra host is deduped",
			extraHosts:  []string{"localhost", "127.0.0.1"},
			wantDNS:     []string{"localhost"},
			wantIPCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cert, err := devtls.GenerateSelfSignedCert(tt.extraHosts...)
			require.NoError(t, err)
			require.NotEmpty(t, cert.Certificate)

			leaf, err := x509.ParseCertificate(cert.Certificate[0])
			require.NoError(t, err)

			for _, dns := range tt.wantDNS {
				assert.Contains(t, leaf.DNSNames, dns)
			}

			assert.Len(t, leaf.IPAddresses, tt.wantIPCount)

			var haveLoopback, haveLoopback6 bool

			for _, ip := range leaf.IPAddresses {
				if ip.Equal(net.IPv4(127, 0, 0, 1)) {
					haveLoopback = true
				}

				if ip.Equal(net.IPv6loopback) {
					haveLoopback6 = true
				}
			}

			assert.True(t, haveLoopback, "expected 127.0.0.1 in IPAddresses")
			assert.True(t, haveLoopback6, "expected ::1 in IPAddresses")
		})
	}
}

func TestGenerateSelfSignedCertPEM(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM, err := devtls.GenerateSelfSignedCertPEM("example.gopherstack.local")
	require.NoError(t, err)
	assert.NotEmpty(t, certPEM)
	assert.NotEmpty(t, keyPEM)
	assert.Contains(t, string(certPEM), "BEGIN CERTIFICATE")
	assert.Contains(t, string(keyPEM), "BEGIN PRIVATE KEY")
}

func TestCA_IssueVerifies(t *testing.T) {
	t.Parallel()

	ca, err := devtls.NewCA()
	require.NoError(t, err)

	leaf, err := ca.Issue("sqs.us-east-1.amazonaws.com", "*.us-east-1.amazonaws.com")
	require.NoError(t, err)

	parsed, err := x509.ParseCertificate(leaf.Certificate[0])
	require.NoError(t, err)

	for _, name := range []string{"sqs.us-east-1.amazonaws.com", "sns.us-east-1.amazonaws.com"} {
		_, err = parsed.Verify(x509.VerifyOptions{Roots: ca.Pool(), DNSName: name})
		require.NoError(t, err, name)
	}

	_, err = parsed.Verify(x509.VerifyOptions{Roots: ca.Pool(), DNSName: "sqs.eu-west-1.amazonaws.com"})
	require.Error(t, err)
}

func TestLoadOrCreateCA_Persists(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	first, err := devtls.LoadOrCreateCA(dir)
	require.NoError(t, err)

	second, err := devtls.LoadOrCreateCA(dir)
	require.NoError(t, err)
	assert.Equal(t, first.CertPEM(), second.CertPEM())

	leaf, err := second.Issue("x.amazonaws.com")
	require.NoError(t, err)

	parsed, err := x509.ParseCertificate(leaf.Certificate[0])
	require.NoError(t, err)
	_, err = parsed.Verify(x509.VerifyOptions{Roots: first.Pool(), DNSName: "x.amazonaws.com"})
	require.NoError(t, err)
}
