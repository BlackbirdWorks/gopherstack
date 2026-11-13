package ociloadbalancer

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type issued struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	der  []byte
}

func issue(t *testing.T, serial int64, name string, ca bool, parent *issued) issued {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: ca, BasicConstraintsValid: true, DNSNames: []string{"localhost"},
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}
	signer, signKey := tpl, key

	if parent != nil {
		signer, signKey = parent.cert, parent.key
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, signer, &key.PublicKey, signKey)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return issued{cert: cert, key: key, der: der}
}

func TestBackendTLSForEnforcesVerifyDepth(t *testing.T) {
	t.Parallel()

	root := issue(t, 1, "root", true, nil)
	inter := issue(t, 2, "inter", true, &root)
	leaf := issue(t, 3, "leaf", false, &inter)

	pool := x509.NewCertPool()
	pool.AddCert(root.cert)

	srvCfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{{Certificate: [][]byte{leaf.der, inter.der}, PrivateKey: leaf.key}},
	}

	yes := true

	tests := []struct {
		depth   *int
		name    string
		wantErr bool
	}{
		{nil, "unset", false},
		{new(2), "deep enough", false},
		{new(1), "too shallow", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}
			cfg := backendTLSFor(base, &SSLConfig{VerifyPeerCertificate: &yes, VerifyDepth: tt.depth}, "localhost")

			c, s := net.Pipe()
			defer c.Close()
			defer s.Close()

			go func() { _ = tls.Server(s, srvCfg).Handshake() }()

			err := tls.Client(c, cfg).Handshake()
			if tt.wantErr {
				require.ErrorContains(t, err, "verifyDepth")

				return
			}

			assert.NoError(t, err)
		})
	}
}
