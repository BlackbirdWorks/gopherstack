package gcpredis

import (
	"crypto/sha1" //nolint:gosec // Memorystore reports SHA-1 certificate fingerprints
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"

	"cloud.google.com/go/redis/apiv1/redispb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

var errNoCertificate = errors.New("gcpredis: CA certificate PEM did not decode")

func newAuthString() string { return uuid.NewString() }

// startEngine launches a Redis server for an instance, with AUTH and server-authenticated TLS as configured, and
// returns the CA certificates clients must trust.
func (b *InMemoryBackend) startEngine(
	mode redispb.Instance_TransitEncryptionMode, auth string,
) (*redisengine.Engine, []*redispb.TlsCertificate, error) {
	var (
		cfg   *tls.Config
		certs []*redispb.TlsCertificate
	)

	if mode == redispb.Instance_SERVER_AUTHENTICATION {
		var err error
		if cfg, certs, err = newServerTLS(); err != nil {
			return nil, nil, err
		}
	}

	b.mu.RLock("startEngine")
	alloc := b.alloc
	b.mu.RUnlock()

	eng, err := redisengine.StartTLS(alloc, "gcpredis", cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("start redis engine: %w", err)
	}

	if auth != "" {
		eng.Mini.RequireAuth(auth)
	}

	return eng, certs, nil
}

func newServerTLS() (*tls.Config, []*redispb.TlsCertificate, error) {
	ca, err := devtls.NewCA()
	if err != nil {
		return nil, nil, fmt.Errorf("create instance CA: %w", err)
	}

	leaf, err := ca.Issue("localhost", "127.0.0.1", "::1")
	if err != nil {
		return nil, nil, fmt.Errorf("issue server certificate: %w", err)
	}

	cert, err := describeCA(ca.CertPEM())
	if err != nil {
		return nil, nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{*leaf},
		MinVersion:   tls.VersionTLS12,
	}, []*redispb.TlsCertificate{
		cert,
	}, nil
}

func describeCA(certPEM []byte) (*redispb.TlsCertificate, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, errNoCertificate
	}

	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CA certificate: %w", err)
	}

	sum := sha1.Sum(c.Raw) //nolint:gosec // fingerprint, not a security boundary

	return &redispb.TlsCertificate{
		SerialNumber:    c.SerialNumber.String(),
		Cert:            string(certPEM),
		CreateTime:      timestamppb.New(c.NotBefore),
		ExpireTime:      timestamppb.New(c.NotAfter),
		Sha1Fingerprint: hex.EncodeToString(sum[:]),
	}, nil
}

// bindEngine publishes a started engine's endpoint on rec.
func bindEngine(rec *instRec, eng *redisengine.Engine, certs []*redispb.TlsCertificate) {
	rec.engine = eng
	rec.pb.Host = eng.Host
	rec.pb.Port = safeInt32(eng.Port)
	rec.pb.ServerCaCerts = certs
	deriveTopology(rec.pb)
}

func safeInt32(n int) int32 {
	return int32(min(max(n, 0), 1<<31-1))
}
