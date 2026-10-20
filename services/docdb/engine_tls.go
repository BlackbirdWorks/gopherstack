package docdb

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	tlsCertTTL    = 10 * 365 * 24 * time.Hour
	tlsSerialBits = 62
	tlsPipes      = 2
	tlsHandshake  = 10 * time.Second
	tlsServerName = "localhost"
)

// tlsAuthority is the per-backend CA that signs each cluster's TLS-terminating front certificate.
type tlsAuthority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
	pem  []byte
}

func randSerial() (*big.Int, error) {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), tlsSerialBits))
	if err != nil {
		return nil, fmt.Errorf("serial: %w", err)
	}

	return n, nil
}

func newTLSAuthority() (*tlsAuthority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate CA key: %w", err)
	}

	serial, err := randSerial()
	if err != nil {
		return nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "Gopherstack DocumentDB CA",
			Organization: []string{"Gopherstack"},
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(tlsCertTTL),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("sign CA: %w", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parse CA: %w", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(cert)

	return &tlsAuthority{
		cert: cert, key: key, pool: pool,
		pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}, nil
}

// serverCert issues a certificate for localhost, loopback and host.
func (a *tlsAuthority) serverCert(host string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate server key: %w", err)
	}

	serial, err := randSerial()
	if err != nil {
		return tls.Certificate{}, err
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: tlsServerName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(tlsCertTTL),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{tlsServerName},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
	}

	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
	} else if host != "" {
		tmpl.DNSNames = append(tmpl.DNSNames, host)
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, a.cert, &key.PublicKey, a.key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("sign server cert: %w", err)
	}

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

// tlsProxy is the public listener of a docker cluster: it terminates TLS while the cluster's tls parameter is
// enabled and forwards plaintext otherwise, so a reboot flips the mode without recreating the container.
type tlsProxy struct {
	ln       net.Listener
	cfg      *tls.Config
	ctx      context.Context
	cancel   context.CancelFunc
	upstream string
	wg       sync.WaitGroup
	require  atomic.Bool
}

func (a *tlsAuthority) startProxy(host string, port int, upstream string, require bool) (*tlsProxy, error) {
	cert, err := a.serverCert(host)
	if err != nil {
		return nil, err
	}

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp",
		net.JoinHostPort(container.BindHostFor(host), strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("listen for docdb: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &tlsProxy{
		ln: ln, ctx: ctx, cancel: cancel, upstream: upstream,
		cfg: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	}
	p.require.Store(require)
	p.wg.Go(p.accept)

	return p, nil
}

func (p *tlsProxy) accept() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}

		p.wg.Go(func() { p.serve(conn) })
	}
}

func (p *tlsProxy) serve(client net.Conn) {
	stop := context.AfterFunc(p.ctx, func() { _ = client.Close() })
	defer stop()
	defer func() { _ = client.Close() }()

	if p.require.Load() {
		tc := tls.Server(client, p.cfg)

		hctx, cancel := context.WithTimeout(p.ctx, tlsHandshake)
		err := tc.HandshakeContext(hctx)

		cancel()

		if err != nil {
			return
		}

		client = tc
	}

	up, err := (&net.Dialer{Timeout: tlsHandshake}).DialContext(p.ctx, "tcp", p.upstream)
	if err != nil {
		return
	}

	defer func() { _ = up.Close() }()

	stopUp := context.AfterFunc(p.ctx, func() { _ = up.Close() })
	defer stopUp()

	done := make(chan struct{}, tlsPipes)
	pipe := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		done <- struct{}{}
	}

	go pipe(up, client)
	go pipe(client, up)
	<-done
}

// Close stops accepting, drops live connections and waits for the handlers to finish.
func (p *tlsProxy) Close() {
	p.cancel()
	_ = p.ln.Close()
	p.wg.Wait()
}

// TLSCAPEM returns the CA certificate (PEM) that signs the docker engine's certificates, or nil when TLS is
// off; clients pass it as tlsCAFile.
func (b *InMemoryBackend) TLSCAPEM() []byte {
	b.mu.RLock("TLSCAPEM")
	defer b.mu.RUnlock()

	if b.engine == nil || b.engine.tls == nil {
		return nil
	}

	return append([]byte(nil), b.engine.tls.pem...)
}

// clusterTLSEnabled reports whether c's parameter group enables TLS; a missing group or value means enabled.
func (b *InMemoryBackend) clusterTLSEnabled(c *DBCluster) bool {
	pg, ok := b.clusterParameterGroupGet(c.region, c.DBClusterParameterGroupName)
	if !ok {
		return true
	}

	v, set := pg.Parameters["tls"]

	return !set || !strs.Equal(v, "disabled")
}

func (b *InMemoryBackend) clusterTLSByID(region, id string) bool {
	c, ok := b.clusterGet(region, id)

	return !ok || b.clusterTLSEnabled(c)
}

// applyTLSMode points the cluster's front at its current tls parameter; a no-op without a docker front.
func (b *InMemoryBackend) applyTLSMode(region, clusterID string) {
	b.mu.RLock("docdbEngineTLS")
	defer b.mu.RUnlock()

	lb := b.unitLocked(region, clusterID)
	if lb == nil || lb.proxy == nil {
		return
	}

	want := b.clusterTLSByID(region, clusterID)
	if lb.proxy.require.Swap(want) != want {
		logger.Load(lb.ctx).InfoContext(lb.ctx, "docdb: tls parameter applied", "cluster", clusterID, "tls", want)
	}
}
