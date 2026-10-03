package eks_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

const (
	testToken = "unit-test-cluster-token-0123"
	testCA    = "-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----\n"
)

var errRuntimeDown = errors.New("runtime down")

type syncBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.buf.String()
}

type fakeClusterRuntime struct {
	failErr error
	started map[string]func()
	specs   []container.Spec
	removed []string
	mu      sync.Mutex
	serve   bool
	wrongCA bool
}

func (f *fakeClusterRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failErr != nil {
		return "", f.failErr
	}

	f.specs = append(f.specs, spec)
	id := "ctr-" + spec.Name

	if f.serve {
		_, host, _, _ := container.ParsePortSpec(spec.Ports[0])
		stop := serveFakeK3s(host, spec, f.wrongCA)

		if f.started == nil {
			f.started = map[string]func(){}
		}

		f.started[id] = stop
	}

	return id, nil
}

func (f *fakeClusterRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, id)

	if stop := f.started[id]; stop != nil {
		stop()
		delete(f.started, id)
	}

	return nil
}

func (f *fakeClusterRuntime) createdSpecs() []container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]container.Spec(nil), f.specs...)
}

func (f *fakeClusterRuntime) removedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.removed...)
}

// serveFakeK3s answers an authenticated /readyz on the published host port with a leaf signed by the spec's CA.
func serveFakeK3s(hostPort string, spec container.Spec, wrongCA bool) func() {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testToken {
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		_, _ = w.Write([]byte("ok"))
	})

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:"+hostPort)
	if err != nil {
		return func() {}
	}

	srv := httptest.NewUnstartedServer(mux)
	srv.Listener = l
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{fakeLeaf(spec, wrongCA)}}
	srv.StartTLS()

	return srv.Close
}

func specEnv(spec container.Spec, key string) string {
	for _, kv := range spec.Env {
		if v, ok := strings.CutPrefix(kv, key+"="); ok {
			return v
		}
	}

	return ""
}

func selfSignedCA() (string, string) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "other-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	keyDER, _ := x509.MarshalECPrivateKey(key)

	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func fakeLeaf(spec container.Spec, wrongCA bool) tls.Certificate {
	certPEM, keyPEM := specEnv(spec, "GOPHERSTACK_EKS_CA_CERT"), specEnv(spec, "GOPHERSTACK_EKS_CA_KEY")
	if wrongCA {
		certPEM, keyPEM = selfSignedCA()
	}

	caBlk, _ := pem.Decode([]byte(certPEM))
	keyBlk, _ := pem.Decode([]byte(keyPEM))
	caCert, _ := x509.ParseCertificate(caBlk.Bytes)
	caKey, _ := x509.ParseECPrivateKey(keyBlk.Bytes)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "k3s"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, caCert, &leafKey.PublicKey, caKey)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: leafKey}
}

type gatedClusterProbe struct {
	err  error
	gate chan struct{}
	mu   sync.Mutex
}

func newGatedClusterProbe() *gatedClusterProbe { return &gatedClusterProbe{gate: make(chan struct{})} }

func (g *gatedClusterProbe) probe(ctx context.Context, _, _ string) ([]byte, error) {
	g.mu.Lock()
	err := g.err
	g.mu.Unlock()

	if err != nil {
		return nil, err
	}

	select {
	case <-g.gate:
		return []byte(testCA), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func dockerClusterBackend(t *testing.T, rt *fakeClusterRuntime, cfg eks.ClusterEngineConfig) *eks.InMemoryBackend {
	t.Helper()

	b := eks.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	cfg.Runtime = rt

	if cfg.Token == "" {
		cfg.Token = testToken
	}

	require.NoError(t, b.EnableClusters(cfg))
	t.Cleanup(b.Close)

	return b
}

func clusterStatus(b *eks.InMemoryBackend, name string) string {
	c, err := b.DescribeCluster(name)
	if err != nil {
		return err.Error()
	}

	return c.Status
}

func waitStatus(t *testing.T, b *eks.InMemoryBackend, want string) {
	t.Helper()

	require.Eventually(t, func() bool { return clusterStatus(b, "c1") == want }, 30*time.Second, 10*time.Millisecond)
}

func waitSpecs(t *testing.T, rt *fakeClusterRuntime) {
	t.Helper()

	require.Eventually(t, func() bool { return len(rt.createdSpecs()) >= 1 }, 30*time.Second, 10*time.Millisecond)
}

func TestDockerCluster_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		version   string
		wantImage string
	}{
		{name: "mapped", version: "1.31", wantImage: "rancher/k3s:v1.31.14-k3s1"},
		{name: "default_version", version: "", wantImage: "rancher/k3s:v1.32.13-k3s1"},
		{name: "unmapped_falls_back", version: "1.20", wantImage: "rancher/k3s:v1.32.13-k3s1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeClusterRuntime{}
			probe := newGatedClusterProbe()
			b := dockerClusterBackend(t, rt, eks.ClusterEngineConfig{Probe: probe.probe, Host: "eks.test"})

			c, err := b.CreateCluster("c1", tt.version, "", nil, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, "CREATING", c.Status)

			waitSpecs(t, rt)

			spec := rt.createdSpecs()[0]
			assert.Equal(t, tt.wantImage, spec.Image)
			assert.True(t, spec.Privileged)
			assert.ElementsMatch(t, []string{"/run", "/var/run"}, spec.Tmpfs)
			assert.Contains(t, spec.Env, "GOPHERSTACK_EKS_HOST=eks.test")
			assert.Equal(t, "CREATING", clusterStatus(b, "c1"), "must stay CREATING until the API server answers")

			close(probe.gate)
			waitStatus(t, b, "ACTIVE")

			d, err := b.DescribeCluster("c1")
			require.NoError(t, err)

			_, port, ctrPort, _ := container.ParsePortSpec(spec.Ports[0])
			assert.Equal(t, "6443", ctrPort)

			assert.Equal(t, "https://eks.test:"+port, d.Endpoint)
			assert.Equal(t, base64.StdEncoding.EncodeToString([]byte(testCA)), d.CertificateAuthority)

			_, err = b.DeleteCluster("c1")
			require.NoError(t, err)

			b.Close()
			assert.Len(t, rt.removedIDs(), 1)
		})
	}
}

func TestDockerCluster_Failures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		runtimeErr  error
		probeErr    error
		name        string
		wantRemoved int
	}{
		{name: "runtime_error", runtimeErr: errRuntimeDown},
		{name: "never_ready", probeErr: errRuntimeDown, wantRemoved: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var logs syncBuffer

			rt := &fakeClusterRuntime{failErr: tt.runtimeErr}
			probe := newGatedClusterProbe()
			probe.err = tt.probeErr
			b := dockerClusterBackend(t, rt, eks.ClusterEngineConfig{
				Probe:        probe.probe,
				StartTimeout: 50 * time.Millisecond,
				Logger:       slog.New(slog.NewTextHandler(&logs, nil)),
			})

			_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
			require.NoError(t, err)

			waitStatus(t, b, "FAILED")

			if tt.wantRemoved > 0 {
				require.Eventually(t, func() bool { return len(rt.removedIDs()) == tt.wantRemoved },
					30*time.Second, 10*time.Millisecond)
			}

			require.Eventually(t, func() bool { return strings.Contains(logs.String(), "CLUSTER_START_FAILED") },
				30*time.Second, 10*time.Millisecond)
			assert.NotContains(t, logs.String(), testToken)

			d, err := b.DescribeCluster("c1")
			require.NoError(t, err)
			assert.Contains(t, d.Endpoint, "eks.amazonaws.com", "failed cluster keeps the synthetic endpoint")
		})
	}
}

func TestDockerCluster_Teardown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act  func(b *eks.InMemoryBackend)
		name string
	}{
		{name: "delete", act: func(b *eks.InMemoryBackend) { _, _ = b.DeleteCluster("c1") }},
		{name: "reset", act: func(b *eks.InMemoryBackend) { b.Reset() }},
		{name: "close", act: func(b *eks.InMemoryBackend) { b.Close() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeClusterRuntime{}
			probe := newGatedClusterProbe()
			b := dockerClusterBackend(t, rt, eks.ClusterEngineConfig{Probe: probe.probe})

			_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
			require.NoError(t, err)
			waitSpecs(t, rt)

			tt.act(b)
			b.Close()

			assert.Len(t, rt.removedIDs(), 1)
		})
	}
}

func TestDockerCluster_RestoreRelaunchesEmptyClusters(t *testing.T) {
	t.Parallel()

	rt := &fakeClusterRuntime{}
	probe := newGatedClusterProbe()
	close(probe.gate)

	src := dockerClusterBackend(t, rt, eks.ClusterEngineConfig{Probe: probe.probe})
	_, err := src.CreateCluster("c1", "1.33", "", nil, nil, nil)
	require.NoError(t, err)
	waitStatus(t, src, "ACTIVE")

	snap := src.Snapshot(t.Context())

	rt2 := &fakeClusterRuntime{}
	dst := dockerClusterBackend(t, rt2, eks.ClusterEngineConfig{Probe: probe.probe})
	require.NoError(t, dst.Restore(t.Context(), snap))

	waitSpecs(t, rt2)
	assert.Equal(t, "rancher/k3s:v1.33.13-k3s1", rt2.createdSpecs()[0].Image)
	waitStatus(t, dst, "ACTIVE")
}

func TestDockerCluster_StubModeUnchanged(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	t.Cleanup(b.Close)

	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)
	waitStatus(t, b, "ACTIVE")

	d, err := b.DescribeCluster("c1")
	require.NoError(t, err)
	assert.Contains(t, d.Endpoint, "eks.amazonaws.com")
}

func TestDockerCluster_DefaultProbe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		token      string
		wantStatus string
		wrongCA    bool
	}{
		{name: "token_accepted", token: testToken, wantStatus: "ACTIVE"},
		{name: "token_rejected", token: "some-other-token-0123456789", wantStatus: "FAILED"},
		{name: "untrusted_ca_rejected", token: testToken, wantStatus: "FAILED", wrongCA: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeClusterRuntime{serve: true, wrongCA: tt.wrongCA}
			b := dockerClusterBackend(t, rt, eks.ClusterEngineConfig{Token: tt.token, StartTimeout: 3 * time.Second})

			_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
			require.NoError(t, err)
			waitStatus(t, b, tt.wantStatus)

			d, err := b.DescribeCluster("c1")
			require.NoError(t, err)

			if tt.wantStatus != "ACTIVE" {
				return
			}

			port := strings.TrimPrefix(d.Endpoint, "https://127.0.0.1:")
			_, convErr := strconv.Atoi(port)
			require.NoError(t, convErr)

			raw, decErr := base64.StdEncoding.DecodeString(d.CertificateAuthority)
			require.NoError(t, decErr)

			blk, _ := pem.Decode(raw)
			require.NotNil(t, blk)
			assert.Equal(t, "CERTIFICATE", blk.Type)
		})
	}
}

func TestEnableClusters_RejectsUnsafeConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		host  string
		token string
	}{
		{name: "token_with_comma", token: "abcdefghijklmnop,qrs"},
		{name: "token_too_short", token: "short"},
		{name: "host_with_quote", host: "a\"; rm -rf /"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eks.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
			t.Cleanup(b.Close)

			cfg := eks.ClusterEngineConfig{Runtime: &fakeClusterRuntime{}, Host: tt.host, Token: tt.token}
			err := b.EnableClusters(cfg)
			require.ErrorIs(t, err, eks.ErrInvalidClusterEngineConfig)
		})
	}
}
