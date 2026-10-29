package k3sengine_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
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
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const token = "unit-test-cluster-token-0123"

func TestTagAndImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		version   string
		wantTag   string
		wantImage string
		wantOK    bool
	}{
		{"minor only", "1.32", "v1.32.13-k3s1", k3sengine.ImageRepo + ":v1.32.13-k3s1", true},
		{"patch", "1.31.4", "v1.31.14-k3s1", k3sengine.ImageRepo + ":v1.31.14-k3s1", true},
		{"gke version", "1.34.2-gke.1008000", "v1.34.12-k3s1", k3sengine.ImageRepo + ":v1.34.12-k3s1", true},
		{"oldest", "1.29", "v1.29.15-k3s1", k3sengine.ImageRepo + ":v1.29.15-k3s1", true},
		{"newest", "1.36.0", "v1.36.5-k3s1", k3sengine.ImageRepo + ":v1.36.5-k3s1", true},
		{"too old", "1.28", "", k3sengine.ImageRepo + ":v1.34.12-k3s1", false},
		{"too new", "1.37", "", k3sengine.ImageRepo + ":v1.34.12-k3s1", false},
		{"not one", "2.0", "", k3sengine.ImageRepo + ":v1.34.12-k3s1", false},
		{"garbage", "latest", "", k3sengine.ImageRepo + ":v1.34.12-k3s1", false},
		{"empty", "", "", k3sengine.ImageRepo + ":v1.34.12-k3s1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tag, ok := k3sengine.Tag(tt.version)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantTag, tag)

			image, exact := k3sengine.Image(tt.version, "1.34")
			assert.Equal(t, tt.wantImage, image)
			assert.Equal(t, tt.wantOK, exact)
		})
	}
}

func TestMinorKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
		ok   bool
	}{
		{"1.32", "1.32", true}, {"1.32.5", "1.32", true}, {"1.x", "", false}, {"2.1", "", false}, {"1", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			got, ok := k3sengine.MinorKey(tt.in)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestScript(t *testing.T) {
	t.Parallel()

	assert.Contains(t, k3sengine.Script(false), "--disable servicelb")
	assert.NotContains(t, k3sengine.Script(true), "servicelb")

	for _, lb := range []bool{false, true} {
		s := k3sengine.Script(lb)
		assert.Contains(t, s, "token-auth-file=")
		assert.Contains(t, s, "--disable traefik")
		assert.Contains(t, s, "server-ca.key")
	}
}

func TestNormalizeAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		host      string
		token     string
		wantHost  string
		wantToken string
	}{
		{name: "defaults", wantHost: "127.0.0.1", wantToken: k3sengine.DefaultToken},
		{name: "explicit", host: "k3s.test", token: token, wantHost: "k3s.test", wantToken: token},
		{name: "bad host", host: "bad host", token: token, wantErr: k3sengine.ErrInvalidConfig},
		{name: "short token", token: "short", wantErr: k3sengine.ErrInvalidConfig},
		{name: "bad token chars", token: "a token with spaces!!", wantErr: k3sengine.ErrInvalidConfig},
		{name: "public default token", host: "192.0.2.10", wantErr: k3sengine.ErrDefaultTokenExposed},
		{
			name: "public explicit token", host: "192.0.2.10", token: token,
			wantHost: "192.0.2.10", wantToken: token,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			host, tok, err := k3sengine.NormalizeAccess(tt.host, tt.token)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantHost, host)
			assert.Equal(t, tt.wantToken, tok)
		})
	}
}

func TestNewCA(t *testing.T) {
	t.Parallel()

	ca, err := k3sengine.NewCA("test-ca")
	require.NoError(t, err)

	block, _ := pem.Decode(ca.CertPEM)
	require.NotNil(t, block)

	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	assert.True(t, cert.IsCA)
	assert.Equal(t, "test-ca", cert.Subject.CommonName)

	keyBlock, _ := pem.Decode(ca.KeyPEM)
	require.NotNil(t, keyBlock)

	other, err := k3sengine.NewCA("test-ca")
	require.NoError(t, err)
	assert.NotEqual(t, ca.CertPEM, other.CertPEM)
}

type runtime struct {
	failErr error
	specs   []container.Spec
	removed []string
	mu      sync.Mutex
}

func (r *runtime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.failErr != nil {
		return "", r.failErr
	}

	r.specs = append(r.specs, spec)

	return "ctr-" + strconv.Itoa(len(r.specs)), nil
}

func (r *runtime) StopAndRemove(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.removed = append(r.removed, id)

	return nil
}

func newEngine(t *testing.T, rt *runtime, mod func(*k3sengine.Config)) (*k3sengine.Engine, *portalloc.Allocator) {
	t.Helper()

	ports, err := portalloc.New(42000, 42050)
	require.NoError(t, err)

	cfg := k3sengine.Config{Runtime: rt, Ports: ports, Prefix: "test", Token: token}
	if mod != nil {
		mod(&cfg)
	}

	e, err := k3sengine.New(cfg)
	require.NoError(t, err)

	return e, ports
}

func TestLaunch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		failErr      error
		wantErr      error
		name         string
		node         k3sengine.Node
		wantRemoved  int
		wantPortSpec int
		decline      bool
	}{
		{name: "single node", node: k3sengine.Node{Name: "c1", Image: "img", JoinToken: "jt"}, wantPortSpec: 1},
		{
			name:         "with load balancer ports",
			wantPortSpec: 3,
			node: k3sengine.Node{
				Name:      "c2",
				Image:     "img",
				JoinToken: "jt",
				LBPorts:   []int{8080, 8443},
				Mounts:    []string{"/a:/b:ro"},
			},
		},
		{
			name:        "declined by caller",
			node:        k3sengine.Node{Name: "c3", Image: "img"},
			decline:     true,
			wantErr:     k3sengine.ErrSuperseded,
			wantRemoved: 1,
		},
		{
			name:    "runtime failure",
			node:    k3sengine.Node{Name: "c4", Image: "img"},
			failErr: errBoom,
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &runtime{failErr: tt.failErr}
			e, ports := newEngine(t, rt, nil)

			var seen *k3sengine.Instance

			inst, err := e.Launch(t.Context(), tt.node, func(i *k3sengine.Instance) bool {
				seen = i

				return !tt.decline
			})

			switch {
			case tt.failErr != nil:
				require.ErrorIs(t, err, tt.failErr)

				return
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
				require.Len(t, rt.removed, tt.wantRemoved)
				assert.False(t, ports.IsAllocated(seen.Port), "ports of a declined container are released")

				return
			}

			require.NoError(t, err)
			assert.Same(t, seen, inst)
			assert.Equal(t, "ctr-1", inst.ContainerID)
			assert.True(t, ports.IsAllocated(inst.Port))
			assert.Len(t, inst.LBHost, len(tt.node.LBPorts))
			assert.NotEmpty(t, inst.CA.CertPEM)

			spec := rt.specs[0]
			assert.Equal(t, tt.node.Image, spec.Image)
			assert.Len(t, spec.Ports, tt.wantPortSpec)
			assert.True(t, strings.HasPrefix(spec.Name, "gopherstack-test-"+tt.node.Name+"-"))
			assert.Equal(t, tt.node.Mounts, spec.Mounts)
			assert.Contains(t, spec.Env, "K3S_TOKEN="+tt.node.JoinToken)
			assert.Contains(t, spec.Env, "GOPHERSTACK_EKS_ADMIN_AUTH="+token)
			assert.Equal(t, strings.Contains(spec.Entrypoint[2], "--disable servicelb"), len(tt.node.LBPorts) == 0)

			e.Remove(inst.ContainerID, append([]int{inst.Port}, inst.LBHost...)...)
			assert.Equal(t, []string{"ctr-1"}, rt.removed)
			assert.False(t, ports.IsAllocated(inst.Port))
		})
	}
}

var (
	errBoom   = errors.New("boom")
	errNotYet = errors.New("not yet")
)

func TestAwaitReady(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		probe   func(ctx context.Context, addr, token string) ([]byte, error)
		name    string
		wantCA  string
		timeout time.Duration
	}{
		{
			name: "ready", timeout: time.Minute, wantCA: "served-ca",
			probe: func(context.Context, string, string) ([]byte, error) { return []byte("served-ca"), nil },
		},
		{
			name: "never ready", timeout: 20 * time.Millisecond, wantErr: errNotYet,
			probe: func(context.Context, string, string) ([]byte, error) { return nil, errNotYet },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, _ := newEngine(
				t,
				&runtime{},
				func(c *k3sengine.Config) { c.Probe, c.StartTimeout = tt.probe, tt.timeout },
			)

			got, err := e.AwaitReady(t.Context(), "127.0.0.1:1", []byte("ca"))
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Contains(t, err.Error(), "API server not ready")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantCA, string(got))
		})
	}
}

func TestProbeAgainstTLSServer(t *testing.T) {
	t.Parallel()

	ca, err := k3sengine.NewCA("probe-ca")
	require.NoError(t, err)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token || r.URL.Path != "/readyz" {
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		_, _ = w.Write([]byte("ok"))
	}))
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{leaf(t, ca)}}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	addr := srv.Listener.Addr().String()
	other, err := k3sengine.NewCA("other-ca")
	require.NoError(t, err)

	tests := []struct {
		name    string
		token   string
		ca      []byte
		wantErr bool
	}{
		{"valid", token, ca.CertPEM, false},
		{"wrong token", "wrong", ca.CertPEM, true},
		{"untrusted ca", token, other.CertPEM, true},
		{"not a certificate", token, []byte("junk"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			perr := k3sengine.Probe(t.Context(), addr, tt.token, tt.ca)
			if tt.wantErr {
				require.Error(t, perr)

				return
			}

			require.NoError(t, perr)
		})
	}

	_, err = k3sengine.HTTPGet(
		t.Context(),
		addr,
		"/other",
		token,
		&tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true},
	)
	require.ErrorIs(t, err, k3sengine.ErrNotReady)
}

func leaf(t *testing.T, ca *k3sengine.CA) tls.Certificate {
	t.Helper()

	caBlk, _ := pem.Decode(ca.CertPEM)
	keyBlk, _ := pem.Decode(ca.KeyPEM)
	caCert, err := x509.ParseCertificate(caBlk.Bytes)
	require.NoError(t, err)

	caKey, err := x509.ParseECPrivateKey(keyBlk.Bytes)
	require.NoError(t, err)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "k3s"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	require.NoError(t, err)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
