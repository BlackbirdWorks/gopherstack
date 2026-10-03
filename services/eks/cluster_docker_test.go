package eks_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

const k3sStartWait = 6 * time.Minute

type recordingClusterRuntime struct {
	container.Runtime
	ids []string
	mu  sync.Mutex
}

func (r *recordingClusterRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	id, err := r.Runtime.CreateAndStart(ctx, spec)
	if err == nil {
		r.mu.Lock()
		r.ids = append(r.ids, id)
		r.mu.Unlock()
	}

	return id, err
}

func (r *recordingClusterRuntime) containerIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.ids...)
}

func dockerHasContainer(t *testing.T, id string) bool {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "ps", "-aq", "--no-trunc", "--filter", "id="+id).Output()
	require.NoError(t, err)

	return strings.TrimSpace(string(out)) != ""
}

func newRealClusterBackend(t *testing.T) (*eks.InMemoryBackend, *recordingClusterRuntime) {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	image, _ := eks.K3sImage("1.32")

	probeID, err := rt.CreateAndStart(t.Context(), container.Spec{
		Image: image, Entrypoint: []string{"/bin/true"}, Privileged: true,
	})
	if err != nil {
		t.Skipf("privileged containers unavailable: %v", err)
	}

	_ = rt.StopAndRemove(t.Context(), probeID)

	rec := &recordingClusterRuntime{Runtime: rt}
	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	require.NoError(t, b.EnableClusters(eks.ClusterEngineConfig{Runtime: rec, StartTimeout: k3sStartWait}))
	t.Cleanup(b.Close)

	return b, rec
}

func kubeGet(t *testing.T, endpoint, caB64, token, path string) (int, string) {
	t.Helper()

	pemBytes, err := base64.StdEncoding.DecodeString(caB64)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pemBytes), "certificateAuthority.data must be PEM")

	tr := &http.Transport{
		TLSClientConfig:   &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		DisableKeepAlives: true,
	}
	defer tr.CloseIdleConnections()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint+path, nil)
	require.NoError(t, err)

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := (&http.Client{Transport: tr}).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(body)
}

func TestDockerCluster_RealK3s(t *testing.T) {
	t.Parallel()

	b, rec := newRealClusterBackend(t)

	_, err := b.CreateCluster("real", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		st := clusterStatus(b, "real")
		require.NotEqual(t, "FAILED", st, "k3s cluster failed to start")

		return st == "ACTIVE"
	}, k3sStartWait, time.Second)

	d, err := b.DescribeCluster("real")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(d.Endpoint, "https://127.0.0.1:"), d.Endpoint)

	code, body := kubeGet(t, d.Endpoint, d.CertificateAuthority, eks.DefaultClusterToken, "/api/v1/namespaces")
	assert.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "kube-system")

	code, _ = kubeGet(t, d.Endpoint, d.CertificateAuthority, "wrong-token", "/api/v1/namespaces")
	assert.Equal(t, http.StatusUnauthorized, code)

	ids := rec.containerIDs()
	require.Len(t, ids, 1)
	require.True(t, dockerHasContainer(t, ids[0]))

	_, err = b.DeleteCluster("real")
	require.NoError(t, err)

	require.Eventually(t, func() bool { return !dockerHasContainer(t, ids[0]) }, 2*time.Minute, time.Second)
}
