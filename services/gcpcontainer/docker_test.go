//nolint:staticcheck // the API still serves fields the proto marks deprecated
package gcpcontainer_test

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

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/services/gcpcontainer"
)

const k3sStartWait = 6 * time.Minute

type recordingRuntime struct {
	container.Runtime
	ids []string
	mu  sync.Mutex
}

func (r *recordingRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	id, err := r.Runtime.CreateAndStart(ctx, spec)
	if err == nil {
		r.mu.Lock()
		r.ids = append(r.ids, id)
		r.mu.Unlock()
	}

	return id, err
}

func (r *recordingRuntime) containerIDs() []string {
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

func kubeGet(t *testing.T, endpoint, caB64, token, path string) (int, string) {
	t.Helper()

	pemBytes, err := base64.StdEncoding.DecodeString(caB64)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pemBytes), "masterAuth.clusterCaCertificate must be base64 PEM")

	tr := &http.Transport{
		TLSClientConfig:   &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		DisableKeepAlives: true,
	}
	defer tr.CloseIdleConnections()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+endpoint+path, nil)
	require.NoError(t, err)

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := (&http.Client{Transport: tr}).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(body)
}

// TestDockerClusterRealK3s starts a real k3s container and talks to its API server with the CA the emulator
// returns. It skips when no usable container runtime is present.
func TestDockerClusterRealK3s(t *testing.T) {
	t.Parallel()

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

	image, _ := k3sengine.Image("1.34", "1.34")

	probe, err := rt.CreateAndStart(
		t.Context(),
		container.Spec{Image: image, Entrypoint: []string{"/bin/true"}, Privileged: true},
	)
	if err != nil {
		t.Skipf("privileged containers unavailable: %v", err)
	}

	_ = rt.StopAndRemove(t.Context(), probe)

	rec := &recordingRuntime{Runtime: rt}
	b := gcpcontainer.NewInMemoryBackend()
	require.NoError(t, b.EnableEngine(gcpcontainer.EngineConfig{Runtime: rec, StartTimeout: k3sStartWait}))

	e := newEnvWith(t, b)
	op := e.create(t, zone, &containerpb.Cluster{Name: "real", InitialNodeCount: 1, InitialClusterVersion: "1.34"})

	require.Eventually(t, func() bool {
		c := e.getCluster(t, zone, "real")
		require.NotEqual(t, containerpb.Cluster_ERROR, c.GetStatus(), c.GetStatusMessage())

		return e.getOp(t, op).GetStatus() == containerpb.Operation_DONE
	}, k3sStartWait, time.Second)

	c := e.getCluster(t, zone, "real")
	require.Equal(t, containerpb.Cluster_RUNNING, c.GetStatus())
	require.True(t, strings.HasPrefix(c.GetEndpoint(), "127.0.0.1:"), c.GetEndpoint())

	code, body := kubeGet(
		t,
		c.GetEndpoint(),
		c.GetMasterAuth().GetClusterCaCertificate(),
		b.EngineToken(),
		"/api/v1/namespaces",
	)
	assert.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "kube-system")

	code, _ = kubeGet(
		t,
		c.GetEndpoint(),
		c.GetMasterAuth().GetClusterCaCertificate(),
		"wrong-token",
		"/api/v1/namespaces",
	)
	assert.Equal(t, http.StatusUnauthorized, code)

	ids := rec.containerIDs()
	require.Len(t, ids, 1)
	require.True(t, dockerHasContainer(t, ids[0]))

	_, err = e.gc.DeleteCluster(t.Context(), &containerpb.DeleteClusterRequest{Name: clusterPath(zone, "real")})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return !dockerHasContainer(t, ids[0]) }, 2*time.Minute, time.Second)
}
