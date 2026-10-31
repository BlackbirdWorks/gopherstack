package ocicontainerengine_test

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

	ke "github.com/oracle/oci-go-sdk/v65/containerengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"

	oke "github.com/blackbirdworks/gopherstack/services/ocicontainerengine"
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

func kubeGet(t *testing.T, server, caB64, token, path string) (int, string) {
	t.Helper()

	pemBytes, err := base64.StdEncoding.DecodeString(caB64)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pemBytes), "certificate-authority-data must be base64 PEM")

	tr := &http.Transport{
		TLSClientConfig:   &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		DisableKeepAlives: true,
	}
	defer tr.CloseIdleConnections()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server+path, nil)
	require.NoError(t, err)

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := (&http.Client{Transport: tr}).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(body)
}

// TestDockerClusterRealK3s starts a real k3s container and talks to its API server with the CA and token from
// CreateKubeconfig. It skips when no usable container runtime is present.
func TestDockerClusterRealK3s(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("k3s start is slow")
	}

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
	e := newEnv(t, envOpts{engine: func(b *oke.InMemoryBackend) {
		require.NoError(t, b.EnableEngine(oke.EngineConfig{Runtime: rec, StartTimeout: k3sStartWait}))
	}})

	_, err = e.ce.CreateCluster(
		t.Context(),
		ke.CreateClusterRequest{CreateClusterDetails: clusterDetails("real")},
	)
	require.NoError(t, err)

	list, err := e.ce.ListClusters(
		t.Context(),
		ke.ListClustersRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	id := list.Items[0].Id

	require.Eventually(t, func() bool {
		c, gerr := e.ce.GetCluster(t.Context(), ke.GetClusterRequest{ClusterId: id})
		require.NoError(t, gerr)
		require.NotEqual(t, ke.ClusterLifecycleStateFailed, c.LifecycleState, c.LifecycleDetails)

		return c.LifecycleState == ke.ClusterLifecycleStateActive
	}, k3sStartWait, time.Second)

	resp, err := e.ce.CreateKubeconfig(t.Context(), ke.CreateKubeconfigRequest{ClusterId: id})
	require.NoError(t, err)

	kc := readKubeconfig(t, resp)
	require.Len(t, kc.Clusters, 1)
	require.True(
		t,
		strings.HasPrefix(kc.Clusters[0].Cluster.Server, "https://127.0.0.1:"),
		kc.Clusters[0].Cluster.Server,
	)

	token := kc.Users[0].User.Token
	assert.Equal(t, e.backend.EngineToken(), token)

	code, body := kubeGet(t, kc.Clusters[0].Cluster.Server, kc.Clusters[0].Cluster.CA, token, "/api/v1/namespaces")
	assert.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, "kube-system")

	code, _ = kubeGet(t, kc.Clusters[0].Cluster.Server, kc.Clusters[0].Cluster.CA, "wrong-token", "/api/v1/namespaces")
	assert.Equal(t, http.StatusUnauthorized, code)

	ids := rec.containerIDs()
	require.Len(t, ids, 1)
	require.True(t, dockerHasContainer(t, ids[0]))

	_, err = e.ce.DeleteCluster(t.Context(), ke.DeleteClusterRequest{ClusterId: id})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return !dockerHasContainer(t, ids[0]) }, 2*time.Minute, time.Second)
}
