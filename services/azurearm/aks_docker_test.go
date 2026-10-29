package azurearm_test

import (
	"crypto/tls"
	"crypto/x509"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const aksK3sStartWait = 6 * time.Minute

type aksRecordingRuntime struct {
	container.Runtime
	ids []string
	mu  sync.Mutex
}

func (r *aksRecordingRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	id, err := r.Runtime.CreateAndStart(ctx, spec)
	if err == nil {
		r.mu.Lock()
		r.ids = append(r.ids, id)
		r.mu.Unlock()
	}

	return id, err
}

func (r *aksRecordingRuntime) containerIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.ids...)
}

func aksDockerHas(t *testing.T, id string) bool {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "ps", "-aq", "--no-trunc", "--filter", "id="+id).Output()
	require.NoError(t, err)

	return strings.TrimSpace(string(out)) != ""
}

// TestAKSDockerRealK3s starts a real k3s container and talks to its API server with the CA and token from the
// cluster's kubeconfig. It skips when no usable container runtime is present.
func TestAKSDockerRealK3s(t *testing.T) {
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

	image, _ := k3sengine.Image("1.36", "1.36")

	probe, err := rt.CreateAndStart(t.Context(),
		container.Spec{Image: image, Entrypoint: []string{"/bin/true"}, Privileged: true})
	if err != nil {
		t.Skipf("privileged containers unavailable: %v", err)
	}

	_ = rt.StopAndRemove(t.Context(), probe)

	rec := &aksRecordingRuntime{Runtime: rt}
	s := newAKSStack(t)
	require.NoError(t, s.provider.EnableEngine(azurearm.AKSEngineConfig{Runtime: rec, StartTimeout: aksK3sStartWait}))

	s.mustPut(t, "real", nil)

	ids := rec.containerIDs()
	require.Len(t, ids, 1)
	require.True(t, aksDockerHas(t, ids[0]))

	doc, ca, err := s.userKubeconfig(t, "real")
	require.NoError(t, err)

	server := strings.TrimPrefix(doc.Clusters[0].Cluster.Server, "https://")
	require.True(t, strings.HasPrefix(server, "127.0.0.1:"), server)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(ca), "kubeconfig CA must be PEM")

	tlsCfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}

	body, err := k3sengine.HTTPGet(t.Context(), server, "/api/v1/namespaces", doc.Users[0].User.Token, tlsCfg)
	require.NoError(t, err)
	assert.Contains(t, string(body), "kube-system")

	_, err = k3sengine.HTTPGet(t.Context(), server, "/api/v1/namespaces", "wrong-token", tlsCfg)
	require.Error(t, err)

	stop, err := s.factory.NewManagedClustersClient().BeginStop(t.Context(), aksRG, "real", nil)
	require.NoError(t, err)

	_, err = stop.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.False(t, aksDockerHas(t, ids[0]), "stop must remove the container")

	start, err := s.factory.NewManagedClustersClient().BeginStart(t.Context(), aksRG, "real", nil)
	require.NoError(t, err)

	_, err = start.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	ids = rec.containerIDs()
	require.Len(t, ids, 2)
	require.True(t, aksDockerHas(t, ids[1]))

	del, err := s.factory.NewManagedClustersClient().BeginDelete(t.Context(), aksRG, "real", nil)
	require.NoError(t, err)

	_, err = del.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.False(t, aksDockerHas(t, ids[1]))
}
