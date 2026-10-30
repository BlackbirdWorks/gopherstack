package azurearm_test

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const aksDockerOptIn = "GOPHERSTACK_TEST_DOCKER"

// aksDockerStack starts an AKS provider on a real container runtime, skipping unless docker is opted in and can
// run privileged k3s containers.
func aksDockerStack(t *testing.T) *aksStack {
	t.Helper()

	if os.Getenv(aksDockerOptIn) != "1" {
		t.Skipf("set %s=1 to run docker-backed AKS tests", aksDockerOptIn)
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

	s := newAKSStack(t)
	require.NoError(t, s.provider.EnableEngine(azurearm.AKSEngineConfig{Runtime: rt, StartTimeout: aksK3sStartWait}))

	return s
}

// aksResourceKey and aksVolumeName mirror how the provider names a cluster's container label and data volume.
func aksResourceKey(cluster string) string {
	return strings.ToLower(aksRG) + "/" + strings.ToLower(cluster)
}

func aksVolumeName(cluster string) string {
	sum := sha256.Sum256([]byte(aksResourceKey(cluster)))

	return "gopherstack-aks-" + hex.EncodeToString(sum[:])[:16]
}

func aksDockerContainers(t *testing.T, cluster string) []string {
	t.Helper()

	out, err := exec.CommandContext(context.WithoutCancel(t.Context()), "docker", "ps", "-aq", "--no-trunc", "--filter",
		"label="+container.LabelResource+"="+aksResourceKey(cluster)).Output()
	require.NoError(t, err)

	return strings.Fields(string(out))
}

func aksDockerVolumeExists(t *testing.T, name string) bool {
	t.Helper()

	return exec.CommandContext(t.Context(), "docker", "volume", "inspect", name).Run() == nil
}

func aksCleanupDocker(t *testing.T, cluster string) {
	t.Helper()

	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())

		for _, id := range aksDockerContainers(t, cluster) {
			_ = exec.CommandContext(ctx, "docker", "rm", "-f", id).Run()
		}

		_ = exec.CommandContext(ctx, "docker", "volume", "rm", "-f", aksVolumeName(cluster)).Run()
	})
}

func (s *aksStack) kubectl(t *testing.T, cluster, command string, zipB64 *string) armcontainerservice.RunCommandResult {
	t.Helper()

	res, err := s.runCommand(t, cluster, armcontainerservice.RunCommandRequest{Command: &command, Context: zipB64})
	require.NoError(t, err)

	return res
}

func (s *aksStack) awaitKubeconfig(t *testing.T, cluster string) {
	t.Helper()

	require.Eventually(t, func() bool {
		_, _, err := s.userKubeconfig(t, cluster)

		return err == nil
	}, aksK3sStartWait, time.Second)
}

func TestAKSDockerRunCommand(t *testing.T) {
	t.Parallel()

	s := aksDockerStack(t)
	aksCleanupDocker(t, "rc")
	s.mustPut(t, "rc", nil)

	var nodes armcontainerservice.RunCommandResult

	require.Eventually(t, func() bool {
		nodes = s.kubectl(t, "rc", "kubectl get nodes", nil)

		return *nodes.Properties.ExitCode == 0 && strings.Contains(*nodes.Properties.Logs, "-vmss000000")
	}, time.Minute, time.Second, "the node registers under the pinned name")

	assert.Equal(t, "succeeded", *nodes.Properties.ProvisioningState)
	assert.Regexp(t, `aks-[0-9a-f]{8}-vmss000000`, *nodes.Properties.Logs)
	assert.False(t, nodes.Properties.FinishedAt.Before(*nodes.Properties.StartedAt))

	cm := aksZipB64(t, map[string]string{
		"cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: fromctx\ndata:\n  k: from-context\n",
	})
	applied := s.kubectl(
		t,
		"rc",
		"kubectl apply -f cm.yaml && kubectl get configmap fromctx -o jsonpath={.data.k}",
		&cm,
	)
	assert.EqualValues(t, 0, *applied.Properties.ExitCode)
	assert.Contains(t, *applied.Properties.Logs, "configmap/fromctx created")
	assert.Contains(t, *applied.Properties.Logs, "from-context")

	bad := s.kubectl(t, "rc", "kubectl get nosuchresource", nil)
	assert.NotZero(t, *bad.Properties.ExitCode)
	assert.Contains(t, *bad.Properties.Logs, "nosuchresource")
}

func tlsSerial(t *testing.T, server string) string {
	t.Helper()

	cfg := &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}

	conn, err := (&tls.Dialer{Config: cfg}).DialContext(t.Context(), "tcp", server)
	require.NoError(t, err)

	defer func() { _ = conn.Close() }()

	tc, ok := conn.(*tls.Conn)
	require.True(t, ok)

	return tc.ConnectionState().PeerCertificates[0].SerialNumber.String()
}

func tlsVerify(t *testing.T, server string, caPEM []byte) error {
	t.Helper()

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caPEM))

	cfg := &tls.Config{RootCAs: pool, ServerName: "kubernetes", MinVersion: tls.VersionTLS12}

	conn, err := (&tls.Dialer{Config: cfg}).DialContext(t.Context(), "tcp", server)
	if err != nil {
		return err
	}

	return conn.Close()
}

func TestAKSDockerRotateCertificates(t *testing.T) {
	t.Parallel()

	s := aksDockerStack(t)
	aksCleanupDocker(t, "rot")
	s.mustPut(t, "rot", nil)

	doc, caBefore, err := s.userKubeconfig(t, "rot")
	require.NoError(t, err)

	server := strings.TrimPrefix(doc.Clusters[0].Cluster.Server, "https://")
	before := tlsSerial(t, server)

	poller, err := s.factory.NewManagedClustersClient().BeginRotateClusterCertificates(t.Context(), aksRG, "rot", nil)
	require.NoError(t, err)

	_, err = poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	after := tlsSerial(t, server)
	assert.NotEqual(t, before, after, "the API server must serve a freshly issued certificate")

	doc2, caAfter, err := s.userKubeconfig(t, "rot")
	require.NoError(t, err)
	assert.NotEqual(t, string(caBefore), string(caAfter), "rotation replaces the cluster CA")
	require.Error(t, tlsVerify(t, server, caBefore), "the old kubeconfig CA no longer verifies the API server")
	require.NoError(t, tlsVerify(t, server, caAfter), "the new kubeconfig CA verifies the API server")

	res := s.kubectl(t, "rot", "kubectl get nodes", nil)
	assert.EqualValues(t, 0, *res.Properties.ExitCode)
	assert.NotEmpty(t, doc2.Users[0].User.Token)
}

func TestAKSDockerStateSurvivesRestore(t *testing.T) {
	t.Parallel()

	s1 := aksDockerStack(t)
	aksCleanupDocker(t, "keep")

	image, _ := k3sengine.Image("1.36", "1.36")
	stale := exec.CommandContext(t.Context(), "docker", "run", "--rm", "-v", aksVolumeName("keep")+":/d",
		"--entrypoint", "/bin/sh", image, "-c", "touch /d/stale-marker")
	out, err := stale.CombinedOutput()
	require.NoError(t, err, string(out))

	s1.mustPut(t, "keep", nil)

	clean := s1.kubectl(t, "keep", "test ! -e "+k3sengine.DataDir+"/stale-marker && echo clean", nil)
	assert.Equal(t, "clean\n", *clean.Properties.Logs, "creating a cluster discards a stale data volume")

	created := s1.kubectl(t, "keep", "kubectl create configmap survives --from-literal=k=v", nil)
	require.EqualValues(t, 0, *created.Properties.ExitCode, *created.Properties.Logs)

	data := s1.handler.Snapshot(t.Context())
	require.True(t, aksDockerVolumeExists(t, aksVolumeName("keep")))
	require.Len(t, aksDockerContainers(t, "keep"), 1)

	s1.provider.Close()
	assert.Empty(t, aksDockerContainers(t, "keep"), "closing the provider removes the node container")
	assert.True(t, aksDockerVolumeExists(t, aksVolumeName("keep")), "the data volume outlives the container")

	s2 := aksDockerStack(t)
	require.NoError(t, s2.handler.Restore(t.Context(), data))
	s2.awaitKubeconfig(t, "keep")

	got := s2.kubectl(t, "keep", "kubectl get configmap survives -o jsonpath={.data.k}", nil)
	assert.EqualValues(t, 0, *got.Properties.ExitCode, *got.Properties.Logs)
	assert.Equal(t, "v", *got.Properties.Logs, "a fresh container on the same volume sees the earlier state")

	require.Eventually(t, func() bool {
		nodes := s2.kubectl(t, "keep", "kubectl get nodes -o name", nil)

		return strings.Count(strings.TrimSpace(*nodes.Properties.Logs), "node/") == 1
	}, time.Minute, time.Second, "the pinned node name keeps one node across relaunches")

	del, err := s2.factory.NewManagedClustersClient().BeginDelete(t.Context(), aksRG, "keep", nil)
	require.NoError(t, err)

	_, err = del.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.False(t, aksDockerVolumeExists(t, aksVolumeName("keep")), "deleting the cluster deletes its volume")
}

type aksOnlyResource struct {
	container.ManagedLister
	key string
}

func (o aksOnlyResource) ListManaged(ctx context.Context) ([]container.ManagedContainer, error) {
	all, err := o.ManagedLister.ListManaged(ctx)
	if err != nil {
		return nil, err
	}

	var out []container.ManagedContainer

	for _, c := range all {
		if c.Labels[container.LabelResource] == o.key {
			out = append(out, c)
		}
	}

	return out, nil
}

func TestAKSDockerAdoptsOrphanedNode(t *testing.T) {
	t.Parallel()

	s1 := aksDockerStack(t)
	aksCleanupDocker(t, "adopt")
	s1.mustPut(t, "adopt", nil)

	created := s1.kubectl(t, "adopt", "kubectl create configmap kept --from-literal=k=adopted", nil)
	require.EqualValues(t, 0, *created.Properties.ExitCode, *created.Properties.Logs)

	data := s1.handler.Snapshot(t.Context())
	before := aksDockerContainers(t, "adopt")
	require.Len(t, before, 1)

	rt, err := container.NewRuntime(container.Config{})
	require.NoError(t, err)

	lister, ok := rt.(container.ManagedLister)
	require.True(t, ok)

	orphans, err := container.Reconcile(
		t.Context(),
		aksOnlyResource{ManagedLister: lister, key: aksResourceKey("adopt")},
		container.CurrentIdentity(),
		nil,
	)
	require.NoError(t, err)

	container.SetOrphans(orphans)
	t.Cleanup(func() { container.SetOrphans(nil) })

	s2 := newAKSStack(t)
	require.NoError(t, s2.provider.EnableEngine(azurearm.AKSEngineConfig{Runtime: rt, StartTimeout: aksK3sStartWait}))
	require.NoError(t, s2.handler.Restore(t.Context(), data))
	s2.awaitKubeconfig(t, "adopt")

	assert.Equal(t, before, aksDockerContainers(t, "adopt"), "the running node is adopted, not replaced")

	got := s2.kubectl(t, "adopt", "kubectl get configmap kept -o jsonpath={.data.k}", nil)
	assert.Equal(t, "adopted", *got.Properties.Logs)

	doc, ca, err := s2.userKubeconfig(t, "adopt")
	require.NoError(t, err)

	server := strings.TrimPrefix(doc.Clusters[0].Cluster.Server, "https://")
	body, err := k3sengine.HTTPGet(t.Context(), server, "/api/v1/namespaces", doc.Users[0].User.Token, aksTLS(t, ca))
	require.NoError(t, err)
	assert.Contains(t, string(body), "kube-system")
}

func aksTLS(t *testing.T, ca []byte) *tls.Config {
	t.Helper()

	pool := newCertPool(t, ca)

	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
}

func newCertPool(t *testing.T, ca []byte) *x509.CertPool {
	t.Helper()

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(ca), "kubeconfig CA must be PEM")

	return pool
}
