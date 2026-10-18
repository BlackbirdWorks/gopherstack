package eks

import (
	"bytes"
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
)

const (
	e2eWait       = 8 * time.Minute
	viewerRoleARN = "arn:aws:iam::" + testAccount + ":role/viewer"
	viewerSession = "arn:aws:sts::" + testAccount + ":assumed-role/viewer/sess"
)

type idRecorder struct {
	container.Runtime
	ids []string
	mu  sync.Mutex
}

func (r *idRecorder) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	id, err := r.Runtime.CreateAndStart(ctx, spec)
	if err == nil {
		r.mu.Lock()
		r.ids = append(r.ids, id)
		r.mu.Unlock()
	}

	return id, err
}

func (r *idRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.ids...)
}

func containerExists(t *testing.T, id string) bool {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", "ps", "-aq", "--no-trunc", "--filter", "id="+id).Output()
	require.NoError(t, err)

	return strings.TrimSpace(string(out)) != ""
}

func dockerRuntimeOrSkip(t *testing.T) container.Runtime {
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

	image, _ := K3sImage("1.32")

	id, err := rt.CreateAndStart(
		t.Context(),
		container.Spec{Image: image, Entrypoint: []string{"/bin/true"}, Privileged: true},
	)
	if err != nil {
		t.Skipf("privileged containers unavailable: %v", err)
	}

	_ = rt.StopAndRemove(t.Context(), id)

	return rt
}

func kubeDo(t *testing.T, method, endpoint, caB64, token, path, body string) (int, string) {
	t.Helper()

	pemBytes, err := base64.StdEncoding.DecodeString(caB64)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pemBytes))

	tr := &http.Transport{
		TLSClientConfig:   &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		DisableKeepAlives: true,
	}
	defer tr.CloseIdleConnections()

	req, err := http.NewRequestWithContext(t.Context(), method, endpoint+path, bytes.NewBufferString(body))
	require.NoError(t, err)

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Transport: tr}).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out)
}

// TestDockerCluster_IAMAuthNodegroupsRBAC drives a real k3s cluster through the IAM-token proxy.
func TestDockerCluster_IAMAuthNodegroupsRBAC(t *testing.T) {
	t.Parallel()

	rec := &idRecorder{Runtime: dockerRuntimeOrSkip(t)}
	b := NewInMemoryBackend(t.Context(), testAccount, "us-east-1")
	require.NoError(t, b.EnableClusters(ClusterEngineConfig{
		Runtime: rec, StartTimeout: e2eWait,
		Credentials: fakeCreds{
			"AKIAROOT":   {PrincipalARN: rootARN},
			"ASIAVIEWER": {PrincipalARN: viewerSession, Secret: "viewersecret"},
		},
	}))
	t.Cleanup(b.Close)

	_, err := b.CreateCluster("e2e", "1.32", "", nil, nil, nil,
		ClusterOptionalConfig{CreatorARN: rootARN, AccessConfig: &AccessConfig{
			AuthenticationMode: authModeAPIAndConfig, BootstrapClusterCreatorAdminPermissions: true,
		}})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		st := clusterStatusOf(b, "e2e")
		require.NotEqual(t, statusFailed, st, "k3s cluster failed to start")

		return st == statusActive
	}, e2eWait, time.Second)

	d, err := b.DescribeCluster("e2e")
	require.NoError(t, err)

	admin := func() string {
		return makeToken(t, "AKIAROOT", "ignored", "e2e", "sts.us-east-1.amazonaws.com", time.Now().UTC())
	}
	viewer := func() string {
		return makeToken(t, "ASIAVIEWER", "viewersecret", "e2e", "sts.us-east-1.amazonaws.com", time.Now().UTC())
	}
	call := func(method, tok, path, body string) (int, string) {
		return kubeDo(t, method, d.Endpoint, d.CertificateAuthority, tok, path, body)
	}

	code, out := call(http.MethodGet, admin(), "/api/v1/namespaces", "")
	require.Equal(t, http.StatusOK, code, out)
	assert.Contains(t, out, "kube-system")

	code, _ = call(http.MethodGet, viewer(), "/api/v1/namespaces", "")
	assert.Equal(t, http.StatusUnauthorized, code, "principal without access entry is rejected")

	_, err = b.CreateAccessEntry("e2e", viewerRoleARN, "STANDARD", "", nil, nil)
	require.NoError(t, err)
	_, err = b.AssociateAccessPolicy("e2e", viewerRoleARN,
		"arn:aws:eks::aws:cluster-access-policy/AmazonEKSViewPolicy", map[string]any{"type": "cluster"})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		c, _ := call(http.MethodGet, viewer(), "/api/v1/namespaces", "")

		return c == http.StatusOK
	}, time.Minute, 500*time.Millisecond, "view policy must bind in the real cluster")

	code, _ = call(http.MethodPost, viewer(), "/api/v1/namespaces",
		`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"viewer-made"}}`)
	assert.Equal(t, http.StatusForbidden, code, "view policy cannot create namespaces")

	code, _ = call(http.MethodPost, admin(), "/api/v1/namespaces",
		`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"admin-made"}}`)
	assert.Equal(t, http.StatusCreated, code, "creator has cluster admin")

	_, err = b.CreateNodegroup("e2e", "ng1", "", "", "", "", "", nil, 1, 1, 2, NodegroupInput{
		Labels: map[string]string{"tier": "web"},
	}, nil)
	require.NoError(t, err)
	_, err = b.CreateAddon("e2e", "coredns", "", "", "", "", "", nil, nil)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ng, derr := b.DescribeNodegroup("e2e", "ng1")
		require.NoError(t, derr)
		require.NotEqual(t, statusCreateFail, ng.Status)

		return ng.Status == statusActive
	}, e2eWait, 2*time.Second)

	code, out = call(
		http.MethodGet,
		admin(),
		"/api/v1/nodes?labelSelector=eks.amazonaws.com/nodegroup%3Dng1,tier%3Dweb",
		"",
	)
	require.Equal(t, http.StatusOK, code, out)
	assert.Contains(t, out, `"eks.amazonaws.com/nodegroup":"ng1"`)

	// coredns needs the pause/coredns images from docker.io; without registry access the addon must stay
	// CREATING instead of reporting a fabricated ACTIVE.
	require.Eventually(t, func() bool {
		a, derr := b.DescribeAddon("e2e", "coredns")
		require.NoError(t, derr)

		if a.Status == statusActive {
			return true
		}

		c, body := call(http.MethodGet, admin(), "/apis/apps/v1/namespaces/kube-system/deployments/coredns", "")

		return c == http.StatusOK && !strings.Contains(body, `"availableReplicas"`) && a.Status == statusCreating
	}, 3*time.Minute, 2*time.Second, "addon status must track the real coredns deployment")

	ids := rec.all()
	require.Len(t, ids, 2, "one k3s server and one agent")

	_, err = b.DeleteNodegroup("e2e", "ng1")
	require.NoError(t, err)

	require.Eventually(t, func() bool { return !containerExists(t, ids[1]) }, 2*time.Minute, time.Second,
		"deleting the nodegroup removes its agent container")

	_, err = b.DeleteCluster("e2e")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !containerExists(t, ids[0]) }, 2*time.Minute, time.Second)
}

func clusterStatusOf(b *InMemoryBackend, name string) string {
	c, err := b.DescribeCluster(name)
	if err != nil {
		return err.Error()
	}

	return c.Status
}
