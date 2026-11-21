package helm_test

import (
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc/portalloctest"

	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
)

const (
	envImage      = "GOPHERSTACK_HELM_IMAGE"
	releaseName   = "gs"
	namespace     = "default"
	nodePort      = 30080
	installBudget = 8 * time.Minute
	pollEvery     = time.Second
	k3sVersion    = "1.34"
	testRegion    = "us-east-1"
	nonRootUID    = 65532
)

var (
	errNotReady = errors.New("not ready")
	errFatal    = errors.New("fatal")
)

type k8sAPI struct {
	client *http.Client
	base   string
	token  string
}

func (k *k8sAPI) do(ctx context.Context, method, path string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, k.base+path, nil)
	if err != nil {
		return 0, nil, err
	}

	req.Header.Set("Authorization", "Bearer "+k.token)

	resp, err := k.client.Do(req)
	if err != nil {
		return 0, nil, err
	}

	defer func() { _ = resp.Body.Close() }()

	var buf bytes.Buffer
	_, err = buf.ReadFrom(resp.Body)

	return resp.StatusCode, buf.Bytes(), err
}

// until polls fn once a second until it reports true or ctx ends.
func until(ctx context.Context, what string, fn func() (bool, error)) error {
	tick := time.NewTicker(pollEvery)
	defer tick.Stop()

	var last error

	for {
		ok, err := fn()
		if ok {
			return nil
		}

		if errors.Is(err, errFatal) {
			return fmt.Errorf("%s: %w", what, err)
		}

		last = err

		select {
		case <-ctx.Done():
			return errors.Join(fmt.Errorf("%s: %w", what, ctx.Err()), last)
		case <-tick.C:
		}
	}
}

// deploymentReady is true once the release's Deployment has exactly one ready pod running the current spec.
func (k *k8sAPI) deploymentReady(ctx context.Context) (bool, error) {
	code, body, err := k.do(
		ctx,
		http.MethodGet,
		"/apis/apps/v1/namespaces/"+namespace+"/deployments/"+releaseName+"-gopherstack",
	)
	if err != nil {
		return false, err
	}

	if code != http.StatusOK {
		return false, fmt.Errorf("%w: deployment get returned %d", errNotReady, code)
	}

	var d struct {
		Status struct {
			ReadyReplicas      int `json:"readyReplicas"`
			UpdatedReplicas    int `json:"updatedReplicas"`
			Replicas           int `json:"replicas"`
			ObservedGeneration int `json:"observedGeneration"`
		} `json:"status"`
	}
	if err = json.Unmarshal(body, &d, jsonx.V1()); err != nil {
		return false, err
	}

	s := d.Status

	if s.ReadyReplicas == 1 && s.Replicas == 1 && s.UpdatedReplicas == 1 {
		return true, nil
	}

	return false, fmt.Errorf("%w: status %+v", errNotReady, s)
}

type podInfo struct {
	Status struct {
		Phase             string `json:"phase"`
		ContainerStatuses []struct {
			RestartCount int `json:"restartCount"`
		} `json:"containerStatuses"`
	} `json:"status"`
	Spec struct {
		SecurityContext struct {
			RunAsUser    int  `json:"runAsUser"`
			RunAsNonRoot bool `json:"runAsNonRoot"`
		} `json:"securityContext"`
	} `json:"spec"`
}

// podRunsNonRoot is true once a release pod is Running with the non-root UID and no restarts.
func (k *k8sAPI) podRunsNonRoot(ctx context.Context) error {
	code, body, err := k.do(ctx, http.MethodGet,
		"/api/v1/namespaces/"+namespace+"/pods?labelSelector=app.kubernetes.io/instance%3D"+releaseName)
	if err != nil {
		return err
	}

	if code != http.StatusOK {
		return fmt.Errorf("%w: list pods returned %d", errNotReady, code)
	}

	var l struct {
		Items []podInfo `json:"items"`
	}
	if err = json.Unmarshal(body, &l, jsonx.V1()); err != nil {
		return err
	}

	if len(l.Items) != 1 {
		return fmt.Errorf("%w: %d pods", errNotReady, len(l.Items))
	}

	pod := l.Items[0]
	if !pod.Spec.SecurityContext.RunAsNonRoot || pod.Spec.SecurityContext.RunAsUser != nonRootUID ||
		pod.Status.Phase != "Running" {
		return fmt.Errorf("%w: pod not running as UID %d: %+v", errNotReady, nonRootUID, pod)
	}

	for _, cs := range pod.Status.ContainerStatuses {
		if cs.RestartCount != 0 {
			return fmt.Errorf("%w: container restarted %d times", errNotReady, cs.RestartCount)
		}
	}

	return nil
}

func (k *k8sAPI) deletePods(ctx context.Context) error {
	code, body, err := k.do(ctx, http.MethodDelete,
		"/api/v1/namespaces/"+namespace+"/pods?labelSelector=app.kubernetes.io/instance%3D"+releaseName)
	if err != nil {
		return err
	}

	if code >= http.StatusMultipleChoices {
		return fmt.Errorf("%w: delete pods returned %d: %s", errNotReady, code, body)
	}

	return nil
}

func buildImage(t *testing.T) string {
	t.Helper()

	if img := os.Getenv(envImage); img != "" {
		return img
	}

	root := repoRoot(t)
	if _, err := os.Stat(filepath.Join(root, "bin", "gopherstack-linux")); err != nil {
		t.Skipf("no %s image and no bin/gopherstack-linux (run `task build-linux`)", envImage)
	}

	img := "gopherstack-helm-test:" + uuid.NewString()[:8]
	dockerfile := filepath.Join(root, "Dockerfile.test")
	out, err := exec.CommandContext(t.Context(), "docker", "build", "-q", "-f", dockerfile, "-t", img, root).
		CombinedOutput()
	require.NoError(t, err, string(out))

	t.Cleanup(func() {
		_ = exec.CommandContext(context.WithoutCancel(t.Context()), "docker", "rmi", "-f", img).Run()
	})

	return img
}

func loadIntoK3s(ctx context.Context, t *testing.T, containerID, img string) {
	t.Helper()

	out, err := exec.CommandContext(
		ctx,
		"bash",
		"-o",
		"pipefail",
		"-c",
		`docker save "$1" | docker exec -i "$2" ctr -n k8s.io images import -`,
		"bash",
		img,
		containerID,
	).CombinedOutput()
	require.NoError(t, err, string(out))
}

func TestChart_InstallK3s(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	if testing.Short() {
		t.Skip("k3s install test skipped in -short")
	}

	img := buildImage(t)

	ctx, cancel := context.WithTimeout(t.Context(), installBudget)
	defer cancel()

	rt, err := container.NewRuntime(container.Config{})
	require.NoError(t, err)

	ports := portalloctest.New(t, 100)

	eng, err := k3sengine.New(k3sengine.Config{Runtime: rt, Ports: ports, Prefix: "helmtest"})
	require.NoError(t, err)

	image, _ := k3sengine.Image(k3sVersion, k3sVersion)

	has, err := rt.HasImage(ctx, image)
	require.NoError(t, err)

	if !has {
		require.NoError(t, rt.PullImage(ctx, image))
	}

	inst, err := eng.Launch(ctx, k3sengine.Node{Name: "node", Image: image, LBPorts: []int{nodePort}}, nil)
	require.NoError(t, err)

	t.Cleanup(func() { eng.Remove(inst.ContainerID, append([]int{inst.Port}, inst.LBHost...)...) })

	addr := eng.Addr(inst.Port)
	servingCA, err := eng.AwaitReady(ctx, addr, inst.CA.CertPEM)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(servingCA))

	api := &k8sAPI{
		client: &http.Client{Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
			DisableKeepAlives: true,
		}},
		base:  "https://" + addr,
		token: eng.Token(),
	}

	require.NoError(t, until(ctx, "default service account", func() (bool, error) {
		code, _, gerr := api.do(ctx, http.MethodGet, "/api/v1/namespaces/"+namespace+"/serviceaccounts/default")

		return code == http.StatusOK, gerr
	}))

	loadIntoK3s(ctx, t, inst.ContainerID, img)

	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(
		t,
		os.WriteFile(kubeconfig, []byte(kubeconfigYAML(addr, servingCA, eng.Token())), 0o644),
	)

	repo, tag := splitImage(img)
	docker := []string{"--network", "host", "-v", kubeconfig + ":/kube/config:ro", "-e", "KUBECONFIG=/kube/config"}

	out, errOut, err := helm(ctx, t, docker, "install", releaseName, "/chart", "--namespace", namespace,
		"--set-string", "image.repository="+repo+",image.tag="+tag+",image.pullPolicy=Never",
		"--set", "service.type=NodePort,service.nodePort="+strconv.Itoa(nodePort),
		"--set", "persistence.enabled=true")
	require.NoError(t, err, out+errOut)

	t.Cleanup(func() {
		uctx := context.WithoutCancel(t.Context())
		_, _, _ = helm(uctx, t, docker, "uninstall", releaseName, "--namespace", namespace)
	})

	require.NoError(t, until(ctx, "deployment ready", func() (bool, error) { return api.deploymentReady(ctx) }))

	require.NoError(t, api.podRunsNonRoot(ctx))

	t.Cleanup(func() { api.dumpDiagnostics(t) })

	base := "http://127.0.0.1:" + strconv.Itoa(inst.LBHost[0])
	require.NoError(t, until(ctx, "service routable", func() (bool, error) {
		return api.serviceRoutable(ctx, base)
	}))

	s3c := newS3(ctx, t, base)
	bucket := "helm-" + uuid.NewString()[:8]
	_, err = s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	assert.Contains(t, listBuckets(ctx, t, s3c), bucket)

	// Persistence: a pod restart on the same PVC keeps the bucket.
	require.NoError(t, api.deletePods(ctx))
	require.NoError(t, until(ctx, "new pod serving", func() (bool, error) {
		_, lerr := s3c.ListBuckets(ctx, &s3.ListBucketsInput{})

		return lerr == nil, lerr
	}))
	assert.Contains(t, listBuckets(ctx, t, s3c), bucket)

	out, errOut, err = helm(ctx, t, docker, "uninstall", releaseName, "--namespace", namespace, "--wait")
	require.NoError(t, err, out+errOut)

	require.NoError(t, until(ctx, "deployment removed", func() (bool, error) {
		code, _, gerr := api.do(ctx, http.MethodGet,
			"/apis/apps/v1/namespaces/"+namespace+"/deployments/"+releaseName+"-gopherstack")

		return code == http.StatusNotFound, gerr
	}))
}

// serviceRoutable is true once the Service has a ready endpoint and a health GET through the node port succeeds.
// A restarted pod is fatal; connection errors mean the node port is not programmed yet.
func (k *k8sAPI) serviceRoutable(ctx context.Context, base string) (bool, error) {
	if err := k.noRestarts(ctx); err != nil {
		return false, err
	}

	code, body, err := k.do(ctx, http.MethodGet,
		"/apis/discovery.k8s.io/v1/namespaces/"+namespace+
			"/endpointslices?labelSelector=kubernetes.io/service-name%3D"+releaseName+"-gopherstack")
	if err != nil {
		return false, err
	}

	if code != http.StatusOK {
		return false, fmt.Errorf("%w: endpointslices returned %d", errNotReady, code)
	}

	var l struct {
		Items []struct {
			Endpoints []struct {
				Conditions struct {
					Ready *bool `json:"ready"`
				} `json:"conditions"`
			} `json:"endpoints"`
		} `json:"items"`
	}
	if err = json.Unmarshal(body, &l, jsonx.V1()); err != nil {
		return false, err
	}

	ready := false

	for _, it := range l.Items {
		for _, ep := range it.Endpoints {
			ready = ready || (ep.Conditions.Ready != nil && *ep.Conditions.Ready)
		}
	}

	if !ready {
		return false, fmt.Errorf("%w: no ready endpoint", errNotReady)
	}

	return probeHealth(ctx, base)
}

func probeHealth(ctx context.Context, base string) (bool, error) {
	rctx, cancel := context.WithTimeout(ctx, pollEvery*5)
	defer cancel()

	req, err := http.NewRequestWithContext(rctx, http.MethodGet, base+"/_gopherstack/health", nil)
	if err != nil {
		return false, err
	}

	tr := httpclient.NewTransport()
	tr.DisableKeepAlives = true

	c := &http.Client{Transport: tr}
	defer c.CloseIdleConnections()

	resp, err := c.Do(req)
	if err != nil {
		return false, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("%w: health returned %d", errNotReady, resp.StatusCode)
	}

	return true, nil
}

func (k *k8sAPI) listPods(ctx context.Context) ([]podDiag, error) {
	code, body, err := k.do(ctx, http.MethodGet,
		"/api/v1/namespaces/"+namespace+"/pods?labelSelector=app.kubernetes.io/instance%3D"+releaseName)
	if err != nil {
		return nil, err
	}

	if code != http.StatusOK {
		return nil, fmt.Errorf("%w: list pods returned %d", errNotReady, code)
	}

	var l struct {
		Items []podDiag `json:"items"`
	}
	if err = json.Unmarshal(body, &l, jsonx.V1()); err != nil {
		return nil, err
	}

	return l.Items, nil
}

type containerDiag struct {
	LastState struct {
		Terminated *struct {
			Reason   string `json:"reason"`
			ExitCode int    `json:"exitCode"`
		} `json:"terminated"`
	} `json:"lastState"`
	Name         string `json:"name"`
	RestartCount int    `json:"restartCount"`
}

type podDiag struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Status struct {
		ContainerStatuses []containerDiag `json:"containerStatuses"`
	} `json:"status"`
}

// noRestarts fails fatally if any release container restarted since it first became ready.
func (k *k8sAPI) noRestarts(ctx context.Context) error {
	pods, err := k.listPods(ctx)
	if err != nil {
		return err
	}

	for _, p := range pods {
		for _, cs := range p.Status.ContainerStatuses {
			if cs.RestartCount != 0 {
				return fmt.Errorf("%w: pod %s container %s restarted %d times", errFatal,
					p.Metadata.Name, cs.Name, cs.RestartCount)
			}
		}
	}

	return nil
}

// dumpDiagnostics logs pod status, current and previous container logs and namespace events when the test failed.
func (k *k8sAPI) dumpDiagnostics(t *testing.T) {
	t.Helper()

	if !t.Failed() {
		return
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
	defer cancel()

	pods, err := k.listPods(ctx)
	if err != nil {
		t.Logf("diagnostics: list pods: %v", err)
	}

	for _, p := range pods {
		for _, cs := range p.Status.ContainerStatuses {
			t.Logf("pod %s container %s restarts=%d lastTerminated=%+v",
				p.Metadata.Name, cs.Name, cs.RestartCount, cs.LastState.Terminated)
		}

		for _, prev := range []string{"false", "true"} {
			code, body, lerr := k.do(ctx, http.MethodGet,
				"/api/v1/namespaces/"+namespace+"/pods/"+p.Metadata.Name+"/log?previous="+prev)
			t.Logf("pod %s logs previous=%s code=%d err=%v\n%s", p.Metadata.Name, prev, code, lerr, body)
		}
	}

	code, body, err := k.do(ctx, http.MethodGet, "/api/v1/namespaces/"+namespace+"/events")
	t.Logf("events code=%d err=%v\n%s", code, err, body)
}

func newS3(ctx context.Context, t *testing.T, endpoint string) *s3.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(testRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = &endpoint; o.UsePathStyle = true })
}

func listBuckets(ctx context.Context, t *testing.T, c *s3.Client) []string {
	t.Helper()

	out, err := c.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.NoError(t, err)

	names := make([]string, 0, len(out.Buckets))
	for _, b := range out.Buckets {
		names = append(names, aws.ToString(b.Name))
	}

	return names
}

func splitImage(img string) (string, string) {
	for i := len(img) - 1; i >= 0; i-- {
		if img[i] == ':' {
			return img[:i], img[i+1:]
		}
	}

	return img, "latest"
}

func kubeconfigYAML(addr string, ca []byte, token string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: k3s
  cluster:
    server: https://%s
    certificate-authority-data: %s
users:
- name: admin
  user:
    token: %s
contexts:
- name: k3s
  context: {cluster: k3s, user: admin}
current-context: k3s
`, addr, base64Std(ca), token)
}

func base64Std(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
