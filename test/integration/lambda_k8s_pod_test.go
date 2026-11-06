package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	pkglogger "github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc/portalloctest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	lambdapkg "github.com/blackbirdworks/gopherstack/services/lambda"
)

const (
	k8sPodBudget   = 8 * time.Minute
	k8sPodVersion  = "1.34"
	k8sPodNS       = "default"
	k8sPodPyImage  = "public.ecr.aws/lambda/python:3.12"
	k8sPodFetchImg = "busybox:1.36"
)

func k8sPodHostImage(ctx context.Context, t *testing.T, rt container.Runtime, img string) {
	t.Helper()

	has, err := rt.HasImage(ctx, img)
	require.NoError(t, err)

	if !has {
		require.NoError(t, rt.PullImage(ctx, img))
	}
}

func k8sPodLoadImage(ctx context.Context, t *testing.T, nodeID, img string) {
	t.Helper()

	out, err := exec.CommandContext(ctx, "bash", "-o", "pipefail", "-c",
		`docker save "$1" | docker exec -i "$2" ctr -n k8s.io images import -`, "bash", img, nodeID).CombinedOutput()
	require.NoError(t, err, string(out))
}

type k3sPodsConfig struct {
	prefix string
	images []string
}

type k3sPods struct {
	cs         *kubernetes.Clientset
	kubeconfig string
}

// startK3sPods launches a one-node k3s cluster in docker, writes a kubeconfig for it and side-loads images.
func startK3sPods(ctx context.Context, t *testing.T, cfgIn k3sPodsConfig) k3sPods {
	t.Helper()

	docker, err := container.NewRuntime(container.Config{Runtime: container.RuntimeDocker})
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}

	t.Cleanup(func() { _ = docker.Close() })

	ports := portalloctest.New(t, 100)

	eng, err := k3sengine.New(k3sengine.Config{Runtime: docker, Ports: ports, Prefix: cfgIn.prefix})
	require.NoError(t, err)

	image, _ := k3sengine.Image(k8sPodVersion, k8sPodVersion)
	k8sPodHostImage(ctx, t, docker, image)

	inst, err := eng.Launch(ctx, k3sengine.Node{Name: "node", Image: image}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { eng.Remove(inst.ContainerID, append([]int{inst.Port}, inst.LBHost...)...) })

	addr := eng.Addr(inst.Port)
	servingCA, err := eng.AwaitReady(ctx, addr, inst.CA.CertPEM)
	require.NoError(t, err)

	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["k3s"] = &clientcmdapi.Cluster{Server: "https://" + addr, CertificateAuthorityData: servingCA}
	cfg.AuthInfos["k3s"] = &clientcmdapi.AuthInfo{Token: eng.Token()}
	cfg.Contexts["k3s"] = &clientcmdapi.Context{Cluster: "k3s", AuthInfo: "k3s", Namespace: k8sPodNS}
	cfg.CurrentContext = "k3s"
	require.NoError(t, clientcmd.WriteToFile(*cfg, kubeconfig))

	restCfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	require.NoError(t, err)

	cs, err := kubernetes.NewForConfig(restCfg)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, getErr := cs.CoreV1().ServiceAccounts(k8sPodNS).Get(ctx, "default", metav1.GetOptions{})

		return getErr == nil
	}, time.Minute, time.Second)

	for _, img := range cfgIn.images {
		k8sPodHostImage(ctx, t, docker, img)
		k8sPodLoadImage(ctx, t, inst.ContainerID, img)
	}

	return k3sPods{cs: cs, kubeconfig: kubeconfig}
}

// TestLambda_KubernetesPodExecutor runs gopherstack in-process on the host against a k3s cluster (kubeconfig
// path) and invokes a Python function that executes as a Pod. Pods reach the host through the docker bridge
// gateway (LAMBDA_DOCKER_HOST), which k3s's node container routes to like any docker container.
func TestLambda_KubernetesPodExecutor(t *testing.T) {
	t.Parallel()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	if testing.Short() {
		t.Skip("k3s pod executor test skipped in -short")
	}

	ctx, cancel := context.WithTimeout(t.Context(), k8sPodBudget)
	defer cancel()

	cluster := startK3sPods(ctx, t, k3sPodsConfig{
		prefix: "k8spodtest", images: []string{k8sPodPyImage, k8sPodFetchImg},
	})
	cs, kubeconfig := cluster.cs, cluster.kubeconfig

	settings := lambdapkg.DefaultSettings()
	pa := portalloctest.New(t, 100)

	codePort, err := pa.Acquire("k8s-code")
	require.NoError(t, err)

	rt, err := container.NewRuntime(container.Config{
		Runtime: container.RuntimeKubernetes,
		Kubernetes: &container.KubernetesConfig{
			Kubeconfig: kubeconfig, AdvertiseHost: settings.DockerHost, FetchImage: k8sPodFetchImg, CodePort: codePort,
		},
	})
	require.NoError(t, err)
	require.NoError(t, rt.Ping(ctx))

	backend := lambdapkg.NewInMemoryBackendWithContext(ctx, rt, pa, settings, "000000000000", "us-east-1")
	t.Cleanup(func() { backend.Close(context.WithoutCancel(ctx)) })

	handler := lambdapkg.NewHandler(backend)
	handler.AccountID = "000000000000"
	handler.DefaultRegion = "us-east-1"

	e := echo.New()
	e.Pre(pkglogger.EchoMiddleware(slog.Default()))
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(handler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	zipB64 := makeZipPayload(
		t,
		"index.py",
		"def handler(event, context):\n    return {'echo': event, 'fn': context.function_name}\n",
	)
	createBody, err := json.Marshal(map[string]any{
		"FunctionName": "k8s-pod-fn", "PackageType": "Zip", "Runtime": "python3.12", "Handler": "index.handler",
		"Code": map[string]string{"ZipFile": zipB64}, "Role": "arn:aws:iam::000000000000:role/test", "MemorySize": 256,
	})
	require.NoError(t, err)

	createResp, err := doLambdaRequest(ctx, http.MethodPost, server.URL+"/2015-03-31/functions", "application/json",
		bytes.NewReader(createBody))
	require.NoError(t, err)
	require.NoError(t, createResp.Body.Close())
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	result, _, _, status, err := backend.InvokeFunctionWithQualifier(
		ctx, "k8s-pod-fn", "", "", "", lambdapkg.InvocationTypeRequestResponse, []byte(`{"n":7}`))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)

	var out struct {
		Echo map[string]int `json:"echo"`
		Fn   string         `json:"fn"`
	}

	require.NoError(t, json.Unmarshal(result, &out), string(result))
	assert.Equal(t, 7, out.Echo["n"])
	assert.Equal(t, "k8s-pod-fn", out.Fn)

	pods, err := cs.CoreV1().
		Pods(k8sPodNS).
		List(ctx, metav1.ListOptions{LabelSelector: container.LabelManaged + "=true"})
	require.NoError(t, err)
	require.Len(t, pods.Items, 1)
	assert.Equal(t, "Never", string(pods.Items[0].Spec.RestartPolicy))
	assert.Equal(t, int64(256<<20), pods.Items[0].Spec.Containers[0].Resources.Limits.Memory().Value())

	backend.Close(context.WithoutCancel(ctx))

	require.Eventually(t, func() bool {
		l, listErr := cs.CoreV1().
			Pods(k8sPodNS).
			List(ctx, metav1.ListOptions{LabelSelector: container.LabelManaged + "=true"})

		return listErr == nil && len(l.Items) == 0
	}, time.Minute, time.Second, "closing the backend removes the function pod")
}
