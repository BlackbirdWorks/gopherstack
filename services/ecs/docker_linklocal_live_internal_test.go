package ecs

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	mobyclient "github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	dockerimage "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	dockernetwork "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/network"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/client"
	"github.com/blackbirdworks/gopherstack/internal/linklocal"
)

const (
	liveImage   = "alpine:3.21"
	liveTimeout = 30 * time.Second
	liveTick    = 200 * time.Millisecond
)

// sleepingClient keeps the container alive and skips the registry pull.
type sleepingClient struct {
	dockerClient
}

func (sleepingClient) ImagePull(context.Context, string, dockerimage.PullOptions) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (s sleepingClient) ContainerCreate(
	ctx context.Context, cfg *dockertypes.Config, host *dockertypes.HostConfig,
	nw *dockernetwork.NetworkingConfig, p *ocispec.Platform, name string,
) (dockertypes.CreateResponse, error) {
	cfg.Cmd = []string{"sleep", "120"}

	return s.dockerClient.ContainerCreate(ctx, cfg, host, nw, p, name)
}

func TestDockerRunnerLiveRelativeCredentialsURI(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not found")
	}

	probe, err := mobyclient.New(mobyclient.FromEnv)
	require.NoError(t, err)

	t.Cleanup(func() { _ = probe.Close() })

	if _, imgErr := probe.ImageInspect(t.Context(), liveImage); imgErr != nil {
		t.Skipf("%s not present: %v", liveImage, imgErr)
	}

	bridge, err := probe.NetworkInspect(t.Context(), "bridge", mobyclient.NetworkInspectOptions{})
	require.NoError(t, err)

	gateway := ""

	for _, c := range bridge.Network.IPAM.Config {
		if c.Gateway.Is4() {
			gateway = c.Gateway.String()
		}
	}

	if gateway == "" {
		t.Skip("docker bridge has no IPv4 gateway")
	}

	const taskID = "0123abcd0123abcd"

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "0.0.0.0:0")
	require.NoError(t, err)

	mux := http.NewServeMux()
	mux.HandleFunc("/_gopherstack/ecs/credentials/"+taskID, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"AccessKeyId": "ASIALIVE", "Token": "tok"})
	})

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second}

	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(func() { _ = srv.Close() })

	_, port, _ := net.SplitHostPort(ln.Addr().String())
	spec := linklocal.ECSSpec("", gateway, port)
	spec.Name = "ecs-livetest"

	mgr, err := linklocal.NewFromEnv(spec)
	require.NoError(t, err)

	t.Cleanup(func() { _ = mgr.Close() })

	if !mgr.Active(t.Context()) {
		t.Skip("link-local relay unavailable (image missing or 169.254.170.0/24 in use)")
	}

	cli, err := client.NewClientWithOpts(client.FromEnv(), client.WithAPIVersionNegotiation())
	require.NoError(t, err)

	t.Cleanup(func() { _ = cli.Close() })

	b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
	b.SetEndpointURL("http://" + net.JoinHostPort(gateway, port))
	b.SetRoleCredentialIssuer(&stubIssuer{})
	b.SetLinkLocal(mgr)

	runner := newDockerRunnerWithClient(t.Context(), sleepingClient{cli})
	runner.SetTaskEnvProvider(b.containerRuntimeEnv)
	runner.SetContainerAttacher(b.attachLinkLocal)

	task := &Task{
		TaskArn: "arn:aws:ecs:us-east-1:123456789012:task/c/" + taskID, ClusterArn: "arn:c",
		TaskRoleArn: "arn:aws:iam::123456789012:role/r",
	}
	td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: liveImage}}}

	require.NoError(t, runner.RunTask(task, td))

	ids := runner.ContainerIDs(task.TaskArn)
	require.Len(t, ids, 1)

	t.Cleanup(func() {
		_, _ = probe.ContainerRemove(context.Background(), ids[0], mobyclient.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
		_ = mgr.Remove(context.Background())
	})

	var body string

	require.Eventually(t, func() bool {
		out, execErr := exec.CommandContext(t.Context(), "docker", "exec", ids[0], "sh", "-c",
			`wget -qO- "http://169.254.170.2$AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"`).Output()
		body = string(out)

		return execErr == nil
	}, liveTimeout, liveTick)

	assert.Contains(t, body, "ASIALIVE")

	env, err := exec.CommandContext(t.Context(), "docker", "exec", ids[0], "env").Output()
	require.NoError(t, err)
	assert.NotContains(t, string(env), "AWS_CONTAINER_CREDENTIALS_FULL_URI")
}
