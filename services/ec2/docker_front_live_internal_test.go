package ec2

import (
	"context"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const frontLiveImage = "alpine:3.20"

type cmdAPI struct {
	*mobyclient.Client
	cmd []string
}

func (c *cmdAPI) ImagePull(
	context.Context, string, mobyclient.ImagePullOptions,
) (mobyclient.ImagePullResponse, error) {
	return fakePullRespAdapter{rc: io.NopCloser(emptyReader{})}, nil
}

func (c *cmdAPI) ContainerCreate(
	ctx context.Context, opts mobyclient.ContainerCreateOptions,
) (mobyclient.ContainerCreateResult, error) {
	opts.Config.Cmd = c.cmd

	return c.Client.ContainerCreate(ctx, opts)
}

func liveFrontCompute(t *testing.T, cmd []string) *DockerCompute {
	t.Helper()

	cli, err := mobyclient.New(mobyclient.FromEnv)
	if err != nil {
		t.Skipf("docker client: %v", err)
	}

	t.Cleanup(func() { _ = cli.Close() })

	if _, pingErr := cli.Ping(t.Context(), mobyclient.PingOptions{}); pingErr != nil {
		t.Skipf("docker unavailable: %v", pingErr)
	}

	if _, imgErr := cli.ImageInspect(t.Context(), frontLiveImage); imgErr != nil {
		t.Skipf("%s not present: %v", frontLiveImage, imgErr)
	}

	return newDockerComputeWithAPI(&cmdAPI{Client: cli, cmd: cmd}, DockerComputeConfig{Image: frontLiveImage})
}

// pong dials addr and returns what the server sends before closing.
func pong(t *testing.T, addr string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return ""
	}

	defer func() { _ = conn.Close() }()

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf, _ := io.ReadAll(conn)

	return string(buf)
}

func TestDockerComputeLiveSecurityGroupEnforcement(t *testing.T) {
	t.Parallel()

	dc := liveFrontCompute(t, []string{"sh", "-c", "while true; do echo pong | nc -l -p 8080; done"})
	bk := NewInMemoryBackend("000000000000", "us-east-1")
	bk.WithCompute(dc)

	h := NewHandler(bk)
	h.SetSecurityGroupEnforcer(newSourceEnforcer(h))

	vpc, err := bk.CreateVpc("10.233.0.0/16", "")
	require.NoError(t, err)

	sub, err := bk.CreateSubnet(vpc.ID, "10.233.1.0/24", "us-east-1a")
	require.NoError(t, err)

	sg, err := bk.CreateSecurityGroup("web", "web", vpc.ID)
	require.NoError(t, err)
	require.NoError(t, bk.AuthorizeSecurityGroupIngress(sg.ID, []SecurityGroupRule{
		{Protocol: "tcp", IPRange: "203.0.113.0/24", FromPort: 8080, ToPort: 8080},
	}))

	t.Cleanup(func() { _ = dc.RemoveVPCNetwork(context.Background(), vpc.ID) })

	runIn(t, h, sub.ID, sg.ID)

	inst := bk.DescribeInstances(nil, "")[0]
	require.NotEmpty(t, inst.ProviderID, "instance must be backed by a container")
	t.Cleanup(func() { _ = dc.Terminate(context.Background(), inst.ID, inst.ProviderID) })

	ports, err := dc.PublishedPorts(t.Context(), inst.ProviderID)
	require.NoError(t, err)
	require.Len(t, ports, 1)

	addr := net.JoinHostPort(ports[0].HostIP, strconv.Itoa(ports[0].HostPort))

	assert.Empty(t, pong(t, addr), "loopback is outside 203.0.113.0/24")

	require.NoError(t, bk.AuthorizeSecurityGroupIngress(sg.ID, []SecurityGroupRule{
		{Protocol: "tcp", IPRange: "127.0.0.1/32", FromPort: 8080, ToPort: 8080},
	}))

	require.Eventually(t, func() bool {
		return pong(t, addr) == "pong\n"
	}, liveTimeout, liveTick, "authorized loopback reaches the container on the next connection")
}
