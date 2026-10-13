package ec2

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	liveImage   = "alpine:3.21"
	liveTimeout = 30 * time.Second
	liveTick    = 200 * time.Millisecond
)

// liveDockerAPI runs a test command in place of the sshd entrypoint and skips the image pull.
type liveDockerAPI struct {
	*mobyclient.Client
	cmds map[string][]string
}

func (l *liveDockerAPI) ImagePull(
	context.Context, string, mobyclient.ImagePullOptions,
) (mobyclient.ImagePullResponse, error) {
	return fakePullRespAdapter{rc: io.NopCloser(emptyReader{})}, nil
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

func (l *liveDockerAPI) ContainerCreate(
	ctx context.Context, opts mobyclient.ContainerCreateOptions,
) (mobyclient.ContainerCreateResult, error) {
	if cmd, ok := l.cmds[opts.Name]; ok {
		opts.Config.Cmd = cmd
	}

	return l.Client.ContainerCreate(ctx, opts)
}

func newLiveCompute(t *testing.T, cmds map[string][]string) (*DockerCompute, *mobyclient.Client) {
	t.Helper()

	cli, err := mobyclient.New(mobyclient.FromEnv)
	if err != nil {
		t.Skipf("docker client: %v", err)
	}

	t.Cleanup(func() { _ = cli.Close() })

	if _, pingErr := cli.Ping(t.Context(), mobyclient.PingOptions{}); pingErr != nil {
		t.Skipf("docker unavailable: %v", pingErr)
	}

	if _, imgErr := cli.ImageInspect(t.Context(), liveImage); imgErr != nil {
		t.Skipf("%s not present: %v", liveImage, imgErr)
	}

	return newDockerComputeWithAPI(&liveDockerAPI{Client: cli, cmds: cmds}, DockerComputeConfig{Image: liveImage}), cli
}

func uniqueVPC(t *testing.T) string {
	t.Helper()

	var b [4]byte

	_, err := rand.Read(b[:])
	require.NoError(t, err)

	return "vpc-live" + hex.EncodeToString(b[:])
}

func launchLive(t *testing.T, dc *DockerCompute, req LaunchRequest) LaunchResult {
	t.Helper()

	res, err := dc.Launch(t.Context(), req)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = dc.Terminate(context.Background(), req.InstanceID, res.ProviderID)
	})

	return res
}

func TestDockerComputeLiveVPCReachability(t *testing.T) {
	t.Parallel()

	const cidr = "10.231.0.0/16"

	vpc := uniqueVPC(t)
	serverIP, clientIP := "10.231.1.4", "10.231.2.4"
	serverName, clientName := "gopherstack-i-live-srv-"+vpc, "gopherstack-i-live-cli-"+vpc
	dc, cli := newLiveCompute(t, map[string][]string{
		serverName: {"sh", "-c", "while true; do echo pong | nc -l -p 9000; done"},
		clientName: {"sh", "-c", "sleep 2; nc -w 3 " + serverIP + " 9000 | grep -q pong"},
	})

	t.Cleanup(func() { _ = dc.RemoveVPCNetwork(context.Background(), vpc) })

	srv := launchLive(t, dc, LaunchRequest{
		InstanceID: "i-live-srv-" + vpc, VPCID: vpc, VPCCIDR: cidr, PrivateIP: serverIP,
	})
	if srv.NetworkNote != "" {
		t.Skipf("host networks overlap the test CIDR: %s", srv.NetworkNote)
	}

	assert.Equal(t, serverIP, srv.PrivateIP)

	cliRes := launchLive(t, dc, LaunchRequest{
		InstanceID: "i-live-cli-" + vpc, VPCID: vpc, VPCCIDR: cidr, PrivateIP: clientIP,
	})
	assert.Equal(t, clientIP, cliRes.PrivateIP)

	require.Eventually(t, func() bool {
		res, err := cli.ContainerInspect(t.Context(), cliRes.ProviderID, mobyclient.ContainerInspectOptions{})
		if err != nil || res.Container.State == nil || res.Container.State.Running {
			return false
		}

		return res.Container.State.ExitCode == 0
	}, liveTimeout, liveTick, "client container must reach the server by private IP and exit 0")
}

func TestDockerComputeLivePublishedPort(t *testing.T) {
	t.Parallel()

	vpc := uniqueVPC(t)
	name := "gopherstack-i-live-web-" + vpc
	dc, _ := newLiveCompute(t, map[string][]string{
		name: {"sh", "-c", "while true; do echo pong | nc -l -p 8080; done"},
	})

	t.Cleanup(func() { _ = dc.RemoveVPCNetwork(context.Background(), vpc) })

	res := launchLive(t, dc, LaunchRequest{
		InstanceID: "i-live-web-" + vpc, VPCID: vpc, VPCCIDR: "10.232.0.0/16", PrivateIP: "10.232.1.4",
		PublishPorts: []PortSpec{{Protocol: "tcp", Port: 8080}},
	})

	require.Len(t, res.PublishedPorts, 1)
	assert.Equal(t, "127.0.0.1", res.PublishedPorts[0].HostIP)

	addr := net.JoinHostPort(res.PublishedPorts[0].HostIP, strconv.Itoa(res.PublishedPorts[0].HostPort))

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()

		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		if err != nil {
			return false
		}

		defer func() { _ = conn.Close() }()

		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf, _ := io.ReadAll(conn)

		return string(buf) == "pong\n"
	}, liveTimeout, liveTick, "host must reach the container through the published port")
}
