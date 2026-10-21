package ecs

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	dockernetwork "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/network"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter/sgtest"
)

type bindingCapture struct {
	*fakeDockerClient
	bindings atomic.Pointer[dockernetwork.PortMap]
}

func (c *bindingCapture) ContainerCreate(
	ctx context.Context, cfg *dockertypes.Config, hc *dockertypes.HostConfig,
	nc *dockernetwork.NetworkingConfig, p *ocispec.Platform, name string,
) (dockertypes.CreateResponse, error) {
	pb := hc.PortBindings
	c.bindings.Store(&pb)

	return c.fakeDockerClient.ContainerCreate(ctx, cfg, hc, nc, p, name)
}

func echoOnPort(t *testing.T, port string) {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", net.JoinHostPort("127.0.0.1", port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			go func() {
				defer func() { _ = c.Close() }()

				_, _ = io.Copy(c, c)
			}()
		}
	}()
}

func echoes(t *testing.T, addr string) bool {
	t.Helper()

	conn, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(t.Context(), "tcp", addr)
	if err != nil {
		return false
	}

	defer func() { _ = conn.Close() }()

	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	_, _ = conn.Write([]byte("x"))

	buf := make([]byte, 1)
	n, _ := conn.Read(buf)

	return n == 1
}

func freePort(t *testing.T) int {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer func() { _ = ln.Close() }()

	return ln.Addr().(*net.TCPAddr).Port
}

func TestRunTaskSecurityGroups(t *testing.T) {
	t.Parallel()

	awsvpc := func(sgs ...string) *NetworkConfiguration {
		return &NetworkConfiguration{AwsvpcConfiguration: &AwsvpcConfiguration{SecurityGroups: sgs}}
	}

	tests := []struct {
		netConfig *NetworkConfiguration
		name      string
		enforce   bool
		relayed   bool
	}{
		{name: "enforced awsvpc task with groups", enforce: true, netConfig: awsvpc("sg-1"), relayed: true},
		{name: "enforced task without groups stays direct", enforce: true},
		{name: "flag off stays direct", netConfig: awsvpc("sg-1")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hostPort := freePort(t)
			cli := &bindingCapture{fakeDockerClient: &fakeDockerClient{}}
			runner := newDockerRunnerWithClient(t.Context(), cli)
			b := NewInMemoryBackend("000000000000", "us-east-1", runner)
			runner.SetAdmitHook(b.taskAdmit)

			src := sgtest.New(sgtest.OpenTo("sg-1", 8080, "203.0.113.0/24"))
			if tt.enforce {
				b.SetSecurityGroupEnforcer(src.Enforcer())
			}

			task := &Task{
				TaskArn:              "arn:aws:ecs:us-east-1:000000000000:task/default/t1",
				NetworkConfiguration: tt.netConfig,
			}
			td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{{
				Name: "web", Image: "img", PortMappings: []PortMapping{{ContainerPort: 8080, HostPort: hostPort}},
			}}}

			require.NoError(t, runner.RunTask(task, td))

			key, err := dockernetwork.ParsePort("8080/tcp")
			require.NoError(t, err)

			bound := (*cli.bindings.Load())[key]
			require.Len(t, bound, 1)

			if !tt.relayed {
				assert.Equal(
					t,
					strconv.Itoa(hostPort),
					bound[0].HostPort,
					"the container publishes the host port itself",
				)

				return
			}

			assert.Equal(t, "127.0.0.1", bound[0].HostIP.String())
			assert.NotEqual(t, strconv.Itoa(hostPort), bound[0].HostPort)
			echoOnPort(t, bound[0].HostPort)

			addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(hostPort))
			assert.False(t, echoes(t, addr), "loopback is outside 203.0.113.0/24")

			g := sgtest.OpenTo("sg-1", 8080, "203.0.113.0/24")
			g.Ingress = append(g.Ingress, sgtest.OpenTo("", 8080, "127.0.0.1/32").Ingress...)
			src.Set(g)
			assert.True(t, echoes(t, addr), "authorized loopback connects on the next connection")

			require.NoError(t, runner.StopTask(task))
			assert.False(t, echoes(t, addr), "relay closes with the task")
		})
	}
}
