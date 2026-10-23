package ec2

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter/sgtest"
)

type portsAPI struct {
	*fakeDockerAPI
	ports mobynetwork.PortMap
}

func (p portsAPI) ContainerInspect(
	_ context.Context, _ string, _ mobyclient.ContainerInspectOptions,
) (mobyclient.ContainerInspectResult, error) {
	return mobyclient.ContainerInspectResult{Container: mobycontainer.InspectResponse{
		NetworkSettings: &mobycontainer.NetworkSettings{Ports: p.ports},
	}}, nil
}

func echoServer(t *testing.T) int {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
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

	return ln.Addr().(*net.TCPAddr).Port
}

// roundTrip reports whether a byte written to addr comes back.
func roundTrip(t *testing.T, addr string) bool {
	t.Helper()

	c, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)

	defer func() { _ = c.Close() }()

	require.NoError(t, c.SetDeadline(time.Now().Add(5*time.Second)))
	_, _ = c.Write([]byte("x"))

	buf := make([]byte, 1)
	n, _ := c.Read(buf)

	return n == 1
}

func TestFrontLaunchAdmitsByLiveGroups(t *testing.T) {
	t.Parallel()

	upstream := echoServer(t)
	hp := strconv.Itoa(upstream)
	tcp80, err := mobynetwork.ParsePort("8080/tcp")
	require.NoError(t, err)

	api := portsAPI{
		fakeDockerAPI: &fakeDockerAPI{},
		ports: mobynetwork.PortMap{tcp80: {{
			HostIP: parseHostIP(publishHostIP), HostPort: hp,
		}}},
	}
	d := newDockerComputeWithAPI(api, DockerComputeConfig{})

	src := sgtest.New(sgtest.OpenTo("sg-1", 8080, "203.0.113.0/24"))
	var groups atomic.Pointer[[]string]

	setGroups := func(ids ...string) { groups.Store(&ids) }
	setGroups("sg-1")

	enf := src.Enforcer()
	req := LaunchRequest{Admit: func(proto string, port int) sgfilter.Admitter {
		return enf.Ingress(func() []string { return *groups.Load() }, proto, port)
	}}
	got := inspected{published: []PublishedPort{
		{Protocol: protoTCP, ContainerPort: 8080, HostIP: publishHostIP, HostPort: upstream},
		{Protocol: protoUDP, ContainerPort: 53, HostIP: publishHostIP, HostPort: 1},
	}}

	f, err := d.frontLaunch(t.Context(), "ctr-1", req, 0, got)
	require.NoError(t, err)
	t.Cleanup(func() { d.closeFronts("ctr-1") })

	require.Len(t, f.ports, 1, "udp is not relayed")
	assert.NotEqual(t, upstream, f.ports[0].HostPort, "clients get the relay port, not the container's")

	addr := net.JoinHostPort(publishHostIP, strconv.Itoa(f.ports[0].HostPort))

	tests := []struct {
		mutate func()
		name   string
		want   bool
	}{
		{name: "foreign cidr refused", mutate: func() {}},
		{name: "authorize loopback admits next connection", want: true, mutate: func() {
			src.Set(sgtest.OpenTo("sg-1", 8080, "203.0.113.0/24"), sgtest.OpenTo("sg-2", 8080, "127.0.0.1/32"))
			setGroups("sg-1", "sg-2")
		}},
		{name: "group swap refuses again", mutate: func() { setGroups("sg-1") }},
	}

	for _, tt := range tests {
		tt.mutate()
		assert.Equal(t, tt.want, roundTrip(t, addr), tt.name)
	}

	pp, err := d.PublishedPorts(t.Context(), "ctr-1")
	require.NoError(t, err)
	assert.Equal(t, f.ports, pp)
}

func TestFrontLaunchSSHPortAndRelease(t *testing.T) {
	t.Parallel()

	upstream := echoServer(t)
	ssh, err := mobynetwork.ParsePort("22/tcp")
	require.NoError(t, err)

	api := portsAPI{
		fakeDockerAPI: &fakeDockerAPI{},
		ports:         mobynetwork.PortMap{ssh: {{HostPort: strconv.Itoa(upstream)}}},
	}
	d := newDockerComputeWithAPI(api, DockerComputeConfig{SSHHostIP: "127.0.0.1"})
	src := sgtest.New(sgtest.OpenTo("sg-ssh", sshPortNumber, "127.0.0.0/8"))
	enf := src.Enforcer()
	req := LaunchRequest{Admit: func(proto string, port int) sgfilter.Admitter {
		return enf.Ingress(func() []string { return []string{"sg-ssh"} }, proto, port)
	}}

	f, err := d.frontLaunch(t.Context(), "ctr-2", req, 0, inspected{})
	require.NoError(t, err)
	assert.Zero(t, f.sshPort, "sshd is not published without a reserved host port")
	d.closeFronts("ctr-2")

	f, err = d.frontLaunch(t.Context(), "ctr-3", req, freeTCPPort(t), inspected{})
	require.NoError(t, err)
	assert.NotZero(t, f.sshPort)
	assert.True(t, roundTrip(t, net.JoinHostPort("127.0.0.1", strconv.Itoa(f.sshPort))))

	port, ok := d.closeFronts("ctr-3")
	assert.True(t, ok)
	assert.Equal(t, f.sshPort, port)

	_, ok = d.closeFronts("ctr-3")
	assert.False(t, ok)
}

func freeTCPPort(t *testing.T) int {
	t.Helper()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer func() { _ = ln.Close() }()

	return ln.Addr().(*net.TCPAddr).Port
}

func TestBuildContainerConfigsFrontedBindsLoopback(t *testing.T) {
	t.Parallel()

	d := newDockerComputeWithAPI(&fakeDockerAPI{}, DockerComputeConfig{SSHHostIP: "0.0.0.0"})
	admit := func(string, int) sgfilter.Admitter { return func(net.Addr) bool { return true } }

	tests := []struct {
		name    string
		admit   func(string, int) sgfilter.Admitter
		sshHost string
		sshPort string
		want    int
	}{
		{"direct publishes on ssh host ip", nil, "0.0.0.0", "2222", 2},
		{"fronted keeps everything on loopback", admit, "127.0.0.1", "", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := LaunchRequest{Admit: tt.admit, PublishPorts: []PortSpec{
				{Protocol: protoTCP, Port: 8080}, {Protocol: protoUDP, Port: 53},
			}}
			host, _, _ := d.buildContainerConfigs(2222, req, vpcNetwork{}, false)

			ssh := host.PortBindings[containerSSHPort]
			require.Len(t, ssh, 1)
			assert.Equal(t, tt.sshHost, ssh[0].HostIP.String())
			assert.Equal(t, tt.sshPort, ssh[0].HostPort)

			published := 0
			for k := range host.PortBindings {
				if k != containerSSHPort {
					published++
				}
			}

			if tt.admit != nil {
				assert.Equal(t, 1, published, "udp ports are not published when fronted")
			} else {
				assert.Equal(t, 2, published)
			}
		})
	}
}

func TestDeclaredTCPPortsCoversRestrictedRules(t *testing.T) {
	t.Parallel()

	sg := &SecurityGroup{IngressRules: []SecurityGroupRule{
		{Protocol: "tcp", IPRange: "10.0.0.0/8", FromPort: 8080, ToPort: 8080},
		{Protocol: "udp", IPRange: worldIPv4, FromPort: 53, ToPort: 53},
		{Protocol: "tcp", SourceGroupID: "sg-x", FromPort: 22, ToPort: 22},
	}}

	assert.Equal(t, []PortSpec{{Protocol: protoTCP, Port: 8080}}, declaredTCPPorts([]*SecurityGroup{sg}))
	assert.Equal(t, []PortSpec{{Protocol: protoUDP, Port: 53}}, worldOpenPorts([]*SecurityGroup{sg}))
}
