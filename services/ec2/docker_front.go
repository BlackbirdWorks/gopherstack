package ec2

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"

	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

var errNoInternalPort = errors.New("docker compute: container port is not published")

// instanceFronts are the security-group relays of one container and the host ports they answer on.
type instanceFronts struct {
	proxies []*sgfilter.Proxy
	ports   []PublishedPort
	sshPort int
}

func (f *instanceFronts) close() {
	for _, p := range f.proxies {
		p.Close()
	}
}

// frontRegistry is shared by every region view of one DockerCompute.
type frontRegistry struct {
	mu          *lockmetrics.RWMutex
	byContainer map[string]*instanceFronts
}

func newFrontRegistry() *frontRegistry {
	return &frontRegistry{mu: lockmetrics.New("ec2fronts"), byContainer: make(map[string]*instanceFronts)}
}

func (r *frontRegistry) put(id string, f *instanceFronts) {
	r.mu.Lock("put")
	defer r.mu.Unlock()

	r.byContainer[id] = f
}

func (r *frontRegistry) get(id string) (*instanceFronts, bool) {
	r.mu.RLock("get")
	defer r.mu.RUnlock()

	f, ok := r.byContainer[id]

	return f, ok
}

func (r *frontRegistry) take(id string) (*instanceFronts, bool) {
	r.mu.Lock("take")
	defer r.mu.Unlock()

	f, ok := r.byContainer[id]
	delete(r.byContainer, id)

	return f, ok
}

func (r *frontRegistry) drain() []*instanceFronts {
	r.mu.Lock("drain")
	defer r.mu.Unlock()

	out := make([]*instanceFronts, 0, len(r.byContainer))
	for id, f := range r.byContainer {
		out = append(out, f)
		delete(r.byContainer, id)
	}

	return out
}

// containerDialer dials a container port through its loopback-only published port, looked up per
// connection because Docker re-allocates the port when the container restarts.
type containerDialer struct {
	api  dockerAPI
	base sgfilter.Dialer
	port mobynetwork.Port
	id   string
}

func (c containerDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	res, err := c.api.ContainerInspect(ctx, c.id, mobyclient.ContainerInspectOptions{})
	if err != nil {
		return nil, fmt.Errorf("container inspect: %w", err)
	}

	hp := 0
	if res.Container.NetworkSettings != nil {
		hp = hostPortOf(res.Container.NetworkSettings.Ports, c.port)
	}

	if hp == 0 {
		return nil, errNoInternalPort
	}

	conn, err := c.base.DialContext(ctx, network, net.JoinHostPort(publishHostIP, strconv.Itoa(hp)))
	if err != nil {
		return nil, fmt.Errorf("dial container port: %w", err)
	}

	return conn, nil
}

func hostPortOf(pm mobynetwork.PortMap, port mobynetwork.Port) int {
	for _, b := range pm[port] {
		if hp, err := strconv.Atoi(b.HostPort); err == nil && hp > 0 {
			return hp
		}
	}

	return 0
}

// frontLaunch relays the container's published ports (and sshd when sshHostPort > 0) through
// security-group-checked listeners and reports those listeners as the instance's ports.
func (d *DockerCompute) frontLaunch(
	ctx context.Context, containerID string, req LaunchRequest, sshHostPort int, got inspected,
) (*instanceFronts, error) {
	f := &instanceFronts{}

	for _, pp := range got.published {
		if pp.Protocol != protoTCP {
			continue
		}

		fronted, err := d.frontPort(ctx, f, containerID, req, publishHostIP, 0, pp.ContainerPort)
		if err != nil {
			f.close()

			return nil, err
		}

		pp.HostPort = fronted
		f.ports = append(f.ports, pp)
	}

	if sshHostPort > 0 {
		port, err := d.frontPort(ctx, f, containerID, req, d.cfg.SSHHostIP, sshHostPort, sshPortNumber)
		if err != nil {
			f.close()

			return nil, err
		}

		f.sshPort = port
	}

	d.fronts.put(containerID, f)

	return f, nil
}

func (d *DockerCompute) frontPort(
	ctx context.Context, f *instanceFronts, containerID string, req LaunchRequest,
	host string, listenPort, containerPort int,
) (int, error) {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(listenPort)))
	if err != nil {
		return 0, fmt.Errorf("listen for port %d: %w", containerPort, err)
	}

	key, kerr := PortSpec{Protocol: protoTCP, Port: containerPort}.key()
	if kerr != nil {
		_ = ln.Close()

		return 0, fmt.Errorf("port %d: %w", containerPort, kerr)
	}

	dialer := containerDialer{api: d.api, base: d.baseDialer(), id: containerID, port: key}
	p := sgfilter.NewProxy(ln, "container:"+strconv.Itoa(containerPort), req.Admit(protoTCP, containerPort), dialer)
	p.Start()
	f.proxies = append(f.proxies, p)

	addr, _ := ln.Addr().(*net.TCPAddr)
	if addr == nil {
		return 0, nil
	}

	return addr.Port, nil
}

func (d *DockerCompute) baseDialer() sgfilter.Dialer {
	if d.dialer != nil {
		return d.dialer
	}

	return &net.Dialer{}
}

// closeFronts stops the relays of one container and returns the SSH host port they held.
func (d *DockerCompute) closeFronts(containerID string) (int, bool) {
	f, ok := d.fronts.take(containerID)
	if !ok {
		return 0, false
	}

	f.close()

	return f.sshPort, true
}
