package ecs

import (
	"context"
	"net"
	"net/netip"
	"strconv"
	"sync/atomic"

	dockernetwork "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/network"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
)

const loopbackAddr = "127.0.0.1"

// admitHookRunner is implemented by runners that can front published ports with a security-group check.
type admitHookRunner interface {
	SetAdmitHook(fn func(task *Task, port int) sgfilter.Admitter)
}

// SetSecurityGroupEnforcer makes awsvpc tasks that carry security groups refuse clients those groups do not
// admit on their published ports; nil leaves them open.
func (b *InMemoryBackend) SetSecurityGroupEnforcer(e *sgfilter.Enforcer) { b.sgEnforcer.Store(e) }

// SetSecurityGroupEnforcer applies to the home backend and every region built so far.
func (h *Handler) SetSecurityGroupEnforcer(e *sgfilter.Enforcer) {
	for _, bk := range h.RegionBackends() {
		if b, ok := bk.(*InMemoryBackend); ok {
			b.SetSecurityGroupEnforcer(e)
		}
	}
}

// taskAdmit returns the check for a task's container port, nil when security groups are not enforced or the
// task has none.
func (b *InMemoryBackend) taskAdmit(task *Task, port int) sgfilter.Admitter {
	enf := b.sgEnforcer.Load()
	ids := taskSecurityGroups(task)

	if enf == nil || len(ids) == 0 {
		return nil
	}

	return func(remote net.Addr) bool {
		return enf.AdmitIngress(ids, sgfilter.ProtoTCP, port, remote)
	}
}

func taskSecurityGroups(task *Task) []string {
	nc := task.NetworkConfiguration
	if nc == nil || nc.AwsvpcConfiguration == nil {
		return nil
	}

	return append([]string(nil), nc.AwsvpcConfiguration.SecurityGroups...)
}

func inheritEnforcer(dst, src *InMemoryBackend) { dst.sgEnforcer.Store(src.sgEnforcer.Load()) }

type pendingFront struct {
	admit         sgfilter.Admitter
	listen        int
	inner         int
	containerPort int
}

type containerFronts struct {
	pending []pendingFront
	relays  []*sgfilter.Proxy
}

// sgFronts tracks, per container, the relays that enforce its security groups.
type sgFronts struct {
	mu          *lockmetrics.RWMutex
	byContainer map[string]*containerFronts
	hook        atomic.Pointer[func(task *Task, port int) sgfilter.Admitter]
}

func newSGFronts() *sgFronts {
	return &sgFronts{mu: lockmetrics.New("ecsfronts"), byContainer: make(map[string]*containerFronts)}
}

// SetAdmitHook installs the per-port security-group check source.
func (r *realDockerRunner) SetAdmitHook(fn func(task *Task, port int) sgfilter.Admitter) {
	r.fronts.hook.Store(&fn)
}

// divertPorts re-publishes the task's host-port mappings that security groups guard onto loopback ports and
// remembers the relays to start once the container runs. Mappings with no check keep their binding.
func (r *realDockerRunner) divertPorts(task *Task, mappings []PortMapping, pb dockernetwork.PortMap) []pendingFront {
	hook := r.fronts.hook.Load()
	if hook == nil {
		return nil
	}

	var out []pendingFront

	for _, pm := range mappings {
		if pm.HostPort <= 0 || pm.ContainerPort <= 0 {
			continue
		}

		admit := (*hook)(task, pm.ContainerPort)
		if admit == nil {
			continue
		}

		key, err := dockernetwork.ParsePort(strconv.Itoa(pm.ContainerPort) + "/" + transportTCP)
		if err != nil || (pm.Protocol != "" && pm.Protocol != transportTCP) {
			continue
		}

		inner, aerr := portalloc.AcquireEphemeral()
		if aerr != nil {
			continue
		}

		pb[key] = []dockernetwork.PortBinding{
			{HostIP: netip.MustParseAddr(loopbackAddr), HostPort: strconv.Itoa(inner)},
		}
		out = append(
			out,
			pendingFront{admit: admit, listen: pm.HostPort, inner: inner, containerPort: pm.ContainerPort},
		)
	}

	return out
}

func (r *realDockerRunner) stashFronts(containerID string, pending []pendingFront) {
	if len(pending) == 0 {
		return
	}

	r.fronts.mu.Lock("stash")
	defer r.fronts.mu.Unlock()

	r.fronts.byContainer[containerID] = &containerFronts{pending: pending}
}

func releasePending(pending []pendingFront) {
	for _, p := range pending {
		portalloc.ReleaseEphemeral(p.inner)
	}
}

// startFronts opens the relays for a started container; on failure it closes what it opened.
func (r *realDockerRunner) startFronts(containerID string) error {
	r.fronts.mu.Lock("start")
	cf := r.fronts.byContainer[containerID]
	r.fronts.mu.Unlock()

	if cf == nil {
		return nil
	}

	for _, p := range cf.pending {
		relay, err := sgfilter.Front(context.Background(),
			net.JoinHostPort(bindAllInterfaces, strconv.Itoa(p.listen)),
			net.JoinHostPort(loopbackAddr, strconv.Itoa(p.inner)), p.admit)
		if err != nil {
			r.closeFronts(containerID)

			return err
		}

		cf.relays = append(cf.relays, relay)
	}

	return nil
}

// closeFronts stops a container's relays and frees its loopback ports.
func (r *realDockerRunner) closeFronts(containerID string) {
	r.fronts.mu.Lock("close")
	cf := r.fronts.byContainer[containerID]
	delete(r.fronts.byContainer, containerID)
	r.fronts.mu.Unlock()

	if cf == nil {
		return
	}

	for _, relay := range cf.relays {
		relay.Close()
	}

	releasePending(cf.pending)
}
