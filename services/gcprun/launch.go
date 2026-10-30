package gcprun

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync/atomic"

	"cloud.google.com/go/run/apiv2/runpb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

// prepared is one container of an instance with its environment, mounts and probes resolved.
type prepared struct {
	c        *runpb.Container
	env      []string
	mounts   []string
	startup  probeSpec
	liveness probeSpec
	hasStart bool
	hasLive  bool
	isIngres bool
}

func probePorts(cs []*runpb.Container) []int {
	var ports []int

	for _, c := range cs {
		for _, p := range []*runpb.Probe{c.GetStartupProbe(), c.GetLivenessProbe()} {
			if spec, ok := resolveProbe(c, p); ok && !slices.Contains(ports, spec.target.ContainerPort) {
				ports = append(ports, spec.target.ContainerPort)
			}
		}
	}

	return ports
}

func (b *InMemoryBackend) prepare(rev *runpb.Revision, paths map[string]string) ([]prepared, error) {
	n, err := parseName(rev.GetName(), "services", "revisions")
	if err != nil {
		return nil, err
	}

	order, err := startOrder(rev.GetContainers())
	if err != nil {
		return nil, err
	}

	ing, ingPort := ingressContainer(rev.GetContainers())
	svc, revName := lastSegment(rev.GetService()), lastSegment(rev.GetName())
	ro := secretVolumes(rev.GetVolumes())
	out := make([]prepared, 0, len(order))

	for _, c := range order {
		env, eerr := b.revisionEnv(n.project, c)
		if eerr != nil {
			return nil, eerr
		}

		env = append(env, "K_SERVICE="+svc, "K_REVISION="+revName, "K_CONFIGURATION="+svc)
		if c == ing {
			env = append(env, "PORT="+strconv.Itoa(ingPort))
		}

		mounts, merr := containerMounts(c, paths, ro)
		if merr != nil {
			return nil, merr
		}

		p := prepared{c: c, env: env, mounts: mounts, isIngres: c == ing}
		p.startup, p.hasStart = resolveProbe(c, c.GetStartupProbe())
		p.liveness, p.hasLive = resolveProbe(c, c.GetLivenessProbe())
		out = append(out, p)
	}

	return out, nil
}

func containerSpec(rev *runpb.Revision, p prepared, seq uint64, idx int) container.Spec {
	return container.Spec{
		Image:      p.c.GetImage(),
		Name:       fmt.Sprintf("gopherstack-run-%s-%d-%d", lastSegment(rev.GetName()), seq, idx),
		Env:        p.env,
		Mounts:     p.mounts,
		WorkingDir: p.c.GetWorkingDir(),
		Entrypoint: p.c.GetCommand(),
		Cmd:        p.c.GetArgs(),
	}
}

// inst is one running copy of a revision: every container of the revision sharing a network namespace.
type inst struct {
	mctx      context.Context
	ports     map[int]int
	cancel    context.CancelFunc
	hostPorts []int
	ids       []string
	dirs      []string
	seq       uint64
	hostPort  int
	cleaned   atomic.Bool
}

func (in *inst) id() string {
	if len(in.ids) == 0 {
		return ""
	}

	return in.ids[0]
}

// startInst brings up one instance of the revision for its pool; ctx ends when the pool retires it.
func (b *InMemoryBackend) startInst(ctx context.Context, name string) (*inst, error) {
	b.mu.RLock("startInst")

	r := b.revisions[name]
	if r == nil {
		b.mu.RUnlock()

		return nil, gcpscale.ErrRetired
	}

	rev := clone(r.pb)
	b.mu.RUnlock()

	in := &inst{seq: b.instSeq.Add(1)}
	in.mctx, in.cancel = context.WithCancel(ctx)

	if err := b.startInstance(in, rev); err != nil {
		b.teardownInst(in)
		b.setStartErr(name, err.Error())

		return nil, err
	}

	b.setStartErr(name, "")
	b.startMonitors(name, in, rev)

	return in, nil
}

func (b *InMemoryBackend) setStartErr(name, msg string) {
	b.mu.Lock("setStartErr")
	defer b.mu.Unlock()

	if r := b.revisions[name]; r != nil {
		r.startErr = msg
	}
}

func (b *InMemoryBackend) startMonitors(name string, in *inst, rev *runpb.Revision) {
	for _, c := range rev.GetContainers() {
		if spec, ok := resolveProbe(c, c.GetLivenessProbe()); ok {
			b.monWG.Go(func() { b.monitorLiveness(in.mctx, spec, in, name) })
		}
	}
}

func (b *InMemoryBackend) startInstance(in *inst, rev *runpb.Revision) error {
	runner, err := b.runner()
	if err != nil {
		return err
	}

	b.mu.RLock("startInstance")
	timeout := b.startupTimeout
	b.mu.RUnlock()

	ctx, cancel := context.WithTimeout(in.mctx, timeout)
	defer cancel()

	project := ""
	if n, perr := parseName(rev.GetName(), "services", "revisions"); perr == nil {
		project = n.project
	}

	paths, dirs, err := b.materializeVolumes(project, rev.GetVolumes())
	in.dirs = dirs

	if err != nil {
		return fmt.Errorf("%w: %w", errInstanceStart, err)
	}

	prep, err := b.prepare(rev, paths)
	if err != nil {
		return fmt.Errorf("%w: %w", errInstanceStart, err)
	}

	for i, p := range prep {
		if err = b.startContainer(ctx, runner, in, rev, p, i); err != nil {
			return err
		}
	}

	_, ingPort := ingressContainer(rev.GetContainers())
	in.hostPort = in.ports[ingPort]

	ing := slices.IndexFunc(prep, func(p prepared) bool { return p.isIngres })
	if !prep[ing].hasStart {
		return gcpserve.WaitReady(ctx, in.hostPort)
	}

	return nil
}

func (b *InMemoryBackend) startContainer(
	ctx context.Context, runner Runner, in *inst, rev *runpb.Revision, p prepared, idx int,
) error {
	spec := containerSpec(rev, p, in.seq, idx)

	var (
		id  string
		err error
	)

	if idx == 0 {
		id, err = b.startOwner(ctx, runner, in, rev, spec)
	} else {
		spec.Network = "container:" + in.ids[0]
		id, err = runner.CreateAndStart(ctx, spec)
	}

	if err != nil {
		return fmt.Errorf("start container %q: %w", p.c.GetName(), err)
	}

	in.ids = append(in.ids, id)
	b.captureOutput(runner, id, rev, in.ids[0])

	if p.hasStart {
		if err = b.runStartupProbe(ctx, p.startup, in.ports); err != nil {
			return err
		}
	}

	return nil
}

// startOwner starts the container that owns the instance's network namespace and publishes the ingress and probe ports.
func (b *InMemoryBackend) startOwner(
	ctx context.Context, runner Runner, in *inst, rev *runpb.Revision, spec container.Spec,
) (string, error) {
	_, ingPort := ingressContainer(rev.GetContainers())
	ports := []int{ingPort}

	for _, p := range probePorts(rev.GetContainers()) {
		if !slices.Contains(ports, p) {
			ports = append(ports, p)
		}
	}

	release := func() {
		for _, hp := range in.hostPorts {
			b.releasePort(hp)
		}

		in.hostPorts, in.ports = nil, nil
	}

	return container.CreateAndStartRetry(ctx, runner,
		func() (container.Spec, error) {
			in.ports = make(map[int]int, len(ports))
			spec.Ports = spec.Ports[:0]

			for _, cp := range ports {
				hp, err := b.acquirePort()
				if err != nil {
					release()

					return container.Spec{}, fmt.Errorf("allocate host port: %w", err)
				}

				in.hostPorts = append(in.hostPorts, hp)
				in.ports[cp] = hp
				spec.Ports = append(spec.Ports, container.PortSpec(
					container.LoopbackHost, strconv.Itoa(hp), strconv.Itoa(cp)))
			}

			return spec, nil
		}, release)
}

// teardownInst retires an instance: its monitors, containers, host ports and volume directories.
func (b *InMemoryBackend) teardownInst(in *inst) {
	if in == nil {
		return
	}

	if in.cancel != nil {
		in.cancel()
	}

	b.cleanupInst(in)
}

func (b *InMemoryBackend) cleanupInst(in *inst) {
	if !in.cleaned.CompareAndSwap(false, true) {
		return
	}

	if len(in.ids) > 0 {
		if runner, err := b.runner(); err == nil {
			for _, id := range slices.Backward(in.ids) {
				b.stopContainer(runner, id)
			}
		}
	}

	for _, hp := range in.hostPorts {
		b.releasePort(hp)
	}

	removeDirs(in.dirs)
}
