package gcpfunctions

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/functions/apiv2/functionspb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	containerPort   = 8080
	teardownTimeout = 15 * time.Second
	sigHTTP         = "http"
	sigCloudEvent   = "cloudevent"
	envExtra        = 7
)

var errStartFailed = errors.New("function failed to start")

// box is one running instance: its container, scratch directories and host port.
type box struct {
	id   string
	dirs []string
	port int
}

func (b *InMemoryBackend) takePoolLocked(r *fnRec) *gcpscale.Pool[box] {
	p := r.pool
	r.pool = nil

	return p
}

func (b *InMemoryBackend) takeAllLocked() []*gcpscale.Pool[box] {
	out := make([]*gcpscale.Pool[box], 0, len(b.functions))

	for _, r := range b.functions {
		if p := b.takePoolLocked(r); p != nil {
			out = append(out, p)
		}
	}

	return out
}

func closeAll(pools []*gcpscale.Pool[box]) {
	for _, p := range pools {
		p.Close()
	}
}

func (b *InMemoryBackend) baseCtx() context.Context {
	b.mu.RLock("baseCtx")
	defer b.mu.RUnlock()

	return b.runCtx
}

func (b *InMemoryBackend) runner() (Runner, error) {
	b.mu.RLock("runner")
	fn := b.runtime
	b.mu.RUnlock()

	if fn == nil {
		return nil, errNoRuntime
	}

	r, err := fn()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errNoRuntime, err)
	}

	return r, nil
}

func (b *InMemoryBackend) portPool() PortPool {
	b.mu.RLock("portPool")
	defer b.mu.RUnlock()

	return b.ports
}

func (b *InMemoryBackend) stopContainer(r Runner, id string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(b.baseCtx()), teardownTimeout)
	defer cancel()

	if err := r.StopAndRemove(ctx, id); err != nil {
		logger.Load(ctx).WarnContext(ctx, "gcpfunctions: container removal failed", "container", id, "error", err)
	}
}

// teardown stops x's container, frees its port and removes its scratch directories.
func (b *InMemoryBackend) teardown(x box) {
	for _, d := range x.dirs {
		removeDir(d)
	}

	if x.id != "" {
		if r, err := b.runner(); err == nil {
			b.stopContainer(r, x.id)
		}
	}

	gcpserve.ReleasePort(b.portPool(), x.port)
}

func signatureType(fn *functionspb.Function) string {
	if fn.GetEventTrigger() != nil {
		return sigCloudEvent
	}

	return sigHTTP
}

func functionSpec(
	fn *functionspb.Function,
	rt *runtimeSpec,
	srcDir string,
	hostPort int,
	sec *secretSet,
) container.Spec {
	id := lastSegment(fn.GetName())
	sc := fn.GetServiceConfig()

	env := make([]string, 0, len(sc.GetEnvironmentVariables())+len(sec.env)+envExtra)
	for k, v := range sc.GetEnvironmentVariables() {
		env = append(env, k+"="+v)
	}

	env = append(env, sec.env...)
	env = append(env,
		"PORT="+strconv.Itoa(containerPort),
		"FUNCTION_TARGET="+fn.GetBuildConfig().GetEntryPoint(),
		"FUNCTION_SIGNATURE_TYPE="+signatureType(fn),
		"K_SERVICE="+id,
		"K_REVISION="+sc.GetRevision(),
		"K_CONFIGURATION="+id,
		"FUNCTION_NAME="+id,
	)

	return container.Spec{
		Image:      rt.image,
		Name:       "gopherstack-gcf-" + id + "-" + strconv.Itoa(hostPort),
		Env:        env,
		Mounts:     append([]string{srcDir + ":" + srcMount + ":ro"}, sec.mounts...),
		Entrypoint: []string{"sh", "-c"},
		Cmd:        []string{rt.script()},
		Ports: []string{
			container.PortSpec(container.LoopbackHost, strconv.Itoa(hostPort), strconv.Itoa(containerPort)),
		},
	}
}

// materialize unpacks the function's source into a fresh scratch directory.
func materialize(data []byte, rt *runtimeSpec) (string, error) {
	dir, err := os.MkdirTemp("", "gopherstack-gcf-")
	if err != nil {
		return "", fmt.Errorf("create source dir: %w", err)
	}

	if err = os.Chmod(dir, dirMode); err != nil {
		removeDir(dir)

		return "", fmt.Errorf("chmod source dir: %w", err)
	}

	if err = extractArchive(data, dir); err == nil && rt.lang == langGo {
		err = prepareGo(dir)
	}

	if err != nil {
		removeDir(dir)

		return "", err
	}

	return dir, nil
}

// launch starts one instance of fn and waits until it accepts connections.
func (b *InMemoryBackend) launch(fn *functionspb.Function, data []byte) (box, error) {
	rt, err := lookupRuntime(fn.GetBuildConfig().GetRuntime())
	if err != nil {
		return box{}, err
	}

	runner, err := b.runner()
	if err != nil {
		return box{}, err
	}

	rt = b.withImage(rt)

	dir, err := materialize(data, rt)
	if err != nil {
		return box{}, err
	}

	sec, err := b.mountSecrets(fn)
	if err != nil {
		removeDir(dir)
		sec.cleanup()

		return box{}, err
	}

	port, err := gcpserve.AcquirePort(b.baseCtx(), b.portPool(), "gcpfunctions")
	if err != nil {
		removeDir(dir)
		sec.cleanup()

		return box{}, fmt.Errorf("allocate host port: %w", err)
	}

	b.mu.RLock("launch")
	timeout := b.startupTimeout
	b.mu.RUnlock()

	ctx, cancel := context.WithTimeout(b.baseCtx(), timeout)
	defer cancel()

	id, err := runner.CreateAndStart(ctx, functionSpec(fn, rt, dir, port, sec))
	if err == nil {
		b.captureOutput(runner, id, fn)

		err = gcpserve.WaitReady(ctx, port)
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("function did not become ready within %s: %w", timeout, err)
		}

		if err != nil {
			b.stopContainer(runner, id)
		}
	}

	x := box{id: id, dirs: append([]string{dir}, sec.dirs...), port: port}
	if err != nil {
		x.id = ""
		b.teardown(x)

		return box{}, fmt.Errorf("start container: %w", err)
	}

	return x, nil
}

// newPoolLocked builds the instance pool for rec's current revision.
func (b *InMemoryBackend) newPoolLocked(rec *fnRec) *gcpscale.Pool[box] {
	pb, data := clone(rec.pb), rec.zip
	sc := pb.GetServiceConfig()

	return gcpscale.New(lastSegment(pb.GetName()), gcpscale.Config[box]{
		Start: func(context.Context) (box, error) { return b.launch(pb, data) },
		Stop:  b.teardown,
		Limits: func() gcpscale.Limits {
			return gcpscale.Limits{
				Min:         int(sc.GetMinInstanceCount()),
				Max:         int(sc.GetMaxInstanceCount()),
				Concurrency: int(sc.GetMaxInstanceRequestConcurrency()),
			}
		},
		Clock:       b.clock,
		Base:        b.runCtx,
		IdleTimeout: b.idleTimeout,
	})
}

// poolFor returns name's instance pool, creating it on first use.
func (b *InMemoryBackend) poolFor(name string) (*gcpscale.Pool[box], error) {
	b.mu.Lock("poolFor")
	defer b.mu.Unlock()

	r, ok := b.functions[name]
	if !ok {
		return nil, errStartFailed
	}

	if r.pool == nil {
		r.pool = b.newPoolLocked(r)
	}

	return r.pool, nil
}

// SetImageResolver lets the base image of a runtime be replaced, for example with a mirror. The resolver gets the
// runtime id and its default image.
func (b *InMemoryBackend) SetImageResolver(fn func(runtime, image string) string) {
	b.mu.Lock("SetImageResolver")
	defer b.mu.Unlock()

	b.images = fn
}

func (b *InMemoryBackend) withImage(rt *runtimeSpec) *runtimeSpec {
	b.mu.RLock("withImage")
	fn := b.images
	b.mu.RUnlock()

	if fn == nil {
		return rt
	}

	out := *rt
	out.image = fn(rt.id, rt.image)

	return &out
}
