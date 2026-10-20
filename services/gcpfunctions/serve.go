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

var (
	errStartFailed = errors.New("function failed to start")
	errStale       = errors.New("function was redeployed while starting; retry")
)

// box is the container and scratch directory serving one function revision.
type box struct {
	id   string
	dir  string
	port int
}

func (b *InMemoryBackend) takeBoxLocked(r *fnRec) box {
	x := box{id: r.containerID, dir: r.srcDir, port: r.hostPort}
	r.containerID, r.srcDir, r.hostPort = "", "", 0

	return x
}

func (b *InMemoryBackend) takeAllLocked() []box {
	out := make([]box, 0, len(b.functions))
	for _, r := range b.functions {
		out = append(out, b.takeBoxLocked(r))
	}

	return out
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

// teardown stops x's container, frees its port and removes its source directory.
func (b *InMemoryBackend) teardown(x box) {
	removeDir(x.dir)

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

func functionSpec(fn *functionspb.Function, rt *runtimeSpec, srcDir string, hostPort int) container.Spec {
	id := lastSegment(fn.GetName())
	sc := fn.GetServiceConfig()

	env := make([]string, 0, len(sc.GetEnvironmentVariables())+envExtra)
	for k, v := range sc.GetEnvironmentVariables() {
		env = append(env, k+"="+v)
	}

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
		Mounts:     []string{srcDir + ":" + srcMount + ":ro"},
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

// launch starts the container for fn and waits until it accepts connections.
func (b *InMemoryBackend) launch(fn *functionspb.Function, data []byte) (box, error) {
	rt, err := lookupRuntime(fn.GetBuildConfig().GetRuntime())
	if err != nil {
		return box{}, err
	}

	runner, err := b.runner()
	if err != nil {
		return box{}, err
	}

	dir, err := materialize(data, rt)
	if err != nil {
		return box{}, err
	}

	pool := b.portPool()

	port, err := gcpserve.AcquirePort(b.baseCtx(), pool, "gcpfunctions")
	if err != nil {
		removeDir(dir)

		return box{}, fmt.Errorf("allocate host port: %w", err)
	}

	b.mu.RLock("launch")
	timeout := b.startupTimeout
	b.mu.RUnlock()

	ctx, cancel := context.WithTimeout(b.baseCtx(), timeout)
	defer cancel()

	id, err := runner.CreateAndStart(ctx, functionSpec(fn, rt, dir, port))
	if err == nil {
		err = gcpserve.WaitReady(ctx, port)
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("function did not become ready within %s: %w", timeout, err)
		}

		if err != nil {
			b.stopContainer(runner, id)
		}
	}

	x := box{id: id, dir: dir, port: port}
	if err != nil {
		x.id = ""
		b.teardown(x)

		return box{}, fmt.Errorf("start container: %w", err)
	}

	return x, nil
}

// startFunction starts name's container once; concurrent callers wait for the same start.
func (b *InMemoryBackend) startFunction(ctx context.Context, name string) error {
	b.mu.Lock("startFunction")

	r, ok := b.functions[name]
	if !ok {
		b.mu.Unlock()

		return errStartFailed
	}

	if r.containerID != "" {
		b.mu.Unlock()

		return nil
	}

	if ch := r.starting; ch != nil {
		b.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}

		return b.startResult(name)
	}

	r.starting = make(chan struct{})
	ch, pb, data, epoch := r.starting, clone(r.pb), r.zip, r.epoch
	b.mu.Unlock()

	x, err := b.launch(pb, data)

	b.mu.Lock("startFunction.done")

	cur, still := b.functions[name]
	if still && cur.epoch != epoch && err == nil {
		err = errStale
	}

	if still {
		cur.starting, cur.startErr = nil, ""

		if err != nil {
			cur.startErr = err.Error()
		} else {
			cur.containerID, cur.srcDir, cur.hostPort = x.id, x.dir, x.port
		}
	}
	b.mu.Unlock()
	close(ch)

	if (!still || err != nil) && x.id != "" {
		b.teardown(x)
	}

	return err
}

func (b *InMemoryBackend) startResult(name string) error {
	b.mu.RLock("startResult")
	defer b.mu.RUnlock()

	if r, ok := b.functions[name]; ok && r.startErr != "" {
		return fmt.Errorf("%w: %s", errStartFailed, r.startErr)
	}

	return nil
}
