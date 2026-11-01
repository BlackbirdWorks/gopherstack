package azurefunctions

import (
	"context"
	"fmt"
	"maps"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	hostPort        = "80"
	siteRoot        = "/home/site/wwwroot"
	secretsMount    = "/azure-functions-host/Secrets"
	teardownTimeout = 15 * time.Second
)

// box is the container and scratch directories serving one app revision.
type box struct {
	id         string
	dir        string
	secretsDir string
	port       int
}

func (b *InMemoryBackend) takeBoxLocked(a *app) box {
	x := box{id: a.containerID, dir: a.dir, secretsDir: a.secretsDir, port: a.hostPort}
	a.containerID, a.dir, a.secretsDir, a.hostPort = "", "", "", 0
	a.epoch++

	return x
}

func (b *InMemoryBackend) takeAllLocked() []box {
	out := make([]box, 0, len(b.apps))
	for _, a := range b.apps {
		out = append(out, b.takeBoxLocked(a))
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

func removeDir(dir string) {
	if dir != "" {
		_ = os.RemoveAll(dir)
	}
}

// teardown stops x's container, frees its port and removes its scratch directories.
func (b *InMemoryBackend) teardown(x box) {
	removeDir(x.dir)
	removeDir(x.secretsDir)

	if x.id != "" {
		if r, err := b.runner(); err == nil {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(b.baseCtx()), teardownTimeout)

			if stopErr := r.StopAndRemove(ctx, x.id); stopErr != nil {
				logger.Load(ctx).
					WarnContext(ctx, "azurefunctions: container removal failed", "container", x.id, "error", stopErr)
			}

			cancel()
		}
	}

	gcpserve.ReleasePort(b.portPool(), x.port)
}

func containerEnv(a *appState, rc runtimeChoice, host string) []string {
	env := map[string]string{
		"AzureWebJobsScriptRoot": siteRoot,
		"WEBSITE_SITE_NAME":      a.Name,
		"WEBSITE_SLOT_NAME":      a.slotName(),
		"WEBSITE_HOSTNAME":       host,
	}

	if rc.worker != "" {
		env[settingWorker] = rc.worker
	}

	maps.Copy(env, a.Settings)

	env["AzureWebJobsSecretStorageType"] = "files"

	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+env[k])
	}

	return out
}

func hostSpec(a *appState, rc runtimeChoice, host, dir, secretsDir string, port int) container.Spec {
	mounts := []string{secretsDir + ":" + secretsMount}
	if dir != "" {
		mounts = append(mounts, dir+":"+siteRoot+":ro")
	}

	return container.Spec{
		Image:  rc.image,
		Name:   "gopherstack-azfn-" + strs.Fold(a.Name) + "-" + strconv.Itoa(port),
		Env:    containerEnv(a, rc, host),
		Mounts: mounts,
		Ports:  []string{container.PortSpec(container.LoopbackHost, strconv.Itoa(port), hostPort)},
	}
}

func mkScratch(pattern string) (string, error) {
	dir, err := os.MkdirTemp("", pattern)
	if err != nil {
		return "", fmt.Errorf("create scratch dir: %w", err)
	}

	if err = os.Chmod(dir, 0o777); err != nil { //nolint:gosec // bind-mounted for the container's root user
		removeDir(dir)

		return "", fmt.Errorf("chmod scratch dir: %w", err)
	}

	return dir, nil
}

// launch starts a host container for a snapshot of the app and waits until it accepts connections.
func (b *InMemoryBackend) launch(st *appState, host string) (box, error) {
	if st.isWeb() {
		return b.launchWeb(st, host)
	}

	rc, err := chooseRuntime(st.Settings, st.LinuxFx)
	if err != nil {
		return box{}, err
	}

	if _, err = b.runner(); err != nil {
		return box{}, err
	}

	x := box{}

	if x.secretsDir, err = mkScratch("gopherstack-azfn-secrets-"); err != nil {
		return box{}, err
	}

	if err = writeSecrets(x.secretsDir, st); err != nil {
		b.teardown(x)

		return box{}, err
	}

	if !rc.custom {
		if x.dir, err = b.materialize(st.Zip); err != nil {
			b.teardown(x)

			return box{}, err
		}
	}

	return b.startContainer(x, func(port int) container.Spec {
		return hostSpec(st, rc, host, x.dir, x.secretsDir, port)
	}, gcpserve.WaitReady)
}

// startContainer publishes a host port, starts the container mk describes and waits until ready passes. On failure
// it tears x down and returns the error.
func (b *InMemoryBackend) startContainer(
	x box,
	mk func(port int) container.Spec,
	ready func(ctx context.Context, port int) error,
) (box, error) {
	runner, err := b.runner()
	if err != nil {
		b.teardown(x)

		return box{}, err
	}

	x.port, err = gcpserve.AcquirePort(b.baseCtx(), b.portPool(), "azurefunctions")
	if err != nil {
		b.teardown(x)

		return box{}, fmt.Errorf("allocate host port: %w", err)
	}

	b.mu.RLock("launch")
	timeout := b.startupTimeout
	b.mu.RUnlock()

	ctx, cancel := context.WithTimeout(b.baseCtx(), timeout)
	defer cancel()

	x.id, err = runner.CreateAndStart(ctx, mk(x.port))
	if err == nil {
		if err = ready(ctx, x.port); err != nil {
			stopCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), teardownTimeout)
			_ = runner.StopAndRemove(stopCtx, x.id)

			stop()

			x.id = ""
		}
	}

	if err != nil {
		x.id = ""
		b.teardown(x)

		return box{}, fmt.Errorf("start container: %w", err)
	}

	return x, nil
}

func (b *InMemoryBackend) materialize(zipData []byte) (string, error) {
	dir, err := mkScratch("gopherstack-azfn-site-")
	if err != nil {
		return "", err
	}

	if err = extractArchive(zipData, dir); err != nil {
		removeDir(dir)

		return "", err
	}

	return dir, nil
}

// servable reports whether the app has something a host can run.
func (a *app) servable() bool {
	if a.isWeb() {
		return a.webServable()
	}

	return len(a.Zip) > 0 || len(a.LinuxFx) > len(dockerPrefix) && a.LinuxFx[:len(dockerPrefix)] == dockerPrefix
}

// startClaim is the outcome of looking for work to do when a request needs the app's host.
type startClaim struct {
	wait  chan struct{}
	host  string
	st    appState
	port  int
	epoch int
	own   bool
}

// claimStart reports the running port, a start to wait for, or hands the caller the right to start the host.
func (b *InMemoryBackend) claimStart(name string) (startClaim, error) {
	b.mu.Lock("claimStart")
	defer b.mu.Unlock()

	a, err := b.appLocked(name)
	if err != nil {
		return startClaim{}, err
	}

	switch {
	case a.Stopped:
		return startClaim{}, errNoHost
	case a.containerID != "":
		return startClaim{port: a.hostPort}, nil
	case a.starting != nil:
		return startClaim{wait: a.starting}, nil
	}

	a.starting = make(chan struct{})

	return startClaim{
		own: true, wait: a.starting, st: a.appState, epoch: a.epoch, host: b.hostNameLocked(a.label()),
	}, nil
}

// ensureStarted starts name's host once; concurrent callers wait for the same start. It returns the host port.
func (b *InMemoryBackend) ensureStarted(ctx context.Context, name string) (int, error) {
	for {
		claim, err := b.claimStart(name)
		if err != nil {
			return 0, err
		}

		switch {
		case claim.own:
			return b.runStart(name, claim)
		case claim.wait == nil:
			return claim.port, nil
		}

		select {
		case <-claim.wait:
		case <-ctx.Done():
			return 0, ctx.Err()
		}

		if port, done, startErr := b.startResult(name); done {
			return port, startErr
		}
	}
}

func (b *InMemoryBackend) runStart(name string, claim startClaim) (int, error) {
	x, launchErr := b.launch(&claim.st, claim.host)

	b.mu.Lock("runStart")

	cur, still := b.apps[strs.Fold(name)]
	if still && cur.epoch != claim.epoch && launchErr == nil {
		launchErr = errStale
	}

	if still {
		cur.starting, cur.startErr = nil, ""

		if launchErr != nil {
			cur.startErr = launchErr.Error()
		} else {
			cur.containerID, cur.dir, cur.secretsDir, cur.hostPort = x.id, x.dir, x.secretsDir, x.port
		}
	}
	b.mu.Unlock()
	close(claim.wait)

	if (!still || launchErr != nil) && x.id != "" {
		b.teardown(x)
	}

	if !still {
		return 0, ErrAppNotFound
	}

	return x.port, launchErr
}

func (b *InMemoryBackend) startResult(name string) (int, bool, error) {
	b.mu.RLock("startResult")
	defer b.mu.RUnlock()

	a, ok := b.apps[strs.Fold(name)]

	switch {
	case !ok:
		return 0, true, ErrAppNotFound
	case a.startErr != "":
		return 0, true, fmt.Errorf("%w: %s", errStartFailed, a.startErr)
	case a.containerID != "":
		return a.hostPort, true, nil
	}

	return 0, false, nil
}
