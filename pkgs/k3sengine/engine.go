package k3sengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	defaultHost    = "127.0.0.1"
	startTimeout   = 5 * time.Minute
	probeInterval  = time.Second
	probeTimeout   = 5 * time.Second
	stopTimeout    = 30 * time.Second
	minTokenLen    = 16
	caPollInterval = 500 * time.Millisecond
	logTailTimeout = 5 * time.Second
	logTailLines   = "40"
)

var (
	// ErrInvalidConfig is returned when the engine host or token is unusable.
	ErrInvalidConfig = errors.New("invalid k3s cluster engine configuration")
	// ErrDefaultTokenExposed is returned when the public default token would be exposed off-host.
	ErrDefaultTokenExposed = errors.New("a cluster token must be set when the cluster host is not loopback")
	// ErrSuperseded is returned by Launch when the caller's record callback declined the new container.
	ErrSuperseded = errors.New("cluster was superseded while starting")
	// ErrNotAdoptable is returned when a leftover container cannot be taken over.
	ErrNotAdoptable = errors.New("container cannot be adopted")
	// ErrNodeExited is returned when the node container stops while the engine waits for its API server.
	ErrNodeExited = errors.New("node container exited")
	// ErrRotateFailed is returned when the node could not drop its certificates.
	ErrRotateFailed = errors.New("certificate rotation failed")
	// ErrStopUnsupported is returned by Stop and Start when the runtime cannot keep a stopped container.
	ErrStopUnsupported = errors.New("container runtime cannot stop and restart a container in place")

	tokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~-]+$`)
	hostPattern  = regexp.MustCompile(`^[A-Za-z0-9.:-]+$`)
)

// Runtime is the slice of container.Runtime the engine needs.
type Runtime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// Stoppable is implemented by runtimes that can stop a container and start it again with its data and ports.
type Stoppable interface {
	StopContainer(ctx context.Context, containerID string) error
	StartContainer(ctx context.Context, containerID string) error
}

// Config configures an Engine. Prefix names containers and port labels ("eks", "gke").
type Config struct {
	Runtime      Runtime
	Ports        *portalloc.Allocator
	Logger       *slog.Logger
	Probe        func(ctx context.Context, addr, token string) ([]byte, error)
	Prefix       string
	Host         string
	Token        string
	StartTimeout time.Duration
}

// Engine starts and stops single-node k3s containers.
type Engine struct {
	cfg Config
}

// NormalizeAccess validates host and token and fills their defaults.
func NormalizeAccess(host, token string) (string, string, error) {
	if host == "" {
		host = defaultHost
	}

	defaulted := token == ""
	if defaulted {
		token = DefaultToken
	}

	if !hostPattern.MatchString(host) || !tokenPattern.MatchString(token) || len(token) < minTokenLen {
		return "", "", fmt.Errorf("%w: host must match %s and token %s (min %d chars)",
			ErrInvalidConfig, hostPattern, tokenPattern, minTokenLen)
	}

	if defaulted && container.BindHostFor(host) != container.LoopbackHost {
		return "", "", ErrDefaultTokenExposed
	}

	return host, token, nil
}

// New validates cfg and returns an Engine with defaults applied.
func New(cfg Config) (*Engine, error) {
	var err error

	if cfg.Host, cfg.Token, err = NormalizeAccess(cfg.Host, cfg.Token); err != nil {
		return nil, err
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = startTimeout
	}

	return &Engine{cfg: cfg}, nil
}

// Host is the host clients reach the API server on.
func (e *Engine) Host() string { return e.cfg.Host }

// Token is the static cluster-admin bearer token.
func (e *Engine) Token() string { return e.cfg.Token }

// Runtime is the container runtime the engine starts nodes on.
func (e *Engine) Runtime() Runtime { return e.cfg.Runtime }

// Addr is the host:port of an API server published on port.
func (e *Engine) Addr(port int) string { return net.JoinHostPort(e.cfg.Host, strconv.Itoa(port)) }

// Log returns the configured logger, or the context's.
func (e *Engine) Log(ctx context.Context) *slog.Logger {
	if e.cfg.Logger != nil {
		return e.cfg.Logger
	}

	return logger.Load(ctx)
}

// AcquirePort takes a host port labelled for name.
func (e *Engine) AcquirePort(name string) (int, error) {
	return portalloc.AcquireFrom(e.cfg.Ports, e.cfg.Prefix+"-"+name)
}

// ReleasePort returns a port taken by AcquirePort.
func (e *Engine) ReleasePort(port int) { portalloc.ReleaseTo(e.cfg.Ports, port) }

// ReleaseAll returns an API port and its load-balancer host ports.
func (e *Engine) ReleaseAll(port int, lbHost []int) {
	e.ReleasePort(port)

	for _, p := range lbHost {
		e.ReleasePort(p)
	}
}

// Remove stops and removes a container, then releases ports.
func (e *Engine) Remove(containerID string, ports ...int) {
	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()

	if err := e.cfg.Runtime.StopAndRemove(ctx, containerID); err != nil {
		e.Log(ctx).WarnContext(ctx, e.cfg.Prefix+": cluster container removal failed",
			"container", containerID, "error", err)
	}

	for _, p := range ports {
		e.ReleasePort(p)
	}
}

// Stop stops the container but keeps it, so its filesystem (the cluster state) and published ports survive.
func (e *Engine) Stop(ctx context.Context, containerID string) error {
	rt, ok := e.cfg.Runtime.(Stoppable)
	if !ok {
		return ErrStopUnsupported
	}

	return rt.StopContainer(ctx, containerID)
}

// Start restarts a container kept by Stop and waits for its API server on addr; it returns the serving CA.
func (e *Engine) Start(ctx context.Context, containerID, addr string, ca []byte) ([]byte, error) {
	rt, ok := e.cfg.Runtime.(Stoppable)
	if !ok {
		return nil, ErrStopUnsupported
	}

	if err := rt.StartContainer(ctx, containerID); err != nil {
		return nil, fmt.Errorf("start cluster container: %w", err)
	}

	return e.awaitStarted(ctx, containerID, addr, ca)
}

// awaitStarted waits for the restarted node's API server; it gives up as soon as the container exits and attaches
// the node's log tail to any failure.
func (e *Engine) awaitStarted(ctx context.Context, containerID, addr string, ca []byte) ([]byte, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	if w, ok := e.cfg.Runtime.(container.ExitWaiter); ok {
		go func() {
			if code, err := w.WaitExit(ctx, containerID); err == nil {
				cancel(fmt.Errorf("%w with code %d", ErrNodeExited, code))
			}
		}()
	}

	got, err := e.AwaitReady(ctx, addr, ca)
	if err == nil {
		return got, nil
	}

	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.DeadlineExceeded) {
		err = fmt.Errorf("%w: %w", cause, err)
	}

	return nil, fmt.Errorf("%w; node log tail:\n%s", err, e.logTail(containerID))
}

func (e *Engine) logTail(containerID string) string {
	lr, ok := e.cfg.Runtime.(container.LogReader)
	if !ok {
		return "(runtime cannot read logs)"
	}

	ctx, cancel := context.WithTimeout(context.Background(), logTailTimeout)
	defer cancel()

	var out bytes.Buffer

	if err := lr.ReadLogs(ctx, containerID, logTailLines, false, &out, &out); err != nil {
		return "(log read failed: " + err.Error() + ")"
	}

	return out.String()
}

// Close closes the runtime when it supports it.
func (e *Engine) Close() {
	if c, ok := e.cfg.Runtime.(io.Closer); ok {
		_ = c.Close()
	}
}

// Node describes one k3s server container.
type Node struct {
	Name      string
	Image     string
	JoinToken string
	// Resource is the owning service's key for the cluster: a restarted gopherstack can adopt the container by it.
	Resource string
	// Volume names a data volume mounted on DataDir, so cluster state outlives the container.
	Volume string
	// NodeName pins the Kubernetes node name; empty keeps the container hostname.
	NodeName string
	Mounts   []string
	LBPorts  []int
}

// Instance is a started k3s container.
type Instance struct {
	CA          *CA
	ContainerID string
	LBHost      []int
	Port        int
}

func (e *Engine) spec(n Node, port int, lbHost []int, ca *CA, vol bool) container.Spec {
	bind := container.BindHostFor(e.cfg.Host)
	ports := make([]string, 0, 1+len(n.LBPorts))
	ports = append(ports, container.PortSpec(bind, strconv.Itoa(port), strconv.Itoa(APIPort)))

	for i, p := range n.LBPorts {
		ports = append(ports, container.PortSpec(bind, strconv.Itoa(lbHost[i]), strconv.Itoa(p)))
	}

	labels := container.ServiceLabels("k3s-" + e.cfg.Prefix)
	if n.Resource != "" {
		labels = container.ResourceLabels("k3s-"+e.cfg.Prefix, n.Resource)
	}

	env := []string{
		envAdminAuth + "=" + e.cfg.Token, envHost + "=" + e.cfg.Host, "K3S_TOKEN=" + n.JoinToken,
		envCACert + "=" + string(ca.CertPEM), envCAKey + "=" + string(ca.KeyPEM),
	}
	if n.NodeName != "" {
		env = append(env, envNodeName+"="+n.NodeName)
	}

	mounts := n.Mounts
	if vol {
		mounts = append(slices.Clone(mounts), n.Volume+":"+DataDir)
	}

	return container.Spec{
		Name:       "gopherstack-" + e.cfg.Prefix + "-" + n.Name + "-" + uuid.NewString()[:8],
		Labels:     labels,
		Image:      n.Image,
		Entrypoint: []string{"/bin/sh", "-c", Script(len(n.LBPorts) > 0)},
		Env:        env,
		Ports:      ports,
		Mounts:     mounts,
		Tmpfs:      []string{"/run", "/var/run"},
		Privileged: true,
	}
}

// Launch starts the container for n. record runs once the container exists and returns false when the caller no
// longer wants it, in which case the container is removed and ErrSuperseded returned.
func (e *Engine) Launch(ctx context.Context, n Node, record func(*Instance) bool) (*Instance, error) {
	ca, err := NewCA("gopherstack-" + e.cfg.Prefix + "-" + n.Name + "-ca")
	if err != nil {
		return nil, err
	}

	vol, err := e.ensureVolume(ctx, n.Volume)
	if err != nil {
		return nil, err
	}

	var (
		port   int
		lbHost []int
	)

	id, err := container.CreateAndStartRetry(ctx, e.cfg.Runtime,
		func() (container.Spec, error) {
			p, perr := e.AcquirePort(n.Name)
			if perr != nil {
				return container.Spec{}, perr
			}

			port = p
			lbHost = nil

			for range n.LBPorts {
				hp, lerr := e.AcquirePort(n.Name)
				if lerr != nil {
					e.ReleaseAll(port, lbHost)

					return container.Spec{}, lerr
				}

				lbHost = append(lbHost, hp)
			}

			return e.spec(n, p, lbHost, ca, vol), nil
		},
		func() { e.ReleaseAll(port, lbHost) })
	if err != nil {
		return nil, err
	}

	inst := &Instance{CA: ca, ContainerID: id, Port: port, LBHost: lbHost}
	if record != nil && !record(inst) {
		e.Remove(id, append([]int{port}, lbHost...)...)

		return nil, ErrSuperseded
	}

	return inst, nil
}

func (e *Engine) probe(ctx context.Context, addr string, ca []byte) ([]byte, error) {
	if e.cfg.Probe != nil {
		return e.cfg.Probe(ctx, addr, e.cfg.Token)
	}

	return ca, Probe(ctx, addr, e.cfg.Token, ca)
}

// AwaitReady polls the API server until it answers or the start timeout passes; it returns the serving CA.
func (e *Engine) AwaitReady(ctx context.Context, addr string, ca []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.StartTimeout)
	defer cancel()

	ticker := time.NewTicker(probeInterval)
	defer ticker.Stop()

	for {
		pctx, pcancel := context.WithTimeout(ctx, probeTimeout)
		got, err := e.probe(pctx, addr, ca)

		pcancel()

		if err == nil {
			return got, nil
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("API server not ready: %w", err)
		case <-ticker.C:
		}
	}
}

// VolumeLabel marks the data volumes the engine creates.
const VolumeLabel = "gopherstack.k3s-volume"

func (e *Engine) volumes() (container.VolumeRuntime, bool) {
	vr, ok := e.cfg.Runtime.(container.VolumeRuntime)

	return vr, ok
}

// ensureVolume creates the named data volume; it reports false when there is no volume to mount (no name, or a
// runtime without volume support, which keeps state in the container only).
func (e *Engine) ensureVolume(ctx context.Context, name string) (bool, error) {
	if name == "" {
		return false, nil
	}

	vr, ok := e.volumes()
	if !ok {
		return false, nil
	}

	if err := vr.CreateVolume(ctx, name, map[string]string{VolumeLabel: e.cfg.Prefix}); err != nil {
		return false, fmt.Errorf("create data volume %s: %w", name, err)
	}

	return true, nil
}

// RemoveVolume deletes a data volume; runtimes without volume support have nothing to delete.
func (e *Engine) RemoveVolume(ctx context.Context, name string) error {
	vr, ok := e.volumes()
	if !ok || name == "" {
		return nil
	}

	if err := vr.RemoveVolume(ctx, name); err != nil {
		return fmt.Errorf("remove data volume %s: %w", name, err)
	}

	return nil
}

// UnusedVolumes lists the engine's data volumes no container references.
func (e *Engine) UnusedVolumes(ctx context.Context) ([]string, error) {
	vr, ok := e.volumes()
	if !ok {
		return nil, nil
	}

	names, err := vr.ListUnusedVolumes(ctx, VolumeLabel+"="+e.cfg.Prefix, 0)
	if err != nil {
		return nil, fmt.Errorf("list data volumes: %w", err)
	}

	return names, nil
}

// Exec runs cmd in a running node; env entries are set on the process, never in argv.
func (e *Engine) Exec(ctx context.Context, containerID string, cmd, env []string) (container.ExecResult, error) {
	x, ok := e.cfg.Runtime.(container.Execer)
	if !ok {
		return container.ExecResult{}, container.ErrExecUnsupported
	}

	res, err := container.ExecWithEnv(ctx, x, containerID, cmd, env)
	if err != nil {
		return container.ExecResult{}, fmt.Errorf("exec in cluster node: %w", err)
	}

	return res, nil
}

// ServingCA reads the node's serving CA certificate, which differs from the one generated for a launch when a
// reused data volume already held a CA. It waits for the node to write it.
func (e *Engine) ServingCA(ctx context.Context, containerID string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.StartTimeout)
	defer cancel()

	ticker := time.NewTicker(caPollInterval)
	defer ticker.Stop()

	for {
		res, err := e.Exec(ctx, containerID, []string{"cat", CACertFile}, nil)
		if errors.Is(err, container.ErrExecUnsupported) {
			return nil, err
		}

		if err == nil && res.ExitCode == 0 && res.Stdout != "" {
			return []byte(res.Stdout), nil
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("read cluster CA: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// Adopt takes over a running node container an earlier gopherstack left behind. The caller owns it afterwards.
func (e *Engine) Adopt(oc container.ManagedContainer) (*Instance, error) {
	port := oc.Ports[APIPort]
	if port == 0 {
		return nil, fmt.Errorf("%w: container %s publishes no API port", ErrNotAdoptable, oc.ID)
	}

	if e.cfg.Ports != nil {
		if err := e.cfg.Ports.Reserve(port, e.cfg.Prefix+"-adopted"); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrNotAdoptable, err)
		}
	}

	return &Instance{ContainerID: oc.ID, Port: port}, nil
}

const rotateCAEnvPerCA = 2

// rotateCANames are the CAs `k3s certificate rotate-ca` replaces, as the file names it stages.
func rotateCANames() []string {
	return []string{"server-ca", "client-ca", "request-header-ca", "etcd/peer-ca", "etcd/server-ca"}
}

func rotateCAScript() string {
	return rotateCAScriptHead + strings.Join(rotateCANames(), " ") + rotateCAScriptTail
}

const rotateCAScriptHead = `d=$(mktemp -d) && umask 077 && mkdir -p "$d/tls/etcd" && ` +
	`cp ` + tlsDir + `/service.key "$d/tls/" && i=0 && for n in `

const rotateCAScriptTail = `; do ` +
	`eval "c=\$GOPHERSTACK_ROTATE_CERT_$i k=\$GOPHERSTACK_ROTATE_KEY_$i" && ` +
	`printf '%s\n' "$c" > "$d/tls/$n.crt" && printf '%s\n' "$k" > "$d/tls/$n.key" && i=$((i+1)); done && ` +
	`/bin/k3s certificate rotate-ca --path="$d" --force && touch ` + rotatedMarker + `; rc=$?; rm -rf "$d"; exit $rc`

// RotateCertificates replaces the node's cluster CAs with freshly generated ones (k3s certificate rotate-ca) and
// restarts it, so k3s reissues every certificate under them. It returns the new serving CA.
func (e *Engine) RotateCertificates(ctx context.Context, containerID, addr string) ([]byte, error) {
	env := make([]string, 0, len(rotateCANames())*rotateCAEnvPerCA)

	var serving []byte

	for i, name := range rotateCANames() {
		ca, err := NewCA("gopherstack-" + e.cfg.Prefix + "-" + name)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			serving = ca.CertPEM
		}

		env = append(env,
			fmt.Sprintf("GOPHERSTACK_ROTATE_CERT_%d=%s", i, ca.CertPEM),
			fmt.Sprintf("GOPHERSTACK_ROTATE_KEY_%d=%s", i, ca.KeyPEM))
	}

	res, err := e.Exec(ctx, containerID, []string{"/bin/sh", "-c", rotateCAScript()}, env)
	if err != nil {
		return nil, err
	}

	if res.ExitCode != 0 {
		return nil, fmt.Errorf("%w: k3s certificate rotate-ca exited %d: %s", ErrRotateFailed, res.ExitCode, res.Stderr)
	}

	if err = e.Stop(ctx, containerID); err != nil {
		return nil, err
	}

	return e.Start(ctx, containerID, addr, serving)
}
