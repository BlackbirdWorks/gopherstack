package extensions

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Status is an extension's lifecycle state.
type Status string

// Lifecycle states reported by the management API.
const (
	StatusStarting   Status = "starting"
	StatusReady      Status = "ready"
	StatusUnhealthy  Status = "unhealthy"
	StatusRestarting Status = "restarting"
	StatusFailed     Status = "failed"
	StatusStopped    Status = "stopped"
)

const (
	tokenBytes = 32
	envBase    = 4
)

// Options wires the manager to its host.
type Options struct {
	Logger *slog.Logger
	// NewRuntime builds the container runtime on first use; nil disables image extensions.
	NewRuntime func() (container.Runtime, error)
	// ControlURL is the base URL extensions use to reach the control API.
	ControlURL string
}

type extension struct {
	proxy    *httputil.ReverseProxy
	reg      *Registration
	endpoint *url.URL
	// registered is closed on the first registration of the current incarnation.
	registered chan struct{}
	// restartReq asks the supervisor to recycle the current incarnation.
	restartReq chan struct{}
	// seq serializes lifecycle events; started/readySent/shutdownSent are per incarnation, guarded by Manager.mu.
	seq          chan struct{}
	token        string
	status       Status
	lastError    string
	output       []string
	spec         Spec
	port         int
	restarts     int
	running      bool
	started      bool
	readySent    bool
	shutdownSent bool
}

// Manager owns extension lifecycles, claims and hooks.
type Manager struct {
	log       *slog.Logger
	mu        *lockmetrics.RWMutex
	transport *http.Transport
	hookHTTP  *http.Client
	cancel    context.CancelFunc
	runCtx    context.Context //nolint:containedctx // supervisor context for manual restarts
	opts      Options
	exts      []*extension
	cfg       Config
	wg        sync.WaitGroup
	ready     atomic.Bool
	stopping  atomic.Bool
}

// New builds a manager for cfg (already normalized by LoadConfig).
func New(cfg Config, opts Options) *Manager {
	m := &Manager{
		cfg:       cfg,
		opts:      opts,
		log:       opts.Logger,
		mu:        lockmetrics.New("extensions"),
		transport: httpclient.NewTransport(),
	}
	m.hookHTTP = &http.Client{Transport: m.transport}

	for _, s := range cfg.Extensions {
		m.exts = append(m.exts, &extension{
			spec:       s,
			status:     StatusStarting,
			seq:        make(chan struct{}, 1),
			restartReq: make(chan struct{}, 1),
		})
	}

	return m
}

// Enabled reports whether any extension is configured.
func (m *Manager) Enabled() bool { return m != nil && len(m.exts) > 0 }

// Start launches every extension supervisor; it returns immediately.
func (m *Manager) Start(ctx context.Context) {
	ctx, m.cancel = context.WithCancel(ctx)
	m.runCtx = ctx

	for _, e := range m.exts {
		e.running = true

		m.wg.Go(func() { m.supervise(ctx, e) })
	}
}

// MarkReady fires on_ready for registered extensions and for any that register later.
func (m *Manager) MarkReady(ctx context.Context) {
	m.ready.Store(true)

	for _, e := range m.registeredExts() {
		m.lifecycle(ctx, e, "on_ready")
	}
}

// Stop calls on_shutdown, then stops every extension, bounded by ctx.
func (m *Manager) Stop(ctx context.Context) {
	if m.cancel == nil {
		return
	}

	m.stopping.Store(true)

	for _, e := range m.registeredExts() {
		m.lifecycle(ctx, e, "on_shutdown")
	}

	m.cancel()

	done := make(chan struct{})

	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		m.log.WarnContext(ctx, "extensions did not stop before the deadline")
	}

	m.transport.CloseIdleConnections()
	m.mu.Close()
}

func (m *Manager) registeredExts() []*extension {
	m.mu.RLock("registeredExts")
	defer m.mu.RUnlock()

	var out []*extension

	for _, e := range m.exts {
		if e.reg != nil && e.status == StatusReady {
			out = append(out, e)
		}
	}

	return out
}

func (m *Manager) setStatus(e *extension, st Status, errMsg string) {
	m.mu.Lock("setStatus")
	defer m.mu.Unlock()

	e.status = st
	if st == StatusFailed || st == StatusStopped {
		e.reg, e.endpoint, e.proxy = nil, nil, nil
	}

	if errMsg != "" {
		e.lastError = errMsg
	}
}

func newToken() (string, error) {
	b := make([]byte, tokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return hex.EncodeToString(b), nil
}

var errNotTCP = errors.New("listener is not tcp")

func freePort() (int, error) {
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}

	defer func() { _ = l.Close() }()

	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, errNotTCP
	}

	return addr.Port, nil
}

// prepare issues a fresh token and port. The previous registration stays until the new
// incarnation registers, so claimed routes answer 503 rather than falling through to built-ins.
func (m *Manager) prepare(e *extension) (string, int, error) {
	token, err := newToken()
	if err != nil {
		return "", 0, err
	}

	port := e.spec.Port
	if port == 0 {
		if port, err = freePort(); err != nil {
			return "", 0, err
		}
	}

	m.mu.Lock("prepare")
	defer m.mu.Unlock()

	e.token, e.port = token, port
	e.registered = make(chan struct{})
	e.started, e.readySent, e.shutdownSent = false, false, false
	e.status = StatusStarting

	select {
	case <-e.restartReq:
	default:
	}

	return token, port, nil
}

func (m *Manager) env(e *extension, token string, port int) []string {
	control := m.opts.ControlURL
	if e.spec.ControlURL != "" {
		control = e.spec.ControlURL
	}

	env := make([]string, 0, envBase+len(e.spec.Env))
	env = append(env,
		"GOPHERSTACK_EXTENSION_NAME="+e.spec.Name,
		"GOPHERSTACK_EXTENSION_TOKEN="+token,
		"GOPHERSTACK_EXTENSION_PORT="+strconv.Itoa(port),
		"GOPHERSTACK_CONTROL_URL="+control,
	)

	for k, v := range e.spec.Env {
		env = append(env, k+"="+v)
	}

	return env
}

func (m *Manager) newRunner(e *extension, port int, token string) runner {
	if e.spec.Image != "" {
		return newContainerRunner(m.opts.NewRuntime, e.spec, port)
	}

	pr := newProcessRunner(m.log, e.spec, token)
	pr.sink = func(line string) { m.appendOutput(e, line) }

	return pr
}

// supervise runs the start/monitor/restart loop for one extension.
func (m *Manager) supervise(ctx context.Context, e *extension) {
	defer func() {
		m.mu.Lock("superviseExit")
		e.running = false
		m.mu.Unlock()
	}()

	for {
		err := m.runOnce(ctx, e)

		if ctx.Err() != nil {
			m.setStatus(e, StatusStopped, "")

			return
		}

		if errors.Is(err, errRestartRequested) {
			m.markRestarting(e, "")

			continue
		}

		msg := errExited.Error()
		if err != nil {
			msg = err.Error()
		}

		m.log.WarnContext(ctx, "extension stopped", "extension", e.spec.Name, "error", msg)

		if !m.shouldRestart(e, err) {
			st := StatusStopped
			if err != nil {
				st = StatusFailed
			}

			m.setStatus(e, st, msg)

			return
		}

		m.markRestarting(e, msg)

		select {
		case <-ctx.Done():
			m.setStatus(e, StatusStopped, "")

			return
		case <-time.After(e.spec.RestartBackoff):
		}
	}
}

func (m *Manager) shouldRestart(e *extension, err error) bool {
	m.mu.RLock("shouldRestart")
	defer m.mu.RUnlock()

	if e.restarts >= e.spec.MaxRestarts {
		return false
	}

	switch e.spec.Restart {
	case RestartAlways:
		return true
	case RestartOnFailure:
		return err != nil
	default:
		return false
	}
}

func (m *Manager) markRestarting(e *extension, msg string) {
	m.mu.Lock("restart")
	defer m.mu.Unlock()

	e.restarts++
	e.status = StatusRestarting

	if msg != "" {
		e.lastError = msg
	}
}
