package bedrockagentcore

import (
	"context"
	"fmt"
	"maps"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	protoHTTP = "HTTP"
	protoMCP  = "MCP"
	protoA2A  = "A2A"

	portHTTP = 8080
	portMCP  = 8000
	portA2A  = 9000
)

// protocolSpec is the AgentCore runtime service contract of one server protocol.
type protocolSpec struct {
	invokePath string
	port       int
}

func protocolOf(p string) protocolSpec {
	switch strings.ToUpper(p) {
	case protoMCP:
		return protocolSpec{invokePath: "/mcp", port: portMCP}
	case protoA2A:
		return protocolSpec{invokePath: "/", port: portA2A}
	default:
		return protocolSpec{invokePath: "/invocations", port: portHTTP}
	}
}

// runtimeSession is one warm agent container bound to a runtimeSessionId.
type runtimeSession struct {
	created     time.Time
	lastUsed    time.Time
	ctx         context.Context
	err         error
	cancel      context.CancelFunc
	ready       chan struct{}
	key         string
	containerID string
	base        string
	proto       protocolSpec
	idle        time.Duration
	maxLife     time.Duration
	port        int
	inflight    int
}

func runtimeKey(sc Scope, runtimeID, qualifier, session string) string {
	return sc.Account + "|" + sc.Region + "|" + runtimeID + "|" + qualifier + "|" + session
}

// session returns the warm session for key, booting a container for it on first use.
func (e *engine) session(ctx context.Context, key string, info RuntimeInfo, sc Scope) (*runtimeSession, error) {
	e.mu.Lock("session")

	rs, ok := e.runtimes[key]
	if !ok {
		rs = e.newRuntimeSession(key, info)
		e.runtimes[key] = rs

		go e.bootRuntime(rs, info, sc)
	}

	rs.inflight++
	rs.lastUsed = e.now()
	e.mu.Unlock()

	select {
	case <-rs.ready:
	case <-ctx.Done():
		e.release(rs)

		return nil, fmt.Errorf("waiting for the runtime container: %w", ctx.Err())
	}

	if rs.err != nil {
		e.release(rs)
		e.dropFailed(rs)

		return nil, rs.err
	}

	return rs, nil
}

func (e *engine) newRuntimeSession(key string, info RuntimeInfo) *runtimeSession {
	idle, life := e.cfg.IdleTimeout, defaultMaxLifetime
	if info.IdleSeconds > 0 {
		idle = time.Duration(info.IdleSeconds) * time.Second
	}

	if info.MaxLifetimeS > 0 {
		life = time.Duration(info.MaxLifetimeS) * time.Second
	}

	now := e.now()
	ctx, cancel := context.WithCancel(context.Background())

	return &runtimeSession{
		ctx:      ctx,
		cancel:   cancel,
		key:      key,
		ready:    make(chan struct{}),
		created:  now,
		lastUsed: now,
		idle:     idle,
		maxLife:  life,
		proto:    protocolOf(info.Protocol),
	}
}

func (e *engine) release(rs *runtimeSession) {
	e.mu.Lock("release")
	defer e.mu.Unlock()

	rs.inflight--
	rs.lastUsed = e.now()
}

func (e *engine) dropFailed(rs *runtimeSession) {
	e.mu.Lock("dropFailed")
	if e.runtimes[rs.key] == rs {
		delete(e.runtimes, rs.key)
	}
	e.mu.Unlock()

	e.teardownRuntime(rs)
}

func (e *engine) bootRuntime(rs *runtimeSession, info RuntimeInfo, sc Scope) {
	defer close(rs.ready)

	ctx, cancel := context.WithTimeout(rs.ctx, e.cfg.StartTimeout)
	defer cancel()

	rs.err = e.startContainer(ctx, rs, info, sc)
}

func (e *engine) startContainer(ctx context.Context, rs *runtimeSession, info RuntimeInfo, sc Scope) error {
	id, err := container.CreateAndStartRetry(ctx, e.cfg.Runtime,
		func() (container.Spec, error) {
			port, perr := e.acquirePort("agentcore-" + info.ID)
			if perr != nil {
				return container.Spec{}, perr
			}

			rs.port = port
			rs.base = "http://" + e.hostAddr(port)

			return e.runtimeSpec(info, sc, rs), nil
		},
		func() { portalloc.ReleaseTo(e.cfg.Ports, rs.port); rs.port = 0 },
	)
	if err != nil {
		return internal("starting the agent runtime container failed: %v", err)
	}

	rs.containerID = id

	if err = e.waitHealthy(ctx, rs); err != nil {
		return internal("the agent runtime did not become healthy: %v", err)
	}

	return nil
}

func (e *engine) runtimeSpec(info RuntimeInfo, sc Scope, rs *runtimeSession) container.Spec {
	env := map[string]string{
		"AWS_REGION": sc.Region, "AWS_DEFAULT_REGION": sc.Region,
		"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
	}

	if e.cfg.EndpointURL != "" {
		env["AWS_ENDPOINT_URL"] = e.cfg.EndpointURL
	}

	maps.Copy(env, info.Env)

	vars := make([]string, 0, len(env))
	for _, k := range sortedKeys(env) {
		vars = append(vars, k+"="+env[k])
	}

	return container.Spec{
		Image: e.imageRef(info.ImageURI),
		Env:   vars,
		Ports: []string{container.PortSpec(
			container.BindHostFor(e.cfg.Host), strconv.Itoa(rs.port), strconv.Itoa(rs.proto.port),
		)},
	}
}

// imageRef points ECR image URIs at the emulated registry when one is configured.
func (e *engine) imageRef(uri string) string {
	host, rest, found := strings.Cut(uri, "/")
	if found && e.cfg.RegistryHost != "" && strings.Contains(host, ".dkr.ecr.") {
		return e.cfg.RegistryHost + "/" + rest
	}

	return uri
}

// waitHealthy polls the runtime service contract: GET /ping for HTTP, an open port for MCP and A2A.
func (e *engine) waitHealthy(ctx context.Context, rs *runtimeSession) error {
	t := time.NewTicker(e.cfg.PollInterval)
	defer t.Stop()

	for {
		if e.healthy(ctx, rs) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (e *engine) healthy(ctx context.Context, rs *runtimeSession) bool {
	if rs.proto.port != portHTTP {
		return portalloc.IsListening(net.JoinHostPort(e.cfg.Host, strconv.Itoa(rs.port)))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rs.base+"/ping", http.NoBody)
	if err != nil {
		return false
	}

	resp, err := e.cfg.HTTP.Do(req)
	if err != nil {
		return false
	}

	defer resp.Body.Close()

	return resp.StatusCode/100 == 2 //nolint:mnd // 2xx
}

func (e *engine) teardownRuntime(rs *runtimeSession) {
	rs.cancel()
	e.stopContainer(rs.containerID)
	portalloc.ReleaseTo(e.cfg.Ports, rs.port)
}

// stopRuntimeSession stops key's container; it reports whether a session existed.
func (e *engine) stopRuntimeSession(key string) bool {
	if e == nil {
		return false
	}

	e.mu.Lock("stopRuntimeSession")
	rs, ok := e.runtimes[key]
	delete(e.runtimes, key)
	e.mu.Unlock()

	if !ok {
		return false
	}

	rs.cancel()
	<-rs.ready
	e.teardownRuntime(rs)

	return true
}

// reapRuntimes stops warm sessions that are idle past their timeout or past their maximum lifetime.
func (e *engine) reapRuntimes() {
	if e == nil {
		return
	}

	now := e.now()

	e.mu.Lock("reapRuntimes")

	var dead []*runtimeSession

	for key, rs := range e.runtimes {
		if rs.inflight == 0 && (!now.Before(rs.lastUsed.Add(rs.idle)) || !now.Before(rs.created.Add(rs.maxLife))) {
			dead = append(dead, rs)

			delete(e.runtimes, key)
		}
	}

	e.mu.Unlock()

	for _, rs := range dead {
		rs.cancel()
		<-rs.ready
		e.teardownRuntime(rs)
	}
}
