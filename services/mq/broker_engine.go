package mq

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	// RabbitMQImage is the pinned RabbitMQ image (management plugin enabled).
	RabbitMQImage = "rabbitmq:3.13.7-management"
	// ActiveMQImage is the pinned ActiveMQ Classic image.
	ActiveMQImage = "apache/activemq-classic:5.18.7"

	// BrokerStateCreationFailed is the state of a docker-backed broker that never became reachable.
	BrokerStateCreationFailed = "CREATION_FAILED"

	brokerDefaultHost   = "127.0.0.1"
	brokerStartTimeout  = 3 * time.Minute
	brokerProbeInterval = 500 * time.Millisecond
	brokerProbeTimeout  = 5 * time.Second
	brokerStopTimeout   = 30 * time.Second
	brokerPortRetries   = 5

	brokerStartFailedCode = "BROKER_START_FAILED"
	brokerPasswordBytes   = 24
	activeMQUnsafeChars   = "/&\\<>\"'\n\r"
)

// BrokerRuntime is the slice of container.Runtime the broker engine needs.
type BrokerRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// BrokerConfig configures docker-backed Amazon MQ brokers.
type BrokerConfig struct {
	Runtime BrokerRuntime
	// Ports hands out host ports; nil falls back to OS-chosen free ports.
	Ports *portalloc.Allocator
	// Probe returns nil once the broker at addr accepts the credentials; nil uses the real protocol clients.
	Probe func(ctx context.Context, engine, addr, username, password string) error
	// TLSProbe returns nil once the TLS listener or console at the host:port addr serves requests;
	// nil skips the check unless Probe is also nil, which uses the real HTTPS/TLS clients.
	TLSProbe func(ctx context.Context, addr string) error
	// Host is the address clients use to reach published ports; default 127.0.0.1.
	Host string
	// RabbitMQImage and ActiveMQImage override the pinned images.
	RabbitMQImage string
	ActiveMQImage string
	// StartTimeout bounds how long a broker may stay CREATION_IN_PROGRESS before CREATION_FAILED.
	StartTimeout time.Duration
}

type portMap struct {
	name      string
	container int
}

type liveNode struct {
	hostPorts   map[string]int
	containerID string
	ports       []int
}

type liveBroker struct {
	ctx    context.Context
	cancel context.CancelFunc
	engine string
	// username and password are the engine credentials.
	username string
	password string
	// standbys are the extra containers of a multi-AZ deployment; the embedded node is the primary.
	standbys []*liveNode
	liveNode
	standbyWant int
	ready       bool
	applying    bool
}

func (lb *liveBroker) nodes() []*liveNode {
	return append([]*liveNode{&lb.liveNode}, lb.standbys...)
}

type brokerEngine struct {
	brokers  map[string]*liveBroker
	failures map[string]string
	cfg      BrokerConfig
	wg       sync.WaitGroup
}

// EnableBrokers switches the backend to docker mode: brokers with a user get a real RabbitMQ or ActiveMQ container.
func (b *InMemoryBackend) EnableBrokers(cfg BrokerConfig) {
	if cfg.Host == "" {
		cfg.Host = brokerDefaultHost
	}

	if cfg.RabbitMQImage == "" {
		cfg.RabbitMQImage = RabbitMQImage
	}

	if cfg.ActiveMQImage == "" {
		cfg.ActiveMQImage = ActiveMQImage
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = brokerStartTimeout
	}

	if cfg.Probe == nil {
		cfg.Probe = probeBroker

		if cfg.TLSProbe == nil {
			cfg.TLSProbe = probeTLS
		}
	}

	b.mu.Lock("EnableBrokers")
	defer b.mu.Unlock()

	b.engine = &brokerEngine{
		cfg: cfg, brokers: make(map[string]*liveBroker), failures: make(map[string]string),
	}
}

// MQConsumerEndpoint returns the engine and consumer address (AMQP for RabbitMQ, STOMP for ActiveMQ)
// of a RUNNING docker-backed broker; ok is false for metadata-only or not-yet-running brokers.
func (b *InMemoryBackend) MQConsumerEndpoint(brokerARN string) (string, string, bool) {
	b.mu.RLock("MQConsumerEndpoint")
	defer b.mu.RUnlock()

	if b.engine == nil {
		return "", "", false
	}

	for _, br := range b.brokers.All() {
		if br.BrokerArn != brokerARN {
			continue
		}

		lb, live := b.engine.brokers[br.BrokerID]
		if !live || !lb.ready || br.BrokerState == BrokerStateDeleting {
			return "", "", false
		}

		return lb.engine, b.engine.addr(lb, consumerPortName(lb.engine)), true
	}

	return "", "", false
}

func consumerPortName(engine string) string {
	if engine == EngineTypeRabbitMQ {
		return portNameAMQP
	}

	return "stomp"
}

func (e *brokerEngine) addr(lb *liveBroker, name string) string {
	return e.nodeAddr(&lb.liveNode, name)
}

func (e *brokerEngine) nodeAddr(n *liveNode, name string) string {
	return net.JoinHostPort(e.cfg.Host, strconv.Itoa(n.hostPorts[name]))
}

// Close removes every broker container and waits for in-flight startups to finish.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	e := b.engine
	lbs := b.detachAllBrokersLocked()
	b.mu.Unlock()

	if e == nil {
		return
	}

	e.reap(lbs)
	e.wg.Wait()

	if c, ok := e.cfg.Runtime.(io.Closer); ok {
		_ = c.Close()
	}
}

func (b *InMemoryBackend) detachAllBrokersLocked() []*liveBroker {
	if b.engine == nil {
		return nil
	}

	lbs := make([]*liveBroker, 0, len(b.engine.brokers))
	for id, lb := range b.engine.brokers {
		lbs = append(lbs, lb)
		delete(b.engine.brokers, id)
	}

	return lbs
}

func (b *InMemoryBackend) detachBrokerLocked(brokerID string) *liveBroker {
	if b.engine == nil {
		return nil
	}

	lb := b.engine.brokers[brokerID]
	delete(b.engine.brokers, brokerID)

	return lb
}

// reapDetached releases brokers detached under the lock; call it after unlocking.
func (b *InMemoryBackend) reapDetached(lbs ...*liveBroker) {
	if b.engine != nil {
		b.engine.reap(lbs)
	}
}

func (e *brokerEngine) reap(lbs []*liveBroker) {
	for _, lb := range lbs {
		if lb == nil {
			continue
		}

		lb.cancel()

		for _, n := range lb.nodes() {
			if n.containerID != "" {
				e.wg.Go(func() { e.remove(n.containerID, n.ports) })
			}
		}
	}
}

func (e *brokerEngine) remove(containerID string, ports []int) {
	ctx, cancel := context.WithTimeout(context.Background(), brokerStopTimeout)
	defer cancel()

	if err := e.cfg.Runtime.StopAndRemove(ctx, containerID); err != nil {
		logger.Load(ctx).WarnContext(ctx, "mq: broker container removal failed",
			"container", containerID, "error", err)
	}

	e.releasePorts(ports)
}

func (e *brokerEngine) releasePorts(ports []int) {
	if e.cfg.Ports == nil {
		return
	}

	for _, p := range ports {
		_ = e.cfg.Ports.Release(p)
	}
}

func (e *brokerEngine) acquirePorts(name string, maps []portMap) ([]int, error) {
	ports := make([]int, 0, len(maps))

	for _, pm := range maps {
		p, err := e.acquirePort("mq-" + name + "-" + pm.name)
		if err != nil {
			e.releasePorts(ports)

			return nil, err
		}

		ports = append(ports, p)
	}

	return ports, nil
}

func (e *brokerEngine) acquirePort(label string) (int, error) {
	if e.cfg.Ports != nil {
		return e.cfg.Ports.Acquire(label) //nolint:wrapcheck // allocator error is self-describing
	}

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find free port: %w", err)
	}

	defer func() { _ = l.Close() }()

	a, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("find free port: %w", io.ErrUnexpectedEOF)
	}

	return a.Port, nil
}

func (e *brokerEngine) spec(name string, lb *liveBroker, n *liveNode) container.Spec {
	maps := portMapsFor(lb.engine)
	ports := make([]string, len(maps))
	bind := container.BindHostFor(e.cfg.Host)

	for i, pm := range maps {
		ports[i] = container.PortSpec(bind, strconv.Itoa(n.ports[i]), strconv.Itoa(pm.container))
	}

	spec := container.Spec{
		Name:  "gopherstack-mq-" + name + "-" + uuid.NewString()[:8],
		Ports: ports,
		Env:   tlsEnv(e.cfg.Host),
	}

	spec.Entrypoint, spec.Cmd = tlsEntrypoint(lb.engine)

	if lb.engine == EngineTypeRabbitMQ {
		spec.Image = e.cfg.RabbitMQImage
		spec.Env = append(spec.Env, "RABBITMQ_DEFAULT_USER="+lb.username, "RABBITMQ_DEFAULT_PASS="+lb.password)

		return spec
	}

	spec.Image = e.cfg.ActiveMQImage
	spec.Env = append(spec.Env, "ACTIVEMQ_CONNECTION_USER="+lb.username, "ACTIVEMQ_CONNECTION_PASSWORD="+lb.password)

	return spec
}

// validateEngineCredentials rejects passwords the ActiveMQ image cannot apply (it seds them into its config).
func (b *InMemoryBackend) validateEngineCredentials(engineType string, users []*User) error {
	if b.engine == nil || engineType != EngineTypeActiveMQ || len(users) == 0 {
		return nil
	}

	u := users[0]
	if strings.ContainsAny(u.Password, activeMQUnsafeChars) || strings.ContainsAny(u.Username, activeMQUnsafeChars) {
		return fmt.Errorf("%w: docker engine cannot configure credentials containing any of %q",
			ErrValidation, activeMQUnsafeChars)
	}

	return nil
}

// launchBrokerLocked registers a pending broker and starts its container in the background.
func (b *InMemoryBackend) launchBrokerLocked(br *Broker, u *User) {
	e := b.engine
	if e == nil || u == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	lb := &liveBroker{
		ctx: ctx, cancel: cancel, engine: br.EngineType, username: u.Username, password: u.Password,
		standbyWant: standbyCount(br.EngineType, br.DeploymentMode),
	}
	e.brokers[br.BrokerID] = lb
	br.BrokerState = BrokerStateCreating

	e.wg.Go(func() { b.runBroker(ctx, br.BrokerID, br.BrokerName, lb) })
}

func (b *InMemoryBackend) runBroker(ctx context.Context, brokerID, name string, lb *liveBroker) {
	e := b.engine

	nodes, err := e.startNodes(ctx, name, lb)
	if err != nil {
		b.finishBroker(ctx, brokerID, lb, err)

		return
	}

	if !b.recordContainers(brokerID, lb, nodes) {
		for _, n := range nodes {
			e.remove(n.containerID, n.ports)
		}

		return
	}

	err = e.awaitAllReady(ctx, lb)
	if err == nil {
		err = b.applyConfigOnStart(ctx, brokerID, lb)
		if err == nil {
			b.markRunning(brokerID, lb)

			return
		}
	}

	b.finishBroker(ctx, brokerID, lb, err)
}

// startNodes starts the primary container and any multi-AZ standbys; on error it removes what it started.
func (e *brokerEngine) startNodes(ctx context.Context, name string, lb *liveBroker) ([]*liveNode, error) {
	nodes := make([]*liveNode, 0, 1+lb.standbyWant)

	for range 1 + lb.standbyWant {
		n, err := e.startNode(ctx, name, lb)
		if err != nil {
			for _, started := range nodes {
				e.remove(started.containerID, started.ports)
			}

			return nil, err
		}

		nodes = append(nodes, n)
	}

	return nodes, nil
}

// startNode retries on a host-port bind race: a probed-free port can be taken before docker binds it.
func (e *brokerEngine) startNode(ctx context.Context, name string, lb *liveBroker) (*liveNode, error) {
	var err error

	for range brokerPortRetries {
		var n *liveNode

		n, err = e.startNodeOnce(ctx, name, lb)
		if err == nil || !isPortBindError(err) || ctx.Err() != nil {
			return n, err
		}
	}

	return nil, err
}

func isPortBindError(err error) bool {
	msg := err.Error()

	return strings.Contains(msg, "port is already allocated") || strings.Contains(msg, "address already in use")
}

func (e *brokerEngine) startNodeOnce(ctx context.Context, name string, lb *liveBroker) (*liveNode, error) {
	maps := portMapsFor(lb.engine)

	ports, err := e.acquirePorts(name, maps)
	if err != nil {
		return nil, err
	}

	n := &liveNode{ports: ports, hostPorts: make(map[string]int, len(ports))}
	for i, pm := range maps {
		n.hostPorts[pm.name] = ports[i]
	}

	id, err := e.cfg.Runtime.CreateAndStart(ctx, e.spec(name, lb, n))
	if err != nil {
		e.releasePorts(ports)

		return nil, fmt.Errorf("start broker container: %w", err)
	}

	n.containerID = id

	return n, nil
}

func (e *brokerEngine) awaitAllReady(ctx context.Context, lb *liveBroker) error {
	for _, n := range lb.nodes() {
		if err := e.awaitReady(ctx, lb, e.nodeAddr(n, consumerPortName(lb.engine))); err != nil {
			return err
		}

		if err := e.awaitTLS(ctx, lb, n); err != nil {
			return err
		}
	}

	return nil
}

func (e *brokerEngine) awaitTLS(ctx context.Context, lb *liveBroker, n *liveNode) error {
	if e.cfg.TLSProbe == nil {
		return nil
	}

	endpoints, console := e.liveEndpoints(lb.engine, n)

	ctx, cancel := context.WithTimeout(ctx, e.cfg.StartTimeout)
	defer cancel()

	ticker := time.NewTicker(brokerProbeInterval)
	defer ticker.Stop()

	for _, raw := range append(endpoints, console) {
		_, addr, _ := strings.Cut(raw, "://")

		for {
			pctx, pcancel := context.WithTimeout(ctx, brokerProbeTimeout)
			err := e.cfg.TLSProbe(pctx, addr)

			pcancel()

			if err == nil {
				break
			}

			select {
			case <-ctx.Done():
				return fmt.Errorf("broker TLS listener %s not ready: %w", raw, err)
			case <-ticker.C:
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) recordContainers(brokerID string, lb *liveBroker, nodes []*liveNode) bool {
	b.mu.Lock("recordBroker")
	defer b.mu.Unlock()

	if b.engine.brokers[brokerID] != lb {
		return false
	}

	lb.liveNode = *nodes[0]
	lb.standbys = nodes[1:]

	return true
}

func (b *InMemoryBackend) markRunning(brokerID string, lb *liveBroker) {
	b.mu.Lock("markBrokerRunning")
	defer b.mu.Unlock()

	if b.engine.brokers[brokerID] != lb {
		return
	}

	lb.ready = true

	if br, ok := b.brokers.Get(brokerID); ok && br.BrokerState == BrokerStateCreating {
		br.BrokerState = BrokerStateRunning
	}
}

func (b *InMemoryBackend) recordStartFailure(brokerID string, cause error) {
	if cause == nil {
		return
	}

	b.mu.Lock("recordStartFailure")
	defer b.mu.Unlock()

	b.engine.failures[brokerID] = cause.Error()
}

// finishBroker marks a still-wanted broker CREATION_FAILED and releases its container.
func (b *InMemoryBackend) finishBroker(ctx context.Context, brokerID string, lb *liveBroker, cause error) {
	b.mu.Lock("failBroker")

	if b.engine.brokers[brokerID] != lb {
		b.mu.Unlock()

		return
	}

	b.detachBrokerLocked(brokerID)

	if br, ok := b.brokers.Get(brokerID); ok {
		br.BrokerState = BrokerStateCreationFailed
	}

	b.mu.Unlock()

	logger.Load(ctx).WarnContext(ctx, "mq: broker failed to start",
		"broker", brokerID, "code", brokerStartFailedCode, "error", cause)
	b.recordStartFailure(brokerID, cause)
	b.engine.reap([]*liveBroker{lb})
}

func (e *brokerEngine) awaitReady(ctx context.Context, lb *liveBroker, addr string) error {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.StartTimeout)
	defer cancel()

	ticker := time.NewTicker(brokerProbeInterval)
	defer ticker.Stop()

	for {
		pctx, pcancel := context.WithTimeout(ctx, brokerProbeTimeout)
		err := e.cfg.Probe(pctx, lb.engine, addr, lb.username, lb.password)

		pcancel()

		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("broker not reachable: %w", err)
		case <-ticker.C:
		}
	}
}

// applyLiveEndpoints replaces the synthetic instances of a ready docker broker with one per container,
// reporting the published TLS listeners in the AWS URL forms.
func (b *InMemoryBackend) applyLiveEndpoints(cp *Broker) {
	if b.engine == nil {
		return
	}

	lb, ok := b.engine.brokers[cp.BrokerID]
	if !ok || !lb.ready {
		return
	}

	nodes := lb.nodes()
	insts := make([]BrokerInstance, len(nodes))

	for i, n := range nodes {
		insts[i].Endpoints, insts[i].ConsoleURL = b.engine.liveEndpoints(lb.engine, n)

		if i < len(cp.BrokerInstances) {
			insts[i].IPAddress = cp.BrokerInstances[i].IPAddress
		}
	}

	cp.BrokerInstances = insts
}

// relaunchBrokersLocked starts an empty container per restored broker with a user; passwords are not
// persisted, so the relaunched broker gets a random one.
func (b *InMemoryBackend) relaunchBrokersLocked() {
	if b.engine == nil {
		return
	}

	for _, br := range b.brokers.All() {
		if br.BrokerState == BrokerStateDeleting {
			continue
		}

		u := firstUser(br)
		if u == nil {
			continue
		}

		if u.Password == "" {
			u = &User{
				Username: u.Username,
				Password: strings.ReplaceAll(uuid.NewString(), "-", "")[:brokerPasswordBytes],
			}
		}

		b.launchBrokerLocked(br, u)
	}
}

func firstUser(br *Broker) *User {
	var first *User

	for name, u := range br.Users {
		if first == nil || name < first.Username {
			first = u
		}
	}

	return first
}

func (b *InMemoryBackend) liveBrokerLocked(brokerID string) (*liveBroker, bool) {
	if b.engine == nil {
		return nil, false
	}

	lb, ok := b.engine.brokers[brokerID]

	return lb, ok
}
