package kafka

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	// BrokerImage is the pinned single-node KRaft Kafka image.
	BrokerImage = "apache/kafka:3.9.1"

	brokerListenPort    = "9092"
	brokerDefaultHost   = "127.0.0.1"
	brokerStartTimeout  = 3 * time.Minute
	brokerProbeInterval = 500 * time.Millisecond
	brokerProbeTimeout  = 5 * time.Second
	brokerStopTimeout   = 30 * time.Second
)

// BrokerRuntime is the slice of container.Runtime the broker engine needs.
type BrokerRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// BrokerConfig configures docker-backed MSK brokers.
type BrokerConfig struct {
	Runtime BrokerRuntime
	// Ports hands out host ports; nil falls back to an OS-chosen free port.
	Ports *portalloc.Allocator
	// Probe reports nil once the broker at addr serves Kafka requests; nil uses a franz-go ping.
	Probe func(ctx context.Context, addr string) error
	// Host is the address clients use to reach published ports; default 127.0.0.1.
	Host string
	// Image overrides BrokerImage.
	Image string
	// StartTimeout bounds how long a broker may stay CREATING before FAILED.
	StartTimeout time.Duration
}

type liveBroker struct {
	cancel      context.CancelFunc
	containerID string
	address     string
	port        int
}

type brokerEngine struct {
	brokers map[string]*liveBroker
	cfg     BrokerConfig
	wg      sync.WaitGroup
}

// EnableBrokers switches the backend to docker mode: provisioned clusters get a real broker.
func (b *InMemoryBackend) EnableBrokers(cfg BrokerConfig) {
	if cfg.Host == "" {
		cfg.Host = brokerDefaultHost
	}

	if cfg.Image == "" {
		cfg.Image = BrokerImage
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = brokerStartTimeout
	}

	if cfg.Probe == nil {
		cfg.Probe = pingBroker
	}

	b.mu.Lock("EnableBrokers")
	defer b.mu.Unlock()

	b.engine = &brokerEngine{cfg: cfg, brokers: make(map[string]*liveBroker)}
}

// ManagedBootstrap returns the real broker addresses of a docker-backed cluster.
// The bool is false for metadata-only clusters; the slice is empty until the broker is reachable.
func (b *InMemoryBackend) ManagedBootstrap(clusterArn string) ([]string, bool) {
	b.mu.RLock("ManagedBootstrap")
	defer b.mu.RUnlock()

	lb, ok := b.managedBrokerLocked(clusterArn)
	if !ok {
		return nil, false
	}

	if lb.address == "" {
		return nil, true
	}

	return []string{lb.address}, true
}

func (b *InMemoryBackend) managedBrokerLocked(clusterArn string) (*liveBroker, bool) {
	if b.engine == nil {
		return nil, false
	}

	lb, ok := b.engine.brokers[clusterArn]

	return lb, ok
}

// BootstrapServers returns the real broker addresses of an ACTIVE docker-backed cluster, else nil.
func (b *InMemoryBackend) BootstrapServers(clusterArn string) []string {
	servers, _ := b.ManagedBootstrap(clusterArn)

	return servers
}

// Close stops every broker container and waits for in-flight startups to finish.
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
	for arn, lb := range b.engine.brokers {
		lbs = append(lbs, lb)
		delete(b.engine.brokers, arn)
	}

	return lbs
}

func (b *InMemoryBackend) detachBrokerLocked(clusterArn string) *liveBroker {
	if b.engine == nil {
		return nil
	}

	lb := b.engine.brokers[clusterArn]
	delete(b.engine.brokers, clusterArn)

	return lb
}

// reapDetached releases brokers detached under the lock; call it after unlocking.
func (b *InMemoryBackend) reapDetached(lbs ...*liveBroker) {
	if b.engine != nil {
		b.engine.reap(lbs)
	}
}

// reap cancels startups and removes started containers in the background.
func (e *brokerEngine) reap(lbs []*liveBroker) {
	for _, lb := range lbs {
		if lb == nil {
			continue
		}

		lb.cancel()

		if lb.containerID != "" {
			e.wg.Go(func() { e.remove(lb.containerID, lb.port) })
		}
	}
}

func (e *brokerEngine) remove(containerID string, port int) {
	ctx, cancel := context.WithTimeout(context.Background(), brokerStopTimeout)
	defer cancel()

	if err := e.cfg.Runtime.StopAndRemove(ctx, containerID); err != nil {
		logger.Load(ctx).WarnContext(ctx, "kafka: broker container removal failed",
			"container", containerID, "error", err)
	}

	e.releasePort(port)
}

func (e *brokerEngine) releasePort(port int) {
	if e.cfg.Ports != nil && port > 0 {
		_ = e.cfg.Ports.Release(port)
	}
}

func (e *brokerEngine) acquirePort(name string) (int, error) {
	if e.cfg.Ports != nil {
		return e.cfg.Ports.Acquire("msk-" + name) //nolint:wrapcheck // allocator error is self-describing
	}

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find free port: %w", err)
	}

	defer func() { _ = l.Close() }()

	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("find free port: %w", io.ErrUnexpectedEOF)
	}

	return addr.Port, nil
}

func (e *brokerEngine) spec(name string, port int) container.Spec {
	advertised := net.JoinHostPort(e.cfg.Host, strconv.Itoa(port))

	return container.Spec{
		Image: e.cfg.Image,
		Name:  "gopherstack-msk-" + name + "-" + uuid.NewString()[:8],
		Ports: []string{strconv.Itoa(port) + ":" + brokerListenPort},
		Env: []string{
			"KAFKA_NODE_ID=1",
			"KAFKA_PROCESS_ROLES=broker,controller",
			"KAFKA_LISTENERS=PLAINTEXT://:" + brokerListenPort + ",CONTROLLER://:9093",
			"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://" + advertised,
			"KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
		},
	}
}

// launchBrokerLocked registers a pending broker for cluster and starts it in the background.
func (b *InMemoryBackend) launchBrokerLocked(clusterArn, name string) {
	e := b.engine
	if e == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	lb := &liveBroker{cancel: cancel}
	e.brokers[clusterArn] = lb

	e.wg.Go(func() { b.runBroker(ctx, clusterArn, name, lb) })
}

func (b *InMemoryBackend) runBroker(ctx context.Context, clusterArn, name string, lb *liveBroker) {
	e := b.engine

	port, err := e.acquirePort(name)
	if err != nil {
		b.finishBroker(ctx, clusterArn, lb, fmt.Errorf("allocate broker port: %w", err))

		return
	}

	id, err := e.cfg.Runtime.CreateAndStart(ctx, e.spec(name, port))
	if err != nil {
		e.releasePort(port)
		b.finishBroker(ctx, clusterArn, lb, fmt.Errorf("start broker container: %w", err))

		return
	}

	if !b.recordContainer(clusterArn, lb, id, port) {
		e.remove(id, port)

		return
	}

	address := net.JoinHostPort(e.cfg.Host, strconv.Itoa(port))
	err = e.awaitReady(ctx, address)

	if err == nil {
		b.markActive(clusterArn, lb, address)

		return
	}

	b.finishBroker(ctx, clusterArn, lb, err)
}

// recordContainer stores the started container; false means the cluster went away first.
func (b *InMemoryBackend) recordContainer(clusterArn string, lb *liveBroker, id string, port int) bool {
	b.mu.Lock("recordBroker")
	defer b.mu.Unlock()

	if b.engine.brokers[clusterArn] != lb {
		return false
	}

	lb.containerID = id
	lb.port = port

	return true
}

func (b *InMemoryBackend) markActive(clusterArn string, lb *liveBroker, address string) {
	b.mu.Lock("markBrokerActive")
	defer b.mu.Unlock()

	if b.engine.brokers[clusterArn] != lb {
		return
	}

	lb.address = address

	if c, ok := b.clusters.Get(clusterArn); ok && c.State == ClusterStateCreating {
		c.State = ClusterStateActive
	}
}

// brokerStartFailedCode is logged instead of the cause, which may echo container env.
const brokerStartFailedCode = "BROKER_START_FAILED"

// finishBroker marks a still-wanted cluster FAILED and releases its broker.
func (b *InMemoryBackend) finishBroker(ctx context.Context, clusterArn string, lb *liveBroker, cause error) {
	b.mu.Lock("failBroker")

	if b.engine.brokers[clusterArn] != lb {
		b.mu.Unlock()

		return
	}

	b.detachBrokerLocked(clusterArn)

	if c, ok := b.clusters.Get(clusterArn); ok {
		c.State = ClusterStateFailed
		c.StateInfo = &StateInfo{Code: brokerStartFailedCode, Message: cause.Error()}
	}

	b.mu.Unlock()

	logger.Load(ctx).WarnContext(ctx, "kafka: broker failed to start",
		"cluster", clusterArn, "code", brokerStartFailedCode)
	b.engine.reap([]*liveBroker{lb})
}

func (e *brokerEngine) awaitReady(ctx context.Context, address string) error {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.StartTimeout)
	defer cancel()

	ticker := time.NewTicker(brokerProbeInterval)
	defer ticker.Stop()

	for {
		pctx, pcancel := context.WithTimeout(ctx, brokerProbeTimeout)
		err := e.cfg.Probe(pctx, address)

		pcancel()

		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("broker %s not reachable: %w", address, err)
		case <-ticker.C:
		}
	}
}

func pingBroker(ctx context.Context, address string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(address))
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}

	defer cl.Close()

	return cl.Ping(ctx)
}

// relaunchBrokersLocked restarts an empty broker for every restored provisioned cluster.
// Container IDs and ports are runtime state, so topics and messages do not survive a restore.
func (b *InMemoryBackend) relaunchBrokersLocked() {
	if b.engine == nil {
		return
	}

	for _, c := range b.clusters.All() {
		if c.ClusterType != ClusterTypeProvisioned {
			continue
		}

		c.State = ClusterStateCreating
		c.StateInfo = nil
		b.launchBrokerLocked(c.ClusterArn, c.ClusterName)
	}
}
