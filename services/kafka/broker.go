package kafka

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	// BrokerImage is the pinned single-node KRaft Kafka image.
	BrokerImage = "apache/kafka:3.9.1"

	brokerDefaultHost   = "127.0.0.1"
	brokerStartTimeout  = 3 * time.Minute
	brokerProbeInterval = 500 * time.Millisecond
	brokerProbeTimeout  = 5 * time.Second
	brokerStopTimeout   = 30 * time.Second

	keystoreDirMode  = 0o755
	keystoreFileMode = 0o644
	iamPasswordBytes = 24
)

var errNoBrokerCA = errors.New("kafka: no CA available to sign broker certificates")

func shortID() string { return uuid.NewString()[:8] }

func sprintfJAAS(password string) string { return fmt.Sprintf(plainJAAS, password) }

// BrokerRuntime is the slice of container.Runtime the broker engine needs.
type BrokerRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// BrokerConfig configures docker-backed MSK brokers.
type BrokerConfig struct {
	Runtime      BrokerRuntime
	Ports        *portalloc.Allocator
	Probe        func(ctx context.Context, addr string) error
	CA           *devtls.CA
	ScramAdmin   ScramAdmin
	Host         string
	Image        string
	StartTimeout time.Duration
}

type liveBroker struct {
	cancel       context.CancelFunc
	res          *brokerRes
	scramApplied map[string]string
	syncGate     chan struct{}
	endpoints    ManagedEndpoints
	containerID  string
	adminAddr    string
	opArn        string
	plan         brokerPlan
	ready        bool
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

	if cfg.ScramAdmin == nil {
		cfg.ScramAdmin = franzScramAdmin
	}

	if cfg.CA == nil {
		if ca, err := devtls.NewCA(); err == nil {
			cfg.CA = ca
		}
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

	if !lb.ready || lb.endpoints.Plaintext == "" {
		return nil, true
	}

	return []string{lb.endpoints.Plaintext}, true
}

// ManagedEndpointsFor returns the per-mode broker endpoints of a docker-backed cluster.
// The bool is false for metadata-only clusters; the endpoints are empty until the broker is reachable.
func (b *InMemoryBackend) ManagedEndpointsFor(clusterArn string) (ManagedEndpoints, bool) {
	b.mu.RLock("ManagedEndpointsFor")
	defer b.mu.RUnlock()

	lb, ok := b.managedBrokerLocked(clusterArn)
	if !ok {
		return ManagedEndpoints{}, false
	}

	return lb.endpoints, true
}

// BrokerCAPEM returns the CA that signs docker-backed broker certificates, or nil when brokers are disabled.
func (b *InMemoryBackend) BrokerCAPEM() []byte {
	b.mu.RLock("BrokerCAPEM")
	defer b.mu.RUnlock()

	if b.engine == nil || b.engine.cfg.CA == nil {
		return nil
	}

	return b.engine.cfg.CA.CertPEM()
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

		if lb.containerID != "" || lb.res != nil {
			e.wg.Go(func() { e.teardown(lb) })
		}
	}
}

func (e *brokerEngine) releasePort(port int) {
	portalloc.ReleaseTo(e.cfg.Ports, port)
}

func (e *brokerEngine) teardown(lb *liveBroker) {
	if lb.containerID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), brokerStopTimeout)
		defer cancel()

		if err := e.cfg.Runtime.StopAndRemove(ctx, lb.containerID); err != nil {
			logger.Load(ctx).WarnContext(ctx, "kafka: broker container removal failed",
				"container", lb.containerID, "error", err)
		}
	}

	if lb.res == nil {
		return
	}

	if lb.res.proxy != nil {
		lb.res.proxy.close()
	}

	if lb.res.keystoreDir != "" {
		_ = os.RemoveAll(lb.res.keystoreDir)
	}

	for _, p := range lb.res.ports {
		e.releasePort(p)
	}
}

// provision allocates ports, the TLS keystore and the IAM proxy a broker of plan needs.
func (e *brokerEngine) provision(
	name string, plan brokerPlan, trust []byte, clusterArn string, auth iamAuthFunc,
) (*brokerRes, error) {
	res := &brokerRes{ports: make(map[string]int)}

	fail := func(err error) (*brokerRes, error) {
		e.teardown(&liveBroker{res: res})

		return nil, err
	}

	for _, l := range plan.listenerNames() {
		p, err := e.acquirePort(name + "-" + l)
		if err != nil {
			return fail(err)
		}

		res.ports[l] = p
	}

	if plan.needsKeystore() {
		dir, err := e.writeKeystore(trust)
		if err != nil {
			return fail(err)
		}

		res.keystoreDir = dir
	}

	if plan.iam {
		if err := e.startProxy(res, clusterArn, auth); err != nil {
			return fail(err)
		}
	}

	return res, nil
}

func (p brokerPlan) listenerNames() []string {
	var names []string

	if p.secure {
		names = append(names, listenerAdmin)
	}

	if p.plaintext {
		names = append(names, listenerPlaintext)
	}

	if p.tls {
		names = append(names, listenerTLS)
	}

	if p.scram {
		names = append(names, listenerScram)
	}

	if p.iam {
		names = append(names, listenerIAM, listenerIAMPublic)
	}

	return names
}

func (e *brokerEngine) tlsHosts() []string {
	return []string{e.cfg.Host, "localhost", brokerDefaultHost, "host.docker.internal"}
}

// writeKeystore writes a PEM keystore (PKCS8 key + chain) the broker mounts for its TLS listeners.
func (e *brokerEngine) writeKeystore(trust []byte) (string, error) {
	if e.cfg.CA == nil {
		return "", errNoBrokerCA
	}

	cert, err := e.cfg.CA.Issue(e.tlsHosts()...)
	if err != nil {
		return "", fmt.Errorf("issue broker certificate: %w", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("marshal broker key: %w", err)
	}

	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	for _, der := range cert.Certificate {
		pemBytes = append(pemBytes, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...)
	}

	dir, err := os.MkdirTemp("", "gopherstack-msk-tls-")
	if err != nil {
		return "", fmt.Errorf("keystore dir: %w", err)
	}

	if err = os.Chmod(dir, keystoreDirMode); err != nil {
		_ = os.RemoveAll(dir)

		return "", fmt.Errorf("keystore dir mode: %w", err)
	}

	if err = os.WriteFile(filepath.Join(dir, keystoreFile), pemBytes, keystoreFileMode); err != nil {
		_ = os.RemoveAll(dir)

		return "", fmt.Errorf("write keystore: %w", err)
	}

	if len(trust) > 0 {
		if err = os.WriteFile(filepath.Join(dir, truststoreFile), trust, keystoreFileMode); err != nil {
			_ = os.RemoveAll(dir)

			return "", fmt.Errorf("write truststore: %w", err)
		}
	}

	return dir, nil
}

func (e *brokerEngine) startProxy(res *brokerRes, clusterArn string, auth iamAuthFunc) error {
	cert, err := e.cfg.CA.Issue(e.tlsHosts()...)
	if err != nil {
		return fmt.Errorf("issue proxy certificate: %w", err)
	}

	buf := make([]byte, iamPasswordBytes)
	if _, err = rand.Read(buf); err != nil {
		return fmt.Errorf("iam proxy credential: %w", err)
	}

	res.iamPassword = hex.EncodeToString(buf)

	backend := net.JoinHostPort(brokerDefaultHost, strconv.Itoa(res.ports[listenerIAM]))
	bind := net.JoinHostPort(container.BindHostFor(e.cfg.Host), strconv.Itoa(res.ports[listenerIAMPublic]))

	res.proxy, err = startIAMProxy(bind, &tls.Config{
		Certificates: []tls.Certificate{*cert}, MinVersion: tls.VersionTLS12,
	}, backend, res.iamPassword, clusterArn, auth)

	return err
}

func (e *brokerEngine) acquirePort(name string) (int, error) {
	return portalloc.AcquireFrom(e.cfg.Ports, "msk-"+name)
}

// launchBrokerLocked registers a pending broker for cluster and starts it in the background.
func (b *InMemoryBackend) launchBrokerLocked(clusterArn, name, opArn string) {
	e := b.engine
	if e == nil {
		return
	}

	plan := brokerPlan{plaintext: true}
	if c, ok := b.clusters.Get(clusterArn); ok {
		plan = planFor(c)
	}

	ctx, cancel := context.WithCancel(context.Background())
	lb := &liveBroker{cancel: cancel, plan: plan, opArn: opArn, syncGate: make(chan struct{}, 1)}
	e.brokers[clusterArn] = lb

	e.wg.Go(func() { b.runBroker(ctx, clusterArn, name, plan, lb) })
}

func (b *InMemoryBackend) runBroker(ctx context.Context, clusterArn, name string, plan brokerPlan, lb *liveBroker) {
	e := b.engine

	trust, err := b.clientTrust(ctx, plan)
	if err != nil {
		b.finishBroker(ctx, clusterArn, lb, err)

		return
	}

	var res *brokerRes

	provisionFailed := false

	id, err := container.CreateAndStartRetry(ctx, e.cfg.Runtime,
		func() (container.Spec, error) {
			r, perr := e.provision(name, plan, trust, clusterArn,
				func(payload []byte, remote string) (iamAuthorizer, error) {
					return b.authenticateIAM(clusterArn, payload, remote, time.Now())
				})
			if perr != nil {
				provisionFailed = true

				return container.Spec{}, perr
			}

			res = r

			return e.spec(name, plan, r), nil
		},
		func() { e.teardown(&liveBroker{res: res}) })
	if err != nil {
		if provisionFailed {
			err = fmt.Errorf("provision broker resources: %w", err)
		} else {
			err = fmt.Errorf("start broker container: %w", err)
		}

		b.finishBroker(ctx, clusterArn, lb, err)

		return
	}

	if !b.attach(clusterArn, lb, func() { lb.res, lb.containerID = res, id }) {
		e.teardown(&liveBroker{containerID: id, res: res})

		return
	}

	probe := e.probeAddress(plan, res)
	if err = e.awaitReady(ctx, probe); err != nil {
		b.finishBroker(ctx, clusterArn, lb, err)

		return
	}

	b.attach(clusterArn, lb, func() { lb.adminAddr = probe })
	b.syncScram(ctx, clusterArn)
	b.markActive(clusterArn, lb, e.endpoints(plan, res))
}

// probeAddress is the loopback endpoint used to detect readiness and to administer SCRAM users.
func (e *brokerEngine) probeAddress(plan brokerPlan, res *brokerRes) string {
	name := listenerAdmin
	if !plan.secure {
		name = listenerPlaintext
	}

	host := brokerDefaultHost
	if !plan.secure {
		host = e.cfg.Host
	}

	return net.JoinHostPort(host, strconv.Itoa(res.ports[name]))
}

func (e *brokerEngine) endpoints(plan brokerPlan, res *brokerRes) ManagedEndpoints {
	at := func(name string) string { return net.JoinHostPort(e.cfg.Host, strconv.Itoa(res.ports[name])) }

	var ep ManagedEndpoints

	if plan.plaintext {
		ep.Plaintext = at(listenerPlaintext)
	}

	if plan.tls {
		ep.TLS = at(listenerTLS)
	}

	if plan.scram {
		ep.SaslScram = at(listenerScram)
	}

	if plan.iam {
		ep.SaslIam = at(listenerIAMPublic)
	}

	return ep
}

// attach runs mutate under the lock if lb is still the cluster's broker; false means the cluster went away.
func (b *InMemoryBackend) attach(clusterArn string, lb *liveBroker, mutate func()) bool {
	b.mu.Lock("attachBroker")
	defer b.mu.Unlock()

	if b.engine.brokers[clusterArn] != lb {
		return false
	}

	mutate()

	return true
}

func (b *InMemoryBackend) markActive(clusterArn string, lb *liveBroker, ep ManagedEndpoints) {
	b.mu.Lock("markBrokerActive")
	defer b.mu.Unlock()

	if b.engine.brokers[clusterArn] != lb {
		return
	}

	lb.endpoints, lb.ready = ep, true

	if c, ok := b.clusters.Get(clusterArn); ok && (c.State == ClusterStateCreating || c.State == ClusterStateUpdating) {
		c.State = ClusterStateActive
	}

	b.settleBrokerOpLocked(clusterArn, lb.opArn, ClusterOperationStateUpdateComplete)
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

	b.settleBrokerOpLocked(clusterArn, lb.opArn, ClusterOperationStateUpdateFailed)

	b.mu.Unlock()

	logger.Load(ctx).WarnContext(ctx, "kafka: broker failed to start",
		"code", brokerStartFailedCode)
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
		b.launchBrokerLocked(c.ClusterArn, c.ClusterName, "")
	}
}

// ClientEndpoint is how an in-process client (such as a Lambda event source poller) dials a docker-backed cluster.
type ClientEndpoint struct {
	Username  string
	Password  string
	Brokers   []string
	RootCAPEM []byte
}

// ClientEndpointFor picks the endpoint an in-process consumer can use: plaintext if offered,
// else SASL/SCRAM when scramSecretArn names a credential, else server-TLS. Mutual-TLS-only and
// AWS_MSK_IAM-only clusters report false.
func (b *InMemoryBackend) ClientEndpointFor(
	ctx context.Context, clusterArn, scramSecretArn string,
) (ClientEndpoint, bool) {
	b.mu.RLock("ClientEndpointFor")
	lb, ok := b.managedBrokerLocked(clusterArn)

	var (
		ep   ManagedEndpoints
		mtls bool
		ca   []byte
	)

	if ok && lb.ready {
		ep, mtls = lb.endpoints, lb.plan.mtls
		if b.engine.cfg.CA != nil {
			ca = b.engine.cfg.CA.CertPEM()
		}
	}

	src := b.scramSecretSource
	b.mu.RUnlock()

	switch {
	case ep.Plaintext != "":
		return ClientEndpoint{Brokers: []string{ep.Plaintext}}, true
	case ep.SaslScram != "" && scramSecretArn != "" && src != nil:
		cred, err := src.ResolveScramSecret(ctx, scramSecretArn)
		if err != nil {
			return ClientEndpoint{}, false
		}

		return ClientEndpoint{
			Brokers: []string{ep.SaslScram}, RootCAPEM: ca, Username: cred.Username, Password: cred.Password,
		}, true
	case ep.TLS != "" && !mtls:
		return ClientEndpoint{Brokers: []string{ep.TLS}, RootCAPEM: ca}, true
	}

	return ClientEndpoint{}, false
}
