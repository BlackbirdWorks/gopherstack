package kafkaconnect

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	// WorkerImage is the pinned image whose bundled Kafka Connect runs the connectors.
	WorkerImage = "apache/kafka:3.9.1"

	workerRESTPort       = "8083"
	workerDefaultHost    = "127.0.0.1"
	workerStartTimeout   = 3 * time.Minute
	workerPollInterval   = 3 * time.Second
	workerStopTimeout    = 30 * time.Second
	workerHTTPTimeout    = 10 * time.Second
	workerMountDir       = "/opt/gopherstack"
	workerDirMode        = 0o755
	workerFileMode       = 0o644
	workerMaxBody        = 1 << 20
	workerStateRunning   = "RUNNING"
	workerStateFailed    = "FAILED"
	workerFailedCode     = "WORKER_FAILED"
	workerEncryptionTLS  = "TLS"
	workerAuthNone       = "NONE"
	workerStatusMessages = 512
)

var (
	errWorkerNoPlugin   = errors.New("custom plugin archive is unavailable")
	errWorkerConnect    = errors.New("kafka connect rejected the connector")
	errWorkerUnsupAuth  = errors.New("only NONE client authentication is supported by the local worker")
	errWorkerBadArchive = errors.New("custom plugin archive is unreadable")
	errWorkerStatus     = errors.New("kafka connect status request failed")
	errWorkerTimeout    = errors.New("kafka connect worker did not become ready")
)

// WorkerRuntime is the slice of container.Runtime the worker engine needs.
type WorkerRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// PluginFetcher returns a custom plugin archive from emulated S3.
type PluginFetcher func(bucketArn, fileKey, version string) ([]byte, error)

// WorkerConfig configures docker-backed Kafka Connect workers.
type WorkerConfig struct {
	Runtime WorkerRuntime
	// Ports hands out host ports for the REST API; nil falls back to an OS-chosen free port.
	Ports *portalloc.Allocator
	// HTTP calls the Connect REST API; nil uses an isolated client.
	HTTP interface {
		Do(*http.Request) (*http.Response, error)
	}
	// Plugins reads plugin archives; nil uses the emulated S3 backend wired through SetAppConfig.
	Plugins PluginFetcher
	// TrustCAPEM returns the CA PEM that signs TLS broker certificates, when known.
	TrustCAPEM func() []byte
	// Host is the address the REST API is reached on; default 127.0.0.1.
	Host string
	// Image overrides WorkerImage.
	Image string
	// StartTimeout bounds how long a worker may stay CREATING before FAILED.
	StartTimeout time.Duration
	// PollInterval is how often a running worker's status is re-read.
	PollInterval time.Duration
}

type liveWorker struct {
	cancel      context.CancelFunc
	containerID string
	dir         string
	port        int
}

type workerEngine struct {
	failures map[string]*StateDescription
	workers  map[string]*liveWorker
	cfg      WorkerConfig
	wg       sync.WaitGroup
	owns     bool
}

// EnableWorkers switches the backend to docker mode: connectors run on a real Kafka Connect worker.
func (b *InMemoryBackend) EnableWorkers(cfg WorkerConfig) {
	if cfg.Host == "" {
		cfg.Host = workerDefaultHost
	}

	if cfg.Image == "" {
		cfg.Image = WorkerImage
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = workerStartTimeout
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = workerPollInterval
	}

	if cfg.HTTP == nil {
		cfg.HTTP = httpclient.New(workerHTTPTimeout)
	}

	if cfg.Plugins == nil {
		cfg.Plugins = b.readPluginBytes
	}

	b.mu.Lock("EnableWorkers")
	defer b.mu.Unlock()

	b.engine = &workerEngine{
		cfg:      cfg,
		workers:  make(map[string]*liveWorker),
		failures: make(map[string]*StateDescription),
		owns:     true,
	}
}

// inheritWorkers shares home's engine settings with a sibling region backend.
func (b *InMemoryBackend) inheritWorkers(home *InMemoryBackend) {
	home.mu.RLock("inheritWorkers")
	e := home.engine
	home.mu.RUnlock()

	if e == nil {
		return
	}

	cfg := e.cfg
	cfg.Plugins = nil
	b.EnableWorkers(cfg)

	b.mu.Lock("inheritWorkers")
	b.engine.owns = false
	b.mu.Unlock()
}

// Close stops every worker container and waits for in-flight startups.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	e := b.engine
	lws := b.detachAllWorkersLocked()
	b.mu.Unlock()

	if e == nil {
		return
	}

	e.reap(lws)
	e.wg.Wait()

	if c, ok := e.cfg.Runtime.(io.Closer); ok && e.owns {
		_ = c.Close()
	}
}

func (b *InMemoryBackend) detachAllWorkersLocked() []*liveWorker {
	if b.engine == nil {
		return nil
	}

	lws := make([]*liveWorker, 0, len(b.engine.workers))
	for arn, lw := range b.engine.workers {
		lws = append(lws, lw)
		delete(b.engine.workers, arn)
	}

	clear(b.engine.failures)

	return lws
}

func (b *InMemoryBackend) detachWorkerLocked(connectorArn string) *liveWorker {
	if b.engine == nil {
		return nil
	}

	lw := b.engine.workers[connectorArn]
	delete(b.engine.workers, connectorArn)
	delete(b.engine.failures, connectorArn)

	return lw
}

func (b *InMemoryBackend) reapDetached(lws ...*liveWorker) {
	b.mu.RLock("reapDetached")
	e := b.engine
	b.mu.RUnlock()

	if e != nil {
		e.reap(lws)
	}
}

func (b *InMemoryBackend) workerManagedLocked(connectorArn string) bool {
	if b.engine == nil {
		return false
	}

	_, ok := b.engine.workers[connectorArn]

	return ok
}

func (e *workerEngine) reap(lws []*liveWorker) {
	for _, lw := range lws {
		if lw == nil {
			continue
		}

		lw.cancel()
		e.wg.Go(func() { e.teardown(lw) })
	}
}

func (e *workerEngine) teardown(lw *liveWorker) {
	if lw.containerID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), workerStopTimeout)
		defer cancel()

		if err := e.cfg.Runtime.StopAndRemove(ctx, lw.containerID); err != nil {
			logger.Load(ctx).WarnContext(ctx, "kafkaconnect: worker container removal failed",
				"container", lw.containerID, "error", err)
		}
	}

	if lw.dir != "" {
		_ = os.RemoveAll(lw.dir)
	}

	if e.cfg.Ports != nil && lw.port > 0 {
		_ = e.cfg.Ports.Release(lw.port)
	}
}

func (e *workerEngine) acquirePort(name string) (int, error) {
	if e.cfg.Ports != nil {
		return e.cfg.Ports.Acquire("mskc-" + name) //nolint:wrapcheck // allocator error is self-describing
	}

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find free port: %w", err)
	}

	defer func() { _ = l.Close() }()

	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, io.ErrUnexpectedEOF
	}

	return addr.Port, nil
}

// launchWorkerLocked registers the connector's worker and starts it in the background.
func (b *InMemoryBackend) launchWorkerLocked(connectorArn string) {
	e := b.engine
	if e == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	lw := &liveWorker{cancel: cancel}
	e.workers[connectorArn] = lw

	e.wg.Go(func() { b.runWorker(ctx, connectorArn, lw) })
}

// relaunchWorkersLocked restarts a worker for every restored connector; container state does not survive a restore.
func (b *InMemoryBackend) relaunchWorkersLocked() {
	if b.engine == nil {
		return
	}

	for _, c := range b.connectors.All() {
		if c.State == deletingState {
			continue
		}

		c.State = connectorStateCreating
		b.launchWorkerLocked(c.ARN)
	}
}

type workerSpecInput struct {
	conn       *Connector
	properties string
	plugins    []CustomPlugin
}

func (b *InMemoryBackend) snapshotWorkerInput(connectorArn string) (workerSpecInput, bool) {
	b.mu.RLock("snapshotWorkerInput")
	defer b.mu.RUnlock()

	c, ok := b.connectors.Get(connectorArn)
	if !ok {
		return workerSpecInput{}, false
	}

	in := workerSpecInput{conn: c.clone()}

	for _, ref := range c.Plugins {
		if p, found := b.customPlugins.Get(ref.CustomPluginArn); found {
			in.plugins = append(in.plugins, *p.clone())
		}
	}

	if c.WorkerConfiguration != nil {
		if w, found := b.workerConfigurations.Get(c.WorkerConfiguration.Arn); found {
			in.properties = decodeProperties(w.LatestRevision.PropertiesFileContent)
		}
	}

	return in, true
}

func (b *InMemoryBackend) runWorker(ctx context.Context, connectorArn string, lw *liveWorker) {
	e := b.engine

	in, ok := b.snapshotWorkerInput(connectorArn)
	if !ok {
		return
	}

	if err := e.checkSupported(in.conn); err != nil {
		b.failWorker(ctx, connectorArn, lw, err)

		return
	}

	dir, err := e.stage(in)
	if err != nil {
		b.failWorker(ctx, connectorArn, lw, err)

		return
	}

	port, err := e.acquirePort(in.conn.Name)
	if err != nil {
		_ = os.RemoveAll(dir)
		b.failWorker(ctx, connectorArn, lw, err)

		return
	}

	id, err := e.cfg.Runtime.CreateAndStart(ctx, e.spec(in.conn.Name, dir, port))
	if err != nil {
		e.teardown(&liveWorker{dir: dir, port: port})
		b.failWorker(ctx, connectorArn, lw, fmt.Errorf("start worker container: %w", err))

		return
	}

	if !b.attachWorker(connectorArn, lw, func() { lw.containerID, lw.dir, lw.port = id, dir, port }) {
		e.teardown(&liveWorker{containerID: id, dir: dir, port: port})

		return
	}

	base := "http://" + net.JoinHostPort(e.cfg.Host, strconv.Itoa(port))

	if err = e.awaitREST(ctx, base); err != nil {
		b.failWorker(ctx, connectorArn, lw, err)

		return
	}

	b.reconcile(ctx, connectorArn, lw, base, true)
	b.watchWorker(ctx, connectorArn, lw, base)
}

func (e *workerEngine) checkSupported(c *Connector) error {
	if c.KafkaClusterClientAuthentication != "" && c.KafkaClusterClientAuthentication != workerAuthNone {
		return fmt.Errorf("%w: %s", errWorkerUnsupAuth, c.KafkaClusterClientAuthentication)
	}

	return nil
}

func (b *InMemoryBackend) attachWorker(connectorArn string, lw *liveWorker, mutate func()) bool {
	b.mu.Lock("attachWorker")
	defer b.mu.Unlock()

	if b.engine.workers[connectorArn] != lw {
		return false
	}

	mutate()

	return true
}

// stage writes worker.properties and the extracted custom plugins into a directory mounted into the worker.
func (e *workerEngine) stage(in workerSpecInput) (string, error) {
	dir, err := os.MkdirTemp("", "gopherstack-mskc-")
	if err != nil {
		return "", fmt.Errorf("worker dir: %w", err)
	}

	if err = os.Chmod(dir, workerDirMode); err != nil {
		_ = os.RemoveAll(dir)

		return "", fmt.Errorf("worker dir mode: %w", err)
	}

	pluginDir := filepath.Join(dir, "plugins")
	if err = os.Mkdir(pluginDir, workerDirMode); err != nil {
		_ = os.RemoveAll(dir)

		return "", fmt.Errorf("plugin dir: %w", err)
	}

	for _, p := range in.plugins {
		if err = e.installPlugin(pluginDir, p); err != nil {
			_ = os.RemoveAll(dir)

			return "", err
		}
	}

	trust := ""

	if in.conn.KafkaClusterEncryptionInTransit == workerEncryptionTLS && e.cfg.TrustCAPEM != nil {
		if pem := e.cfg.TrustCAPEM(); len(pem) > 0 {
			if err = os.WriteFile(filepath.Join(dir, "ca.pem"), pem, workerFileMode); err != nil {
				_ = os.RemoveAll(dir)

				return "", fmt.Errorf("write ca: %w", err)
			}

			trust = workerMountDir + "/ca.pem"
		}
	}

	props := workerProperties(in, trust)
	if err = os.WriteFile(filepath.Join(dir, "worker.properties"), []byte(props), workerFileMode); err != nil {
		_ = os.RemoveAll(dir)

		return "", fmt.Errorf("write worker properties: %w", err)
	}

	return dir, nil
}

func (e *workerEngine) installPlugin(pluginDir string, p CustomPlugin) error {
	data, err := e.cfg.Plugins(p.BucketArn, p.FileKey, p.ObjectVersion)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", errWorkerNoPlugin, p.Name, err)
	}

	dst := filepath.Join(pluginDir, safeName(p.Name))
	if err = os.Mkdir(dst, workerDirMode); err != nil {
		return fmt.Errorf("plugin dir: %w", err)
	}

	if p.ContentType == "ZIP" {
		return unzipPlugin(data, dst)
	}

	return os.WriteFile(
		filepath.Join(dst, safeName(p.Name)+".jar"),
		data,
		workerFileMode,
	) //nolint:wrapcheck // write errors carry their own context
}

func safeName(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			return r
		}

		return '_'
	}, s)
}

// workerProperties builds the distributed worker config; a referenced worker configuration overrides defaults.
func workerProperties(in workerSpecInput, trustCA string) string {
	id := safeName(in.conn.Name)
	props := map[string]string{
		"bootstrap.servers":                       in.conn.ApacheKafkaCluster.BootstrapServers,
		"group.id":                                "gs-worker-" + id,
		"key.converter":                           "org.apache.kafka.connect.converters.ByteArrayConverter",
		"value.converter":                         "org.apache.kafka.connect.converters.ByteArrayConverter",
		"config.storage.topic":                    "__connect-configs-" + id,
		"offset.storage.topic":                    "__connect-offsets-" + id,
		"status.storage.topic":                    "__connect-status-" + id,
		"config.storage.replication.factor":       "1",
		"offset.storage.replication.factor":       "1",
		"status.storage.replication.factor":       "1",
		"offset.flush.interval.ms":                "1000",
		"listeners":                               "http://0.0.0.0:" + workerRESTPort,
		"plugin.path":                             "/opt/kafka/libs," + workerMountDir + "/plugins",
		"connector.client.config.override.policy": "All",
	}

	if in.conn.KafkaClusterEncryptionInTransit == workerEncryptionTLS {
		for _, prefix := range []string{"", "producer.", "consumer.", "admin."} {
			props[prefix+"security.protocol"] = "SSL"

			if trustCA != "" {
				props[prefix+"ssl.truststore.type"] = "PEM"
				props[prefix+"ssl.truststore.location"] = trustCA
			}
		}
	}

	maps.Copy(props, parseProperties(in.properties))

	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	var sb strings.Builder

	for _, k := range keys {
		sb.WriteString(k + "=" + props[k] + "\n")
	}

	return sb.String()
}

func decodeProperties(b64 string) string {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return b64
	}

	return string(raw)
}

func parseProperties(content string) map[string]string {
	out := make(map[string]string)

	for line := range strings.SplitSeq(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}

		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}

		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}

	return out
}

func (e *workerEngine) spec(name, dir string, port int) container.Spec {
	return container.Spec{
		Image:  e.cfg.Image,
		Name:   awsid.New("gopherstack-mskc-" + safeName(name) + "-"),
		Ports:  []string{container.PortSpec(container.BindHostFor(e.cfg.Host), strconv.Itoa(port), workerRESTPort)},
		Mounts: []string{dir + ":" + workerMountDir + ":ro"},
		Cmd:    []string{"/opt/kafka/bin/connect-distributed.sh", workerMountDir + "/worker.properties"},
		Env:    []string{"KAFKA_HEAP_OPTS=-Xms128m -Xmx512m"},
	}
}

func (e *workerEngine) awaitREST(ctx context.Context, base string) error {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.StartTimeout)
	defer cancel()

	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()

	for {
		if _, code, err := e.call(ctx, http.MethodGet, base+"/connectors", nil); err == nil && code == http.StatusOK {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %s", errWorkerTimeout, base)
		case <-ticker.C:
		}
	}
}

// call performs a Connect REST request and returns the body and status code.
func (e *workerEngine) call(ctx context.Context, method, url string, body any) ([]byte, int, error) {
	var rdr io.Reader

	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, 0, fmt.Errorf("encode request: %w", err)
		}

		rdr = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return nil, 0, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := e.cfg.HTTP.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("connect rest %s: %w", url, err)
	}

	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, workerMaxBody))

	return data, resp.StatusCode, err //nolint:wrapcheck // body read errors carry their own context
}

type connectStatus struct {
	Connector struct {
		State string `json:"state"`
		Trace string `json:"trace"`
	} `json:"connector"`
	Tasks []struct {
		State string `json:"state"`
		Trace string `json:"trace"`
	} `json:"tasks"`
}

// readStatus returns the worker-reported state and, on failure, its trace.
func (e *workerEngine) readStatus(ctx context.Context, base, name string) (string, string, error) {
	data, code, err := e.call(ctx, http.MethodGet, base+"/connectors/"+name+"/status", nil)
	if err != nil {
		return "", "", err
	}

	if code != http.StatusOK {
		return "", "", fmt.Errorf("%w: %d", errWorkerStatus, code)
	}

	var st connectStatus
	if err = json.Unmarshal(data, &st); err != nil {
		return "", "", fmt.Errorf("decode status: %w", err)
	}

	if st.Connector.State == workerStateFailed {
		return workerStateFailed, truncate(st.Connector.Trace), nil
	}

	for _, t := range st.Tasks {
		if t.State == workerStateFailed {
			return workerStateFailed, truncate(t.Trace), nil
		}
	}

	if st.Connector.State == workerStateRunning && len(st.Tasks) > 0 {
		return workerStateRunning, "", nil
	}

	return st.Connector.State, "", nil
}

func truncate(s string) string {
	if first, _, ok := strings.Cut(s, "\n"); ok {
		s = first
	}

	if len(s) > workerStatusMessages {
		return s[:workerStatusMessages]
	}

	return s
}

// reconcile pushes the connector configuration to the worker and settles the connector state from its status.
func (b *InMemoryBackend) reconcile(ctx context.Context, connectorArn string, lw *liveWorker, base string, apply bool) {
	e := b.engine

	in, ok := b.snapshotWorkerInput(connectorArn)
	if !ok {
		return
	}

	name := in.conn.Name

	if apply {
		cfg := make(map[string]string, len(in.conn.ConnectorConfiguration)+1)
		maps.Copy(cfg, in.conn.ConnectorConfiguration)

		cfg["name"] = name

		body, code, err := e.call(ctx, http.MethodPut, base+"/connectors/"+name+"/config", cfg)
		if err != nil || code >= http.StatusMultipleChoices {
			msg := string(body)
			if err != nil {
				msg = err.Error()
			}

			b.failWorker(ctx, connectorArn, lw, fmt.Errorf("%w: %s", errWorkerConnect, truncate(msg)))

			return
		}
	}

	b.settleFromStatus(ctx, connectorArn, lw, base, name)
}

// settleFromStatus waits for a terminal worker state (bounded by StartTimeout) and records it.
func (b *InMemoryBackend) settleFromStatus(
	ctx context.Context,
	connectorArn string,
	lw *liveWorker,
	base, name string,
) {
	e := b.engine

	deadline, cancel := context.WithTimeout(ctx, e.cfg.StartTimeout)
	defer cancel()

	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()

	for {
		state, trace, err := e.readStatus(deadline, base, name)

		switch {
		case err == nil && state == workerStateRunning:
			b.setWorkerState(connectorArn, lw, connectorStateRunning, nil)

			return
		case err == nil && state == workerStateFailed:
			b.setWorkerState(
				connectorArn,
				lw,
				workerStateFailed,
				&StateDescription{Code: workerFailedCode, Message: trace},
			)

			return
		}

		select {
		case <-deadline.Done():
			if ctx.Err() == nil {
				b.setWorkerState(connectorArn, lw, workerStateFailed,
					&StateDescription{Code: workerFailedCode, Message: errWorkerTimeout.Error()})
			}

			return
		case <-ticker.C:
		}
	}
}

// watchWorker keeps the connector state in step with the worker until the connector goes away.
func (b *InMemoryBackend) watchWorker(ctx context.Context, connectorArn string, lw *liveWorker, base string) {
	e := b.engine
	ticker := time.NewTicker(e.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		in, ok := b.snapshotWorkerInput(connectorArn)
		if !ok || in.conn.State == connectorStateUpdating || in.conn.State == connectorStateRestarting {
			continue
		}

		state, trace, err := e.readStatus(ctx, base, in.conn.Name)

		switch {
		case err != nil:
			continue
		case state == workerStateFailed && in.conn.State != workerStateFailed:
			b.setWorkerState(
				connectorArn,
				lw,
				workerStateFailed,
				&StateDescription{Code: workerFailedCode, Message: trace},
			)
		case state == workerStateRunning && in.conn.State != connectorStateRunning:
			b.setWorkerState(connectorArn, lw, connectorStateRunning, nil)
		}
	}
}

// setWorkerState records a worker-driven state; operations in flight complete or fail with it.
func (b *InMemoryBackend) setWorkerState(connectorArn string, lw *liveWorker, state string, desc *StateDescription) {
	b.mu.Lock("setWorkerState")
	defer b.mu.Unlock()

	if b.engine.workers[connectorArn] != lw {
		return
	}

	c, ok := b.connectors.Get(connectorArn)
	if !ok || c.State == deletingState {
		return
	}

	c.State = state
	b.engine.failures[connectorArn] = desc

	if state == workerStateFailed {
		b.failOperationsLocked(connectorArn, time.Now().UTC())

		return
	}

	b.completeOperationsLocked(connectorArn, time.Now().UTC())
}

func (b *InMemoryBackend) failOperationsLocked(connectorArn string, at time.Time) {
	for _, op := range b.connectorOperations.All() {
		if op.ConnectorArn != connectorArn || !op.EndTime.IsZero() {
			continue
		}

		switch op.State {
		case connectorOperationStateInProgress:
			op.State = "UPDATE_FAILED"
		case connectorOperationStateRestartInProgress:
			op.State = "RESTART_FAILED"
		default:
			continue
		}

		for i := range op.Steps {
			op.Steps[i].StepState = "FAILED"
		}

		op.EndTime = at
	}
}

func (b *InMemoryBackend) failWorker(ctx context.Context, connectorArn string, lw *liveWorker, cause error) {
	logger.Load(ctx).WarnContext(ctx, "kafkaconnect: worker failed", "error", cause)

	b.setWorkerState(
		connectorArn,
		lw,
		workerStateFailed,
		&StateDescription{Code: workerFailedCode, Message: cause.Error()},
	)
}

// workerReconfigure re-applies the connector configuration (or restarts it) on a live worker in the background.
func (b *InMemoryBackend) workerReconfigure(connectorArn string, restart bool) {
	e := b.engine
	if e == nil {
		return
	}

	lw := e.workers[connectorArn]
	if lw == nil {
		return
	}

	e.wg.Go(func() {
		ctx, cancel := context.WithTimeout(context.Background(), e.cfg.StartTimeout)
		defer cancel()

		b.mu.RLock("workerReconfigure")
		cur, port := e.workers[connectorArn], 0

		if cur != nil {
			port = cur.port
		}
		b.mu.RUnlock()

		if cur == nil || port == 0 {
			return
		}

		base := "http://" + net.JoinHostPort(e.cfg.Host, strconv.Itoa(port))

		if restart {
			in, ok := b.snapshotWorkerInput(connectorArn)
			if !ok {
				return
			}

			_, _, _ = e.call(ctx, http.MethodPost, base+"/connectors/"+in.conn.Name+"/restart?includeTasks=true", nil)
		}

		b.reconcile(ctx, connectorArn, lw, base, !restart)
	})
}

// SetTrustCAPEM supplies the CA that signs TLS broker certificates so workers can trust them.
func (b *InMemoryBackend) SetTrustCAPEM(fn func() []byte) {
	b.mu.Lock("SetTrustCAPEM")
	defer b.mu.Unlock()

	if b.engine != nil {
		b.engine.cfg.TrustCAPEM = fn
	}
}

// ConnectorStateDescription reports why a docker-backed connector is FAILED; nil otherwise.
func (b *InMemoryBackend) ConnectorStateDescription(connectorArn string) *StateDescription {
	b.mu.RLock("ConnectorStateDescription")
	defer b.mu.RUnlock()

	if b.engine == nil {
		return nil
	}

	return b.engine.failures[connectorArn]
}
