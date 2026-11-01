package lambdaweb

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	appPort          = "8080"
	taskRoot         = "/var/task"
	defaultEntry     = "index.js"
	entrypointEnvVar = "AWS_LAMBDA_NODEJS_ENTRYPOINT"
	listenTimeout    = 10 * time.Second
	listenTick       = 100 * time.Millisecond
	probeReadWindow  = 150 * time.Millisecond
	maxUnzippedBytes = 512 << 20
	environmentMem   = 2 << 30
	environmentCPUs  = 2_000_000_000
	dirPerm          = 0o755
	logPortLabel     = "lambdaweb"
)

var (
	errListenTimeout = errors.New("application did not start listening")
	errZipTooLarge   = errors.New("deployment package exceeds the unzipped size limit")
	errZipSlip       = errors.New("deployment package entry escapes the extraction directory")
)

// Runtime is the slice of the container runtime the launcher uses.
type Runtime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// DockerConfig configures the docker launcher.
type DockerConfig struct {
	Runtime Runtime
	Ports   *portalloc.Allocator
	Image   string
	Host    string
	// ListenTimeout bounds how long a started app has to begin listening; zero means 10s.
	ListenTimeout time.Duration
}

type dockerLauncher struct {
	b   *InMemoryBackend
	cfg DockerConfig
}

// NewDockerLauncher returns a Launcher running each revision in a node container.
func NewDockerLauncher(b *InMemoryBackend, cfg DockerConfig) Launcher {
	if cfg.Image == "" {
		cfg.Image = defaultRuntimeImage
	}

	if cfg.Host == "" {
		cfg.Host = container.LoopbackHost
	}

	if cfg.ListenTimeout <= 0 {
		cfg.ListenTimeout = listenTimeout
	}

	return &dockerLauncher{b: b, cfg: cfg}
}

type dockerEnv struct {
	l         *dockerLauncher
	cancel    context.CancelFunc
	host      string
	codeDir   string
	logStream string
	id        string
	port      int
}

func (e *dockerEnv) Addr() (string, int) { return e.host, e.port }

func (e *dockerEnv) LogStream() string { return e.logStream }

func (e *dockerEnv) Close(ctx context.Context) {
	e.cancel()
	_ = e.l.cfg.Runtime.StopAndRemove(ctx, e.id)

	portalloc.ReleaseTo(e.l.cfg.Ports, e.port)

	if e.codeDir != "" {
		_ = os.RemoveAll(e.codeDir)
	}
}

// Launch implements Launcher.
func (l *dockerLauncher) Launch(ctx context.Context, spec LaunchSpec) (Environment, error) {
	rev := spec.Revision
	codes, logs, creds, endpointURL := l.b.environmentServices()

	if codes == nil {
		return nil, fmt.Errorf("%w: no S3 backend is wired", ErrCodeUnavailable)
	}

	zipData, err := codes.GetObject(ctx, rev.S3.Bucket, rev.S3.Key, rev.S3.VersionID)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCodeUnavailable, err)
	}

	dir, err := extractZip(zipData)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCodeUnavailable, err)
	}

	env := l.environmentVars(rev, creds, endpointURL)

	var port int

	id, err := container.CreateAndStartRetry(ctx, l.cfg.Runtime, func() (container.Spec, error) {
		p, aerr := portalloc.AcquireFrom(l.cfg.Ports, logPortLabel)
		if aerr != nil {
			return container.Spec{}, fmt.Errorf("host port: %w", aerr)
		}

		port = p

		return l.containerSpec(rev, dir, env, p), nil
	}, func() { portalloc.ReleaseTo(l.cfg.Ports, port) })
	if err != nil {
		_ = os.RemoveAll(dir)

		return nil, fmt.Errorf("start %s: %w", l.cfg.Image, err)
	}

	lctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	de := &dockerEnv{l: l, cancel: cancel, host: l.cfg.Host, codeDir: dir, id: id, port: port}

	if err = waitListening(ctx, net.JoinHostPort(l.cfg.Host, strconv.Itoa(port)), l.cfg.ListenTimeout); err != nil {
		de.Close(context.WithoutCancel(ctx))

		return nil, err
	}

	de.logStream = l.followLogs(lctx, logs, rev, id)

	return de, nil
}

func (l *dockerLauncher) containerSpec(rev *Revision, dir string, env []string, port int) container.Spec {
	entry := defaultEntry
	if v, ok := rev.ServiceConfig.EnvironmentVariables[entrypointEnvVar]; ok && v != "" {
		entry = v
	}

	return container.Spec{
		Name:  "gopherstack-lambdaweb-" + rev.FunctionName + "-" + rev.ID + "-" + strconv.Itoa(port),
		Image: l.cfg.Image, Env: env, Cmd: []string{"node", entry}, WorkingDir: taskRoot,
		Mounts: []string{dir + ":" + taskRoot + ":ro"}, MemoryBytes: environmentMem, NanoCPUs: environmentCPUs,
		Ports: []string{container.PortSpec(container.BindHostFor(l.cfg.Host), strconv.Itoa(port), appPort)},
	}
}

func (l *dockerLauncher) environmentVars(rev *Revision, creds CredentialIssuer, endpointURL string) []string {
	vars := map[string]string{
		"AWS_REGION": rev.Region, "AWS_DEFAULT_REGION": rev.Region, "TZ": ":UTC",
	}

	if endpointURL != "" {
		vars["AWS_ENDPOINT_URL"] = endpointURL
	}

	if creds != nil {
		role, session := rev.ServiceConfig.ExecutionRoleArn, "web-function-"+rev.FunctionName
		if c, err := creds.IssueCredentials(role, session); err == nil {
			vars["AWS_ACCESS_KEY_ID"], vars["AWS_SECRET_ACCESS_KEY"] = c.AccessKeyID, c.SecretAccessKey
			vars["AWS_SESSION_TOKEN"] = c.SessionToken
		}
	}

	maps.Copy(vars, rev.ServiceConfig.EnvironmentVariables)

	vars["AWS_LAMBDA_HTTP_ENDPOINT"] = "0.0.0.0:" + appPort

	out := make([]string, 0, len(vars))
	for k, v := range vars {
		out = append(out, k+"="+v)
	}

	return out
}

// waitListening polls until addr holds a connection open: the docker proxy accepts and then closes when the app
// is not yet listening, so an immediate EOF means "not ready" and a quiet connection means "listening".
func waitListening(ctx context.Context, addr string, limit time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()

	tick := time.NewTicker(listenTick)
	defer tick.Stop()

	for {
		if listening(ctx, addr) {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w within %s", errListenTimeout, limit)
		case <-tick.C:
		}
	}
}

func listening(ctx context.Context, addr string) bool {
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(probeReadWindow))

	var one [1]byte

	_, err = conn.Read(one[:])

	var ne net.Error

	return errors.As(err, &ne) && ne.Timeout()
}

func extractZip(data []byte) (string, error) {
	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("open zip: %w", err)
	}

	dir, err := os.MkdirTemp("", "gopherstack-lambdaweb-*")
	if err != nil {
		return "", fmt.Errorf("create temp dir: %w", err)
	}

	var total int64

	for _, f := range r.File {
		n, xerr := extractEntry(dir, f, maxUnzippedBytes-total)
		if xerr != nil {
			_ = os.RemoveAll(dir)

			return "", xerr
		}

		total += n
	}

	return dir, nil
}

func extractEntry(dir string, f *zip.File, budget int64) (int64, error) {
	dest := filepath.Join(dir, filepath.Clean(f.Name))
	if !strings.HasPrefix(dest, filepath.Clean(dir)+string(filepath.Separator)) {
		return 0, fmt.Errorf("%w: %q", errZipSlip, f.Name)
	}

	if f.FileInfo().IsDir() {
		return 0, os.MkdirAll(dest, dirPerm)
	}

	if err := os.MkdirAll(filepath.Dir(dest), dirPerm); err != nil {
		return 0, fmt.Errorf("mkdir: %w", err)
	}

	src, err := f.Open()
	if err != nil {
		return 0, fmt.Errorf("open entry %q: %w", f.Name, err)
	}
	defer src.Close()

	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode()|0o444)
	if err != nil {
		return 0, fmt.Errorf("create %q: %w", dest, err)
	}
	defer out.Close()

	n, err := io.Copy(out, io.LimitReader(src, budget+1))
	if err != nil {
		return n, fmt.Errorf("write %q: %w", dest, err)
	}

	if n > budget {
		return n, errZipTooLarge
	}

	return n, nil
}

// followLogs ships the container's output to the revision's log group and its .local companion, and returns the
// log stream name.
func (l *dockerLauncher) followLogs(ctx context.Context, sink LogSink, rev *Revision, id string) string {
	stream := time.Now().UTC().Format("2006/01/02") + "/[" + rev.ID + "]" + awsid.NewN("", 32) //nolint:mnd // hex id

	streamer, ok := l.cfg.Runtime.(container.LogStreamer)
	if !ok || sink == nil {
		return stream
	}

	group := logGroupOf(rev)
	w := &lineWriter{
		ctx:    ctx,
		sink:   sink,
		region: rev.Region,
		groups: []string{group, group + ".local"},
		stream: stream,
	}

	for _, g := range w.groups {
		if err := sink.EnsureLogGroupAndStream(ctx, rev.Region, g, stream); err != nil {
			return stream
		}
	}

	go func() {
		_ = streamer.StreamLogs(ctx, id, w, w)
		w.flush()
	}()

	return stream
}

func logGroupOf(rev *Revision) string {
	if lg := rev.ServiceConfig.Logging; lg != nil && lg.LogGroup != "" {
		return lg.LogGroup
	}

	return "/aws/lambda/web/" + rev.FunctionName
}

type lineWriter struct {
	ctx     context.Context
	sink    LogSink
	region  string
	stream  string
	groups  []string
	partial []byte
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.partial = append(w.partial, p...)

	var lines []string

	for {
		i := bytes.IndexByte(w.partial, '\n')
		if i < 0 {
			break
		}

		if line := strings.TrimRight(string(w.partial[:i]), "\r"); line != "" {
			lines = append(lines, line)
		}

		w.partial = w.partial[i+1:]
	}

	w.ship(lines)

	return len(p), nil
}

func (w *lineWriter) flush() {
	if len(w.partial) > 0 {
		w.ship([]string{string(w.partial)})
		w.partial = nil
	}
}

func (w *lineWriter) ship(lines []string) {
	if len(lines) == 0 {
		return
	}

	for _, g := range w.groups {
		_ = w.sink.PutLogLines(w.ctx, w.region, g, w.stream, lines)
	}
}
