package dsql

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/pgengine"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	secretBytes    = 18
	dataPlaneDB    = "postgres"
	connectTimeout = 10 * time.Second
	enginePrefix   = "dsql"
)

var errDataPlane = errors.New("dsql: data plane setup failed")

// DockerConfig configures the docker data plane: one PostgreSQL container and one TLS proxy per cluster.
type DockerConfig struct {
	Runtime pgengine.Runtime
	Ports   *portalloc.Allocator
	Logger  *slog.Logger
	// Auth returns the IAM wiring; nil refuses every connection.
	Auth func() *IAMAuthConfig
	// Now is the clock token expiry is checked against; nil means time.Now.
	Now          func() time.Time
	Image        string
	Host         string
	CADir        string
	StartTimeout time.Duration
}

type dockerLauncher struct {
	eng *pgengine.Engine
	ca  *devtls.CA
	cfg DockerConfig
}

// EnableDockerEngine backs every cluster with a PostgreSQL container behind an IAM-token proxy.
func (b *InMemoryBackend) EnableDockerEngine(cfg DockerConfig) error {
	if cfg.Image == "" {
		cfg.Image = defaultEngineImage
	}

	if cfg.Host == "" {
		cfg.Host = container.LoopbackHost
	}

	if cfg.Auth == nil {
		cfg.Auth = b.iamAuthConfig
	}

	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	eng, err := pgengine.New(pgengine.Config{
		Runtime: cfg.Runtime, Ports: cfg.Ports, Logger: cfg.Logger, Prefix: enginePrefix, StartTimeout: cfg.StartTimeout,
	})
	if err != nil {
		return fmt.Errorf("dsql engine: %w", err)
	}

	ca, err := loadCA(cfg.CADir)
	if err != nil {
		return err
	}

	l := &dockerLauncher{eng: eng, ca: ca, cfg: cfg}
	b.EnableEngine(l)

	b.mu.Lock("EnableDockerEngine")
	b.caPEM = ca.CertPEM()
	b.mu.Unlock()

	return nil
}

func loadCA(dir string) (*devtls.CA, error) {
	if dir == "" {
		ca, err := devtls.NewCA()
		if err != nil {
			return nil, fmt.Errorf("dsql engine: %w", err)
		}

		return ca, nil
	}

	ca, err := devtls.LoadOrCreateCA(dir)
	if err != nil {
		return nil, fmt.Errorf("dsql engine: %w", err)
	}

	return ca, nil
}

// EngineCAPEM returns the PEM of the CA that signs docker-mode cluster endpoints, or nil without an engine.
func (b *InMemoryBackend) EngineCAPEM() []byte {
	b.mu.RLock("EngineCAPEM")
	defer b.mu.RUnlock()

	return append([]byte(nil), b.caPEM...)
}

func randomSecret() string {
	buf := make([]byte, secretBytes)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
}

// Launch implements Launcher.
func (l *dockerLauncher) Launch(ctx context.Context, ref ClusterRef) (Instance, error) {
	rootPW, adminPW := randomSecret(), randomSecret()

	srv, err := l.eng.Launch(ctx, pgengine.Spec{ID: ref.ID, Image: l.cfg.Image, Password: rootPW})
	if err != nil {
		return nil, fmt.Errorf("dsql engine: %w", err)
	}

	inst, err := l.assemble(ctx, ref, srv, rootPW, adminPW)
	if err != nil {
		srv.Close(context.WithoutCancel(ctx), true)

		return nil, err
	}

	return inst, nil
}

func (l *dockerLauncher) assemble(
	ctx context.Context, ref ClusterRef, srv *pgengine.Server, rootPW, adminPW string,
) (*dockerInstance, error) {
	if err := srv.EnsureAdmin(ctx, adminRole, adminPW); err != nil {
		return nil, fmt.Errorf("dsql engine: %w", err)
	}

	if err := srv.Exec(ctx, dataPlaneDB, bootstrapSQL); err != nil {
		return nil, fmt.Errorf("%w: system objects: %w", errDataPlane, err)
	}

	cert, err := l.ca.Issue(hostLocalhost, "127.0.0.1", "::1", ref.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("dsql engine: %w", err)
	}

	port, err := portalloc.AcquireFrom(l.cfg.Ports, enginePrefix)
	if err != nil {
		return nil, fmt.Errorf("dsql engine: proxy port: %w", err)
	}

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp",
		net.JoinHostPort(container.BindHostFor(l.cfg.Host), strconv.Itoa(port)))
	if err != nil {
		portalloc.ReleaseTo(l.cfg.Ports, port)

		return nil, fmt.Errorf("dsql engine: proxy listen: %w", err)
	}

	wctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	inst := &dockerInstance{
		l: l, ref: ref, srv: srv, ln: ln, port: port, rootPW: rootPW, adminPW: adminPW, cancel: cancel,
		tlsCfg: &tls.Config{Certificates: []tls.Certificate{*cert}, MinVersion: tls.VersionTLS12},
	}

	inst.wg.Go(func() { inst.runJobs(wctx) })
	inst.wg.Go(inst.acceptLoop)

	return inst, nil
}

type dockerInstance struct {
	l       *dockerLauncher
	srv     *pgengine.Server
	ln      net.Listener
	tlsCfg  *tls.Config
	cancel  context.CancelFunc
	ref     ClusterRef
	rootPW  string
	adminPW string
	conns   connSet
	wg      sync.WaitGroup
	port    int
	once    sync.Once
}

// Addr implements Instance: the proxy, never the raw database.
func (i *dockerInstance) Addr() (string, int) { return i.l.cfg.Host, i.port }

// Close implements Instance.
func (i *dockerInstance) Close(ctx context.Context) {
	i.once.Do(func() {
		_ = i.ln.Close()

		i.conns.closeAll()
		i.cancel()
		i.wg.Wait()
		i.srv.Close(ctx, true)
		portalloc.ReleaseTo(i.l.cfg.Ports, i.port)
	})
}

// dsn builds a loopback connection URL to the container for role user; the password never reaches a command line.
func (i *dockerInstance) connect(ctx context.Context, user, password string) (*pgx.Conn, error) {
	h, p := i.srv.Endpoint()
	u := url.URL{
		Scheme: "postgres", User: url.UserPassword(user, password), Host: net.JoinHostPort(h, strconv.Itoa(p)),
		Path: "/" + dataPlaneDB, RawQuery: "sslmode=disable",
	}

	cfg, err := pgx.ParseConfig(u.String())
	if err != nil {
		return nil, errDataPlane
	}

	cfg.ConnectTimeout = connectTimeout

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("dsql engine: connect: %w", err)
	}

	return conn, nil
}

func (i *dockerInstance) superConn(ctx context.Context) (*pgx.Conn, error) {
	return i.connect(ctx, pgengine.Superuser, i.rootPW)
}

// roleLogin returns the engine-private password of a role mapped by AWS IAM GRANT.
func (i *dockerInstance) roleLogin(ctx context.Context, role, principalARN string) (string, bool) {
	conn, err := i.superConn(ctx)
	if err != nil {
		return "", false
	}
	defer conn.Close(ctx)

	var secret string

	const q = `SELECT s.secret FROM sys.gs_iam_grants g JOIN sys.gs_role_secrets s USING (role_name)
		WHERE g.role_name = $1 AND g.arn = $2`
	if err = conn.QueryRow(ctx, q, role, principalARN).Scan(&secret); err != nil {
		return "", false
	}

	return secret, true
}

func (i *dockerInstance) logWarn(ctx context.Context, msg string, args ...any) {
	logger.Load(ctx).WarnContext(ctx, msg, args...)
}
