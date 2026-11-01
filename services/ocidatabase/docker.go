package ocidatabase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	goora "github.com/sijms/go-ora/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

// DefaultImage is the Oracle Database Free image behind every Autonomous Database in docker mode: the 26ai Free
// release in gvenzl's slim, pre-initialised variant (about 1.3 GB compressed). Override it with OCI_DATABASE_IMAGE.
const DefaultImage = "gvenzl/oracle-free:23.26.3-slim-faststart@" +
	"sha256:f5ff19033860d662c821cb04eb10483fa94f14f78eae252d054291ea07028093"

const (
	listenerPort    = "1521"
	adminUser       = "ADMIN"
	readyTimeout    = 10 * time.Minute
	readyPoll       = 2 * time.Second
	probeTimeout    = 15 * time.Second
	containerPrefix = "gopherstack-ocidb-"
	nameTailLen     = 10
)

var errNoRuntime = errors.New("the container runtime cannot stop and start containers")

type containerAPI interface {
	gcpserve.Runner
	StopContainer(ctx context.Context, containerID string) error
	StartContainer(ctx context.Context, containerID string) error
}

// DockerConfig configures the docker launcher.
type DockerConfig struct {
	// Runtime dials the container runtime on first use.
	Runtime func() (gcpserve.Runner, error)
	// Ports hands out the loopback ports the database listeners are published on; nil probes for free ones.
	Ports        *portalloc.Allocator
	StartTimeout time.Duration
}

// DockerLauncher runs each database in its own Oracle Database Free container.
type DockerLauncher struct {
	cfg DockerConfig
}

// NewDockerLauncher creates a launcher.
func NewDockerLauncher(cfg DockerConfig) *DockerLauncher {
	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = readyTimeout
	}

	return &DockerLauncher{cfg: cfg}
}

// Launch starts the container and returns once ADMIN can log in to FREEPDB1.
func (l *DockerLauncher) Launch(ctx context.Context, spec LaunchSpec) (Instance, error) {
	rt, err := l.cfg.Runtime()
	if err != nil {
		return nil, fmt.Errorf("the container runtime is unavailable: %w", err)
	}

	api, ok := rt.(containerAPI)
	if !ok {
		return nil, errNoRuntime
	}

	port, err := portalloc.AcquireFrom(l.cfg.Ports, "ocidatabase")
	if err != nil {
		return nil, fmt.Errorf("allocate port: %w", err)
	}

	id, err := api.CreateAndStart(ctx, container.Spec{
		Image: spec.Image, Name: containerPrefix + awsid.NewN("", nameTailLen),
		Env: []string{
			"ORACLE_PASSWORD=" + spec.Password, "APP_USER=" + adminUser, "APP_USER_PASSWORD=" + spec.Password,
		},
		Ports:  []string{container.PortSpec(container.LoopbackHost, strconv.Itoa(port), listenerPort)},
		Labels: map[string]string{"gopherstack.service": "ocidatabase", "gopherstack.ocidatabase.id": spec.ID},
	})
	if err != nil {
		portalloc.ReleaseTo(l.cfg.Ports, port)

		return nil, fmt.Errorf("start the database container: %w", err)
	}

	inst := &dockerInstance{
		api:      api,
		id:       id,
		port:     port,
		password: spec.Password,
		ports:    l.cfg.Ports,
		timeout:  l.cfg.StartTimeout,
	}
	if err = inst.waitReady(ctx); err != nil {
		inst.Close(context.WithoutCancel(ctx))

		return nil, err
	}

	return inst, nil
}

type dockerInstance struct {
	api      containerAPI
	ports    *portalloc.Allocator
	id       string
	password string
	port     int
	timeout  time.Duration
}

func (d *dockerInstance) Endpoint() (string, int) { return container.LoopbackHost, d.port }

func (d *dockerInstance) Stop(ctx context.Context) error {
	if err := d.api.StopContainer(ctx, d.id); err != nil {
		return fmt.Errorf("stop the database container: %w", err)
	}

	return nil
}

func (d *dockerInstance) Start(ctx context.Context) error {
	if err := d.api.StartContainer(ctx, d.id); err != nil {
		return fmt.Errorf("start the database container: %w", err)
	}

	return d.waitReady(ctx)
}

func (d *dockerInstance) Close(ctx context.Context) {
	_ = d.api.StopAndRemove(ctx, d.id)
	portalloc.ReleaseTo(d.ports, d.port)
}

func (d *dockerInstance) open(user, password string) (*sql.DB, error) {
	db, err := sql.Open("oracle", goora.BuildUrl(container.LoopbackHost, d.port, engineService, user, password, nil))
	if err != nil {
		return nil, fmt.Errorf("open the database: %w", err)
	}

	db.SetMaxOpenConns(1)

	return db, nil
}

func (d *dockerInstance) probe(ctx context.Context) error {
	db, err := d.open(adminUser, d.password)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping the database: %w", err)
	}

	return nil
}

func (d *dockerInstance) waitReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	t := time.NewTicker(readyPoll)
	defer t.Stop()

	var last error

	for {
		if last = d.probe(ctx); last == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("the database did not accept connections in time: %w", last)
		case <-t.C:
		}
	}
}

// SetAdminPassword changes the ADMIN password; ADMIN replaces its own password, so no extra privilege is needed.
func (d *dockerInstance) SetAdminPassword(ctx context.Context, current, next string) error {
	db, err := d.open(adminUser, current)
	if err != nil {
		return err
	}
	defer db.Close()

	//nolint:gosec // DDL cannot bind the password; both values passed validation and never reach a log
	stmt := `ALTER USER ` + adminUser + ` IDENTIFIED BY "` + next + `" REPLACE "` + current + `"`

	if _, err = db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("change the ADMIN password: %w", err)
	}

	d.password = next

	return nil
}
