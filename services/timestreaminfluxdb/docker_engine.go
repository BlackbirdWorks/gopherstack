package timestreaminfluxdb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	// Image is the InfluxDB 2 image each engine-backed instance runs.
	Image = "influxdb:2"

	influxPort      = "8086"
	healthPath      = "/health"
	pollInterval    = 500 * time.Millisecond
	healthTimeout   = 3 * time.Minute
	healthHTTPLimit = 5 * time.Second
	defaultHost     = "127.0.0.1"
	suffixLen       = 6
	initEnvCount    = 6
	dataMount       = "/var/lib/influxdb2"
	backupsMount    = "/backups"
	localURL        = "http://127.0.0.1:" + influxPort
)

var (
	errNotReady = errors.New("influxdb did not become healthy")
	// ErrExecFailed marks a command that exited non-zero inside the container.
	ErrExecFailed = errors.New("command failed in container")
)

// ContainerRuntime is the slice of container.Runtime the docker engine needs.
type ContainerRuntime interface {
	HasImage(ctx context.Context, imageRef string) (bool, error)
	PullImage(ctx context.Context, imageRef string) error
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string) error
	StartContainer(ctx context.Context, containerID string) error
	Exec(ctx context.Context, containerID string, cmd []string) (container.ExecResult, error)
}

// HTTPDoer sends the readiness probe.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// DockerEngine runs one influxdb:2 container per instance.
type DockerEngine struct {
	Runtime      ContainerRuntime
	HTTP         HTTPDoer
	Ports        *portalloc.Allocator
	logs         sync.Map
	Host         string
	Image        string
	PollInterval time.Duration
	StartTimeout time.Duration
}

func (d *DockerEngine) image() string {
	if d.Image != "" {
		return d.Image
	}

	return Image
}

func (d *DockerEngine) host() string {
	if d.Host != "" {
		return d.Host
	}

	return defaultHost
}

func (d *DockerEngine) ensureImage(ctx context.Context) error {
	if has, err := d.Runtime.HasImage(ctx, d.image()); err != nil || !has {
		if ierr := d.Runtime.PullImage(ctx, d.image()); ierr != nil {
			return fmt.Errorf("pull %s: %w", d.image(), ierr)
		}
	}

	return nil
}

// Start pulls the image if needed, runs the container initialised from spec and waits for /health.
func (d *DockerEngine) Start(ctx context.Context, spec EngineSpec) (EngineHandle, error) {
	if err := d.ensureImage(ctx); err != nil {
		return EngineHandle{}, err
	}

	return d.launch(ctx, spec)
}

func (d *DockerEngine) launch(ctx context.Context, spec EngineSpec) (EngineHandle, error) {
	var port int

	reuse := spec.HostPort > 0

	id, err := container.CreateAndStartRetry(ctx, d.Runtime,
		func() (container.Spec, error) {
			var perr error

			if port, perr = d.pickPort(spec, &reuse); perr != nil {
				return container.Spec{}, perr
			}

			return d.containerSpec(spec, port), nil
		},
		func() { portalloc.ReleaseTo(d.Ports, port) })
	if err != nil {
		return EngineHandle{}, fmt.Errorf("start influxdb: %w", err)
	}

	h := EngineHandle{ContainerID: id, Host: d.host(), Port: int32(port)} //nolint:gosec // allocator ports fit in int32

	if herr := d.becomeReady(ctx, h, spec); herr != nil {
		_ = d.Stop(context.WithoutCancel(ctx), h)

		return EngineHandle{}, herr
	}

	d.followLogs(h.ContainerID)

	return h, nil
}

func (d *DockerEngine) pickPort(spec EngineSpec, reuse *bool) (int, error) {
	if *reuse {
		*reuse = false

		return int(spec.HostPort), nil
	}

	p, err := portalloc.AcquireFrom(d.Ports, "influxdb-"+spec.Name)
	if err != nil {
		return 0, fmt.Errorf("allocate host port: %w", err)
	}

	return p, nil
}

func (d *DockerEngine) containerSpec(spec EngineSpec, port int) container.Spec {
	cs := container.Spec{
		Image: d.image(),
		Name:  "gopherstack-influxdb-" + spec.Name + "-" + awsid.NewN("", suffixLen),
		Env:   engineEnv(spec),
		Ports: []string{container.PortSpec(container.BindHostFor(d.host()), strconv.Itoa(port), influxPort)},
	}

	if spec.DataDir != "" {
		cs.Mounts = append(cs.Mounts, spec.DataDir+":"+dataMount)
		cs.User = hostUser()
		cs.Env = append(cs.Env, "HOME=/tmp")
	}

	if spec.BackupsDir != "" {
		cs.Mounts = append(cs.Mounts, spec.BackupsDir+":"+backupsMount)
	}

	return cs
}

// hostUser runs the server as the emulator's user so the bind-mounted files can be removed by it.
func hostUser() string { return strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid()) }

func engineEnv(spec EngineSpec) []string {
	env := make([]string, 0, len(spec.Env)+initEnvCount)

	password := spec.Password
	if password == "" && spec.RestoreFrom != "" {
		password = spec.Token // placeholder: the restore replaces every user
	}

	env = append(env,
		"DOCKER_INFLUXDB_INIT_MODE=setup",
		"DOCKER_INFLUXDB_INIT_USERNAME="+spec.Username,
		"DOCKER_INFLUXDB_INIT_PASSWORD="+password,
		"DOCKER_INFLUXDB_INIT_ORG="+spec.Organization,
		"DOCKER_INFLUXDB_INIT_BUCKET="+spec.Bucket,
	)

	if spec.Token != "" {
		env = append(env, "DOCKER_INFLUXDB_INIT_ADMIN_TOKEN="+spec.Token)
	}

	return append(env, spec.Env...)
}

// becomeReady waits for /health and, for a restore, replays the backup into the empty server.
func (d *DockerEngine) becomeReady(ctx context.Context, h EngineHandle, spec EngineSpec) error {
	if err := d.waitHealthy(ctx, h); err != nil {
		return err
	}

	if spec.RestoreFrom == "" {
		return nil
	}

	res, err := d.Runtime.Exec(ctx, h.ContainerID,
		[]string{"influx", "restore", "--full", "--host", localURL, "--token", spec.Token,
			backupsMount + "/" + spec.RestoreFrom})
	if err != nil {
		return fmt.Errorf("influx restore: %w", err)
	}

	if res.ExitCode != 0 {
		return fmt.Errorf("%w: influx restore exited %d: %s", ErrExecFailed, res.ExitCode, res.Stderr)
	}

	return d.waitHealthy(ctx, h)
}

func (d *DockerEngine) waitHealthy(ctx context.Context, h EngineHandle) error {
	client := d.HTTP
	if client == nil {
		client = httpclient.New(healthHTTPLimit)
	}

	interval, limit := d.PollInterval, d.StartTimeout
	if interval <= 0 {
		interval = pollInterval
	}

	if limit <= 0 {
		limit = healthTimeout
	}

	deadline := time.NewTimer(limit)
	defer deadline.Stop()

	tick := time.NewTicker(interval)
	defer tick.Stop()

	url := "http://" + h.Host + ":" + strconv.Itoa(int(h.Port)) + healthPath

	for {
		if healthy(ctx, client, url) {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", errNotReady, ctx.Err())
		case <-deadline.C:
			return errNotReady
		case <-tick.C:
		}
	}
}

func healthy(ctx context.Context, client HTTPDoer, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}

	res, err := client.Do(req)
	if err != nil {
		return false
	}

	defer res.Body.Close()

	return res.StatusCode == http.StatusOK
}

// Stop removes the container and frees its host port.
func (d *DockerEngine) Stop(ctx context.Context, h EngineHandle) error {
	d.dropLogs(h.ContainerID)

	err := d.Runtime.StopAndRemove(ctx, h.ContainerID)

	portalloc.ReleaseTo(d.Ports, int(h.Port))

	return err
}
