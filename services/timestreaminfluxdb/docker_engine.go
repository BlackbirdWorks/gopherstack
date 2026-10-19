package timestreaminfluxdb

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
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
)

var (
	errNotReady = errors.New("influxdb did not become healthy")
	errNoPorts  = errors.New("no port allocator configured")
)

// ContainerRuntime is the slice of container.Runtime the docker engine needs.
type ContainerRuntime interface {
	HasImage(ctx context.Context, imageRef string) (bool, error)
	PullImage(ctx context.Context, imageRef string) error
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// HTTPDoer sends the readiness probe.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// DockerEngine runs one influxdb:2 container per instance.
type DockerEngine struct {
	Runtime ContainerRuntime
	Ports   *portalloc.Allocator
	HTTP    HTTPDoer
	Host    string
	Image   string
	// PollInterval and StartTimeout bound the readiness wait; zero uses the defaults.
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

func (d *DockerEngine) hostPort(label string) (int, error) {
	if d.Ports == nil {
		return 0, errNoPorts
	}

	p, err := d.Ports.Acquire(label)
	if err != nil {
		return 0, fmt.Errorf("allocate host port: %w", err)
	}

	return p, nil
}

// Start pulls the image if needed, runs the container initialised from spec and waits for /health.
func (d *DockerEngine) Start(ctx context.Context, spec EngineSpec) (EngineHandle, error) {
	if has, err := d.Runtime.HasImage(ctx, d.image()); err != nil || !has {
		if ierr := d.Runtime.PullImage(ctx, d.image()); ierr != nil {
			return EngineHandle{}, fmt.Errorf("pull %s: %w", d.image(), ierr)
		}
	}

	port, err := d.hostPort("influxdb-" + spec.Name)
	if err != nil {
		return EngineHandle{}, err
	}

	id, err := d.Runtime.CreateAndStart(ctx, container.Spec{
		Image: d.image(),
		Name:  "gopherstack-influxdb-" + spec.Name + "-" + awsid.NewN("", suffixLen),
		Env: []string{
			"DOCKER_INFLUXDB_INIT_MODE=setup",
			"DOCKER_INFLUXDB_INIT_USERNAME=" + spec.Username,
			"DOCKER_INFLUXDB_INIT_PASSWORD=" + spec.Password,
			"DOCKER_INFLUXDB_INIT_ORG=" + spec.Organization,
			"DOCKER_INFLUXDB_INIT_BUCKET=" + spec.Bucket,
		},
		Ports: []string{container.PortSpec(container.BindHostFor(d.host()), strconv.Itoa(port), influxPort)},
	})
	if err != nil {
		_ = d.Ports.Release(port)

		return EngineHandle{}, fmt.Errorf("start influxdb: %w", err)
	}

	h := EngineHandle{ContainerID: id, Host: d.host(), Port: int32(port)} //nolint:gosec // allocator ports fit in int32

	if ierr := d.waitHealthy(ctx, h); ierr != nil {
		_ = d.Stop(context.WithoutCancel(ctx), h)

		return EngineHandle{}, ierr
	}

	return h, nil
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
	err := d.Runtime.StopAndRemove(ctx, h.ContainerID)

	if d.Ports != nil && h.Port > 0 {
		_ = d.Ports.Release(int(h.Port))
	}

	return err
}
