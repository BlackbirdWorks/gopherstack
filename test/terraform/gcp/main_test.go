// Package gcp_test is the Terraform acceptance suite for the GCP emulators:
// an unmodified hashicorp/google provider applies, re-plans (no drift) and
// destroys resources against a running gopherstack container.
//
// The gRPC-first services (Pub/Sub, Secret Manager, KMS, Cloud Tasks,
// Scheduler) are reached through the REST transcoder in pkgs/gcprest, which
// shares each service's gRPC port.
package gcp_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	dockercontainer "github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/blackbirdworks/gopherstack/test/internal/buildcheck"
	"github.com/blackbirdworks/gopherstack/test/internal/tofu"
)

const (
	containerStoragePort    = "4443/tcp"
	containerBigQueryPort   = "9050/tcp"
	hostStoragePort         = "14443"
	hostBigQueryPort        = "19050"
	containerSQLPort        = "8130/tcp"
	hostSQLPort             = "18130"
	containerLoggingPort    = "8131/tcp"
	hostLoggingPort         = "18131"
	containerRedisPort      = "8133/tcp"
	hostRedisPort           = "18133"
	containerMonitoringPort = "8132/tcp"
	hostMonitoringPort      = "18132"
	containerSpannerPort    = "8135/tcp"
	hostSpannerPort         = "18135"
	containerArtifactPort   = "8134/tcp"
	hostArtifactPort        = "18134"
	containerCloudBuildPort = "8136/tcp"
	hostCloudBuildPort      = "18136"
	containerComputePort    = "8142/tcp"
	hostComputePort         = "18142"

	containerPubSubPort   = "8085/tcp"
	containerSecretsPort  = "8086/tcp"
	containerKMSPort      = "8087/tcp"
	containerTasksPort    = "8123/tcp"
	containerSchedulePort = "8124/tcp"
	hostPubSubPort        = "18085"
	hostSecretsPort       = "18086"
	hostKMSPort           = "18087"
	hostTasksPort         = "18123"
	hostSchedulePort      = "18124"
)

var errDockerPanic = errors.New("docker check panicked")

//nolint:gochecknoglobals // set once in TestMain, read-only during tests
var (
	tofuBinaryPath     string
	sharedContainer    testcontainers.Container
	providerCacheDir   = filepath.Join(os.TempDir(), "gopherstack-tofu-google-provider-cache")
	storageEndpoint    = "http://localhost:" + hostStoragePort + "/storage/v1/"
	bigQueryEndpoint   = "http://localhost:" + hostBigQueryPort + "/bigquery/v2/"
	pubSubEndpoint     = "http://localhost:" + hostPubSubPort + "/v1/"
	secretsEndpoint    = "http://localhost:" + hostSecretsPort + "/v1/"
	kmsEndpoint        = "http://localhost:" + hostKMSPort + "/v1/"
	tasksEndpoint      = "http://localhost:" + hostTasksPort + "/v2/"
	schedulerEndpoint  = "http://localhost:" + hostSchedulePort + "/v1/"
	sqlEndpoint        = "http://localhost:" + hostSQLPort + "/sql/v1beta4/"
	loggingEndpoint    = "http://localhost:" + hostLoggingPort + "/v2/"
	redisEndpoint      = "http://localhost:" + hostRedisPort + "/v1/"
	monitoringEndpoint = "http://localhost:" + hostMonitoringPort + "/"
	spannerEndpoint    = "http://localhost:" + hostSpannerPort + "/v1/"
	artifactEndpoint   = "http://localhost:" + hostArtifactPort + "/v1/"
	cloudBuildEndpoint = "http://localhost:" + hostCloudBuildPort + "/v1/"
	computeEndpoint    = "http://localhost:" + hostComputePort + "/compute/v1/"
)

func TestMain(m *testing.M) {
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	if testing.Short() {
		logger.Info("skipping gcp terraform tests in short mode")
		os.Exit(0)
	}

	os.Exit(run(m, logger))
}

func run(m *testing.M, logger *slog.Logger) int {
	if err := checkDocker(); err != nil {
		logger.Error("gcp terraform tests require docker", "error", err)

		return 1
	}

	if reason := prepareTofu(logger); reason != "" {
		logger.Warn("skipping gcp terraform suite", "reason", reason)

		return 0
	}

	ctx := context.Background()

	container, err := startContainer(ctx, logger)
	if err != nil {
		logger.Error("failed to start gopherstack container", "error", err)

		return 1
	}

	sharedContainer = container
	code := m.Run()

	if tErr := container.Terminate(ctx); tErr != nil {
		logger.Error("failed to terminate container", "error", tErr)
	}

	return code
}

func checkDocker() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", errDockerPanic, r)
		}
	}()

	_, err = testcontainers.NewDockerProvider()

	return err
}

func fixedPortMap(bindings map[string]string) network.PortMap {
	portMap := make(network.PortMap, len(bindings))

	for containerPort, hostPort := range bindings {
		port, err := network.ParsePort(containerPort)
		if err != nil {
			panic(fmt.Sprintf("invalid container port %q: %v", containerPort, err))
		}

		portMap[port] = []network.PortBinding{{HostIP: netip.IPv4Unspecified(), HostPort: hostPort}}
	}

	return portMap
}

func startContainer(ctx context.Context, logger *slog.Logger) (testcontainers.Container, error) {
	dockerfile := "Dockerfile"
	binPath := "../../../bin/gopherstack-linux"

	if runtime.GOOS == "darwin" {
		cmd := exec.Command("go", "build", "-trimpath", "-o", "bin/gopherstack-linux", ".")
		cmd.Dir = "../../../"
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOTOOLCHAIN=local")

		if out, err := cmd.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("building linux binary: %w: %s", err, out)
		}
	}

	if binInfo, err := os.Stat(binPath); err == nil {
		if freshErr := buildcheck.CheckFreshness(logger, binInfo, "../../.."); freshErr != nil {
			return nil, freshErr
		}

		dockerfile = "Dockerfile.test"
	}

	req := testcontainers.ContainerRequest{
		Context:       "../../../",
		Dockerfile:    dockerfile,
		PrintBuildLog: true,
		BuildOptionsModifier: func(options *client.ImageBuildOptions) {
			options.NoCache = false
			options.PullParent = false
		},
		AutoRemove: true,
		ExposedPorts: []string{
			containerStoragePort, containerBigQueryPort, containerPubSubPort, containerSecretsPort,
			containerKMSPort, containerTasksPort, containerSchedulePort, containerSQLPort,
			containerLoggingPort, containerRedisPort, containerMonitoringPort, containerSpannerPort,
			containerArtifactPort, containerCloudBuildPort, containerComputePort,
		},
		HostConfigModifier: func(hc *dockercontainer.HostConfig) {
			hc.PortBindings = fixedPortMap(map[string]string{
				containerStoragePort:    hostStoragePort,
				containerBigQueryPort:   hostBigQueryPort,
				containerPubSubPort:     hostPubSubPort,
				containerSecretsPort:    hostSecretsPort,
				containerKMSPort:        hostKMSPort,
				containerTasksPort:      hostTasksPort,
				containerSchedulePort:   hostSchedulePort,
				containerSQLPort:        hostSQLPort,
				containerLoggingPort:    hostLoggingPort,
				containerRedisPort:      hostRedisPort,
				containerMonitoringPort: hostMonitoringPort,
				containerSpannerPort:    hostSpannerPort,
				containerArtifactPort:   hostArtifactPort,
				containerCloudBuildPort: hostCloudBuildPort,
				containerComputePort:    hostComputePort,
			})
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(containerStoragePort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerBigQueryPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerPubSubPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerSecretsPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerKMSPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerTasksPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerSchedulePort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerSQLPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerLoggingPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerRedisPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerMonitoringPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerSpannerPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerArtifactPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerCloudBuildPort).WithStartupTimeout(60*time.Second),
			wait.ForListeningPort(containerComputePort).WithStartupTimeout(60*time.Second),
		),
	}

	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
}

func prepareTofu(logger *slog.Logger) string {
	for _, name := range []string{"tofu", "terraform"} {
		if path, err := exec.LookPath(name); err == nil {
			tofuBinaryPath = path

			return ""
		}
	}

	path, err := tofu.DownloadBinary(context.Background(), func(format string, args ...any) {
		logger.Info(fmt.Sprintf(format, args...))
	})
	if err != nil {
		return fmt.Sprintf("could not obtain a tofu/terraform binary: %v", err)
	}

	tofuBinaryPath = path

	return ""
}
