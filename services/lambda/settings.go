package lambda

import (
	"os"
	"runtime"
	"strconv"
	"time"
)

// Settings holds configurable settings for the Lambda service.
type Settings struct {
	// DockerHost is the host/IP that Lambda containers use to reach the runtime API.
	// Defaults to "172.17.0.1" (Docker bridge gateway on Linux).
	// For Podman rootless on Linux, use the host's routable IP or "host.containers.internal".
	DockerHost string `name:"docker-host" env:"LAMBDA_DOCKER_HOST" default:"172.17.0.1" help:"Host that Lambda containers use to reach the Runtime API."` //nolint:lll // config struct tags are intentionally verbose
	// ContainerRuntime selects the container runtime: docker, podman, or auto.
	// Defaults to "docker". Can be overridden via CONTAINER_RUNTIME env var.
	ContainerRuntime string `name:"container-runtime" env:"CONTAINER_RUNTIME" default:"docker" help:"Container runtime to use: docker, podman, or auto."` //nolint:lll // config struct tags are intentionally verbose
	// PoolSize is the max number of warm containers per function.
	PoolSize int `name:"pool-size" env:"LAMBDA_POOL_SIZE" default:"3" help:"Max warm containers per Lambda function."` //nolint:lll // config struct tags are intentionally verbose
	// IdleTimeout is how long an idle container is kept before reaping.
	IdleTimeout time.Duration `name:"idle-timeout" env:"LAMBDA_IDLE_TIMEOUT" default:"10m" help:"Idle container timeout."` //nolint:lll // config struct tags are intentionally verbose
}

const (
	defaultPoolSize         = 3
	defaultIdleTimeout      = 10 * time.Minute
	defaultContainerRuntime = "docker"
)

// DefaultSettings returns Settings with sensible defaults for use without Kong.
func DefaultSettings() Settings {
	dockerHost := "172.17.0.1"
	if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
		dockerHost = "host.docker.internal"
	}

	if h := os.Getenv("LAMBDA_DOCKER_HOST"); h != "" {
		dockerHost = h
	}

	poolSize := defaultPoolSize
	if s := os.Getenv("LAMBDA_POOL_SIZE"); s != "" {
		if val, err := strconv.Atoi(s); err == nil {
			poolSize = val
		}
	}

	idleTimeout := defaultIdleTimeout
	if t := os.Getenv("LAMBDA_IDLE_TIMEOUT"); t != "" {
		if val, err := time.ParseDuration(t); err == nil {
			idleTimeout = val
		}
	}

	containerRuntime := defaultContainerRuntime
	if r := os.Getenv("CONTAINER_RUNTIME"); r != "" {
		containerRuntime = r
	}

	return Settings{
		DockerHost:       dockerHost,
		ContainerRuntime: containerRuntime,
		PoolSize:         poolSize,
		IdleTimeout:      idleTimeout,
	}
}
