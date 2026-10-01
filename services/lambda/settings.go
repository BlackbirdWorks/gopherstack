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
	DockerHost string `json:"docker_host"       name:"docker-host"       env:"LAMBDA_DOCKER_HOST"     default:"172.17.0.1" help:"Host that Lambda containers use to reach the Runtime API."` //nolint:lll,golines // config struct tags are intentionally verbose
	// ContainerRuntime selects the container runtime: docker, podman, or auto.
	// Defaults to "docker". Can be overridden via CONTAINER_RUNTIME env var.
	ContainerRuntime string `json:"container_runtime" name:"container-runtime" env:"CONTAINER_RUNTIME"      default:"docker"     help:"Container runtime to use: docker, podman, or auto."` //nolint:lll,golines // config struct tags are intentionally verbose
	// PoolSize is the max number of warm containers per function.
	PoolSize int `json:"pool_size"         name:"pool-size"         env:"LAMBDA_POOL_SIZE"       default:"3"          help:"Max warm containers per Lambda function."` //nolint:lll,golines // config struct tags are intentionally verbose
	// IdleTimeout is how long an idle container is kept before reaping.
	IdleTimeout time.Duration `json:"idle_timeout"      name:"idle-timeout"      env:"LAMBDA_IDLE_TIMEOUT"    default:"10m"        help:"Idle container timeout."` //nolint:lll,golines // config struct tags are intentionally verbose
	// MaxRuntimes is the maximum number of per-function runtimes kept alive simultaneously.
	// When the limit is exceeded, the least-recently-used runtime is stopped and evicted.
	// Defaults to defaultMaxRuntimes. Set to 0 to use the default.
	MaxRuntimes int `json:"max_runtimes"      name:"max-runtimes"      env:"LAMBDA_MAX_RUNTIMES"    default:"50"         help:"Maximum number of simultaneous per-function Lambda runtimes."` //nolint:lll,golines // config struct tags are intentionally verbose
	// DisableHotReload turns the hot-reload magic bucket off (LocalStack enables it by default).
	DisableHotReload bool `json:"disable_hot_reload" name:"disable-hot-reload" env:"LAMBDA_DISABLE_HOT_RELOAD" default:"false" help:"Disable Lambda hot reloading from local directories."` //nolint:lll // config struct tags are intentionally verbose
	// KeepContainers keeps Lambda containers alive after execution (debugging).
	KeepContainers bool `json:"keep_containers"   name:"keep-containers"   env:"LAMBDA_KEEP_CONTAINERS" default:"false"      help:"If true, keep Lambda containers alive for debugging."` //nolint:lll,golines // config struct tags are intentionally verbose
	// HotReloadIntervalMS bounds how often the hot-reload directory is re-scanned (int32 keeps Settings compact).
	HotReloadIntervalMS int32 `json:"hot_reload_interval_ms" name:"hot-reload-interval-ms" env:"LAMBDA_HOT_RELOAD_INTERVAL_MS" default:"250" help:"Minimum milliseconds between hot-reload change scans."` //nolint:lll // config struct tags are intentionally verbose
}

const (
	defaultPoolSize         = 3
	defaultIdleTimeout      = 10 * time.Minute
	defaultContainerRuntime = "docker"
	defaultMaxRuntimes      = 50
	defaultHotReloadBucket  = "hot-reload"
	defaultHotReloadScanMS  = 250
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

	maxRuntimes := defaultMaxRuntimes
	if m := os.Getenv("LAMBDA_MAX_RUNTIMES"); m != "" {
		if val, err := strconv.Atoi(m); err == nil && val > 0 {
			maxRuntimes = val
		}
	}

	keepContainers := false
	if k := os.Getenv("LAMBDA_KEEP_CONTAINERS"); k != "" {
		if val, err := strconv.ParseBool(k); err == nil {
			keepContainers = val
		}
	}

	st := Settings{
		DockerHost:       dockerHost,
		ContainerRuntime: containerRuntime,
		PoolSize:         poolSize,
		IdleTimeout:      idleTimeout,
		MaxRuntimes:      maxRuntimes,
		KeepContainers:   keepContainers,
	}

	return st.withHotReloadEnv()
}

func (s Settings) withHotReloadEnv() Settings {
	s.HotReloadIntervalMS = defaultHotReloadScanMS
	if v := os.Getenv("LAMBDA_HOT_RELOAD_INTERVAL_MS"); v != "" {
		if val, err := strconv.ParseInt(v, 10, 32); err == nil && val >= 0 {
			s.HotReloadIntervalMS = int32(val)
		}
	}

	if v := os.Getenv("LAMBDA_DISABLE_HOT_RELOAD"); v != "" {
		if val, err := strconv.ParseBool(v); err == nil {
			s.DisableHotReload = val
		}
	}

	return s
}
