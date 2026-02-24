package lambda

import "time"

// Settings holds configurable settings for the Lambda service.
type Settings struct {
	// DockerHost is the host/IP that Lambda containers use to reach the runtime API.
	// Defaults to "172.17.0.1" (Docker bridge gateway on Linux).
	DockerHost string `name:"docker-host" env:"LAMBDA_DOCKER_HOST" default:"172.17.0.1" help:"Host that Lambda containers use to reach the Runtime API."` //nolint:lll // config struct tags are intentionally verbose
	// PoolSize is the max number of warm containers per function.
	PoolSize int `name:"pool-size" env:"LAMBDA_POOL_SIZE" default:"3" help:"Max warm containers per Lambda function."` //nolint:lll // config struct tags are intentionally verbose
	// IdleTimeout is how long an idle container is kept before reaping.
	IdleTimeout time.Duration `name:"idle-timeout" env:"LAMBDA_IDLE_TIMEOUT" default:"10m" help:"Idle container timeout."` //nolint:lll // config struct tags are intentionally verbose
}

const (
	defaultPoolSize    = 3
	defaultIdleTimeout = 10 * time.Minute
)

// DefaultSettings returns Settings with sensible defaults for use without Kong.
func DefaultSettings() Settings {
	return Settings{
		DockerHost:  "172.17.0.1",
		PoolSize:    defaultPoolSize,
		IdleTimeout: defaultIdleTimeout,
	}
}
