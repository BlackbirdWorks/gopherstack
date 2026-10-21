package gcpsql

import "time"

const (
	// DefaultPort is the dedicated listener's default TCP port.
	DefaultPort = 8130
	// EngineAuto launches real database containers when a container runtime is reachable.
	EngineAuto = "auto"
	// EngineNone never starts containers: instances are state-only.
	EngineNone = "none"
	// DefaultStartupTimeout bounds how long a database container may take to accept connections.
	DefaultStartupTimeout = 5 * time.Minute
)

// Settings holds service-level configuration, embedded in the root CLI command as CLI.GCPSQL.
type Settings struct {
	Engine         string        `json:"engine"         env:"GCP_SQL_ENGINE"          default:"auto" name:"engine"          enum:"auto,none" help:"Database engine: auto (container runtime if reachable, else state-only) or none (never start containers)."` //nolint:lll // config struct tags are intentionally verbose
	Port           int           `json:"port"           env:"GCP_SQL_PORT"            default:"8130" name:"port"                             help:"Fixed TCP port for the dedicated Cloud SQL Admin listener; startup fails if it is unavailable."`            //nolint:lll // config struct tags are intentionally verbose
	StartupTimeout time.Duration `json:"startupTimeout" env:"GCP_SQL_STARTUP_TIMEOUT" default:"5m"   name:"startup-timeout"                  help:"How long a database container may take to pull its image and accept connections."`                          //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Engine: EngineAuto, Port: DefaultPort, StartupTimeout: DefaultStartupTimeout}
}
