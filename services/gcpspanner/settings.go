package gcpspanner

import "time"

const (
	// DefaultPort is the dedicated gRPC and REST listener's default TCP port.
	DefaultPort = 8135
	// EngineAuto runs the Cloud Spanner emulator container when a container runtime is reachable.
	EngineAuto = "auto"
	// EngineNone never starts a container: instances and databases are metadata only.
	EngineNone = "none"
	// DefaultImage is the pinned Google Cloud Spanner emulator image.
	DefaultImage = "gcr.io/cloud-spanner-emulator/emulator:1.5.40"
	// DefaultStartupTimeout bounds how long the emulator may take to pull its image and accept calls.
	DefaultStartupTimeout = 5 * time.Minute
)

// Settings holds service-level configuration, embedded in the root CLI command as CLI.GCPSpanner.
type Settings struct {
	Engine         string        `json:"engine"         env:"GCP_SPANNER_ENGINE"          default:"auto"                                          name:"engine"          enum:"auto,none" help:"Data plane: auto (run the Spanner emulator container if a runtime is reachable, else metadata only) or none."` //nolint:lll // config struct tags are intentionally verbose
	Image          string        `json:"image"          env:"GCP_SPANNER_IMAGE"           default:"gcr.io/cloud-spanner-emulator/emulator:1.5.40" name:"image"                            help:"Cloud Spanner emulator image serving databases, sessions and queries."`                                        //nolint:lll // config struct tags are intentionally verbose
	Port           int           `json:"port"           env:"GCP_SPANNER_PORT"            default:"8135"                                          name:"port"                             help:"Fixed TCP port for the dedicated Cloud Spanner gRPC and REST listener; startup fails if it is unavailable."`   //nolint:lll // config struct tags are intentionally verbose
	StartupTimeout time.Duration `json:"startupTimeout" env:"GCP_SPANNER_STARTUP_TIMEOUT" default:"5m"                                            name:"startup-timeout"                  help:"How long the emulator container may take to pull its image and accept calls."`                                 //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Engine: EngineAuto, Image: DefaultImage, Port: DefaultPort, StartupTimeout: DefaultStartupTimeout}
}
