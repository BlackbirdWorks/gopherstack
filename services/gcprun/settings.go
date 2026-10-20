package gcprun

import "time"

const (
	// DefaultPort is the dedicated gRPC listener's default TCP port.
	DefaultPort = 8126
	// DefaultIngressPort is the default TCP port of the HTTP listener that fronts deployed services.
	DefaultIngressPort = 8127
	// EngineAuto runs workloads in the container runtime when one is reachable.
	EngineAuto = "auto"
	// EngineNone never starts containers: services stay deployable but serve 503, jobs fail.
	EngineNone = "none"
	// DefaultStartupTimeout bounds how long a new revision may take to accept connections.
	DefaultStartupTimeout = 4 * time.Minute
)

// Settings holds service-level configuration, embedded in the root CLI
// command as CLI.GCPRun.
type Settings struct {
	Engine         string        `json:"engine"         env:"GCP_RUN_ENGINE"          default:"auto" name:"engine"          enum:"auto,none" help:"Workload engine: auto (container runtime if reachable) or none (never start containers)."`                                               //nolint:lll // config struct tags are intentionally verbose
	Port           int           `json:"port"           env:"GCP_RUN_PORT"            default:"8126" name:"port"                             help:"Fixed TCP port for the dedicated Google Cloud Run gRPC listener; startup fails if it is unavailable."`                                   //nolint:lll // config struct tags are intentionally verbose
	IngressPort    int           `json:"ingressPort"    env:"GCP_RUN_INGRESS_PORT"    default:"8127" name:"ingress-port"                     help:"Fixed TCP port of the HTTP ingress that routes <service>-<hash>-<region>.localhost hosts to deployed services; startup fails if taken."` //nolint:lll // config struct tags are intentionally verbose
	StartupTimeout time.Duration `json:"startupTimeout" env:"GCP_RUN_STARTUP_TIMEOUT" default:"4m"   name:"startup-timeout"                  help:"How long a new revision may take to accept connections before it is marked failed."`                                                     //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{
		Engine: EngineAuto, Port: DefaultPort, IngressPort: DefaultIngressPort, StartupTimeout: DefaultStartupTimeout,
	}
}
