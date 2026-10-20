package gcpfunctions

import "time"

const (
	// DefaultPort is the dedicated gRPC listener's default TCP port.
	DefaultPort = 8128
	// DefaultIngressPort is the default TCP port of the HTTP listener that fronts deployed functions.
	DefaultIngressPort = 8129
	// EngineAuto runs functions in the container runtime when one is reachable.
	EngineAuto = "auto"
	// EngineNone never starts containers: functions deploy but invocations return 503.
	EngineNone = "none"
	// DefaultStartupTimeout bounds how long a function container may take to accept connections.
	DefaultStartupTimeout = 5 * time.Minute
)

// Settings holds service-level configuration, embedded in the root CLI command as CLI.GCPFunctions.
type Settings struct {
	Engine         string        `json:"engine"         env:"GCP_FUNCTIONS_ENGINE"          default:"auto" name:"engine"          enum:"auto,none" help:"Function engine: auto (container runtime if reachable) or none (never start containers)."`                                                                               //nolint:lll // config struct tags are intentionally verbose
	Port           int           `json:"port"           env:"GCP_FUNCTIONS_PORT"            default:"8128" name:"port"                             help:"Fixed TCP port for the dedicated Google Cloud Functions gRPC listener; startup fails if it is unavailable."`                                                             //nolint:lll // config struct tags are intentionally verbose
	IngressPort    int           `json:"ingressPort"    env:"GCP_FUNCTIONS_INGRESS_PORT"    default:"8129" name:"ingress-port"                     help:"Fixed TCP port of the HTTP ingress that routes <function>-<hash>-<region>.localhost hosts to functions and serves source upload/download URLs; startup fails if taken."` //nolint:lll // config struct tags are intentionally verbose
	StartupTimeout time.Duration `json:"startupTimeout" env:"GCP_FUNCTIONS_STARTUP_TIMEOUT" default:"5m"   name:"startup-timeout"                  help:"How long a function container may take to install dependencies and accept connections."`                                                                                 //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{
		Engine: EngineAuto, Port: DefaultPort, IngressPort: DefaultIngressPort, StartupTimeout: DefaultStartupTimeout,
	}
}
