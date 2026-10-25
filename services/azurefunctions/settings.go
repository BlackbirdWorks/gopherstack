package azurefunctions

import "time"

const (
	// DefaultPort is the function-app and Kudu listener's default TCP port.
	DefaultPort = 10012
	// DefaultDomain is the default hostname suffix: <app>.<domain> serves functions, <app>.scm.<domain> Kudu.
	DefaultDomain = "azurewebsites.localhost"
	// EngineAuto runs function apps in the container runtime when one is reachable.
	EngineAuto = "auto"
	// EngineNone never starts containers: apps deploy but invocations return 503.
	EngineNone = "none"
	// DefaultStartupTimeout bounds how long a function host container may take to accept connections.
	DefaultStartupTimeout = 5 * time.Minute
)

// Settings is the Kong-embedded configuration of the Azure Functions emulator.
type Settings struct {
	Engine         string        `json:"engine"         env:"AZURE_FUNCTIONS_ENGINE"          name:"engine"          enum:"auto,none" default:"auto"                    help:"Function engine: auto (container runtime if reachable) or none (never start containers)."`                                                                        //nolint:lll // config struct tags are intentionally verbose
	Domain         string        `json:"domain"         env:"AZURE_FUNCTIONS_DOMAIN"          name:"domain"                           default:"azurewebsites.localhost" help:"Hostname suffix: <app>.<domain> reaches the app's functions and <app>.scm.<domain> its Kudu API; an unknown host reaches the only app, if there is exactly one."` //nolint:lll // config struct tags are intentionally verbose
	Port           int           `json:"port"           env:"AZURE_FUNCTIONS_PORT"            name:"port"                             default:"10012"                   help:"Fixed TCP port for the Azure Functions listener (function invocation and Kudu); startup fails if it is unavailable."`                                             //nolint:lll // config struct tags are intentionally verbose
	StartupTimeout time.Duration `json:"startupTimeout" env:"AZURE_FUNCTIONS_STARTUP_TIMEOUT" name:"startup-timeout"                  default:"5m"                      help:"How long a function host container may take to pull its image and accept connections."`                                                                           //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Engine: EngineAuto, Domain: DefaultDomain, Port: DefaultPort, StartupTimeout: DefaultStartupTimeout}
}
