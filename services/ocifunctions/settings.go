package ocifunctions

import "time"

const (
	// DefaultPort is the dedicated listener's default TCP port.
	DefaultPort = 8154
	// DefaultStartupTimeout bounds image pull plus container start until the FDK socket accepts calls.
	DefaultStartupTimeout = 5 * time.Minute
	// DefaultIdleTimeout is how long an unused warm container is kept.
	DefaultIdleTimeout = 10 * time.Minute
	// DefaultMaxContainers caps live function containers (busy and warm).
	DefaultMaxContainers = 32
)

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCIFunctions.
type Settings struct {
	StartupTimeout time.Duration `json:"startupTimeout" env:"OCI_FUNCTIONS_STARTUP_TIMEOUT" name:"startup-timeout" default:"5m"   help:"How long a function container may take to pull its image and open its FDK socket."`                                                   //nolint:lll // config struct tags are intentionally verbose
	IdleTimeout    time.Duration `json:"idleTimeout"    env:"OCI_FUNCTIONS_IDLE_TIMEOUT"    name:"idle-timeout"    default:"10m"  help:"How long an idle warm function container is kept before it is removed."`                                                              //nolint:lll // config struct tags are intentionally verbose
	Port           int           `json:"port"           env:"OCI_FUNCTIONS_PORT"            name:"port"            default:"8154" help:"Fixed TCP port for the dedicated OCI Functions (management and invoke) listener; startup fails if it is unavailable."`                //nolint:lll // config struct tags are intentionally verbose
	MaxContainers  int           `json:"maxContainers"  env:"OCI_FUNCTIONS_MAX_CONTAINERS"  name:"max-containers"  default:"32"   help:"Maximum number of live function containers (busy plus warm); further concurrent invocations are throttled with 429."`                 //nolint:lll // config struct tags are intentionally verbose
	DisableEngine  bool          `json:"disableEngine"  env:"OCI_FUNCTIONS_DISABLE_ENGINE"  name:"disable-engine"                 help:"Never start containers: invocations fail with 503 (management APIs keep working)."`                                                   //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth    bool          `json:"enforceAuth"    env:"OCI_FUNCTIONS_ENFORCE_AUTH"    name:"enforce-auth"                   help:"Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{
		Port: DefaultPort, StartupTimeout: DefaultStartupTimeout,
		IdleTimeout: DefaultIdleTimeout, MaxContainers: DefaultMaxContainers,
	}
}
