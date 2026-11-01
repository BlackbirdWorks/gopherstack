package ocimysql

// DefaultPort is the dedicated listener's default TCP port.
const DefaultPort = 8159

// Engine modes (OCI_MYSQL_ENGINE).
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCIMySQL.
type Settings struct {
	Engine      string `json:"engine"      env:"OCI_MYSQL_ENGINE"       default:"stub" name:"engine"       help:"OCI MySQL engine mode: stub (control plane only) or docker (each ACTIVE DB system runs a real MySQL container)."`                     //nolint:lll // config struct tags are intentionally verbose
	Port        int    `json:"port"        env:"OCI_MYSQL_PORT"         default:"8159" name:"port"         help:"Fixed TCP port for the dedicated OCI MySQL HeatWave Database Service listener; startup fails if it is unavailable."`                  //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth bool   `json:"enforceAuth" env:"OCI_MYSQL_ENFORCE_AUTH"                name:"enforce-auth" help:"Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort} }
