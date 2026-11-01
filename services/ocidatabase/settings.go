package ocidatabase

// DefaultPort is the dedicated control-plane listener's default TCP port.
const DefaultPort = 8170

// Engine modes (--oci-database-engine).
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCIDatabase.
type Settings struct {
	Engine      string `json:"engine"      env:"OCI_DATABASE_ENGINE"       default:"stub" name:"engine"       help:"OCI Database engine: stub (control plane only) or docker (each Autonomous Database runs an Oracle Database Free container behind TLS listeners)."  enum:"stub,docker"` //nolint:lll // config struct tags are intentionally verbose
	Port        int    `json:"port"        env:"OCI_DATABASE_PORT"         default:"8170" name:"port"         help:"Fixed TCP port for the dedicated OCI Database listener; startup fails if it is unavailable."`                                                                          //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth bool   `json:"enforceAuth" env:"OCI_DATABASE_ENFORCE_AUTH"                name:"enforce-auth" help:"Reject unsigned control-plane requests and signatures from API keys not registered with the emulator; off by default so any signing config works."`                    //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort, Engine: EngineStub} }
