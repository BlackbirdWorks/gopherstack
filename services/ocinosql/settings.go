package ocinosql

// DefaultPort is the dedicated listener's default TCP port.
const DefaultPort = 8162

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCINoSQL.
type Settings struct {
	Port        int  `json:"port"        env:"OCI_NOSQL_PORT"         default:"8162" name:"port"         help:"Fixed TCP port for the dedicated OCI NoSQL Database listener; startup fails if it is unavailable."`                                   //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth bool `json:"enforceAuth" env:"OCI_NOSQL_ENFORCE_AUTH"                name:"enforce-auth" help:"Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort} }
