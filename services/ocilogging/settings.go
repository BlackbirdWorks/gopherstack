package ocilogging

// DefaultPort is the dedicated listener's default TCP port.
const DefaultPort = 8160

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCILogging.
type Settings struct {
	Port        int  `json:"port"        env:"OCI_LOGGING_PORT"         default:"8160" name:"port"         help:"Fixed TCP port for the dedicated OCI Logging (control plane, ingestion and search) listener; startup fails if it is unavailable."`    //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth bool `json:"enforceAuth" env:"OCI_LOGGING_ENFORCE_AUTH"                name:"enforce-auth" help:"Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort} }
