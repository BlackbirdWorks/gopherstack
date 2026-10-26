package ocicore

// DefaultPort is the dedicated listener's default TCP port.
const DefaultPort = 8155

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCICore.
type Settings struct {
	Port        int  `json:"port"        env:"OCI_CORE_PORT"         default:"8155" name:"port"         help:"Fixed TCP port for the dedicated OCI Core Services (networking) listener; startup fails if it is unavailable."`                       //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth bool `json:"enforceAuth" env:"OCI_CORE_ENFORCE_AUTH"                name:"enforce-auth" help:"Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort} }
