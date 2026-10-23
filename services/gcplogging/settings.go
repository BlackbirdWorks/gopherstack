package gcplogging

// DefaultPort is the dedicated gRPC listener's default TCP port.
const DefaultPort = 8131

// Settings holds service-level configuration, embedded in the root CLI
// command as CLI.GCPLogging.
type Settings struct {
	Port int `json:"port" env:"GCP_LOGGING_PORT" default:"8131" name:"port" help:"Fixed TCP port for the dedicated Google Cloud Logging gRPC and REST listener; startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
