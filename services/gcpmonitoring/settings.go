package gcpmonitoring

// DefaultPort is the dedicated gRPC listener's default TCP port.
const DefaultPort = 8132

// Settings holds service-level configuration, embedded in the root CLI
// command as CLI.GCPMonitoring.
type Settings struct {
	Port int `json:"port" env:"GCP_MONITORING_PORT" default:"8132" name:"port" help:"Fixed TCP port for the dedicated Google Cloud Monitoring gRPC and REST listener; startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
