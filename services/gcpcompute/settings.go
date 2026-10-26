package gcpcompute

// DefaultPort is the dedicated Compute Engine REST listener's default TCP port.
const DefaultPort = 8142

// Settings holds service-level configuration, embedded in the root CLI as CLI.GCPCompute.
type Settings struct {
	Port int `json:"port" env:"GCP_COMPUTE_PORT" default:"8142" name:"port" help:"Fixed TCP port for the dedicated Compute Engine REST listener; startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
