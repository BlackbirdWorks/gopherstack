package gcpeventarc

// DefaultPort is the dedicated gRPC and REST listener's default TCP port.
const DefaultPort = 8138

// Settings holds service-level configuration, embedded in the root CLI as CLI.GCPEventarc.
type Settings struct {
	Port int `json:"port" env:"GCP_EVENTARC_PORT" default:"8138" name:"port" help:"Fixed TCP port for the dedicated Google Cloud Eventarc gRPC and REST listener; startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
