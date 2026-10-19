package gcpiam

// DefaultPort is the dedicated gRPC listener's default TCP port.
const DefaultPort = 8125

// Settings holds service-level configuration, embedded in the root CLI as CLI.GCPIAM.
type Settings struct {
	Port int `json:"port" env:"GCP_IAM_PORT" default:"8125" name:"port" help:"Fixed TCP port for the dedicated Google Cloud IAM admin gRPC listener; startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
