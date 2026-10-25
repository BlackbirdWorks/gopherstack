package gcpbigtable

// DefaultPort is the dedicated gRPC and REST listener's default TCP port.
const DefaultPort = 8137

// Settings holds service-level configuration, embedded in the root CLI
// command as CLI.GCPBigtable.
type Settings struct {
	Port int `json:"port" env:"GCP_BIGTABLE_PORT" default:"8137" name:"port" help:"Fixed TCP port for the dedicated Google Cloud Bigtable gRPC and REST listener (BIGTABLE_EMULATOR_HOST); startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
