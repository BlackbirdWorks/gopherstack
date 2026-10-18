package gcppubsub

// DefaultPort is the dedicated gRPC listener's default TCP port (the standard Pub/Sub emulator port).
const DefaultPort = 8085

// Settings holds service-level configuration, embedded in the root CLI
// command as CLI.GCPPubSub.
type Settings struct {
	Port int `json:"port" env:"GCP_PUBSUB_PORT" default:"8085" name:"port" help:"Fixed TCP port for the dedicated Google Cloud Pub/Sub gRPC listener; startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
