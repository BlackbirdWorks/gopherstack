package gcpfirestore

// DefaultPort is the dedicated gRPC listener's default TCP port (the standard Firestore emulator port).
const DefaultPort = 8080

// Settings holds service-level configuration, embedded in the root CLI
// command as CLI.GCPFirestore.
type Settings struct {
	Port int `json:"port" env:"GCP_FIRESTORE_PORT" default:"8080" name:"port" help:"Fixed TCP port for the dedicated Google Cloud Firestore gRPC listener (FIRESTORE_EMULATOR_HOST); startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
