package gcpdatastore

// DefaultPort is the dedicated gRPC and REST listener's default TCP port (DATASTORE_EMULATOR_HOST).
const DefaultPort = 8143

// Settings holds service-level configuration, embedded in the root CLI as CLI.GCPDatastore.
type Settings struct {
	Port           int  `json:"port"           env:"GCP_DATASTORE_PORT"            default:"8143" name:"port"            help:"Fixed TCP port for the dedicated Google Cloud Datastore gRPC and REST listener (DATASTORE_EMULATOR_HOST); startup fails if it is unavailable."`           //nolint:lll // config struct tags are intentionally verbose
	EnforceIndexes bool `json:"enforceIndexes" env:"GCP_DATASTORE_ENFORCE_INDEXES" default:"true" name:"enforce-indexes" help:"Fail queries that need a composite index with FAILED_PRECONDITION unless a matching index was created through the admin API; false accepts every query."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort, EnforceIndexes: true}
}
