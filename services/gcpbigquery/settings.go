package gcpbigquery

// DefaultPort is the dedicated listener's default TCP port.
const DefaultPort = 9050

// Settings holds service-level configuration, embedded in the root CLI command as CLI.GCPBigQuery.
type Settings struct {
	Port int `json:"port" env:"GCP_BIGQUERY_PORT" default:"9050" name:"port" help:"Fixed TCP port for the dedicated Google BigQuery listener; startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort} }
