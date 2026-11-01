package gcpstorage

// DefaultPort is the dedicated listener's default TCP port.
const DefaultPort = 4443

// Settings holds service-level configuration, embedded in the root CLI
// command as CLI.GCPStorage.
type Settings struct {
	RequireAuth bool `json:"requireAuth" env:"GCP_STORAGE_REQUIRE_AUTH" default:"false" name:"require-auth" help:"Reject Cloud Storage requests that are neither signed (V2/V4 signed URL) nor carry an Authorization header. Signed requests are always verified."` //nolint:lll // config struct tags are intentionally verbose
	Port        int  `json:"port" env:"GCP_STORAGE_PORT" default:"4443" name:"port" help:"Fixed TCP port for the dedicated Google Cloud Storage listener; startup fails if it is unavailable."`                                                                      //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort}
}
