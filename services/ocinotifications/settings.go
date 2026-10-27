package ocinotifications

// DefaultPort is the dedicated listener's default TCP port.
const DefaultPort = 8157

// Settings holds service-level configuration, embedded in the root CLI as CLI.OCINotifications.
type Settings struct {
	Port        int  `json:"port"        env:"OCI_NOTIFICATIONS_PORT"         default:"8157" name:"port"         help:"Fixed TCP port for the dedicated OCI Notifications (topics, subscriptions and publish) listener; startup fails if it is unavailable."` //nolint:lll // config struct tags are intentionally verbose
	EnforceAuth bool `json:"enforceAuth" env:"OCI_NOTIFICATIONS_ENFORCE_AUTH"                name:"enforce-auth" help:"Reject unsigned requests and signatures from API keys not registered with the emulator; off by default so any signing config works."`  //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings { return Settings{Port: DefaultPort} }
