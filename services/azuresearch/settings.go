package azuresearch

import "time"

// DefaultPort is the Azure AI Search data-plane HTTPS port.
const DefaultPort = 10015

// DefaultPollInterval is how often the indexer scheduler checks for due schedules.
const DefaultPollInterval = 30 * time.Second

// Settings is the Kong-embedded configuration of the Azure AI Search emulator.
type Settings struct {
	TLSCertFile  string `json:"tlsCertFile"  env:"AZURE_SEARCH_TLS_CERT"      name:"tls-cert"      help:"Path to a PEM certificate for the Azure AI Search HTTPS listener. Empty: generate a self-signed certificate on every start. Requires --azure-search-tls-key."`                 //nolint:lll // config struct tags are intentionally verbose
	TLSKeyFile   string `json:"tlsKeyFile"   env:"AZURE_SEARCH_TLS_KEY"       name:"tls-key"       help:"Path to the PEM key matching --azure-search-tls-cert."`                                                                                                                        //nolint:lll // config struct tags are intentionally verbose
	PollInterval string `json:"pollInterval" env:"AZURE_SEARCH_POLL_INTERVAL" name:"poll-interval" help:"How often the indexer scheduler looks for due schedules (Go duration)."                                                                                       default:"30s"`   //nolint:lll // config struct tags are intentionally verbose
	Port         int    `json:"port"         env:"AZURE_SEARCH_PORT"          name:"port"          help:"Fixed TCP port for the Azure AI Search HTTPS listener; startup fails if it is unavailable."                                                                   default:"10015"` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider exists.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort, PollInterval: DefaultPollInterval.String()}
}
