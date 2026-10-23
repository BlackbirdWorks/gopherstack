package azureredis

// Settings is the Kong-embedded configuration of the Azure Cache for Redis emulator.
type Settings struct {
	TLSCertFile string `json:"tlsCertFile" env:"AZURE_REDIS_TLS_CERT" name:"tls-cert" help:"Path to a PEM certificate for the caches' TLS endpoints. Empty: generate a self-signed certificate on every start. Requires --azure-redis-tls-key."` //nolint:lll // config struct tags are intentionally verbose
	TLSKeyFile  string `json:"tlsKeyFile"  env:"AZURE_REDIS_TLS_KEY"  name:"tls-key"  help:"Path to the PEM key matching --azure-redis-tls-cert."`                                                                                               //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider exists.
func DefaultSettings() Settings { return Settings{} }
