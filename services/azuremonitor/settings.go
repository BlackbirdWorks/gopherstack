package azuremonitor

// DefaultPort is the Azure Monitor Logs HTTPS listener (query and ingestion APIs).
const DefaultPort = 10013

// Settings is the Kong-embedded configuration of the Azure Monitor Logs emulator.
type Settings struct {
	TLSCertFile    string `json:"tlsCertFile"    env:"AZURE_MONITOR_TLS_CERT"       name:"tls-cert"        help:"Path to a PEM certificate for the Azure Monitor Logs HTTPS listener. Empty: generate a self-signed certificate on every start. Requires --azure-monitor-tls-key."` //nolint:lll // config struct tags are intentionally verbose
	TLSKeyFile     string `json:"tlsKeyFile"     env:"AZURE_MONITOR_TLS_KEY"        name:"tls-key"         help:"Path to the PEM key matching --azure-monitor-tls-cert."`                                                                                                         //nolint:lll // config struct tags are intentionally verbose
	Advertise      string `json:"advertise"      env:"AZURE_MONITOR_ADVERTISE_URL"  name:"advertise"       help:"Base URL (scheme://host:port) advertised as the logs ingestion endpoint of data collection endpoints. Default: https://<ARM request host>:<port>."`                //nolint:lll // config struct tags are intentionally verbose
	Port           int    `json:"port"           env:"AZURE_MONITOR_PORT"           name:"port"            help:"Fixed TCP port for the Azure Monitor Logs HTTPS listener; startup fails if it is unavailable." default:"10013"`                                                  //nolint:lll // config struct tags are intentionally verbose
	ValidateTokens bool   `json:"validateTokens" env:"AZURE_MONITOR_VALIDATE_TOKENS" name:"validate-tokens" help:"Require a well-formed, unexpired JWT bearer token (opt-in); by default any bearer token is accepted." default:"false"`                                         //nolint:lll // config struct tags are intentionally verbose
	LenientIngest  bool   `json:"lenientIngest"  env:"AZURE_MONITOR_LENIENT_INGEST" name:"lenient-ingest"  help:"Accept uploads whose records mismatch the stream schema and drop the bad records silently, as Azure does asynchronously. Default: reject the upload with 400." default:"false"` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider exists.
func DefaultSettings() Settings { return Settings{Port: DefaultPort} }
