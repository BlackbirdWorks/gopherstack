package azureeventgrid

// DefaultPort is the Event Grid HTTPS data-plane port.
const DefaultPort = 10007

// Settings is the Kong-embedded configuration of the Event Grid emulator.
type Settings struct {
	TLSCertFile string `json:"tlsCertFile" env:"AZURE_EVENTGRID_TLS_CERT"      name:"tls-cert"     help:"Path to a PEM certificate for the Event Grid HTTPS listener. Empty: generate a self-signed certificate on every start. Requires --azure-eventgrid-tls-key."`                 //nolint:lll // config struct tags are intentionally verbose
	TLSKeyFile  string `json:"tlsKeyFile"  env:"AZURE_EVENTGRID_TLS_KEY"       name:"tls-key"      help:"Path to the PEM key matching --azure-eventgrid-tls-cert."`                                                                                                                   //nolint:lll // config struct tags are intentionally verbose
	Advertise   string `json:"advertise"   env:"AZURE_EVENTGRID_ADVERTISE_URL" name:"advertise"    help:"Base URL (scheme://host:port) used in webhook validationUrl values. Default: https://localhost:<port>."`                                                                     //nolint:lll // config struct tags are intentionally verbose
	Port        int    `json:"port"        env:"AZURE_EVENTGRID_PORT"          name:"port"         help:"Fixed TCP port for the Event Grid HTTPS listener; startup fails if it is unavailable."                                                                      default:"10007"` //nolint:lll // config struct tags are intentionally verbose
	ValidateSAS bool   `json:"validateSAS" env:"AZURE_EVENTGRID_VALIDATE_SAS"  name:"validate-sas" help:"Cryptographically validate aeg-sas-key / SAS tokens against topic and namespace keys (opt-in); by default any credential is accepted."                      default:"false"` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider exists.
func DefaultSettings() Settings { return Settings{Port: DefaultPort} }
