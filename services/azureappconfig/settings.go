package azureappconfig

// DefaultPort is the App Configuration HTTPS data-plane port.
const DefaultPort = 10008

// DefaultStoreName is the store addressed when a request names none.
const DefaultStoreName = "default"

// Settings is the Kong-embedded configuration of the App Configuration emulator.
type Settings struct {
	TLSCertFile  string `json:"tlsCertFile"  env:"AZURE_APPCONFIG_TLS_CERT"      name:"tls-cert"      help:"Path to a PEM certificate for the App Configuration HTTPS listener. Empty: generate a self-signed certificate on every start. Requires --azure-appconfig-tls-key."`                 //nolint:lll // config struct tags are intentionally verbose
	TLSKeyFile   string `json:"tlsKeyFile"   env:"AZURE_APPCONFIG_TLS_KEY"       name:"tls-key"       help:"Path to the PEM key matching --azure-appconfig-tls-cert."`                                                                                                                          //nolint:lll // config struct tags are intentionally verbose
	Port         int    `json:"port"         env:"AZURE_APPCONFIG_PORT"          name:"port"          help:"Fixed TCP port for the App Configuration HTTPS listener; startup fails if it is unavailable."                                                                      default:"10008"` //nolint:lll // config struct tags are intentionally verbose
	ValidateHMAC bool   `json:"validateHMAC" env:"AZURE_APPCONFIG_VALIDATE_HMAC" name:"validate-hmac" help:"Cryptographically validate HMAC-SHA256 request signatures against the store's access keys (opt-in); by default any credential is accepted."                        default:"false"` //nolint:lll // config struct tags are intentionally verbose
	AutoCreate   bool   `json:"autoCreate"   env:"AZURE_APPCONFIG_AUTO_CREATE"   name:"auto-create"   help:"Create a store on first data-plane use when it was not created through ARM."                                                                                       default:"true"`  //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider exists.
func DefaultSettings() Settings { return Settings{Port: DefaultPort, AutoCreate: true} }
