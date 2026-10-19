package azurekeyvault

// DefaultPort is the fixed TCP port for the Key Vault data-plane listener.
const DefaultPort = 10004

// DefaultVaultName is the vault addressed when the request carries neither a
// vault host label nor a /vaults/<name> path prefix.
const DefaultVaultName = "default"

// Settings holds Key Vault service configuration.
type Settings struct {
	TLSCertFile      string `json:"tlsCertFile"      env:"AZURE_KEYVAULT_TLS_CERT"           name:"tls-cert"           help:"Path to a PEM certificate for the Key Vault HTTPS listener. Empty: generate a self-signed certificate on every start. Requires --azure-keyvault-tls-key."`                 //nolint:lll // config struct tags are intentionally verbose
	TLSKeyFile       string `json:"tlsKeyFile"       env:"AZURE_KEYVAULT_TLS_KEY"            name:"tls-key"            help:"Path to the PEM key matching --azure-keyvault-tls-cert."`                                                                                                                  //nolint:lll // config struct tags are intentionally verbose
	Port             int    `json:"port"             env:"AZURE_KEYVAULT_PORT"               name:"port"               help:"Fixed TCP port for the Key Vault HTTPS listener; startup fails if it is unavailable."                                                                     default:"10004"` //nolint:lll // config struct tags are intentionally verbose
	ValidateTokens   bool   `json:"validateTokens"   env:"AZURE_KEYVAULT_VALIDATE_TOKENS"    name:"validate-tokens"    help:"Require a well-formed, unexpired JWT bearer token (opt-in); by default any bearer token is accepted after the challenge."                                 default:"false"` //nolint:lll // config struct tags are intentionally verbose
	AutoCreateVaults bool   `json:"autoCreateVaults" env:"AZURE_KEYVAULT_AUTO_CREATE_VAULTS" name:"auto-create-vaults" help:"Create a vault on first data-plane use when it was not created through ARM."                                                                              default:"true"`  //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the default Settings.
func DefaultSettings() Settings {
	return Settings{Port: DefaultPort, AutoCreateVaults: true}
}
