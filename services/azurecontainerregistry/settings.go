package azurecontainerregistry

// DefaultPort is the registry data plane's TCP port.
const DefaultPort = 10011

// Settings is the Kong-embedded configuration of the Azure Container Registry emulator.
type Settings struct {
	TLSCertFile string `json:"tlsCertFile" env:"AZURE_ACR_TLS_CERT" name:"tls-cert" help:"Path to a PEM certificate; with --azure-acr-tls-key the registry listener serves HTTPS. Empty: plain HTTP (docker treats localhost registries as insecure)."`                 //nolint:lll // config struct tags are intentionally verbose
	TLSKeyFile  string `json:"tlsKeyFile"  env:"AZURE_ACR_TLS_KEY"  name:"tls-key"  help:"Path to the PEM key matching --azure-acr-tls-cert."`                                                                                                                          //nolint:lll // config struct tags are intentionally verbose
	Port        int    `json:"port"        env:"AZURE_ACR_PORT"     name:"port"     help:"Fixed TCP port for the Container Registry data plane (Docker Registry v2, token and /acr/v1 APIs); startup fails if it is unavailable."                      default:"10011"` //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider exists.
func DefaultSettings() Settings { return Settings{Port: DefaultPort} }
