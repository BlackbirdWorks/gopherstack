package azuregraph

// DefaultPort is the Microsoft Graph HTTPS listener.
const DefaultPort = 10014

// DefaultTenantID is the dev tenant; it must equal --azure-arm-tenant-id for tokens to validate.
const DefaultTenantID = "00000000-0000-0000-0000-000000000000"

// DefaultDomain is the tenant's initial, default verified domain.
const DefaultDomain = "gopherstack.onmicrosoft.com"

// Settings is the Kong-embedded configuration of the Microsoft Graph emulator.
type Settings struct {
	TLSCertFile    string `json:"tlsCertFile"    env:"AZURE_GRAPH_TLS_CERT"        name:"tls-cert"        help:"Path to a PEM certificate for the Microsoft Graph HTTPS listener. Empty: generate a self-signed certificate on every start. Requires --azure-graph-tls-key."`                                                //nolint:lll // config struct tags are intentionally verbose
	TLSKeyFile     string `json:"tlsKeyFile"     env:"AZURE_GRAPH_TLS_KEY"         name:"tls-key"         help:"Path to the PEM key matching --azure-graph-tls-cert."`                                                                                                                                                       //nolint:lll // config struct tags are intentionally verbose
	TenantID       string `json:"tenantId"       env:"AZURE_GRAPH_TENANT_ID"       name:"tenant-id"       help:"Tenant ID of the directory; must match --azure-arm-tenant-id."                                                                                               default:"00000000-0000-0000-0000-000000000000"` //nolint:lll // config struct tags are intentionally verbose
	DefaultDomain  string `json:"defaultDomain"  env:"AZURE_GRAPH_DEFAULT_DOMAIN"  name:"default-domain"  help:"The tenant's default verified domain; user principal names must use a verified domain."                                                                      default:"gopherstack.onmicrosoft.com"`          //nolint:lll // config struct tags are intentionally verbose
	DevClientID    string `json:"devClientId"    env:"AZURE_GRAPH_DEV_CLIENT_ID"   name:"dev-client-id"   help:"Client ID of the built-in dev service principal that accepts any secret at the token endpoint; must match --azure-arm-client-id."                            default:"00000000-0000-0000-0000-000000000000"` //nolint:lll // config struct tags are intentionally verbose
	ExtraDomains   string `json:"extraDomains"   env:"AZURE_GRAPH_EXTRA_DOMAINS"   name:"extra-domains"   help:"Comma-separated additional verified domains."`                                                                                                                                                               //nolint:lll // config struct tags are intentionally verbose
	Port           int    `json:"port"           env:"AZURE_GRAPH_PORT"            name:"port"            help:"Fixed TCP port for the Microsoft Graph HTTPS listener; startup fails if it is unavailable."                                                                  default:"10014"`                                //nolint:lll // config struct tags are intentionally verbose
	ValidateTokens bool   `json:"validateTokens" env:"AZURE_GRAPH_VALIDATE_TOKENS" name:"validate-tokens" help:"Verify bearer token signature, expiry and audience against the emulator's Entra issuer (opt-in); by default any well-formed bearer token is accepted."       default:"false"`                                //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider exists.
func DefaultSettings() Settings {
	return Settings{
		Port:          DefaultPort,
		TenantID:      DefaultTenantID,
		DefaultDomain: DefaultDomain,
		DevClientID:   DefaultTenantID,
	}
}
