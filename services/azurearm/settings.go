package azurearm

// DefaultPort is the fixed TCP port for the dedicated ARM listener. It sits
// inside --port-range-start/--port-range-end's default range (10000-10100)
// and leaves Key Vault (10004, M11) and App Configuration (10005, M12)
// untouched -- see AZURE.md section 10.7.
const DefaultPort = 10006

// DefaultTenantID is the fixed dev AAD tenant ID. It, DefaultSubscriptionID,
// and DefaultClientID are all-zeros GUIDs, matching LocalStack's own Azure
// emulator convention: valid GUIDs (azurerm parses subscription_id/tenant_id
// as GUIDs) that are obviously not real Azure identifiers. See AZURE.md
// section 10.5.
const DefaultTenantID = "00000000-0000-0000-0000-000000000000"

// DefaultSubscriptionID is the fixed dev subscription ID.
const DefaultSubscriptionID = "00000000-0000-0000-0000-000000000000"

// DefaultClientID is the fixed dev client (application) ID.
const DefaultClientID = "00000000-0000-0000-0000-000000000000"

// DefaultClientSecret is the fixed dev client secret accepted by the token
// endpoint when validation is off (the default).
const DefaultClientSecret = "gopherstack"

// DefaultEnvironmentName is the environment name advertised by the metadata
// document; must equal the Terraform provider's own `environment` setting.
const DefaultEnvironmentName = "gopherstack"

// DefaultLocation is the default Azure "location" value used when a request
// doesn't specify one.
const DefaultLocation = "local"

// DefaultStorageVHostPort is the default port ARM's Microsoft.Storage RP
// advertises in primaryEndpoints (AZURE.md section 10.4/10.8). This
// intentionally duplicates services/azurestoragevhost.DefaultPort as a
// literal constant rather than importing that package: services/azurearm
// has no other reason to depend on it (no delegation happens here, see
// rp_storage.go), and importing a package just for one int constant would
// be a needless coupling for a documented, stable port number. Unlike the
// three separate per-service ports this replaced, there is only one port
// now: terraform-provider-azurerm's data-plane SDK requires Blob/Queue/Table
// to share a single domain suffix, which (since Go's url.URL.Host always
// includes the port) forces them onto a single shared port too -- see
// services/azurestoragevhost's package doc comment for the full derivation.
const DefaultStorageVHostPort = 10010

// Settings holds service-level configuration for the ARM emulation backend.
// Fields are picked up by the Kong CLI parser when embedded in the root CLI
// command (see cli.go's CLI.AzureARM field), mirroring services/cosmosdb's
// Settings pattern.
type Settings struct {
	TenantID              string `json:"tenantId"              env:"AZURE_ARM_TENANT_ID"               default:"00000000-0000-0000-0000-000000000000" name:"tenant-id"               help:"Fixed dev AAD tenant ID advertised by the metadata/token endpoints."`                                                                                                                                   //nolint:lll // config struct tags are intentionally verbose
	SubscriptionID        string `json:"subscriptionId"        env:"AZURE_ARM_SUBSCRIPTION_ID"         default:"00000000-0000-0000-0000-000000000000" name:"subscription-id"         help:"Fixed dev Azure subscription ID."`                                                                                                                                                                      //nolint:lll // config struct tags are intentionally verbose
	ClientID              string `json:"clientId"              env:"AZURE_ARM_CLIENT_ID"               default:"00000000-0000-0000-0000-000000000000" name:"client-id"               help:"Fixed dev AAD application (client) ID."`                                                                                                                                                                //nolint:lll // config struct tags are intentionally verbose
	ClientSecret          string `json:"clientSecret"          env:"AZURE_ARM_CLIENT_SECRET"           default:"gopherstack"                          name:"client-secret"           help:"Fixed dev client secret; any value is accepted unless --azure-arm-validate-tokens is set."`                                                                                                             //nolint:lll // config struct tags are intentionally verbose
	Environment           string `json:"environment"           env:"AZURE_ARM_ENVIRONMENT"             default:"gopherstack"                          name:"environment"             help:"Environment name advertised by the metadata document; must match the Terraform provider's environment setting."`                                                                                        //nolint:lll // config struct tags are intentionally verbose
	Location              string `json:"location"              env:"AZURE_ARM_LOCATION"                default:"local"                                name:"location"                help:"Default Azure location value used when a request doesn't specify one."`                                                                                                                                 //nolint:lll // config struct tags are intentionally verbose
	AdvertiseStorageVHost string `json:"advertiseStorageVhost" env:"AZURE_ARM_ADVERTISE_STORAGE_VHOST"                                                name:"advertise-storage-vhost" help:"Override the host:port advertised in storage account primaryEndpoints (before the {account}.{blob,queue,table}. prefix). Default: derived from the request's Host and --azure-arm-storage-vhost-port."` //nolint:lll // config struct tags are intentionally verbose
	TLSCertFile           string `json:"tlsCertFile"           env:"AZURE_ARM_TLS_CERT"                                                               name:"tls-cert"                help:"Path to a PEM certificate for the ARM HTTPS listener. Empty (default): generate a self-signed certificate on every start. Requires --azure-arm-tls-key."`                                               //nolint:lll // config struct tags are intentionally verbose
	TLSKeyFile            string `json:"tlsKeyFile"            env:"AZURE_ARM_TLS_KEY"                                                                name:"tls-key"                 help:"Path to the PEM key matching --azure-arm-tls-cert. Required when --azure-arm-tls-cert is set."`                                                                                                         //nolint:lll // config struct tags are intentionally verbose
	Port                  int    `json:"port"                  env:"AZURE_ARM_PORT"                    default:"10006"                                name:"port"                    help:"Fixed TCP port for the dedicated ARM listener; startup fails if it's unavailable (no fallback pool)."`                                                                                                  //nolint:lll // config struct tags are intentionally verbose
	StorageVHostPort      int    `json:"storageVhostPort"      env:"AZURE_ARM_STORAGE_VHOST_PORT"      default:"10010"                                name:"storage-vhost-port"      help:"The shared services/azurestoragevhost listener's port, used to build primaryEndpoints when --azure-arm-advertise-storage-vhost isn't set. Must match --azure-storage-vhost-port."`                      //nolint:lll // config struct tags are intentionally verbose
	ValidateTokens        bool   `json:"validateTokens"        env:"AZURE_ARM_VALIDATE_TOKENS"         default:"false"                                name:"validate-tokens"         help:"Cryptographically validate ARM bearer tokens (opt-in); by default any token, or none, is accepted."`                                                                                                    //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the default Settings. Used when no ConfigProvider
// is available at init time (e.g. tests constructing a Provider directly).
func DefaultSettings() Settings {
	return Settings{
		TenantID:         DefaultTenantID,
		SubscriptionID:   DefaultSubscriptionID,
		ClientID:         DefaultClientID,
		ClientSecret:     DefaultClientSecret,
		Environment:      DefaultEnvironmentName,
		Location:         DefaultLocation,
		Port:             DefaultPort,
		StorageVHostPort: DefaultStorageVHostPort,
		ValidateTokens:   false,
	}
}
