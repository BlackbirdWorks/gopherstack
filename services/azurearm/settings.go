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

// ACIEngineAuto runs container groups in the container runtime; ACIEngineNone fails them to provision.
const (
	ACIEngineAuto = "auto"
	ACIEngineNone = "none"
)

// AKSEngineStub keeps managed clusters as ARM state; AKSEngineDocker runs each as a k3s container.
const (
	AKSEngineStub   = "stub"
	AKSEngineDocker = "docker"
)

// DBEngineStub keeps flexible servers as ARM state; DBEngineDocker runs each as a real postgres or mysql container.
const (
	DBEngineStub   = "stub"
	DBEngineDocker = "docker"
)

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

// DefaultServiceBusPort is the default port ARM's Microsoft.ServiceBus RP
// advertises as serviceBusEndpoint (AZURE.md section 10.10's M9 entry).
// Mirrors DefaultStorageVHostPort's rationale for duplicating rather than
// importing services/azureservicebus.DefaultPort: this package has no other
// reason to depend on that package (rp_servicebus.go delegates through the
// narrow ServiceBusEntities interface, not a direct import), and importing
// it just for one int constant would be a needless coupling.
const DefaultServiceBusPort = 10003

// DefaultKeyVaultPort is the default port ARM's Microsoft.KeyVault RP
// advertises in vaultUri; it must equal services/azurekeyvault.DefaultPort.
const DefaultKeyVaultPort = 10004

// DefaultEventHubPort and DefaultEventHubAMQPPort are the Event Hubs REST and
// AMQP ports ARM advertises; they must equal services/azureeventhubs's defaults.
const (
	DefaultEventHubPort     = 10005
	DefaultEventHubAMQPPort = 5673
)

// DefaultEventGridPort is the Event Grid data plane's HTTPS port ARM advertises; it
// must equal services/azureeventgrid.DefaultPort.
const DefaultEventGridPort = 10007

// DefaultAppConfigPort is the App Configuration data plane's HTTPS port ARM advertises; it
// must equal services/azureappconfig.DefaultPort.
const DefaultAppConfigPort = 10008

// DefaultContainerRegistryPort is the Container Registry data plane's port ARM advertises in loginServer; it
// must equal services/azurecontainerregistry.DefaultPort.
const DefaultContainerRegistryPort = 10011

// DefaultGraphPort is the Microsoft Graph listener's HTTPS port ARM advertises as the environment's Graph
// endpoint; it must equal services/azuregraph.DefaultPort.
const DefaultGraphPort = 10014

// DefaultCosmosDBPort is the Cosmos DB data plane's default port, used for documentEndpoint when no
// data-plane adapter is wired; it must equal services/cosmosdb.DefaultPort.
const DefaultCosmosDBPort = 8081

// Settings holds service-level configuration for the ARM emulation backend.
// Fields are picked up by the Kong CLI parser when embedded in the root CLI
// command (see cli.go's CLI.AzureARM field), mirroring services/cosmosdb's
// Settings pattern.
// Field order below is grouped by type (all strings, then all ints, then the
// bool) rather than by topic, to satisfy govet's fieldalignment check --
// interleaving AdvertiseServiceBus (a string) between the int fields, as a
// topic-grouped order would, widens the struct's leading run of
// pointer-containing fields from 152 to 168 bytes. Safe to reorder: every
// construction of Settings (DefaultSettings, and Kong's tag-driven parsing)
// uses field names, never positional literals.
type Settings struct {
	TenantID              string `json:"tenantId"              env:"AZURE_ARM_TENANT_ID"                     default:"00000000-0000-0000-0000-000000000000" name:"tenant-id"               help:"Fixed dev AAD tenant ID advertised by the metadata/token endpoints."`                                                                                                                                                                //nolint:lll // config struct tags are intentionally verbose
	SubscriptionID        string `json:"subscriptionId"        env:"AZURE_ARM_SUBSCRIPTION_ID"               default:"00000000-0000-0000-0000-000000000000" name:"subscription-id"         help:"Fixed dev Azure subscription ID."`                                                                                                                                                                                                   //nolint:lll // config struct tags are intentionally verbose
	ClientID              string `json:"clientId"              env:"AZURE_ARM_CLIENT_ID"                     default:"00000000-0000-0000-0000-000000000000" name:"client-id"               help:"Fixed dev AAD application (client) ID."`                                                                                                                                                                                             //nolint:lll // config struct tags are intentionally verbose
	ClientSecret          string `json:"clientSecret"          env:"AZURE_ARM_CLIENT_SECRET"                 default:"gopherstack"                          name:"client-secret"           help:"Fixed dev client secret; any value is accepted unless --azure-arm-validate-tokens is set."`                                                                                                                                          //nolint:lll // config struct tags are intentionally verbose
	Environment           string `json:"environment"           env:"AZURE_ARM_ENVIRONMENT"                   default:"gopherstack"                          name:"environment"             help:"Environment name advertised by the metadata document; must match the Terraform provider's environment setting."`                                                                                                                     //nolint:lll // config struct tags are intentionally verbose
	Location              string `json:"location"              env:"AZURE_ARM_LOCATION"                      default:"local"                                name:"location"                help:"Default Azure location value used when a request doesn't specify one."`                                                                                                                                                              //nolint:lll // config struct tags are intentionally verbose
	AdvertiseStorageVHost string `json:"advertiseStorageVhost" env:"AZURE_ARM_ADVERTISE_STORAGE_VHOST"                                                      name:"advertise-storage-vhost" help:"Override the host:port advertised in storage account primaryEndpoints (before the {account}.{blob,queue,table}. prefix). Default: derived from the request's Host and --azure-arm-storage-vhost-port."`                              //nolint:lll // config struct tags are intentionally verbose
	TLSCertFile           string `json:"tlsCertFile"           env:"AZURE_ARM_TLS_CERT"                                                                     name:"tls-cert"                help:"Path to a PEM certificate for the ARM HTTPS listener. Empty (default): generate a self-signed certificate on every start. Requires --azure-arm-tls-key."`                                                                            //nolint:lll // config struct tags are intentionally verbose
	TLSKeyFile            string `json:"tlsKeyFile"            env:"AZURE_ARM_TLS_KEY"                                                                      name:"tls-key"                 help:"Path to the PEM key matching --azure-arm-tls-cert. Required when --azure-arm-tls-cert is set."`                                                                                                                                      //nolint:lll // config struct tags are intentionally verbose
	AdvertiseServiceBus   string `json:"advertiseServiceBus"   env:"AZURE_ARM_ADVERTISE_SERVICEBUS_ENDPOINT"                                                name:"advertise-servicebus"    help:"Override the host:port advertised as serviceBusEndpoint. Default: derived from the request's Host and --azure-arm-servicebus-port."`                                                                                                 //nolint:lll // config struct tags are intentionally verbose
	AdvertiseKeyVault     string `json:"advertiseKeyVault"     env:"AZURE_ARM_ADVERTISE_KEYVAULT_ENDPOINT"                                                  name:"advertise-keyvault"      help:"Override the host:port advertised in vaultUri (after the {vault}. prefix). Default: derived from the request's Host and --azure-arm-keyvault-port."`                                                                                 //nolint:lll // config struct tags are intentionally verbose
	AdvertiseCosmosDB     string `json:"advertiseCosmosDb"     env:"AZURE_ARM_ADVERTISE_COSMOSDB_ENDPOINT"                                                  name:"advertise-cosmosdb"      help:"Override the host:port advertised in documentEndpoint. Default: derived from the request's Host and the Cosmos DB listener's port."`                                                                                                 //nolint:lll // config struct tags are intentionally verbose
	AdvertiseEventHub     string `json:"advertiseEventHub"     env:"AZURE_ARM_ADVERTISE_EVENTHUB_ENDPOINT"                                                  name:"advertise-eventhub"      help:"Override the host:port advertised as the AMQP endpoint in Event Hubs connection strings. Default: derived from the request's Host and --azure-arm-eventhub-amqp-port."`                                                              //nolint:lll // config struct tags are intentionally verbose
	AdvertiseEventGrid    string `json:"advertiseEventGrid"    env:"AZURE_ARM_ADVERTISE_EVENTGRID_ENDPOINT"                                                 name:"advertise-eventgrid"     help:"Override the host:port advertised in Event Grid topic endpoints and namespace hostnames. Default: derived from the request's Host and --azure-arm-eventgrid-port."`                                                                  //nolint:lll // config struct tags are intentionally verbose
	AdvertiseAppConfig    string `json:"advertiseAppConfig"    env:"AZURE_ARM_ADVERTISE_APPCONFIG_ENDPOINT"                                                 name:"advertise-appconfig"     help:"Override the host:port advertised in configuration store endpoints (after the {store}. prefix). Default: derived from the request's Host and --azure-arm-appconfig-port."`                                                           //nolint:lll // config struct tags are intentionally verbose
	AdvertiseACR          string `json:"advertiseAcr"          env:"AZURE_ARM_ADVERTISE_ACR_ENDPOINT"                                                       name:"advertise-acr"           help:"Override the host:port advertised in container registry loginServer (after the {registry}.azurecr. prefix). Default: derived from the request's Host and --azure-arm-acr-port."`                                                     //nolint:lll // config struct tags are intentionally verbose
	ACIEngine             string `json:"aciEngine"             env:"AZURE_ARM_ACI_ENGINE"                    default:"auto"                                 name:"aci-engine"              help:"Container Instances engine: auto (use the container runtime) or none (groups fail to provision)."                                                                                                      enum:"auto,none"`             //nolint:lll // config struct tags are intentionally verbose
	AKSEngine             string `json:"aksEngine"             env:"AKS_ENGINE"                              default:"stub"                                 name:"aks-engine"              help:"Kubernetes Service engine: stub (ARM state only) or docker (a real single-node k3s cluster per managed cluster)."                                                                                      enum:"stub,docker"`           //nolint:lll // config struct tags are intentionally verbose
	DBEngine              string `json:"dbEngine"              env:"AZURE_ARM_DB_ENGINE"                     default:"stub"                                 name:"db-engine"               help:"PostgreSQL and MySQL flexible server engine: stub (ARM state only) or docker (a real container per server)."                                                                                           enum:"stub,docker"`           //nolint:lll // config struct tags are intentionally verbose
	Port                  int    `json:"port"                  env:"AZURE_ARM_PORT"                          default:"10006"                                name:"port"                    help:"Fixed TCP port for the dedicated ARM listener; startup fails if it's unavailable (no fallback pool)."`                                                                                                                               //nolint:lll // config struct tags are intentionally verbose
	StorageVHostPort      int    `json:"storageVhostPort"      env:"AZURE_ARM_STORAGE_VHOST_PORT"            default:"10010"                                name:"storage-vhost-port"      help:"The shared services/azurestoragevhost listener's port, used to build primaryEndpoints when --azure-arm-advertise-storage-vhost isn't set. Must match --azure-storage-vhost-port."`                                                   //nolint:lll // config struct tags are intentionally verbose
	ServiceBusPort        int    `json:"serviceBusPort"        env:"AZURE_ARM_SERVICEBUS_PORT"               default:"10003"                                name:"servicebus-port"         help:"The services/azureservicebus listener's port, used to build serviceBusEndpoint when --azure-arm-advertise-servicebus isn't set. Must match --azure-servicebus-port."`                                                                //nolint:lll // config struct tags are intentionally verbose
	KeyVaultPort          int    `json:"keyVaultPort"          env:"AZURE_ARM_KEYVAULT_PORT"                 default:"10004"                                name:"keyvault-port"           help:"The services/azurekeyvault listener's port, used to build vaultUri when --azure-arm-advertise-keyvault isn't set. Must match --azure-keyvault-port."`                                                                                //nolint:lll // config struct tags are intentionally verbose
	EventHubPort          int    `json:"eventHubPort"          env:"AZURE_ARM_EVENTHUB_PORT"                 default:"10005"                                name:"eventhub-port"           help:"The services/azureeventhubs HTTP listener's port, used for serviceBusEndpoint. Must match --azure-eventhubs-port."`                                                                                                                  //nolint:lll // config struct tags are intentionally verbose
	EventHubAMQPPort      int    `json:"eventHubAmqpPort"      env:"AZURE_ARM_EVENTHUB_AMQP_PORT"            default:"5673"                                 name:"eventhub-amqp-port"      help:"The services/azureeventhubs plain AMQP port, used to build connection strings when --azure-arm-advertise-eventhub isn't set. Must match --azure-eventhubs-amqp-port."`                                                               //nolint:lll // config struct tags are intentionally verbose
	EventGridPort         int    `json:"eventGridPort"         env:"AZURE_ARM_EVENTGRID_PORT"                default:"10007"                                name:"eventgrid-port"          help:"The services/azureeventgrid HTTPS listener's port, used to build topic endpoints when --azure-arm-advertise-eventgrid isn't set. Must match --azure-eventgrid-port."`                                                                //nolint:lll // config struct tags are intentionally verbose
	AppConfigPort         int    `json:"appConfigPort"         env:"AZURE_ARM_APPCONFIG_PORT"                default:"10008"                                name:"appconfig-port"          help:"The services/azureappconfig HTTPS listener's port, used to build store endpoints when --azure-arm-advertise-appconfig isn't set. Must match --azure-appconfig-port."`                                                                //nolint:lll // config struct tags are intentionally verbose
	ACRPort               int    `json:"acrPort"               env:"AZURE_ARM_ACR_PORT"                      default:"10011"                                name:"acr-port"                help:"The services/azurecontainerregistry listener's port, used to build loginServer when --azure-arm-advertise-acr isn't set. Must match --azure-acr-port."`                                                                              //nolint:lll // config struct tags are intentionally verbose
	GraphPort             int    `json:"graphPort"             env:"AZURE_ARM_GRAPH_PORT"                    default:"10014"                                name:"graph-port"              help:"The services/azuregraph listener's port, advertised as the environment's Microsoft Graph endpoint when --azure-arm-advertise-graph isn't set. Must match --azure-graph-port."`                                                       //nolint:lll // config struct tags are intentionally verbose
	AdvertiseGraph        string `json:"advertiseGraph"        env:"AZURE_ARM_ADVERTISE_GRAPH_ENDPOINT"                                                     name:"advertise-graph"         help:"Override the host:port advertised as the Microsoft Graph endpoint and resource ID. Default: derived from the request's Host and --azure-arm-graph-port."`                                                                            //nolint:lll // config struct tags are intentionally verbose
	ValidatePrincipals    bool   `json:"validatePrincipals"    env:"AZURE_ARM_VALIDATE_PRINCIPALS"           default:"false"                                name:"validate-principals"     help:"Reject role assignments whose principalId is neither a Microsoft Graph directory object nor a user-assigned managed identity (PrincipalNotFound). Needs the Graph service; system-assigned resource identities are not recognised."` //nolint:lll // config struct tags are intentionally verbose
	APIMAllowExternal     bool   `json:"apimAllowExternal"     env:"AZURE_ARM_APIM_ALLOW_EXTERNAL"           default:"false"                                name:"apim-allow-external"     help:"Let API Management gateway backends reach hosts outside loopback (opt-in; refused by default so the gateway cannot be used as an open proxy)."`                                                                                      //nolint:lll // config struct tags are intentionally verbose
	ValidateTokens        bool   `json:"validateTokens"        env:"AZURE_ARM_VALIDATE_TOKENS"               default:"false"                                name:"validate-tokens"         help:"Cryptographically validate ARM bearer tokens (opt-in); by default any token, or none, is accepted."`                                                                                                                                 //nolint:lll // config struct tags are intentionally verbose
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
		ACIEngine:        ACIEngineAuto,
		AKSEngine:        AKSEngineStub,
		DBEngine:         DBEngineStub,
		StorageVHostPort: DefaultStorageVHostPort,
		ServiceBusPort:   DefaultServiceBusPort,
		KeyVaultPort:     DefaultKeyVaultPort,
		EventHubPort:     DefaultEventHubPort,
		EventHubAMQPPort: DefaultEventHubAMQPPort,
		EventGridPort:    DefaultEventGridPort,
		AppConfigPort:    DefaultAppConfigPort,
		ACRPort:          DefaultContainerRegistryPort,
		GraphPort:        DefaultGraphPort,
		ValidateTokens:   false,
	}
}
