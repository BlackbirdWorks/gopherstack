package azurearm

import (
	"net"
	"strconv"
	"strings"
)

// EnvironmentDescriptor is the GET /metadata/endpoints response body: a
// SINGLE JSON object, not an array (see BuildMetadataEndpoints's doc comment
// for why this is non-obvious and easy to get backwards). Field set and
// semantics verified against hashicorp/go-azure-sdk's
// environments.FromEndpoint, which hard-fails on a document missing Name,
// ResourceManagerEndpoint, or MicrosoftGraphResourceID (AZURE.md section
// 10.8) -- every one of those three, plus every other field FromEndpoint
// reads, is populated below.
//
// MicrosoftGraphResourceID is a TOP-LEVEL field ("microsoftGraphResourceId"),
// not nested under a "resourceIdentifiers" object -- verified against
// go-azure-sdk's sdk/internal/metadata/client.go's unexported
// metaDataResponse wire struct, which has no ResourceIdentifiers field at
// all. Nesting it silently zero-values the field on unmarshal (FromEndpoint
// then fails with "no `microsoftGraphResourceId` was returned").
type EnvironmentDescriptor struct {
	Suffixes                 EnvironmentSuffixes `json:"suffixes"`
	Gallery                  string              `json:"gallery"`
	Media                    string              `json:"media,omitempty"`
	Graph                    string              `json:"graph"`
	GraphAudience            string              `json:"graphAudience"`
	Name                     string              `json:"name"`
	ResourceManager          string              `json:"resourceManager"`
	ResourceManagerEndpoint  string              `json:"resourceManagerEndpoint"`
	ActiveDirectoryDataLake  string              `json:"activeDirectoryDataLake,omitempty"`
	SQLManagement            string              `json:"sqlManagement,omitempty"`
	Batch                    string              `json:"batch,omitempty"`
	Portal                   string              `json:"portal"`
	MicrosoftGraphResourceID string              `json:"microsoftGraphResourceId"`
	Authentication           EnvironmentAuth     `json:"authentication"`
}

// EnvironmentAuth is EnvironmentDescriptor's "authentication" field.
//
// Tenant must be the literal string "common", and IdentityProvider must be
// "AAD" -- not gopherstack's fixed tenant GUID. hashicorp/go-azure-sdk's
// Environment.IsAzureStack() (sdk/environments/azure_stack.go) treats any
// other combination as an Azure Stack environment, which
// terraform-provider-azurerm explicitly refuses to run against
// (internal/clients/builder.go: "does not support Azure Stack"). Real
// Azure's own AzurePublic() environment (sdk/environments/azure_public.go)
// hardcodes exactly these two values even though it obviously isn't
// single-tenant -- this is a control field for auth flow selection, not a
// place to plug in a real tenant ID.
type EnvironmentAuth struct {
	LoginEndpoint    string   `json:"loginEndpoint"`
	IdentityProvider string   `json:"identityProvider"`
	Tenant           string   `json:"tenant"`
	Audiences        []string `json:"audiences"`
}

// EnvironmentSuffixes is EnvironmentDescriptor's "suffixes" field.
type EnvironmentSuffixes struct {
	Storage            string `json:"storage"`
	KeyVaultDNS        string `json:"keyVaultDns"`
	SQLServerHostname  string `json:"sqlServerHostname"`
	ACRLoginServer     string `json:"acrLoginServer"`
	AzureDatalakeStore string `json:"azureDataLakeStoreFileSystem,omitempty"`
}

// BuildMetadataEndpoints builds the GET /metadata/endpoints response body
// for settings.Environment, with every URL pointing back at baseURL (the ARM
// listener's own scheme://host:port, e.g. "https://host:10006").
//
// Returns a SINGLE EnvironmentDescriptor, not a slice -- deliberately, and
// non-obviously so. Real Azure's public /metadata/endpoints does return a
// JSON array (the multi-cloud discovery list: public/US Gov/China/etc), and
// that public shape is what most docs and examples show. But
// terraform-provider-azurerm's metadata_host custom-environment path never
// hits that public endpoint or its array-shaped client: it goes through
// hashicorp/go-azure-sdk's environments.FromEndpoint ->
// sdk/internal/metadata/client.go's GetMetaData, which does
// `json.Unmarshal(respBody, &metadata)` into a single `*metaDataResponse`
// struct. An array response there fails with "json: cannot unmarshal array
// into Go value of type metadata.metaDataResponse" -- a real bug M7 shipped
// with, undetected because no `terraform apply` had ever reached this
// endpoint until M8's CI-discovery-glob fix let the Terraform suite actually
// run (AZURE.md section 10.8 records this). Do not "fix" this back to an
// array without re-reading that section first.
func BuildMetadataEndpoints(baseURL string, settings Settings) EnvironmentDescriptor {
	return EnvironmentDescriptor{
		Name:   settings.Environment,
		Portal: baseURL + "/portal",
		Authentication: EnvironmentAuth{
			LoginEndpoint:    baseURL + "/",
			Audiences:        []string{baseURL + "/", "https://management.core.windows.net/"},
			IdentityProvider: "AAD",
			Tenant:           "common",
		},
		Graph:                   baseURL + "/graph",
		GraphAudience:           baseURL + "/graph",
		Gallery:                 baseURL + "/gallery",
		ResourceManager:         baseURL + "/",
		ResourceManagerEndpoint: baseURL + "/",
		Suffixes: EnvironmentSuffixes{
			Storage:           storageVHostHostAndPort(baseURL, settings),
			KeyVaultDNS:       ".vault." + hostnameOnly(baseURL),
			SQLServerHostname: ".database." + hostnameOnly(baseURL),
			ACRLoginServer:    ".azurecr." + hostnameOnly(baseURL),
		},
		MicrosoftGraphResourceID: baseURL + "/graph",
	}
}

// storageVHostHostAndPort returns the "host:port" the shared
// services/azurestoragevhost listener is reachable on -- exactly the
// domainSuffix jackofallops/giovanni's ParseAccountID needs to successfully
// strip "{account}.{blob,queue,table}." and parse the remaining label as
// the account name (see services/azurestoragevhost's package doc comment
// and AZURE.md section 10.8). settings.AdvertiseStorageVHost overrides this
// the same way AZURE_ARM_ADVERTISE_* used to override the old per-service
// endpoints (needed whenever the vhost listener's externally-reachable port
// differs from its configured one, e.g. a published Docker port); absent an
// override, it's derived from this ARM request's own Host (so it tracks
// whatever host the client actually dialed, mirroring baseURLFor) and the
// configured vhost port.
func storageVHostHostAndPort(baseURL string, settings Settings) string {
	if settings.AdvertiseStorageVHost != "" {
		return settings.AdvertiseStorageVHost
	}

	port := settings.StorageVHostPort
	if port == 0 {
		port = DefaultStorageVHostPort
	}

	return net.JoinHostPort(hostnameOnly(baseURL), strconv.Itoa(port))
}

// hostnameOnly strips scheme and port from baseURL, returning just the
// hostname, for building plausible-looking DNS suffixes.
func hostnameOnly(baseURL string) string {
	s := baseURL

	for _, prefix := range []string{"https://", "http://"} {
		if len(s) > len(prefix) && s[:len(prefix)] == prefix {
			s = s[len(prefix):]

			break
		}
	}

	if slash := strings.IndexByte(s, '/'); slash >= 0 {
		s = s[:slash]
	}

	// net.SplitHostPort handles both "host:port" and the bracketed IPv6
	// form "[::1]:port" correctly; a naive scan for the first ':' would
	// instead return "[" for an IPv6 host (CodeRabbit-flagged bug).
	if host, _, err := net.SplitHostPort(s); err == nil {
		return host
	}

	return s
}
