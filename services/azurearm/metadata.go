package azurearm

import (
	"net"
	"strings"
)

// EnvironmentDescriptor is the GET /metadata/endpoints response body: a
// SINGLE JSON object, not an array (see BuildMetadataEndpoints's doc comment
// for why this is non-obvious and easy to get backwards). Field set and
// semantics verified against hashicorp/go-azure-sdk's
// environments.FromEndpoint, which hard-fails on a document missing Name,
// ResourceManagerEndpoint, or ResourceIdentifiers.MicrosoftGraphResourceID
// (AZURE.md section 10.8) -- every one of those three, plus every other
// field FromEndpoint reads, is populated below.
type EnvironmentDescriptor struct {
	Suffixes                EnvironmentSuffixes    `json:"suffixes"`
	Gallery                 string                 `json:"gallery"`
	Media                   string                 `json:"media,omitempty"`
	Graph                   string                 `json:"graph"`
	GraphAudience           string                 `json:"graphAudience"`
	Name                    string                 `json:"name"`
	ResourceManager         string                 `json:"resourceManager"`
	ResourceManagerEndpoint string                 `json:"resourceManagerEndpoint"`
	ActiveDirectoryDataLake string                 `json:"activeDirectoryDataLake,omitempty"`
	SQLManagement           string                 `json:"sqlManagement,omitempty"`
	Batch                   string                 `json:"batch,omitempty"`
	Portal                  string                 `json:"portal"`
	ResourceIdentifiers     EnvironmentResourceIDs `json:"resourceIdentifiers"`
	Authentication          EnvironmentAuth        `json:"authentication"`
}

// EnvironmentAuth is EnvironmentDescriptor's "authentication" field.
type EnvironmentAuth struct {
	LoginEndpoint string   `json:"loginEndpoint"`
	Tenant        string   `json:"tenant"`
	Audiences     []string `json:"audiences"`
}

// EnvironmentSuffixes is EnvironmentDescriptor's "suffixes" field.
type EnvironmentSuffixes struct {
	Storage            string `json:"storage"`
	KeyVaultDNS        string `json:"keyVaultDns"`
	SQLServerHostname  string `json:"sqlServerHostname"`
	ACRLoginServer     string `json:"acrLoginServer"`
	AzureDatalakeStore string `json:"azureDataLakeStoreFileSystem,omitempty"`
}

// EnvironmentResourceIDs is EnvironmentDescriptor's "resourceIdentifiers"
// field -- FromEndpoint hard-fails without MicrosoftGraphResourceID.
type EnvironmentResourceIDs struct {
	MicrosoftGraphResourceID string `json:"microsoftGraphResourceId"`
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
			LoginEndpoint: baseURL + "/",
			Audiences:     []string{baseURL + "/", "https://management.core.windows.net/"},
			Tenant:        settings.TenantID,
		},
		Graph:                   baseURL + "/graph",
		GraphAudience:           baseURL + "/graph",
		Gallery:                 baseURL + "/gallery",
		ResourceManager:         baseURL + "/",
		ResourceManagerEndpoint: baseURL + "/",
		Suffixes: EnvironmentSuffixes{
			Storage:           "localhost",
			KeyVaultDNS:       ".vault." + hostnameOnly(baseURL),
			SQLServerHostname: ".database." + hostnameOnly(baseURL),
			ACRLoginServer:    ".azurecr." + hostnameOnly(baseURL),
		},
		ResourceIdentifiers: EnvironmentResourceIDs{
			MicrosoftGraphResourceID: baseURL + "/graph",
		},
	}
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
