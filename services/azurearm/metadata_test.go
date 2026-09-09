package azurearm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

// TestBuildMetadataEndpoints asserts the presence of every field
// hashicorp/go-azure-sdk's environments.FromEndpoint requires or reads --
// AZURE.md section 10.8 is explicit that missing name, resourceManagerEndpoint,
// or resourceIdentifiers.microsoftGraphResourceId is a hard failure in the
// real provider, and that graph/graphAudience/suffixes/authentication are
// all part of the schema FromEndpoint parses.
//
// It also asserts the return type is a single EnvironmentDescriptor, not a
// slice: FromEndpoint's underlying client (go-azure-sdk's GetMetaData)
// unmarshals the response body into a single JSON object and hard-fails on
// an array (AZURE.md section 10.8) -- a real M7 bug this test would not have
// caught if it still indexed into docs[0] the way it originally did.
func TestBuildMetadataEndpoints(t *testing.T) {
	t.Parallel()

	settings := azurearm.DefaultSettings()
	doc := azurearm.BuildMetadataEndpoints("https://host:10006", settings)

	// Every one of these must be non-empty: FromEndpoint hard-fails without them.
	assert.Equal(t, settings.Environment, doc.Name, "name must equal the configured environment")
	assert.NotEmpty(t, doc.ResourceManager)
	assert.NotEmpty(t, doc.ResourceManagerEndpoint)
	assert.NotEmpty(t, doc.ResourceIdentifiers.MicrosoftGraphResourceID)

	assert.NotEmpty(t, doc.Authentication.LoginEndpoint)
	assert.NotEmpty(t, doc.Authentication.Audiences)
	assert.Equal(t, settings.TenantID, doc.Authentication.Tenant)

	assert.NotEmpty(t, doc.Graph)
	assert.NotEmpty(t, doc.GraphAudience)
	assert.NotEmpty(t, doc.Gallery)
	assert.NotEmpty(t, doc.Portal)

	assert.NotEmpty(t, doc.Suffixes.Storage)
	assert.NotEmpty(t, doc.Suffixes.KeyVaultDNS)
	assert.NotEmpty(t, doc.Suffixes.SQLServerHostname)
	assert.NotEmpty(t, doc.Suffixes.ACRLoginServer)

	// Every URL should point back at gopherstack's own base URL.
	assert.Contains(t, doc.ResourceManager, "host:10006")
	assert.Contains(t, doc.Graph, "host:10006")
	assert.Contains(t, doc.Portal, "host:10006")
}

// TestBuildMetadataEndpoints_IPv6Host proves hostnameOnly correctly strips
// an IPv6 host's brackets (baseURLFor can produce "https://[::1]:10006"),
// rather than a naive first-colon scan returning just "[" (CodeRabbit-flagged).
func TestBuildMetadataEndpoints_IPv6Host(t *testing.T) {
	t.Parallel()

	settings := azurearm.DefaultSettings()
	doc := azurearm.BuildMetadataEndpoints("https://[::1]:10006", settings)

	assert.NotContains(t, doc.Suffixes.KeyVaultDNS, "[")
	assert.NotContains(t, doc.Suffixes.SQLServerHostname, "[")
	assert.NotContains(t, doc.Suffixes.ACRLoginServer, "[")
	assert.Contains(t, doc.Suffixes.KeyVaultDNS, "::1")
}

func TestBuildMetadataEndpoints_CustomEnvironmentName(t *testing.T) {
	t.Parallel()

	settings := azurearm.DefaultSettings()
	settings.Environment = "my-custom-cloud"

	doc := azurearm.BuildMetadataEndpoints("https://host:10006", settings)

	assert.Equal(t, "my-custom-cloud", doc.Name)
}
