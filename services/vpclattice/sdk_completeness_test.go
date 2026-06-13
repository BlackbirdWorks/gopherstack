package vpclattice_test

import (
	"testing"

	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// vpclattice client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	h := vpclattice.NewHandler(backend)

	notImplemented := []string{
		// Resource Gateway and Configuration (newer feature set)
		"CreateResourceConfiguration",
		"CreateResourceGateway",
		"DeleteResourceConfiguration",
		"DeleteResourceEndpointAssociation",
		"DeleteResourceGateway",
		"GetResourceConfiguration",
		"GetResourceGateway",
		"ListResourceConfigurations",
		"ListResourceEndpointAssociations",
		"ListResourceGateways",
		"UpdateResourceConfiguration",
		"UpdateResourceGateway",
		// ServiceNetworkResourceAssociation (resource gateway feature)
		"CreateServiceNetworkResourceAssociation",
		"DeleteServiceNetworkResourceAssociation",
		"GetServiceNetworkResourceAssociation",
		"ListServiceNetworkResourceAssociations",
		// VPC endpoint associations
		"ListServiceNetworkVpcEndpointAssociations",
		// Domain verification
		"DeleteDomainVerification",
		"GetDomainVerification",
		"ListDomainVerifications",
		"StartDomainVerification",
	}

	sdkcheck.CheckCompleteness(t, &vpclatticesdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
