package apprunner_test

import (
	"testing"

	apprunnersdk "github.com/aws/aws-sdk-go-v2/service/apprunner"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// apprunner client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := apprunner.NewInMemoryBackend("000000000000", "us-east-1")
	h := apprunner.NewHandler(backend)

	// Operations not yet implemented.
	notImplemented := []string{
		"AssociateCustomDomain",
		"CreateAutoScalingConfiguration",
		"CreateConnection",
		"CreateObservabilityConfiguration",
		"CreateVpcConnector",
		"CreateVpcIngressConnection",
		"DeleteAutoScalingConfiguration",
		"DeleteConnection",
		"DeleteObservabilityConfiguration",
		"DeleteVpcConnector",
		"DeleteVpcIngressConnection",
		"DescribeAutoScalingConfiguration",
		"DescribeCustomDomains",
		"DescribeObservabilityConfiguration",
		"DescribeVpcConnector",
		"DescribeVpcIngressConnection",
		"DisassociateCustomDomain",
		"ListAutoScalingConfigurations",
		"ListConnections",
		"ListObservabilityConfigurations",
		"ListServicesForAutoScalingConfiguration",
		"ListVpcConnectors",
		"ListVpcIngressConnections",
		"UpdateDefaultAutoScalingConfiguration",
		"UpdateVpcIngressConnection",
	}

	sdkcheck.CheckCompleteness(t, &apprunnersdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
