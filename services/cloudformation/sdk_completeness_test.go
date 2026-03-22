package cloudformation_test

import (
	"testing"

	cloudformationsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// cloudformation client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := cloudformation.NewInMemoryBackend()
	h := cloudformation.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &cloudformationsdk.Client{}, h.GetSupportedOperations(), []string{
		"ActivateOrganizationsAccess",
		"ActivateType",
		"BatchDescribeTypeConfigurations",
		"CreateGeneratedTemplate",
		"CreateStackInstances",
		"CreateStackRefactor",
		"CreateStackSet",
		"DeactivateOrganizationsAccess",
		"DeactivateType",
		"DeleteGeneratedTemplate",
		"DeleteStackInstances",
		"DeleteStackSet",
		"DeregisterType",
		"DescribeChangeSetHooks",
		"DescribeEvents",
		"DescribeGeneratedTemplate",
		"DescribeOrganizationsAccess",
		"DescribePublisher",
		"DescribeResourceScan",
		"DescribeStackInstance",
		"DescribeStackRefactor",
		"DescribeStackSet",
		"DescribeStackSetOperation",
		"DescribeTypeRegistration",
		"DetectStackSetDrift",
		"ExecuteStackRefactor",
		"GetGeneratedTemplate",
		"GetHookResult",
		"ImportStacksToStackSet",
		"ListGeneratedTemplates",
		"ListHookResults",
		"ListResourceScanRelatedResources",
		"ListResourceScanResources",
		"ListResourceScans",
		"ListStackInstanceResourceDrifts",
		"ListStackInstances",
		"ListStackRefactorActions",
		"ListStackRefactors",
		"ListStackSetAutoDeploymentTargets",
		"ListStackSetOperationResults",
		"ListStackSetOperations",
		"ListStackSets",
		"ListTypeRegistrations",
		"ListTypeVersions",
		"ListTypes",
		"PublishType",
		"RecordHandlerProgress",
		"RegisterPublisher",
		"RegisterType",
		"RollbackStack",
		"SetTypeConfiguration",
		"SetTypeDefaultVersion",
		"SignalResource",
		"StartResourceScan",
		"StopStackSetOperation",
		"TestType",
		"UpdateGeneratedTemplate",
		"UpdateStackInstances",
		"UpdateStackSet",
		"UpdateTerminationProtection",
		"ValidateTemplate",
	})
}
