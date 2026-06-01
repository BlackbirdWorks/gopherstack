package opsworks_test

import (
	"testing"

	opswerkssdk "github.com/aws/aws-sdk-go-v2/service/opsworks"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// opsworks client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := opsworks.NewInMemoryBackend("000000000000", "us-east-1")
	h := opsworks.NewHandler(backend)

	// Operations not yet implemented — volume, elastic IP, ECS, RDS, load balancer,
	// user profile, permission, auto-scaling, and stack lifecycle management.
	notImplemented := []string{
		"AssignInstance",
		"AssignVolume",
		"AssociateElasticIp",
		"AttachElasticLoadBalancer",
		"CloneStack",
		"CreateUserProfile",
		"DeleteUserProfile",
		"DeregisterEcsCluster",
		"DeregisterElasticIp",
		"DeregisterInstance",
		"DeregisterRdsDbInstance",
		"DeregisterVolume",
		"DescribeAgentVersions",
		"DescribeEcsClusters",
		"DescribeElasticIps",
		"DescribeElasticLoadBalancers",
		"DescribeLoadBasedAutoScaling",
		"DescribeMyUserProfile",
		"DescribeOperatingSystems",
		"DescribePermissions",
		"DescribeRaidArrays",
		"DescribeRdsDbInstances",
		"DescribeServiceErrors",
		"DescribeStackProvisioningParameters",
		"DescribeStackSummary",
		"DescribeTimeBasedAutoScaling",
		"DescribeUserProfiles",
		"DescribeVolumes",
		"DetachElasticLoadBalancer",
		"DisassociateElasticIp",
		"GetHostnameSuggestion",
		"GrantAccess",
		"RegisterEcsCluster",
		"RegisterElasticIp",
		"RegisterInstance",
		"RegisterRdsDbInstance",
		"RegisterVolume",
		"SetLoadBasedAutoScaling",
		"SetPermission",
		"SetTimeBasedAutoScaling",
		"StartStack",
		"StopStack",
		"UnassignInstance",
		"UnassignVolume",
		"UpdateElasticIp",
		"UpdateMyUserProfile",
		"UpdateRdsDbInstance",
		"UpdateUserProfile",
		"UpdateVolume",
	}

	sdkcheck.CheckCompleteness(t, &opswerkssdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
