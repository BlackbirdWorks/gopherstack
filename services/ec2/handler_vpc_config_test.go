package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- VPC ClassicLink HTTP dispatch integration tests ----

func TestVpcConfig_HTTP_ClassicLinkLifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	createVpcResp, err := dispatchHandler(h, url.Values{
		"Action":    []string{"CreateVpc"},
		"CidrBlock": []string{"10.80.0.0/16"},
	})
	require.NoError(t, err)
	vpcID := accuracyExtractXMLValue(createVpcResp, "vpcId")
	require.NotEmpty(t, vpcID)

	runResp, err := dispatchHandler(h, url.Values{
		"Action":       []string{"RunInstances"},
		"ImageId":      []string{"ami-123"},
		"InstanceType": []string{"t3.micro"},
		"MinCount":     []string{"1"},
		"MaxCount":     []string{"1"},
	})
	require.NoError(t, err)
	instanceID := accuracyExtractXMLValue(runResp, "instanceId")
	require.NotEmpty(t, instanceID)

	// Attach before enabling ClassicLink must fail, not silently succeed as a stub would.
	_, err = dispatchHandler(h, url.Values{
		"Action":     []string{"AttachClassicLinkVpc"},
		"InstanceId": []string{instanceID},
		"VpcId":      []string{vpcID},
	})
	require.Error(t, err)

	enableResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"EnableVpcClassicLink"},
		"VpcId":  []string{vpcID},
	})
	require.NoError(t, err)
	assert.Contains(t, enableResp, "<EnableVpcClassicLinkResponse")
	assert.Contains(t, enableResp, "<return>true</return>")

	describeLinkResp, err := dispatchHandler(h, url.Values{
		"Action":  []string{"DescribeVpcClassicLink"},
		"VpcId.1": []string{vpcID},
	})
	require.NoError(t, err)
	assert.Contains(t, describeLinkResp, "<classicLinkEnabled>true</classicLinkEnabled>")

	createSGResp, err := dispatchHandler(h, url.Values{
		"Action":      []string{"CreateSecurityGroup"},
		"GroupName":   []string{"classic-link-sg"},
		"Description": []string{"classic link test sg"},
		"VpcId":       []string{vpcID},
	})
	require.NoError(t, err)
	groupID := accuracyExtractXMLValue(createSGResp, "groupId")
	require.NotEmpty(t, groupID)

	attachResp, err := dispatchHandler(h, url.Values{
		"Action":            []string{"AttachClassicLinkVpc"},
		"InstanceId":        []string{instanceID},
		"VpcId":             []string{vpcID},
		"SecurityGroupId.1": []string{groupID},
	})
	require.NoError(t, err)
	assert.Contains(t, attachResp, "<AttachClassicLinkVpcResponse")
	assert.NotContains(t, attachResp, "StubResponse")

	describeInstResp, err := dispatchHandler(h, url.Values{"Action": []string{"DescribeClassicLinkInstances"}})
	require.NoError(t, err)
	assert.Contains(t, describeInstResp, instanceID)
	assert.Contains(t, describeInstResp, groupID)

	// Disabling while linked must fail (DependencyViolation), not silently succeed.
	_, err = dispatchHandler(h, url.Values{
		"Action": []string{"DisableVpcClassicLink"},
		"VpcId":  []string{vpcID},
	})
	require.Error(t, err)

	detachResp, err := dispatchHandler(h, url.Values{
		"Action":     []string{"DetachClassicLinkVpc"},
		"InstanceId": []string{instanceID},
		"VpcId":      []string{vpcID},
	})
	require.NoError(t, err)
	assert.Contains(t, detachResp, "<DetachClassicLinkVpcResponse")

	disableResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"DisableVpcClassicLink"},
		"VpcId":  []string{vpcID},
	})
	require.NoError(t, err)
	assert.Contains(t, disableResp, "<DisableVpcClassicLinkResponse")
}

func TestVpcConfig_HTTP_ClassicLinkDNSSupport(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	createVpcResp, err := dispatchHandler(h, url.Values{
		"Action":    []string{"CreateVpc"},
		"CidrBlock": []string{"10.81.0.0/16"},
	})
	require.NoError(t, err)
	vpcID := accuracyExtractXMLValue(createVpcResp, "vpcId")

	enableResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"EnableVpcClassicLinkDnsSupport"},
		"VpcId":  []string{vpcID},
	})
	require.NoError(t, err)
	assert.Contains(t, enableResp, "<EnableVpcClassicLinkDnsSupportResponse")

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":   []string{"DescribeVpcClassicLinkDnsSupport"},
		"VpcIds.1": []string{vpcID},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<classicLinkDnsSupported>true</classicLinkDnsSupported>")

	disableResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"DisableVpcClassicLinkDnsSupport"},
		"VpcId":  []string{vpcID},
	})
	require.NoError(t, err)
	assert.Contains(t, disableResp, "<DisableVpcClassicLinkDnsSupportResponse")
}

// ---- VPC Block Public Access HTTP dispatch integration tests ----

func TestVpcConfig_HTTP_BlockPublicAccessOptions(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	describeResp, err := dispatchHandler(h, url.Values{"Action": []string{"DescribeVpcBlockPublicAccessOptions"}})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<internetGatewayBlockMode>off</internetGatewayBlockMode>")

	modifyResp, err := dispatchHandler(h, url.Values{
		"Action":                   []string{"ModifyVpcBlockPublicAccessOptions"},
		"InternetGatewayBlockMode": []string{"block-ingress"},
	})
	require.NoError(t, err)
	assert.Contains(t, modifyResp, "<internetGatewayBlockMode>block-ingress</internetGatewayBlockMode>")

	// Invalid mode should error, not silently succeed as a stub would.
	_, err = dispatchHandler(h, url.Values{
		"Action":                   []string{"ModifyVpcBlockPublicAccessOptions"},
		"InternetGatewayBlockMode": []string{"nonsense"},
	})
	require.Error(t, err)
}

func TestVpcConfig_HTTP_BlockPublicAccessExclusionLifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	createVpcResp, err := dispatchHandler(h, url.Values{
		"Action":    []string{"CreateVpc"},
		"CidrBlock": []string{"10.82.0.0/16"},
	})
	require.NoError(t, err)
	vpcID := accuracyExtractXMLValue(createVpcResp, "vpcId")

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                       []string{"CreateVpcBlockPublicAccessExclusion"},
		"VpcId":                        []string{vpcID},
		"InternetGatewayExclusionMode": []string{"allow-egress"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<CreateVpcBlockPublicAccessExclusionResponse")
	exclusionID := accuracyExtractXMLValue(createResp, "exclusionId")
	require.NotEmpty(t, exclusionID)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"DescribeVpcBlockPublicAccessExclusions"},
		"ExclusionId.1": []string{exclusionID},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, exclusionID)

	modifyResp, err := dispatchHandler(h, url.Values{
		"Action":                       []string{"ModifyVpcBlockPublicAccessExclusion"},
		"ExclusionId":                  []string{exclusionID},
		"InternetGatewayExclusionMode": []string{"allow-bidirectional"},
	})
	require.NoError(t, err)
	assert.Contains(t, modifyResp, "<internetGatewayExclusionMode>allow-bidirectional</internetGatewayExclusionMode>")

	deleteResp, err := dispatchHandler(h, url.Values{
		"Action":      []string{"DeleteVpcBlockPublicAccessExclusion"},
		"ExclusionId": []string{exclusionID},
	})
	require.NoError(t, err)
	assert.Contains(t, deleteResp, "<DeleteVpcBlockPublicAccessExclusionResponse")

	// Missing VpcId/SubnetId should error, not silently succeed as a stub would.
	_, err = dispatchHandler(h, url.Values{
		"Action":                       []string{"CreateVpcBlockPublicAccessExclusion"},
		"InternetGatewayExclusionMode": []string{"allow-egress"},
	})
	require.Error(t, err)
}

// TestVpcConfig_HTTP_BlockPublicAccessExclusionTagDualWritePathVisibility
// proves that vpc_config.go's VpcBlockPublicAccessExclusion consolidated onto
// the shared tag store: a tag supplied at create time (TagSpecification) and
// a tag added afterwards via CreateTags are BOTH visible through
// DescribeVpcBlockPublicAccessExclusions AND through the generic DescribeTags
// call. Before the fix, the exclusion carried its own embedded Tags field
// populated only at create time, invisible to a post-creation CreateTags call.
func TestVpcConfig_HTTP_BlockPublicAccessExclusionTagDualWritePathVisibility(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createVpcResp, err := dispatchHandler(h, url.Values{
		"Action":    []string{"CreateVpc"},
		"CidrBlock": []string{"10.83.0.0/16"},
	})
	require.NoError(t, err)
	vpcID := accuracyExtractXMLValue(createVpcResp, "vpcId")

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateVpcBlockPublicAccessExclusion"},
		"VpcId":                           []string{vpcID},
		"InternetGatewayExclusionMode":    []string{"allow-egress"},
		"TagSpecification.1.ResourceType": []string{"vpc-block-public-access-exclusion"},
		"TagSpecification.1.Tag.1.Key":    []string{"CreateTime"},
		"TagSpecification.1.Tag.1.Value":  []string{"yes"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "CreateTime")
	exclusionID := accuracyExtractXMLValue(createResp, "exclusionId")
	require.NotEmpty(t, exclusionID)

	_, err = dispatchHandler(h, url.Values{
		"Action":       []string{"CreateTags"},
		"ResourceId.1": []string{exclusionID},
		"Tag.1.Key":    []string{"AddedLater"},
		"Tag.1.Value":  []string{"yes"},
	})
	require.NoError(t, err)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"DescribeVpcBlockPublicAccessExclusions"},
		"ExclusionId.1": []string{exclusionID},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "CreateTime")
	assert.Contains(t, describeResp, "AddedLater")

	tagsResp, err := dispatchHandler(h, url.Values{
		"Action":           []string{"DescribeTags"},
		"Filter.1.Name":    []string{"resource-id"},
		"Filter.1.Value.1": []string{exclusionID},
	})
	require.NoError(t, err)
	assert.Contains(t, tagsResp, "CreateTime")
	assert.Contains(t, tagsResp, "AddedLater")
}

// ---- VPC Endpoint Service private DNS verification ----

//nolint:paralleltest // existing issue.
func TestVpcConfig_HTTP_StartVpcEndpointServicePrivateDnsVerification(t *testing.T) {
	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"CreateVpcEndpointServiceConfiguration"},
		"NetworkLoadBalancerArn.1": []string{
			"arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/test/abc",
		},
	})
	require.NoError(t, err)
	serviceID := accuracyExtractXMLValue(createResp, "serviceId")
	require.NotEmpty(t, serviceID)

	startResp, err := dispatchHandler(h, url.Values{
		"Action":    []string{"StartVpcEndpointServicePrivateDnsVerification"},
		"ServiceId": []string{serviceID},
	})
	require.NoError(t, err)
	assert.Contains(t, startResp, "<StartVpcEndpointServicePrivateDnsVerificationResponse")
	assert.Contains(t, startResp, "<return>true</return>")

	_, err = dispatchHandler(h, url.Values{
		"Action":    []string{"StartVpcEndpointServicePrivateDnsVerification"},
		"ServiceId": []string{"vpce-svc-doesnotexist"},
	})
	require.Error(t, err)
}
