package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVpcEncryptionControl_HTTP_Lifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	createVpcResp, err := dispatchHandler(h, url.Values{
		"Action":    []string{"CreateVpc"},
		"CidrBlock": []string{"10.91.0.0/16"},
	})
	require.NoError(t, err)
	vpcID := accuracyExtractXMLValue(createVpcResp, "vpcId")
	require.NotEmpty(t, vpcID)

	createResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"CreateVpcEncryptionControl"},
		"VpcId":  []string{vpcID},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<CreateVpcEncryptionControlResponse")
	assert.NotContains(t, createResp, "StubResponse")
	assert.Contains(t, createResp, "<mode>monitor</mode>")
	controlID := accuracyExtractXMLValue(createResp, "vpcEncryptionControlId")
	require.NotEmpty(t, controlID)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                   []string{"DescribeVpcEncryptionControls"},
		"VpcEncryptionControlId.1": []string{controlID},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, controlID)

	modifyResp, err := dispatchHandler(h, url.Values{
		"Action":                   []string{"ModifyVpcEncryptionControl"},
		"VpcEncryptionControlId":   []string{controlID},
		"Mode":                     []string{"enforce"},
		"InternetGatewayExclusion": []string{"enable"},
	})
	require.NoError(t, err)
	assert.Contains(t, modifyResp, "<mode>enforce</mode>")
	assert.Contains(t, modifyResp, "<internetGateway><state>enabled</state></internetGateway>")

	blockingResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetVpcResourcesBlockingEncryptionEnforcement"},
		"VpcId":  []string{vpcID},
	})
	require.NoError(t, err)
	assert.Contains(t, blockingResp, "<GetVpcResourcesBlockingEncryptionEnforcementResponse")

	deleteResp, err := dispatchHandler(h, url.Values{
		"Action":                 []string{"DeleteVpcEncryptionControl"},
		"VpcEncryptionControlId": []string{controlID},
	})
	require.NoError(t, err)
	assert.Contains(t, deleteResp, "<DeleteVpcEncryptionControlResponse")

	// Duplicate create on the same VPC should fail, not silently succeed as a stub would.
	_, err = dispatchHandler(h, url.Values{
		"Action": []string{"CreateVpcEncryptionControl"},
		"VpcId":  []string{vpcID},
	})
	require.NoError(t, err) // control was deleted above, so a fresh create succeeds again

	_, err = dispatchHandler(h, url.Values{
		"Action": []string{"CreateVpcEncryptionControl"},
		"VpcId":  []string{vpcID},
	})
	require.Error(t, err)
}
