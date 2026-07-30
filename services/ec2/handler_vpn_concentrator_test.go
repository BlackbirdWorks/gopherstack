package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVpnConcentrator_HTTP_Lifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{"Action": []string{"CreateVpnConcentrator"}})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<CreateVpnConcentratorResponse")
	assert.NotContains(t, createResp, "StubResponse")
	assert.Contains(t, createResp, "<type>ipsec.1</type>")
	concentratorID := accuracyExtractXMLValue(createResp, "vpnConcentratorId")
	require.NotEmpty(t, concentratorID)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":              []string{"DescribeVpnConcentrators"},
		"VpnConcentratorId.1": []string{concentratorID},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, concentratorID)

	deleteResp, err := dispatchHandler(h, url.Values{
		"Action":            []string{"DeleteVpnConcentrator"},
		"VpnConcentratorId": []string{concentratorID},
	})
	require.NoError(t, err)
	assert.Contains(t, deleteResp, "<DeleteVpnConcentratorResponse")
	assert.Contains(t, deleteResp, "<return>true</return>")

	_, err = dispatchHandler(h, url.Values{
		"Action":            []string{"DeleteVpnConcentrator"},
		"VpnConcentratorId": []string{concentratorID},
	})
	require.Error(t, err)
}

func TestVpnTunnelExtras_HTTP(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	cgwResp, err := dispatchHandler(h, url.Values{
		"Action":    []string{"CreateCustomerGateway"},
		"Type":      []string{"ipsec.1"},
		"IpAddress": []string{"203.0.113.5"},
		"BgpAsn":    []string{"65000"},
	})
	require.NoError(t, err)
	cgwID := accuracyExtractXMLValue(cgwResp, "customerGatewayId")
	require.NotEmpty(t, cgwID)

	vgwResp, err := dispatchHandler(h, url.Values{
		"Action": []string{"CreateVpnGateway"},
		"Type":   []string{"ipsec.1"},
	})
	require.NoError(t, err)
	vgwID := accuracyExtractXMLValue(vgwResp, "vpnGatewayId")
	require.NotEmpty(t, vgwID)

	connResp, err := dispatchHandler(h, url.Values{
		"Action":            []string{"CreateVpnConnection"},
		"Type":              []string{"ipsec.1"},
		"CustomerGatewayId": []string{cgwID},
		"VpnGatewayId":      []string{vgwID},
	})
	require.NoError(t, err)
	connID := accuracyExtractXMLValue(connResp, "vpnConnectionId")
	require.NotEmpty(t, connID)
	outsideIP := accuracyExtractXMLValue(connResp, "outsideIpAddress")
	require.NotEmpty(t, outsideIP)

	statusResp, err := dispatchHandler(h, url.Values{
		"Action":                    []string{"GetActiveVpnTunnelStatus"},
		"VpnConnectionId":           []string{connID},
		"VpnTunnelOutsideIpAddress": []string{outsideIP},
	})
	require.NoError(t, err)
	assert.Contains(t, statusResp, "<GetActiveVpnTunnelStatusResponse")
	assert.Contains(t, statusResp, "<provisioningStatus>available</provisioningStatus>")

	replaceResp, err := dispatchHandler(h, url.Values{
		"Action":                    []string{"ReplaceVpnTunnel"},
		"VpnConnectionId":           []string{connID},
		"VpnTunnelOutsideIpAddress": []string{outsideIP},
	})
	require.NoError(t, err)
	assert.Contains(t, replaceResp, "<ReplaceVpnTunnelResponse")
	assert.Contains(t, replaceResp, "<return>true</return>")

	_, err = dispatchHandler(h, url.Values{
		"Action":                    []string{"GetActiveVpnTunnelStatus"},
		"VpnConnectionId":           []string{connID},
		"VpnTunnelOutsideIpAddress": []string{"198.51.100.200"},
	})
	require.Error(t, err)
}

// TestVpnConcentrator_TagDualWritePathVisibility proves that
// vpn_concentrator.go's VpnConcentrator consolidated onto the shared tag
// store: a tag supplied at create time (TagSpecification) and a tag added
// afterwards via CreateTags are BOTH visible through DescribeVpnConcentrators
// AND through the generic DescribeTags call. Before the fix, VpnConcentrator
// carried its own embedded Tags field populated only at create time, so a
// post-creation CreateTags call was invisible to DescribeVpnConcentrators.
func TestVpnConcentrator_TagDualWritePathVisibility(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateVpnConcentrator"},
		"TagSpecification.1.ResourceType": []string{"vpn-concentrator"},
		"TagSpecification.1.Tag.1.Key":    []string{"CreateTime"},
		"TagSpecification.1.Tag.1.Value":  []string{"yes"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "CreateTime")
	concentratorID := accuracyExtractXMLValue(createResp, "vpnConcentratorId")
	require.NotEmpty(t, concentratorID)

	_, err = dispatchHandler(h, url.Values{
		"Action":       []string{"CreateTags"},
		"ResourceId.1": []string{concentratorID},
		"Tag.1.Key":    []string{"AddedLater"},
		"Tag.1.Value":  []string{"yes"},
	})
	require.NoError(t, err)

	descResp, err := dispatchHandler(h, url.Values{"Action": []string{"DescribeVpnConcentrators"}})
	require.NoError(t, err)
	assert.Contains(t, descResp, "CreateTime")
	assert.Contains(t, descResp, "AddedLater")

	tagsResp, err := dispatchHandler(h, url.Values{
		"Action":           []string{"DescribeTags"},
		"Filter.1.Name":    []string{"resource-id"},
		"Filter.1.Value.1": []string{concentratorID},
	})
	require.NoError(t, err)
	assert.Contains(t, tagsResp, "CreateTime")
	assert.Contains(t, tagsResp, "AddedLater")
}
