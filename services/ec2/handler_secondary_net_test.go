package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestHandler_SecondaryNetwork_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createVals := url.Values{}
	createVals.Set("Action", "CreateSecondaryNetwork")
	createVals.Set("Version", "2016-11-15")
	createVals.Set("Ipv4CidrBlock", "10.100.0.0/16")
	createVals.Set("NetworkType", "rdma")

	createRec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := createRec.Body.String()
	assert.Contains(t, createBody, "<CreateSecondaryNetworkResponse")
	assert.Contains(t, createBody, "<state>create-complete</state>")

	netID := extractXMLValue(t, createBody, "secondaryNetworkId")
	require.NotEmpty(t, netID)

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeSecondaryNetworks")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("SecondaryNetworkId.1", netID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), netID)

	deleteVals := url.Values{}
	deleteVals.Set("Action", "DeleteSecondaryNetwork")
	deleteVals.Set("Version", "2016-11-15")
	deleteVals.Set("SecondaryNetworkId", netID)

	deleteRec := postForm(t, h, deleteVals.Encode())
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Contains(t, deleteRec.Body.String(), "<state>delete-complete</state>")
}

func TestHandler_SecondarySubnet_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	h := newHandler()

	net, err := h.Backend.CreateSecondaryNetwork("10.100.0.0/16", "rdma", nil)
	require.NoError(t, err)

	createVals := url.Values{}
	createVals.Set("Action", "CreateSecondarySubnet")
	createVals.Set("Version", "2016-11-15")
	createVals.Set("Ipv4CidrBlock", "10.100.1.0/24")
	createVals.Set("SecondaryNetworkId", net.SecondaryNetworkID)
	createVals.Set("AvailabilityZone", "us-east-1a")

	createRec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := createRec.Body.String()
	assert.Contains(t, createBody, "<CreateSecondarySubnetResponse")

	subID := extractXMLValue(t, createBody, "secondarySubnetId")
	require.NotEmpty(t, subID)

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeSecondarySubnets")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("SecondarySubnetId.1", subID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), subID)

	deleteVals := url.Values{}
	deleteVals.Set("Action", "DeleteSecondarySubnet")
	deleteVals.Set("Version", "2016-11-15")
	deleteVals.Set("SecondarySubnetId", subID)

	deleteRec := postForm(t, h, deleteVals.Encode())
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Contains(t, deleteRec.Body.String(), "<state>delete-complete</state>")
}

func TestHandler_DescribeSecondaryInterfaces_Empty(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "DescribeSecondaryInterfaces")
	vals.Set("Version", "2016-11-15")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<DescribeSecondaryInterfacesResponse")
}

func TestHandler_DescribeOutpostLags_SeededResult(t *testing.T) {
	t.Parallel()

	h := newHandler()

	lag, err := h.Backend.SeedOutpostLag(ec2.OutpostLag{
		OutpostArn: "arn:aws:outposts:us-east-1:123456789012:outpost/op-1",
	})
	require.NoError(t, err)

	vals := url.Values{}
	vals.Set("Action", "DescribeOutpostLags")
	vals.Set("Version", "2016-11-15")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), lag.OutpostLagID)
}

func TestHandler_DescribeServiceLinkVirtualInterfaces_Empty(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "DescribeServiceLinkVirtualInterfaces")
	vals.Set("Version", "2016-11-15")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<DescribeServiceLinkVirtualInterfacesResponse")
}
