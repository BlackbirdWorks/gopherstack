package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CoipPool_CreateCidrDescribeDelete(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createVals := url.Values{}
	createVals.Set("Action", "CreateCoipPool")
	createVals.Set("Version", "2016-11-15")
	createVals.Set("LocalGatewayRouteTableId", "lgw-rtb-12345")

	createRec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := createRec.Body.String()
	assert.Contains(t, createBody, "<CreateCoipPoolResponse")

	poolID := extractXMLValue(t, createBody, "poolId")
	require.NotEmpty(t, poolID)

	cidrVals := url.Values{}
	cidrVals.Set("Action", "CreateCoipCidr")
	cidrVals.Set("Version", "2016-11-15")
	cidrVals.Set("CoipPoolId", poolID)
	cidrVals.Set("Cidr", "10.0.0.0/24")

	cidrRec := postForm(t, h, cidrVals.Encode())
	require.Equal(t, http.StatusOK, cidrRec.Code)
	assert.Contains(t, cidrRec.Body.String(), "<CreateCoipCidrResponse")

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribeCoipPools")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("PoolId.1", poolID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), "10.0.0.0/24")

	usageVals := url.Values{}
	usageVals.Set("Action", "GetCoipPoolUsage")
	usageVals.Set("Version", "2016-11-15")
	usageVals.Set("PoolId", poolID)

	usageRec := postForm(t, h, usageVals.Encode())
	require.Equal(t, http.StatusOK, usageRec.Code)
	assert.Contains(t, usageRec.Body.String(), "lgw-rtb-12345")

	deleteVals := url.Values{}
	deleteVals.Set("Action", "DeleteCoipPool")
	deleteVals.Set("Version", "2016-11-15")
	deleteVals.Set("CoipPoolId", poolID)

	deleteRec := postForm(t, h, deleteVals.Encode())
	require.Equal(t, http.StatusOK, deleteRec.Code)

	describeRec2 := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec2.Code)
	assert.NotContains(t, describeRec2.Body.String(), poolID)
}

func TestHandler_CoipPool_DeleteUnknownFails(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "DeleteCoipPool")
	vals.Set("Version", "2016-11-15")
	vals.Set("CoipPoolId", "ipv4pool-coip-doesnotexist")

	rec := postForm(t, h, vals.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_PublicIpv4Pool_CreateProvisionDescribeDelete(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createVals := url.Values{}
	createVals.Set("Action", "CreatePublicIpv4Pool")
	createVals.Set("Version", "2016-11-15")

	createRec := postForm(t, h, createVals.Encode())
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := createRec.Body.String()
	assert.Contains(t, createBody, "<CreatePublicIpv4PoolResponse")

	poolID := extractXMLValue(t, createBody, "poolId")
	require.NotEmpty(t, poolID)

	provisionVals := url.Values{}
	provisionVals.Set("Action", "ProvisionPublicIpv4PoolCidr")
	provisionVals.Set("Version", "2016-11-15")
	provisionVals.Set("PoolId", poolID)
	provisionVals.Set("NetmaskLength", "28")
	provisionVals.Set("IpamPoolId", "ipam-pool-12345")

	provisionRec := postForm(t, h, provisionVals.Encode())
	require.Equal(t, http.StatusOK, provisionRec.Code)
	provisionBody := provisionRec.Body.String()
	assert.Contains(t, provisionBody, "<ProvisionPublicIpv4PoolCidrResponse")

	describeVals := url.Values{}
	describeVals.Set("Action", "DescribePublicIpv4Pools")
	describeVals.Set("Version", "2016-11-15")
	describeVals.Set("PoolId.1", poolID)

	describeRec := postForm(t, h, describeVals.Encode())
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), poolID)

	deleteVals := url.Values{}
	deleteVals.Set("Action", "DeletePublicIpv4Pool")
	deleteVals.Set("Version", "2016-11-15")
	deleteVals.Set("PoolId", poolID)

	deleteRec := postForm(t, h, deleteVals.Encode())
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Contains(t, deleteRec.Body.String(), "<returnValue>true</returnValue>")
}

func TestHandler_Ipv6Pool_DescribeAndAssociatedCidrs(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "DescribeIpv6Pools")
	vals.Set("Version", "2016-11-15")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<DescribeIpv6PoolsResponse")

	assocVals := url.Values{}
	assocVals.Set("Action", "GetAssociatedIpv6PoolCidrs")
	assocVals.Set("Version", "2016-11-15")
	assocVals.Set("PoolId", "ipv6pool-ec2-doesnotexist")

	assocRec := postForm(t, h, assocVals.Encode())
	assert.Equal(t, http.StatusBadRequest, assocRec.Code)
}
