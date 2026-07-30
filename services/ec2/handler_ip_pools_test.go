package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
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

// TestHandler_IPPools_TagDualWritePathVisibility proves that ip_pools.go's
// address pool resources consolidated onto the shared tag store: a tag
// supplied at create time (TagSpecification) and a tag added afterwards via
// CreateTags are BOTH visible through the resource's own Describe call AND
// through the generic DescribeTags call. Before the fix, these types carried
// their own embedded Tags field that was populated only at create time, so a
// post-creation CreateTags call was invisible to Describe.
func TestHandler_IPPools_TagDualWritePathVisibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody     func() string
		name           string
		idField        string
		describeAction string
	}{
		{
			name: "coip pool",
			createBody: func() string {
				return "Action=CreateCoipPool&Version=2016-11-15&LocalGatewayRouteTableId=lgw-rtb-12345" +
					"&TagSpecification.1.ResourceType=coip-pool" +
					"&TagSpecification.1.Tag.1.Key=CreateTime&TagSpecification.1.Tag.1.Value=yes"
			},
			idField:        "poolId",
			describeAction: "DescribeCoipPools",
		},
		{
			name: "public ipv4 pool",
			createBody: func() string {
				return "Action=CreatePublicIpv4Pool&Version=2016-11-15" +
					"&TagSpecification.1.ResourceType=ipv4pool-ec2" +
					"&TagSpecification.1.Tag.1.Key=CreateTime&TagSpecification.1.Tag.1.Value=yes"
			},
			idField:        "poolId",
			describeAction: "DescribePublicIpv4Pools",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			createRec := postForm(t, h, tt.createBody())
			require.Equal(t, http.StatusOK, createRec.Code)
			id := extractXMLValue(t, createRec.Body.String(), tt.idField)
			require.NotEmpty(t, id)

			tagRec := postForm(t, h,
				"Action=CreateTags&Version=2016-11-15&ResourceId.1="+id+
					"&Tag.1.Key=AddedLater&Tag.1.Value=yes")
			require.Equal(t, http.StatusOK, tagRec.Code)

			// Both tags must be visible through the resource's own Describe. (The
			// CreatePublicIpv4Pool response itself only echoes the PoolID, not
			// TagSet, matching the real API -- so the create-time tag is checked
			// here rather than on the create response.)
			descRec := postForm(t, h, "Action="+tt.describeAction+"&Version=2016-11-15")
			require.Equal(t, http.StatusOK, descRec.Code)
			descBody := descRec.Body.String()
			assert.Contains(t, descBody, "CreateTime")
			assert.Contains(t, descBody, "AddedLater")

			tagsRec := postForm(t, h,
				"Action=DescribeTags&Version=2016-11-15&Filter.1.Name=resource-id&Filter.1.Value.1="+id)
			require.Equal(t, http.StatusOK, tagsRec.Code)
			tagsBody := tagsRec.Body.String()
			assert.Contains(t, tagsBody, "CreateTime")
			assert.Contains(t, tagsBody, "AddedLater")
		})
	}
}

// TestHandler_Ipv6Pool_TagVisibility proves that Ipv6Pool -- which has no
// Create HTTP API (real AWS creates these implicitly via ProvisionByoipCidr,
// so this backend only exposes a Seed-style CreateIpv6Pool helper for tests)
// -- now renders tags written through CreateTags via DescribeIpv6Pools and
// the generic DescribeTags call.
func TestHandler_Ipv6Pool_TagVisibility(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	h := ec2.NewHandler(bk)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"
	pool := bk.CreateIpv6Pool("test pool", []string{"2001:db8::/32"})

	tagRec := postForm(t, h,
		"Action=CreateTags&Version=2016-11-15&ResourceId.1="+pool.PoolID+
			"&Tag.1.Key=AddedLater&Tag.1.Value=yes")
	require.Equal(t, http.StatusOK, tagRec.Code)

	descRec := postForm(t, h, "Action=DescribeIpv6Pools&Version=2016-11-15")
	require.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), "AddedLater")

	tagsRec := postForm(t, h,
		"Action=DescribeTags&Version=2016-11-15&Filter.1.Name=resource-id&Filter.1.Value.1="+pool.PoolID)
	require.Equal(t, http.StatusOK, tagsRec.Code)
	assert.Contains(t, tagsRec.Body.String(), "AddedLater")
}
