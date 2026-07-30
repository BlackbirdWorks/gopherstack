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

// TestHandler_SecondaryNet_TagDualWritePathVisibility proves that
// secondary_net.go's resources consolidated onto the shared tag store: a tag
// supplied at create time (TagSpecification) and a tag added afterwards via
// CreateTags are BOTH visible through the resource's own Describe call AND
// through the generic DescribeTags call. Before the fix, these types carried
// their own embedded Tags field that was never even rendered on the wire, so
// neither write path was visible.
func TestHandler_SecondaryNet_TagDualWritePathVisibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody     func(t *testing.T, h *ec2.Handler) string
		name           string
		idField        string
		describeAction string
	}{
		{
			name: "secondary network",
			createBody: func(_ *testing.T, _ *ec2.Handler) string {
				return "Action=CreateSecondaryNetwork&Version=2016-11-15&Ipv4CidrBlock=10.101.0.0/16" +
					"&TagSpecification.1.ResourceType=secondary-network" +
					"&TagSpecification.1.Tag.1.Key=CreateTime&TagSpecification.1.Tag.1.Value=yes"
			},
			idField:        "secondaryNetworkId",
			describeAction: "DescribeSecondaryNetworks",
		},
		{
			name: "secondary subnet",
			createBody: func(t *testing.T, h *ec2.Handler) string {
				t.Helper()

				net, err := h.Backend.CreateSecondaryNetwork("10.102.0.0/16", "rdma", nil)
				require.NoError(t, err)

				return "Action=CreateSecondarySubnet&Version=2016-11-15&Ipv4CidrBlock=10.102.1.0/24" +
					"&SecondaryNetworkId=" + net.SecondaryNetworkID + "&AvailabilityZone=us-east-1a" +
					"&TagSpecification.1.ResourceType=secondary-subnet" +
					"&TagSpecification.1.Tag.1.Key=CreateTime&TagSpecification.1.Tag.1.Value=yes"
			},
			idField:        "secondarySubnetId",
			describeAction: "DescribeSecondarySubnets",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			createRec := postForm(t, h, tt.createBody(t, h))
			require.Equal(t, http.StatusOK, createRec.Code)
			id := extractXMLValue(t, createRec.Body.String(), tt.idField)
			require.NotEmpty(t, id)
			assert.Contains(t, createRec.Body.String(), "CreateTime")

			tagRec := postForm(t, h,
				"Action=CreateTags&Version=2016-11-15&ResourceId.1="+id+
					"&Tag.1.Key=AddedLater&Tag.1.Value=yes")
			require.Equal(t, http.StatusOK, tagRec.Code)

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

// TestHandler_SecondaryNet_SeedOnlyResourcesTagVisibility proves that the
// Seed-only resources in this family (secondary interfaces, Outpost LAGs,
// service link virtual interfaces -- none of which have a Create API, only
// Describe) now render tags written through CreateTags. These types have no
// create-time tag path (Seed takes no tags param), so there was never a
// "second writer" to drift out of sync; the bug fixed here was that TagSet
// was entirely absent from the wire response.
func TestHandler_SecondaryNet_SeedOnlyResourcesTagVisibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed           func(t *testing.T, h *ec2.Handler) string
		name           string
		describeAction string
	}{
		{
			name: "outpost lag",
			seed: func(t *testing.T, h *ec2.Handler) string {
				t.Helper()

				lag, err := h.Backend.SeedOutpostLag(ec2.OutpostLag{
					OutpostArn: "arn:aws:outposts:us-east-1:000000000000:outpost/op-1",
				})
				require.NoError(t, err)

				return lag.OutpostLagID
			},
			describeAction: "DescribeOutpostLags",
		},
		{
			name: "service link virtual interface",
			seed: func(t *testing.T, h *ec2.Handler) string {
				t.Helper()

				vif, err := h.Backend.SeedServiceLinkVirtualInterface(ec2.ServiceLinkVirtualInterface{
					OutpostArn: "arn:aws:outposts:us-east-1:000000000000:outpost/op-1",
				})
				require.NoError(t, err)

				return vif.ServiceLinkVirtualInterfaceID
			},
			describeAction: "DescribeServiceLinkVirtualInterfaces",
		},
		{
			name: "secondary interface",
			seed: func(t *testing.T, h *ec2.Handler) string {
				t.Helper()

				si, err := h.Backend.SeedSecondaryInterface(ec2.SecondaryInterface{})
				require.NoError(t, err)

				return si.SecondaryInterfaceID
			},
			describeAction: "DescribeSecondaryInterfaces",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			id := tt.seed(t, h)

			tagRec := postForm(t, h,
				"Action=CreateTags&Version=2016-11-15&ResourceId.1="+id+
					"&Tag.1.Key=AddedLater&Tag.1.Value=yes")
			require.Equal(t, http.StatusOK, tagRec.Code)

			descRec := postForm(t, h, "Action="+tt.describeAction+"&Version=2016-11-15")
			require.Equal(t, http.StatusOK, descRec.Code)
			assert.Contains(t, descRec.Body.String(), "AddedLater")

			tagsRec := postForm(t, h,
				"Action=DescribeTags&Version=2016-11-15&Filter.1.Name=resource-id&Filter.1.Value.1="+id)
			require.Equal(t, http.StatusOK, tagsRec.Code)
			assert.Contains(t, tagsRec.Body.String(), "AddedLater")
		})
	}
}
