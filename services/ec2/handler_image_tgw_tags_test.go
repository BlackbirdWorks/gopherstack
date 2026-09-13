package ec2_test

import (
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreate_Tags_RoundTrip_ImageAndTgwPeering covers batch 3 of gopherstack-wjlrn: the
// image/snapshot family (CreateImage, CreateFpgaImage, CreateImageUsageReport,
// CreateRestoreImageTask, CreateReplaceRootVolumeTask) and the transit gateway peering
// family (CreateTransitGatewayConnect, CreateTransitGatewayConnectPeer,
// CreateTransitGatewayPeeringAttachment) never called parseTagSpecification, so
// TagSpecifications were silently dropped. Each case creates a resource with a tag, then
// confirms the tag comes back on the matching Describe.
func TestCreate_Tags_RoundTrip_ImageAndTgwPeering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "image", run: testCreateImageTags},
		{name: "fpga image", run: testCreateFpgaImageTags},
		{name: "image usage report", run: testCreateImageUsageReportTags},
		{name: "restore image task", run: testCreateRestoreImageTaskTags},
		{name: "replace root volume task", run: testCreateReplaceRootVolumeTaskTags},
		{name: "transit gateway peering attachment", run: testCreateTransitGatewayPeeringAttachmentTags},
		{name: "transit gateway connect", run: testCreateTransitGatewayConnectTags},
		{name: "transit gateway connect peer", run: testCreateTransitGatewayConnectPeerTags},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func testCreateImageTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	insts, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateImage"},
		"InstanceId":                      []string{insts[0].ID},
		"Name":                            []string{"my-image"},
		"TagSpecification.1.ResourceType": []string{"image"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)

	id := extractBetween(t, createResp, "<imageId>", "</imageId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":    []string{"DescribeImages"},
		"ImageId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateFpgaImageTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateFpgaImage"},
		"Name":                            []string{"my-afi"},
		"InputStorageLocation.Bucket":     []string{"afi-bucket"},
		"InputStorageLocation.Key":        []string{"afi.tar"},
		"TagSpecification.1.ResourceType": []string{"fpga-image"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)

	id := extractBetween(t, createResp, "<fpgaImageId>", "</fpgaImageId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":        []string{"DescribeFpgaImages"},
		"FpgaImageId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateImageUsageReportTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	img, err := h.Backend.RegisterImage("source-image", "desc", "")
	require.NoError(t, err)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateImageUsageReport"},
		"ImageId":                         []string{img.ImageID},
		"TagSpecification.1.ResourceType": []string{"image-usage-report"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)

	id := extractBetween(t, createResp, "<reportId>", "</reportId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":     []string{"DescribeImageUsageReports"},
		"ReportId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateRestoreImageTaskTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	img, err := h.Backend.RegisterImage("source-image", "desc", "")
	require.NoError(t, err)

	_, err = h.Backend.CreateStoreImageTask(img.ImageID, "my-bucket")
	require.NoError(t, err)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateRestoreImageTask"},
		"Bucket":                          []string{"my-bucket"},
		"ObjectKey":                       []string{img.ImageID + ".bin"},
		"TagSpecification.1.ResourceType": []string{"image"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)

	id := extractBetween(t, createResp, "<imageId>", "</imageId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":    []string{"DescribeImages"},
		"ImageId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateReplaceRootVolumeTaskTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	insts, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateReplaceRootVolumeTask"},
		"InstanceId":                      []string{insts[0].ID},
		"TagSpecification.1.ResourceType": []string{"replace-root-volume-task"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<replaceRootVolumeTaskId>", "</replaceRootVolumeTaskId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                    []string{"DescribeReplaceRootVolumeTasks"},
		"ReplaceRootVolumeTaskId.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateTransitGatewayPeeringAttachmentTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	tgw, err := h.Backend.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "requester"})
	require.NoError(t, err)
	peerTgw, err := h.Backend.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "accepter"})
	require.NoError(t, err)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateTransitGatewayPeeringAttachment"},
		"TransitGatewayId":                []string{tgw.ID},
		"PeerTransitGatewayId":            []string{peerTgw.ID},
		"PeerAccountId":                   []string{"999999999999"},
		"PeerRegion":                      []string{"us-west-2"},
		"TagSpecification.1.ResourceType": []string{"transit-gateway-attachment"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<transitGatewayAttachmentId>", "</transitGatewayAttachmentId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                        []string{"DescribeTransitGatewayPeeringAttachments"},
		"TransitGatewayAttachmentIds.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateTransitGatewayConnectTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	tgw, err := h.Backend.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "tgw"})
	require.NoError(t, err)
	transport, err := h.Backend.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-connect-transport", nil, nil)
	require.NoError(t, err)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                              []string{"CreateTransitGatewayConnect"},
		"TransportTransitGatewayAttachmentId": []string{transport.TransitGatewayAttachmentID},
		"TagSpecification.1.ResourceType":     []string{"transit-gateway-attachment"},
		"TagSpecification.1.Tag.1.Key":        []string{"Name"},
		"TagSpecification.1.Tag.1.Value":      []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<transitGatewayAttachmentId>", "</transitGatewayAttachmentId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                        []string{"DescribeTransitGatewayConnects"},
		"TransitGatewayAttachmentIds.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}

func testCreateTransitGatewayConnectPeerTags(t *testing.T) {
	t.Helper()

	h := newTestHandler()

	tgw, err := h.Backend.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "tgw"})
	require.NoError(t, err)
	transport, err := h.Backend.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-connect-peer-transport", nil, nil)
	require.NoError(t, err)
	conn, err := h.Backend.CreateTransitGatewayConnect(transport.TransitGatewayAttachmentID, tgw.ID)
	require.NoError(t, err)

	createResp, err := dispatchHandler(h, url.Values{
		"Action":                          []string{"CreateTransitGatewayConnectPeer"},
		"TransitGatewayAttachmentId":      []string{conn.TransitGatewayAttachmentID},
		"PeerAddress":                     []string{"169.254.6.1"},
		"TagSpecification.1.ResourceType": []string{"transit-gateway-connect-peer"},
		"TagSpecification.1.Tag.1.Key":    []string{"Name"},
		"TagSpecification.1.Tag.1.Value":  []string{"demo"},
	})
	require.NoError(t, err)
	assert.Contains(t, createResp, "<key>Name</key><value>demo</value>")

	id := extractBetween(t, createResp, "<transitGatewayConnectPeerId>", "</transitGatewayConnectPeerId>")
	require.NotEmpty(t, id)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                         []string{"DescribeTransitGatewayConnectPeers"},
		"TransitGatewayConnectPeerIds.1": []string{id},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, "<key>Name</key><value>demo</value>")
}
