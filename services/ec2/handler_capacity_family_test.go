package ec2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestHandler_CreateDescribeModifyCancelCapacityReservationFleet(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "CreateCapacityReservationFleet")
	vals.Set("Version", "2016-11-15")
	vals.Set("TotalTargetCapacity", "5")
	vals.Set("InstanceTypeSpecification.1.InstanceType", "m5.xlarge")
	vals.Set("InstanceTypeSpecification.1.AvailabilityZone", "us-east-1a")
	vals.Set("InstanceTypeSpecification.1.Priority", "1")
	vals.Set("InstanceTypeSpecification.1.Weight", "1")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var created struct {
		XMLName                    xml.Name `xml:"CreateCapacityReservationFleetResponse"`
		CapacityReservationFleetID string   `xml:"capacityReservationFleetId"`
		State                      string   `xml:"state"`
		TotalTargetCapacity        int32    `xml:"totalTargetCapacity"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &created))
	assert.NotEmpty(t, created.CapacityReservationFleetID)
	assert.True(t, len(created.CapacityReservationFleetID) > 4 && created.CapacityReservationFleetID[:4] == "crf-")
	assert.Equal(t, "active", created.State)
	assert.Equal(t, int32(5), created.TotalTargetCapacity)

	// Describe.
	descVals := url.Values{}
	descVals.Set("Action", "DescribeCapacityReservationFleets")
	descVals.Set("Version", "2016-11-15")
	descVals.Set("CapacityReservationFleetId.1", created.CapacityReservationFleetID)

	rec = postForm(t, h, descVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "DescribeCapacityReservationFleetsResponse")
	assert.Contains(t, body, created.CapacityReservationFleetID)
	assert.Contains(t, body, "<instanceType>m5.xlarge</instanceType>")

	// Modify.
	modVals := url.Values{}
	modVals.Set("Action", "ModifyCapacityReservationFleet")
	modVals.Set("Version", "2016-11-15")
	modVals.Set("CapacityReservationFleetId", created.CapacityReservationFleetID)
	modVals.Set("TotalTargetCapacity", "9")

	rec = postForm(t, h, modVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<return>true</return>")

	// Cancel.
	cancelVals := url.Values{}
	cancelVals.Set("Action", "CancelCapacityReservationFleets")
	cancelVals.Set("Version", "2016-11-15")
	cancelVals.Set("CapacityReservationFleetId.1", created.CapacityReservationFleetID)

	rec = postForm(t, h, cancelVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	body = rec.Body.String()
	assert.Contains(t, body, "CancelCapacityReservationFleetsResponse")
	assert.Contains(t, body, "<currentFleetState>cancelled</currentFleetState>")
	assert.Contains(t, body, "<previousFleetState>active</previousFleetState>")
}

func TestHandler_CreateCapacityReservationFleet_MissingTotalTargetCapacity(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "CreateCapacityReservationFleet")
	vals.Set("Version", "2016-11-15")
	vals.Set("InstanceTypeSpecification.1.InstanceType", "m5.xlarge")

	rec := postForm(t, h, vals.Encode())
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

func TestHandler_CapacityBlockOfferingsAndPurchase(t *testing.T) {
	t.Parallel()

	h := newHandler()

	descVals := url.Values{}
	descVals.Set("Action", "DescribeCapacityBlockOfferings")
	descVals.Set("Version", "2016-11-15")
	descVals.Set("InstanceType", "p4d.24xlarge")
	descVals.Set("CapacityDurationHours", "24")
	descVals.Set("InstanceCount", "1")

	rec := postForm(t, h, descVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var offerings struct {
		XMLName xml.Name `xml:"DescribeCapacityBlockOfferingsResponse"`
		Set     struct {
			Items []struct {
				CapacityBlockOfferingID string `xml:"capacityBlockOfferingId"`
			} `xml:"item"`
		} `xml:"capacityBlockOfferingSet"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &offerings))
	require.NotEmpty(t, offerings.Set.Items)

	purchaseVals := url.Values{}
	purchaseVals.Set("Action", "PurchaseCapacityBlock")
	purchaseVals.Set("Version", "2016-11-15")
	purchaseVals.Set("CapacityBlockOfferingId", offerings.Set.Items[0].CapacityBlockOfferingID)
	purchaseVals.Set("InstancePlatform", "Linux/UNIX")

	rec = postForm(t, h, purchaseVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "PurchaseCapacityBlockResponse")
	assert.Contains(t, body, "<capacityBlockSet>")
	assert.Contains(t, body, "<capacityReservation>")

	// DescribeCapacityBlocks now shows the purchase.
	listVals := url.Values{}
	listVals.Set("Action", "DescribeCapacityBlocks")
	listVals.Set("Version", "2016-11-15")

	rec = postForm(t, h, listVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeCapacityBlocksResponse")

	// DescribeCapacityBlockStatus.
	statusVals := url.Values{}
	statusVals.Set("Action", "DescribeCapacityBlockStatus")
	statusVals.Set("Version", "2016-11-15")

	rec = postForm(t, h, statusVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<interconnectStatus>ok</interconnectStatus>")
}

func TestHandler_PurchaseCapacityBlock_OfferingNotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "PurchaseCapacityBlock")
	vals.Set("Version", "2016-11-15")
	vals.Set("CapacityBlockOfferingId", "cbo-doesnotexist")
	vals.Set("InstancePlatform", "Linux/UNIX")

	rec := postForm(t, h, vals.Encode())
	assert.NotEqual(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidCapacityBlockOfferingId.NotFound")
}

func TestHandler_CreateCapacityReservationBySplittingAndMove(t *testing.T) {
	t.Parallel()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	src, err := bk.CreateCapacityReservation("m5.xlarge", "us-east-1a", 10)
	require.NoError(t, err)

	h := ec2.NewHandler(bk)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	splitVals := url.Values{}
	splitVals.Set("Action", "CreateCapacityReservationBySplitting")
	splitVals.Set("Version", "2016-11-15")
	splitVals.Set("SourceCapacityReservationId", src.CapacityReservationID)
	splitVals.Set("InstanceCount", "3")

	rec := postForm(t, h, splitVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var splitResp struct {
		XMLName xml.Name `xml:"CreateCapacityReservationBySplittingResponse"`
		Dest    struct {
			CapacityReservationID string `xml:"capacityReservationId"`
			TotalInstanceCount    int    `xml:"totalInstanceCount"`
		} `xml:"destinationCapacityReservation"`
		InstanceCount int `xml:"instanceCount"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &splitResp))
	assert.NotEmpty(t, splitResp.Dest.CapacityReservationID)
	assert.Equal(t, 3, splitResp.Dest.TotalInstanceCount)
	assert.Equal(t, 3, splitResp.InstanceCount)

	moveVals := url.Values{}
	moveVals.Set("Action", "MoveCapacityReservationInstances")
	moveVals.Set("Version", "2016-11-15")
	moveVals.Set("SourceCapacityReservationId", src.CapacityReservationID)
	moveVals.Set("DestinationCapacityReservationId", splitResp.Dest.CapacityReservationID)
	moveVals.Set("InstanceCount", "2")

	rec = postForm(t, h, moveVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "MoveCapacityReservationInstancesResponse")
}

func TestHandler_CapacityReservationBillingOwnerFlow(t *testing.T) {
	t.Parallel()

	bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	cr, err := bk.CreateCapacityReservation("m5.xlarge", "us-east-1a", 1)
	require.NoError(t, err)

	h := ec2.NewHandler(bk)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	assocVals := url.Values{}
	assocVals.Set("Action", "AssociateCapacityReservationBillingOwner")
	assocVals.Set("Version", "2016-11-15")
	assocVals.Set("CapacityReservationId", cr.CapacityReservationID)
	assocVals.Set("UnusedReservationBillingOwnerId", "111111111111")

	rec := postForm(t, h, assocVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AssociateCapacityReservationBillingOwnerResponse")

	descVals := url.Values{}
	descVals.Set("Action", "DescribeCapacityReservationBillingRequests")
	descVals.Set("Version", "2016-11-15")
	descVals.Set("Role", "odcr-owner")

	rec = postForm(t, h, descVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "DescribeCapacityReservationBillingRequestsResponse")
	assert.Contains(t, body, "<status>pending</status>")

	rejectVals := url.Values{}
	rejectVals.Set("Action", "RejectCapacityReservationBillingOwnership")
	rejectVals.Set("Version", "2016-11-15")
	rejectVals.Set("CapacityReservationId", cr.CapacityReservationID)

	rec = postForm(t, h, rejectVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RejectCapacityReservationBillingOwnershipResponse")
}

func TestHandler_CapacityManagerLifecycle(t *testing.T) {
	t.Parallel()

	h := newHandler()

	enableVals := url.Values{}
	enableVals.Set("Action", "EnableCapacityManager")
	enableVals.Set("Version", "2016-11-15")
	enableVals.Set("OrganizationsAccess", "true")

	rec := postForm(t, h, enableVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "EnableCapacityManagerResponse")
	assert.Contains(t, body, "<capacityManagerStatus>enabled</capacityManagerStatus>")
	assert.Contains(t, body, "<organizationsAccess>true</organizationsAccess>")

	attrVals := url.Values{}
	attrVals.Set("Action", "GetCapacityManagerAttributes")
	attrVals.Set("Version", "2016-11-15")

	rec = postForm(t, h, attrVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<capacityManagerStatus>enabled</capacityManagerStatus>")

	exportVals := url.Values{}
	exportVals.Set("Action", "CreateCapacityManagerDataExport")
	exportVals.Set("Version", "2016-11-15")
	exportVals.Set("OutputFormat", "parquet")
	exportVals.Set("S3BucketName", "my-bucket")
	exportVals.Set("Schedule", "hourly")

	rec = postForm(t, h, exportVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	var created struct {
		XMLName                     xml.Name `xml:"CreateCapacityManagerDataExportResponse"`
		CapacityManagerDataExportID string   `xml:"capacityManagerDataExportId"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &created))
	assert.NotEmpty(t, created.CapacityManagerDataExportID)

	listVals := url.Values{}
	listVals.Set("Action", "DescribeCapacityManagerDataExports")
	listVals.Set("Version", "2016-11-15")

	rec = postForm(t, h, listVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), created.CapacityManagerDataExportID)

	deleteVals := url.Values{}
	deleteVals.Set("Action", "DeleteCapacityManagerDataExport")
	deleteVals.Set("Version", "2016-11-15")
	deleteVals.Set("CapacityManagerDataExportId", created.CapacityManagerDataExportID)

	rec = postForm(t, h, deleteVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DeleteCapacityManagerDataExportResponse")

	disableVals := url.Values{}
	disableVals.Set("Action", "DisableCapacityManager")
	disableVals.Set("Version", "2016-11-15")

	rec = postForm(t, h, disableVals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<capacityManagerStatus>disabled</capacityManagerStatus>")
}

func TestHandler_GetCapacityManagerMetricData_RequiresParams(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "GetCapacityManagerMetricData")
	vals.Set("Version", "2016-11-15")

	rec := postForm(t, h, vals.Encode())
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

func TestHandler_GetCapacityManagerMetricData_EmptyResult(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{}
	vals.Set("Action", "GetCapacityManagerMetricData")
	vals.Set("Version", "2016-11-15")
	vals.Set("StartTime", "2026-01-01T00:00:00Z")
	vals.Set("EndTime", "2026-01-02T00:00:00Z")
	vals.Set("Period", "3600")
	vals.Set("MetricName.1", "UsedInstanceCount")

	rec := postForm(t, h, vals.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetCapacityManagerMetricDataResponse")
}

// ---- Capacity Reservation ---- //nolint:godot // existing issue.
func TestCapacityReservation(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var crID string

	t.Run("create reservation", func(t *testing.T) { //nolint:paralleltest // existing issue.
		cr, err := b.CreateCapacityReservation("t3.micro", "us-east-1a", 2)
		require.NoError(t, err)
		assert.NotEmpty(t, cr.CapacityReservationID)
		assert.Equal(t, "active", cr.State)
		assert.Equal(t, 2, cr.TotalInstanceCount)
		crID = cr.CapacityReservationID
	})

	t.Run("modify instance count", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyCapacityReservation(crID, 5))
	})

	t.Run("get groups returns empty", func(t *testing.T) { //nolint:paralleltest // existing issue.
		groups, err := b.GetGroupsForCapacityReservation(crID)
		require.NoError(t, err)
		assert.Empty(t, groups)
	})

	t.Run("cancel reservation", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.CancelCapacityReservation(crID))
	})

	t.Run("empty ID returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.CancelCapacityReservation(""))
	})
}

// ---- Instance Connect Endpoint ---- //nolint:godot // existing issue.

// ---- HTTP dispatch tests ---- //nolint:godot // existing issue.
func TestHTTP_CreateCapacityReservation(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":        []string{"CreateCapacityReservation"},
		"InstanceType":  []string{"t3.micro"},
		"InstanceCount": []string{"1"},
	})
	require.NoError(t, err)
}

func TestInterruptibleCapacityReservationAllocationHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	cr, err := h.Backend.CreateCapacityReservation("m5.large", "us-east-1a", 10)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                {"CreateInterruptibleCapacityReservationAllocation"},
		"CapacityReservationId": {cr.CapacityReservationID},
		"InstanceCount":         {"3"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<CreateInterruptibleCapacityReservationAllocationResponse>")
	assert.Contains(t, resp, "<targetInstanceCount>3</targetInstanceCount>")
	assert.Contains(t, resp, "<status>active</status>")

	resp, err = ec2.ExportDispatch(h, url.Values{
		"Action":                {"UpdateInterruptibleCapacityReservationAllocation"},
		"CapacityReservationId": {cr.CapacityReservationID},
		"TargetInstanceCount":   {"5"},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<UpdateInterruptibleCapacityReservationAllocationResponse>")
	assert.Contains(t, resp, "<targetInstanceCount>5</targetInstanceCount>")
}

func TestGetCapacityReservationUsageHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	cr, err := h.Backend.CreateCapacityReservation("m5.large", "us-east-1a", 10)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{
		"Action":                {"GetCapacityReservationUsage"},
		"CapacityReservationId": {cr.CapacityReservationID},
	})
	require.NoError(t, err)
	assert.Contains(t, resp, "<GetCapacityReservationUsageResponse>")
	assert.Contains(t, resp, "<availableInstanceCount>10</availableInstanceCount>")
	assert.Contains(t, resp, "<totalInstanceCount>10</totalInstanceCount>")
}

func TestDescribeCapacityReservationTopologyHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	cr, err := h.Backend.CreateCapacityReservation("m5.large", "us-east-1a", 10)
	require.NoError(t, err)

	resp, err := ec2.ExportDispatch(h, url.Values{"Action": {"DescribeCapacityReservationTopology"}})
	require.NoError(t, err)
	assert.Contains(t, resp, "<DescribeCapacityReservationTopologyResponse>")
	assert.Contains(t, resp, "<capacityReservationId>"+cr.CapacityReservationID+"</capacityReservationId>")
}

// TestAllOpsReturn200HTTP is a broad smoke test verifying every
// de-stubbed op returns 200 OK through the full HTTP handler for a minimal
// (frequently zero-value/empty) request, mirroring the coverage the retired
// registerStubOps registration used to get for free.
func TestAllOpsReturn200HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	cr, err := h.Backend.CreateCapacityReservation("m5.large", "us-east-1a", 10)
	require.NoError(t, err)
	addr, err := h.Backend.AllocateAddress()
	require.NoError(t, err)
	instances, err := h.Backend.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	tests := []struct {
		vals url.Values
		name string
	}{
		{name: "DescribeMovingAddresses", vals: url.Values{"Action": {"DescribeMovingAddresses"}}},
		{
			name: "DescribeCapacityReservationTopology",
			vals: url.Values{"Action": {"DescribeCapacityReservationTopology"}},
		},
		{
			name: "DescribeTransitGatewayAttachments",
			vals: url.Values{"Action": {"DescribeTransitGatewayAttachments"}},
		},
		{name: "DescribeElasticGpus", vals: url.Values{"Action": {"DescribeElasticGpus"}}},
		{name: "DescribeImageReferences", vals: url.Values{
			"Action": {"DescribeImageReferences"}, "ImageId.1": {"ami-test"},
		}},
		{name: "GetSpotPlacementScores", vals: url.Values{
			"Action": {"GetSpotPlacementScores"}, "InstanceType.1": {"m5.large"}, "TargetCapacity": {"1"},
		}},
		{name: "SendDiagnosticInterrupt", vals: url.Values{
			"Action": {"SendDiagnosticInterrupt"}, "InstanceId": {instances[0].ID},
		}},
		{name: "EnableReachabilityAnalyzerOrganizationSharing", vals: url.Values{
			"Action": {"EnableReachabilityAnalyzerOrganizationSharing"},
		}},
		{name: "GetCapacityReservationUsage", vals: url.Values{
			"Action": {"GetCapacityReservationUsage"}, "CapacityReservationId": {cr.CapacityReservationID},
		}},
		{name: "CancelImageLaunchPermission", vals: url.Values{
			"Action": {"CancelImageLaunchPermission"}, "ImageId": {"ami-test"},
		}},
		{name: "MoveAddressToVpc", vals: url.Values{"Action": {"MoveAddressToVpc"}, "PublicIp": {addr.PublicIP}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postForm(t, h, tt.vals.Encode())
			assert.Equal(t, http.StatusOK, rec.Code, "action %s should return 200", tt.name)
		})
	}
}

// TestHandler_CapacityReservationCancellationQuote verifies
// CreateCapacityReservationCancellationQuote and
// DescribeCapacityReservationCancellationQuotes routing, error codes, and
// wire shapes (parity-4). The quote reflects real Capacity Reservation
// state (instance count) rather than fabricated data.
func TestHandler_CapacityReservationCancellationQuote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *ec2.Handler) url.Values
		name     string
		wantBody []string
		wantCode int
	}{
		{
			name: "create_unknown_reservation_not_found",
			setup: func(*testing.T, *ec2.Handler) url.Values {
				return url.Values{
					"Action":                {"CreateCapacityReservationCancellationQuote"},
					"CapacityReservationId": {"cr-missing"},
				}
			},
			wantCode: http.StatusBadRequest,
			wantBody: []string{"InvalidCapacityReservationId.NotFound"},
		},
		{
			name: "create_reflects_real_reservation_state",
			setup: func(t *testing.T, h *ec2.Handler) url.Values {
				t.Helper()

				cr, err := h.Backend.CreateCapacityReservation("m5.large", "us-east-1a", 4)
				require.NoError(t, err)

				return url.Values{
					"Action":                {"CreateCapacityReservationCancellationQuote"},
					"CapacityReservationId": {cr.CapacityReservationID},
				}
			},
			wantCode: http.StatusOK,
			wantBody: []string{
				"CreateCapacityReservationCancellationQuoteResponse",
				"<capacityReservationCancellationQuoteId>crcq-",
				"<quoteState>active</quoteState>",
				"<instanceCount>4</instanceCount>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			vals := tt.setup(t, h)

			rec := postForm(t, h, vals.Encode())
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, want := range tt.wantBody {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

// TestHandler_CapacityReservationCancellationQuote_DescribeRoundTrip verifies
// a created quote is visible via DescribeCapacityReservationCancellationQuotes.
func TestHandler_CapacityReservationCancellationQuote_DescribeRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandler()

	cr, err := h.Backend.CreateCapacityReservation("t3.micro", "us-east-1a", 1)
	require.NoError(t, err)

	quote, err := h.Backend.CreateCapacityReservationCancellationQuote(cr.CapacityReservationID, nil)
	require.NoError(t, err)

	rec := postForm(t, h, "Action=DescribeCapacityReservationCancellationQuotes&Version=2016-11-15")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), quote.CapacityReservationCancellationQuoteID)
}

// TestHandler_CapacityManagerMonitoredTagKeys verifies
// GetCapacityManagerMonitoredTagKeys and UpdateCapacityManagerMonitoredTagKeys
// mutate real per-account tag-key state (parity-4).
func TestHandler_CapacityManagerMonitoredTagKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals     url.Values
		name     string
		wantBody []string
		wantCode int
	}{
		{
			name: "update_requires_at_least_one_key",
			vals: url.Values{
				"Action": {"UpdateCapacityManagerMonitoredTagKeys"},
			},
			wantCode: http.StatusBadRequest,
			wantBody: []string{"InvalidParameterValue"},
		},
		{
			name: "activate_two_keys",
			vals: url.Values{
				"Action":           {"UpdateCapacityManagerMonitoredTagKeys"},
				"ActivateTagKey.1": {"team"},
				"ActivateTagKey.2": {"env"},
			},
			wantCode: http.StatusOK,
			wantBody: []string{
				"UpdateCapacityManagerMonitoredTagKeysResponse",
				"<tagKey>team</tagKey>",
				"<status>activated</status>",
				"<tagKey>env</tagKey>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			rec := postForm(t, h, tt.vals.Encode())
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, want := range tt.wantBody {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

// TestHandler_CapacityManagerMonitoredTagKeys_GetReflectsUpdates verifies
// GetCapacityManagerMonitoredTagKeys reflects a prior activate/deactivate,
// including the suspended terminal state.
func TestHandler_CapacityManagerMonitoredTagKeys_GetReflectsUpdates(t *testing.T) {
	t.Parallel()

	h := newHandler()

	activateRec := postForm(t, h,
		"Action=UpdateCapacityManagerMonitoredTagKeys&Version=2016-11-15&ActivateTagKey.1=cost-center")
	require.Equal(t, http.StatusOK, activateRec.Code)

	getRec := postForm(t, h, "Action=GetCapacityManagerMonitoredTagKeys&Version=2016-11-15")
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "<tagKey>cost-center</tagKey>")
	assert.Contains(t, getRec.Body.String(), "<status>activated</status>")

	deactivateRec := postForm(t, h,
		"Action=UpdateCapacityManagerMonitoredTagKeys&Version=2016-11-15&DeactivateTagKey.1=cost-center")
	require.Equal(t, http.StatusOK, deactivateRec.Code)
	assert.Contains(t, deactivateRec.Body.String(), "<status>suspended</status>")

	getRec2 := postForm(t, h, "Action=GetCapacityManagerMonitoredTagKeys&Version=2016-11-15")
	require.Equal(t, http.StatusOK, getRec2.Code)
	assert.Contains(t, getRec2.Body.String(), "<status>suspended</status>")
}
