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
