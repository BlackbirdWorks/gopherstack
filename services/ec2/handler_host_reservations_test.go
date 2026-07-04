package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHostReservations_HTTP_Lifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	allocResp, err := dispatchHandler(h, url.Values{
		"Action":           []string{"AllocateHosts"},
		"AvailabilityZone": []string{"us-east-1a"},
		"InstanceType":     []string{"m5.large"},
		"Quantity":         []string{"1"},
	})
	require.NoError(t, err)
	hostID := accuracyExtractXMLValue(allocResp, "item")
	require.NotEmpty(t, hostID)

	offeringsResp, err := dispatchHandler(h, url.Values{"Action": []string{"DescribeHostReservationOfferings"}})
	require.NoError(t, err)
	assert.Contains(t, offeringsResp, "<DescribeHostReservationOfferingsResponse")
	assert.NotContains(t, offeringsResp, "StubResponse")
	offeringID := accuracyExtractXMLValue(offeringsResp, "offeringId")
	require.NotEmpty(t, offeringID)

	previewResp, err := dispatchHandler(h, url.Values{
		"Action":      []string{"GetHostReservationPurchasePreview"},
		"OfferingId":  []string{offeringID},
		"HostIdSet.1": []string{hostID},
	})
	require.NoError(t, err)
	assert.Contains(t, previewResp, "<currencyCode>USD</currencyCode>")

	purchaseResp, err := dispatchHandler(h, url.Values{
		"Action":      []string{"PurchaseHostReservation"},
		"OfferingId":  []string{offeringID},
		"HostIdSet.1": []string{hostID},
	})
	require.NoError(t, err)
	assert.Contains(t, purchaseResp, "<PurchaseHostReservationResponse")
	reservationID := accuracyExtractXMLValue(purchaseResp, "hostReservationId")
	require.NotEmpty(t, reservationID)

	describeResp, err := dispatchHandler(h, url.Values{
		"Action":                 []string{"DescribeHostReservations"},
		"HostReservationIdSet.1": []string{reservationID},
	})
	require.NoError(t, err)
	assert.Contains(t, describeResp, reservationID)

	releaseResp, err := dispatchHandler(h, url.Values{
		"Action":   []string{"ReleaseHosts"},
		"HostId.1": []string{hostID},
	})
	require.NoError(t, err)
	assert.Contains(t, releaseResp, "<ReleaseHostsResponse")
	assert.Contains(t, releaseResp, hostID)

	describeHostsResp, err := dispatchHandler(h, url.Values{
		"Action":   []string{"DescribeHosts"},
		"HostId.1": []string{hostID},
	})
	require.NoError(t, err)
	assert.NotContains(t, describeHostsResp, hostID)
}
