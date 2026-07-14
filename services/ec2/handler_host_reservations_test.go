package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
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

// TestSortedDescribeHosts verifies sorted output.
func TestSortedDescribeHosts(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	hosts, err := b.AllocateHosts("us-east-1a", "t3.micro", 2)
	require.NoError(t, err)
	require.Len(t, hosts, 2)

	result := b.DescribeHosts(nil)
	require.Len(t, result, 2)
	// Sorted: first <= second.
	assert.LessOrEqual(t, result[0].HostID, result[1].HostID)
}

// TestDescribeCapacityReservations_Filter verifies ID filter works.

// TestDescribeHosts_Filter verifies ID filter works.
func TestDescribeHosts_Filter(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	hosts, err := b.AllocateHosts("us-east-1a", "t3.micro", 2)
	require.NoError(t, err)
	require.Len(t, hosts, 2)

	result := b.DescribeHosts([]string{hosts[0].HostID})
	require.Len(t, result, 1)
	assert.Equal(t, hosts[0].HostID, result[0].HostID)
}

// TestPersistenceRoundTrip verifies new resource maps survive Snapshot/Restore.

// TestHTTPDescribeHosts verifies the HTTP handler for DescribeHosts.
func TestHTTPDescribeHosts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "empty_list",
			body:     "Action=DescribeHosts&Version=2016-11-15",
			wantCode: http.StatusOK,
			wantBody: "DescribeHostsResponse",
		},
		{
			name:     "allocate_then_describe",
			body:     "Action=DescribeHosts&Version=2016-11-15",
			wantCode: http.StatusOK,
			wantBody: "DescribeHostsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.name == "allocate_then_describe" {
				rec := postForm(t, h,
					"Action=AllocateHosts&Version=2016-11-15"+
						"&AvailabilityZone=us-east-1a&InstanceType=t3.micro&Quantity=1")
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHTTPDescribeByoipCidrs verifies the HTTP handler for DescribeByoipCidrs.
