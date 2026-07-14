package ec2_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCapacityReservationOwnedBy verifies OwnedBy is populated.
func TestCapacityReservationOwnedBy(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-1"})

	reservations := b.DescribeCapacityReservations(nil)
	require.Len(t, reservations, 1)
	assert.Equal(t, "123456789012", reservations[0].OwnedBy, "OwnedBy should be set from AccountID")
}

// TestVpcPeeringConnectionExpirationTime verifies ExpirationTime is set.

// TestSortedDescribeCapacityReservations verifies sorted output.
func TestSortedDescribeCapacityReservations(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-z"})
	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-a"})
	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-m"})

	result := b.DescribeCapacityReservations(nil)
	require.Len(t, result, 3)
	assert.Equal(t, "cr-a", result[0].CapacityReservationID)
	assert.Equal(t, "cr-m", result[1].CapacityReservationID)
	assert.Equal(t, "cr-z", result[2].CapacityReservationID)
}

// TestSortedDescribeVpcPeeringConnections verifies sorted output.

// TestDescribeCapacityReservations_Filter verifies ID filter works.
func TestDescribeCapacityReservations_Filter(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-1"})
	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-2"})

	result := b.DescribeCapacityReservations([]string{"cr-1"})
	require.Len(t, result, 1)
	assert.Equal(t, "cr-1", result[0].CapacityReservationID)
}

// TestDescribeByoipCidrs_StateFilter verifies state filter works.

// TestHTTPDescribeCapacityReservations verifies the HTTP handler.
func TestHTTPDescribeCapacityReservations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *ec2.InMemoryBackend)
		name     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name:     "empty_list",
			body:     "Action=DescribeCapacityReservations&Version=2016-11-15",
			wantCode: http.StatusOK,
			wantBody: "DescribeCapacityReservationsResponse",
		},
		{
			name: "with_reservations",
			setup: func(b *ec2.InMemoryBackend) {
				b.AddCapacityReservationInternal(&ec2.CapacityReservation{
					CapacityReservationID: "cr-abc123",
					InstanceType:          "t3.micro",
					State:                 "active",
				})
			},
			body:     "Action=DescribeCapacityReservations&Version=2016-11-15",
			wantCode: http.StatusOK,
			wantBody: "cr-abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.setup != nil {
				b, ok := h.Backend.(*ec2.InMemoryBackend)
				require.True(t, ok)
				tt.setup(b)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHTTPDescribeHosts verifies the HTTP handler for DescribeHosts.

// TestAcceptCapacityReservation_SetsOwnedBy verifies OwnedBy is updated.
func TestAcceptCapacityReservation_SetsOwnedBy(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("111222333444", "us-east-1")
	b.AddCapacityReservationInternal(&ec2.CapacityReservation{
		CapacityReservationID: "cr-1",
		State:                 "pending",
		OwnedBy:               "999888777666",
	})

	cr, err := b.AcceptCapacityReservationBillingOwnership("cr-1")
	require.NoError(t, err)
	assert.Equal(t, "active", cr.State)
	assert.Equal(t, "111222333444", cr.OwnedBy, "OwnedBy should be updated to accepter AccountID")
}

// TestErrValidationMapping verifies ErrInvalidParameter maps to HTTP 400.
