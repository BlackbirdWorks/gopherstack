package ec2_test

import (
	"context"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ec2"
)

type ec2ConfigProvider struct{ settings ec2.Settings }

func (c ec2ConfigProvider) GetEC2Settings() ec2.Settings { return c.settings }

// TestEC2Provider_Init_WithConfigProvider verifies that Provider.Init picks up
// JanitorInterval, TerminatedTTL and CancelledSpotTTL from the ConfigProvider.
func TestEC2Provider_Init_WithConfigProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		config               any
		name                 string
		wantInterval         time.Duration
		wantTerminatedTTL    time.Duration
		wantCancelledSpotTTL time.Duration
	}{
		{
			name: "custom_settings_propagated",
			config: ec2ConfigProvider{
				ec2.Settings{
					JanitorInterval:  5 * time.Minute,
					TerminatedTTL:    2 * time.Hour,
					CancelledSpotTTL: 12 * time.Hour,
				},
			},
			wantInterval:         5 * time.Minute,
			wantTerminatedTTL:    2 * time.Hour,
			wantCancelledSpotTTL: 12 * time.Hour,
		},
		{
			name:                 "no_config_provider",
			config:               nil,
			wantInterval:         ec2.DefaultJanitorInterval,
			wantTerminatedTTL:    ec2.DefaultTerminatedTTL,
			wantCancelledSpotTTL: ec2.DefaultCancelledSpotTTL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, err := (&ec2.Provider{}).Init(
				&service.AppContext{Config: tt.config, Logger: slog.Default()},
			)
			require.NoError(t, err)

			h, ok := svc.(*ec2.Handler)
			require.True(t, ok)
			t.Cleanup(func() { h.Shutdown(context.Background()) })
			assert.Equal(t, tt.wantInterval, h.GetJanitorInterval())
			assert.Equal(t, tt.wantTerminatedTTL, h.GetJanitorTerminatedTTL())
			assert.Equal(t, tt.wantCancelledSpotTTL, h.GetJanitorCancelledSpotTTL())
		})
	}
}

// TestReset verifies that Backend.Reset() clears all resource maps
// and repopulates the default VPC, subnet, and security group.
func TestReset(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	// Add resources.
	b.AddAddressTransferInternal(&ec2.AddressTransfer{PublicIP: "1.2.3.4"})
	b.AddCapacityReservationInternal(&ec2.CapacityReservation{CapacityReservationID: "cr-1"})
	b.AddVpcPeeringConnectionInternal(&ec2.VpcPeeringConnection{VpcPeeringConnectionID: "pcx-1"})
	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "203.0.113.0/24", State: "advertised"})

	require.Equal(t, 1, b.AddressTransferCount())
	require.Equal(t, 1, b.CapacityReservationCount())

	// Reset.
	b.Reset()

	assert.Equal(t, 0, b.AddressTransferCount())
	assert.Equal(t, 0, b.CapacityReservationCount())
	assert.Equal(t, 0, b.VpcPeeringConnectionCount())
	assert.Equal(t, 0, b.ByoipCidrCount())
	assert.Equal(t, 0, b.DedicatedHostCount())

	// Defaults re-populated.
	vpcs := b.DescribeVpcs(nil)
	assert.Len(t, vpcs, 1)
	assert.Equal(t, "vpc-default", vpcs[0].ID)
}

// TestMultipleResetCycle ensures repeated resets are idempotent.

// TestMultipleResetCycle ensures repeated resets are idempotent.
func TestMultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	for range 3 {
		b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "10.0.0.0/8"})
		b.Reset()
		assert.Equal(t, 0, b.ByoipCidrCount())
	}
}

// TestHandlerReset verifies Handler.Reset() delegates to Backend.Reset().

// TestHandlerReset verifies Handler.Reset() delegates to Backend.Reset().
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "10.0.0.0/8"})
	require.Equal(t, 1, b.ByoipCidrCount())

	h.Reset()

	assert.Equal(t, 0, b.ByoipCidrCount())
}

// TestProviderInit_NilCtx verifies that Provider.Init returns ErrNilAppContext
// when called with a nil context.

// TestProviderInit_NilCtx verifies that Provider.Init returns ErrNilAppContext
// when called with a nil context.
func TestProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &ec2.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, ec2.ErrNilAppContext)
}

// TestHandlerOpsPreBuilt verifies the dispatch table is cached at construction.

// TestHandlerOpsPreBuilt verifies the dispatch table is cached at construction.
func TestHandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	// ops map should be non-empty (contains all registered ops).
	assert.Positive(t, h.HandlerOpsLen())
}

// TestSeedHelpers verifies all seed helper functions work correctly.

// TestGetSupportedOperations_NewDescribeOps verifies the 4 new Describe
// operations appear in GetSupportedOperations().
func TestGetSupportedOperations_NewDescribeOps(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	ops := h.GetSupportedOperations()
	opSet := make(map[string]bool, len(ops))
	for _, op := range ops {
		opSet[op] = true
	}

	assert.True(t, opSet["DescribeCapacityReservations"])
	assert.True(t, opSet["DescribeByoipCidrs"])
	assert.True(t, opSet["DescribeHosts"])
	assert.True(t, opSet["DescribeVpcPeeringConnections"])
}

// TestHTTPDescribeCapacityReservations verifies the HTTP handler.

// TestErrValidationMapping verifies ErrInvalidParameter maps to HTTP 400.
func TestErrValidationMapping(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=AcceptAddressTransfer&Version=2016-11-15")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}
