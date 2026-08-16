package ec2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- BYOIP ----.
func TestBYOIP(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("provision cidr", func(t *testing.T) { //nolint:paralleltest // existing issue.
		entry, err := b.ProvisionByoipCidr("198.51.100.0/24", "test provision")
		require.NoError(t, err)
		assert.Equal(t, "198.51.100.0/24", entry.Cidr)
		assert.Equal(t, "pending-provision", entry.State)
	})

	t.Run("provision empty cidr returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.ProvisionByoipCidr("", "")
		require.Error(t, err)
	})

	t.Run("deprovision cidr", func(t *testing.T) { //nolint:paralleltest // existing issue.
		entry, err := b.DeprovisionByoipCidr("198.51.100.0/24")
		require.NoError(t, err)
		assert.Equal(t, "pending-deprovision", entry.State)
	})

	t.Run("deprovision non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DeprovisionByoipCidr("10.0.0.0/8")
		require.Error(t, err)
	})

	t.Run("withdraw cidr", func(t *testing.T) { //nolint:paralleltest // existing issue.
		// Re-provision first
		_, err := b.ProvisionByoipCidr("203.0.113.0/24", "")
		require.NoError(t, err)

		entry, err := b.WithdrawByoipCidr("203.0.113.0/24")
		require.NoError(t, err)
		assert.Equal(t, "advertised", entry.State)
	})

	t.Run("withdraw creates entry if not exists", func(t *testing.T) { //nolint:paralleltest // existing issue.
		entry, err := b.WithdrawByoipCidr("192.0.2.0/24")
		require.NoError(t, err)
		assert.Equal(t, "advertised", entry.State)
	})
}

// ---- Carrier Gateway ----.

// TestSortedDescribeByoipCidrs verifies sorted output.
func TestSortedDescribeByoipCidrs(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "203.0.113.0/24"})
	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "10.0.0.0/8"})

	result := b.DescribeByoipCidrs("")
	require.Len(t, result, 2)
	assert.Equal(t, "10.0.0.0/8", result[0].Cidr)
}

// TestSortedDescribeHosts verifies sorted output.

// TestDescribeByoipCidrs_StateFilter verifies state filter works.
func TestDescribeByoipCidrs_StateFilter(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "10.0.0.0/8", State: "advertised"})
	b.AddByoipCidrInternal(&ec2.ByoipCidr{Cidr: "203.0.113.0/24", State: "provisioned"})

	advertised := b.DescribeByoipCidrs("advertised")
	require.Len(t, advertised, 1)
	assert.Equal(t, "10.0.0.0/8", advertised[0].Cidr)
}

// TestDescribeHosts_Filter verifies ID filter works.

// TestHTTPDescribeByoipCidrs verifies the HTTP handler for DescribeByoipCidrs.
func TestHTTPDescribeByoipCidrs(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Seed by advertising.
	rec := postForm(t, h, "Action=AdvertiseByoipCidr&Version=2016-11-15&Cidr=203.0.113.0/24")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postForm(t, h, "Action=DescribeByoipCidrs&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "203.0.113.0/24")
}

// TestHTTPDescribeVpcPeeringConnections verifies the HTTP handler.
