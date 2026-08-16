package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Carrier Gateway ----.
func TestCarrierGateway(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var gwID string

	t.Run("create carrier gateway", func(t *testing.T) { //nolint:paralleltest // existing issue.
		gw, err := b.CreateCarrierGateway("vpc-12345678")
		require.NoError(t, err)
		assert.NotEmpty(t, gw.CarrierGatewayID)
		assert.Equal(t, "vpc-12345678", gw.VpcID)
		assert.Equal(t, "available", gw.State)
		assert.Equal(t, "000000000000", gw.OwnerID)
		gwID = gw.CarrierGatewayID
	})

	t.Run("describe returns created gateway", func(t *testing.T) { //nolint:paralleltest // existing issue.
		gws := b.DescribeCarrierGateways([]string{gwID})
		require.Len(t, gws, 1)
		assert.Equal(t, "vpc-12345678", gws[0].VpcID)
	})

	t.Run("describe all", func(t *testing.T) { //nolint:paralleltest // existing issue.
		gws := b.DescribeCarrierGateways(nil)
		assert.NotEmpty(t, gws)
	})

	t.Run("create second gateway", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateCarrierGateway("vpc-87654321")
		require.NoError(t, err)

		gws := b.DescribeCarrierGateways(nil)
		assert.Len(t, gws, 2)
	})

	t.Run("delete gateway", func(t *testing.T) { //nolint:paralleltest // existing issue.
		deleted, err := b.DeleteCarrierGateway(gwID)
		require.NoError(t, err)
		assert.Equal(t, gwID, deleted.CarrierGatewayID)
		gws := b.DescribeCarrierGateways([]string{gwID})
		assert.Empty(t, gws)
	})

	t.Run("delete non-existent returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DeleteCarrierGateway("cagw-nonexistent")
		require.Error(t, err)
	})

	t.Run("create with empty vpc returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateCarrierGateway("")
		require.Error(t, err)
	})
}

// ---- Reserved Instances ----.
