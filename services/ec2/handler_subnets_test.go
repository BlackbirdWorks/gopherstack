package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestCreateDefaultSubnet(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("creates default subnet in AZ", func(t *testing.T) {
		subnet, err := b.CreateDefaultSubnet("us-east-1b")
		require.NoError(t, err)
		assert.True(t, subnet.IsDefault)
		assert.Equal(t, "us-east-1b", subnet.AvailabilityZone)
	})
}

func TestSubnetCIDRBlock(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("associate and disassociate", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assoc, err := b.AssociateSubnetCidrBlock("subnet-default", "2001:db8::/56")
		require.NoError(t, err)
		assert.Equal(t, "associated", assoc.State)

		subnetID, err := b.DisassociateSubnetCidrBlock(assoc.AssociationID)
		require.NoError(t, err)
		assert.Equal(t, "subnet-default", subnetID)
	})

	t.Run("unknown subnet returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.AssociateSubnetCidrBlock("subnet-nope", "2001:db8::/56")
		require.Error(t, err)
	})
}

// ---- Subnet CIDR reservations ---- //nolint:godot // existing issue.
func TestSubnetCidrReservations(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var reservationID string

	t.Run("create reservation", func(t *testing.T) { //nolint:paralleltest // existing issue.
		res, err := b.CreateSubnetCidrReservation("subnet-default", "172.31.0.0/28", "prefix", "test")
		require.NoError(t, err)
		assert.NotEmpty(t, res.SubnetCIDRReservationID)
		assert.Equal(t, "172.31.0.0/28", res.CIDR)
		reservationID = res.SubnetCIDRReservationID
	})

	t.Run("delete reservation", func(t *testing.T) { //nolint:paralleltest // existing issue.
		deleted, err := b.DeleteSubnetCidrReservation(reservationID)
		require.NoError(t, err)
		assert.Equal(t, reservationID, deleted.SubnetCIDRReservationID)
	})

	t.Run("unknown subnet returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateSubnetCidrReservation("subnet-missing", "10.0.0.0/28", "prefix", "")
		require.Error(t, err)
	})
}

// ---- HTTP dispatch tests ---- //nolint:godot // existing issue.

func TestHTTP_CreateSubnetCidrReservation(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":          []string{"CreateSubnetCidrReservation"},
		"SubnetId":        []string{"subnet-default"},
		"Cidr":            []string{"172.31.0.0/28"},
		"ReservationType": []string{"prefix"},
	})
	require.NoError(t, err)
}

// ---- Regression: existing Filter.1.Value.1 state filter still works ----

// TestHandlerModifySubnetAttribute covers handleModifySubnetAttribute.
func TestHandlerModifySubnetAttribute(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	h.AccountID = "000000000000"
	h.Region = "us-east-1"

	vpc, err := b.CreateVpc("10.9.0.0/16")
	require.NoError(t, err)

	subnet, err := b.CreateSubnet(vpc.ID, "10.9.1.0/24", "us-east-1a")
	require.NoError(t, err)

	rec := postForm(t, h, "Action=ModifySubnetAttribute&Version=2016-11-15"+
		"&SubnetId="+subnet.ID+
		"&MapPublicIpOnLaunch.Value=true")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Default branch (no MapPublicIpOnLaunch.Value).
	rec = postForm(t, h, "Action=ModifySubnetAttribute&Version=2016-11-15&SubnetId="+subnet.ID)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandlerNetworkACLHandlers covers handleDeleteNetworkACL, handleCreateNetworkACLEntry,
// handleDeleteNetworkACLEntry, handleModifySecurityGroupRules.
