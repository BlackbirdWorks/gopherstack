package rds_test

import (
	"regexp"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// reservedDBInstanceIDPattern locks in the fix for
// PurchaseReservedDBInstancesOffering's auto-generated ReservedDBInstanceId, which used to collide under synctest.
var reservedDBInstanceIDPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)

func TestRDSBackend_ReservedDBInstanceID_Unique(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := rds.NewInMemoryBackend("000000000000", "us-east-1")

		ri1, err := b.PurchaseReservedDBInstancesOffering("some-offering-id", "", 1)
		require.NoError(t, err)

		ri2, err := b.PurchaseReservedDBInstancesOffering("some-offering-id", "", 1)
		require.NoError(t, err)

		assert.NotEqual(t, ri1.ReservedDBInstanceID, ri2.ReservedDBInstanceID,
			"two reservations created back-to-back must get distinct IDs")
		assert.Regexp(t, reservedDBInstanceIDPattern, ri1.ReservedDBInstanceID)
		assert.Regexp(t, reservedDBInstanceIDPattern, ri2.ReservedDBInstanceID)
	})
}
