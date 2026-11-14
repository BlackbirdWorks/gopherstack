package ocivault_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testsnap"
	oci "github.com/blackbirdworks/gopherstack/services/ocivault"
)

// Rotation details handed out with a key must not alias the live record the
// sweeper updates on auto-rotation.
func TestKeyRotationDetails_NotAliasedToSweep(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	v := e.mkVault(t, "alias")
	e.backend.Sweep()

	key, _, err := e.backend.CreateKey(*v.Id, oci.CreateKeyIn{
		CompartmentID: testCompartment, DisplayName: "auto", ProtectionMode: "SOFTWARE",
		KeyShape:              oci.KeyShape{Algorithm: "AES", Length: 32},
		IsAutoRotationEnabled: true, AutoKeyRotationDetails: &oci.RotationDetails{IntervalDays: 60},
	})
	require.NoError(t, err)
	require.NotNil(t, key.AutoKeyRotationDetails)

	e.settle()

	testsnap.Unchanged(t, key.AutoKeyRotationDetails, func() {
		e.clock.Advance(61 * 24 * time.Hour)
		e.backend.Sweep()
	})
}
