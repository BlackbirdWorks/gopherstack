package ocidatabase_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVaultRotationTarget(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.available(t, createOpts{})

	require.NoError(t, e.backend.VerifyAdb(t.Context(), *db.Id))
	require.Error(t, e.backend.VerifyAdb(t.Context(), "ocid1.autonomousdatabase.oc1..nope"))

	require.NoError(t, e.backend.SetAdbAdminPassword(t.Context(), *db.Id, "N3wPassw0rd_Adb1"))
	require.Error(t, e.backend.SetAdbAdminPassword(t.Context(), *db.Id, "weak"), "the password policy applies")
	require.Error(
		t,
		e.backend.SetAdbAdminPassword(t.Context(), "ocid1.autonomousdatabase.oc1..nope", "N3wPassw0rd_Adb1"),
	)
}
