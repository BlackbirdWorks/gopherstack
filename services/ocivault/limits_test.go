package ocivault_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/keymanagement"
	"github.com/oracle/oci-go-sdk/v65/vault"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocivault"
)

func assertLimit(t *testing.T, err error) {
	t.Helper()

	se := svcErr(t, err)
	assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
	assert.Equal(t, "LimitExceeded", se.GetCode())
}

func TestVaultLimits(t *testing.T) {
	t.Parallel()

	t.Run("vaults per region", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})

		for i := range 10 {
			e.mkVault(t, fmt.Sprintf("limit-%d", i))
		}

		_, err := e.vaults.CreateVault(t.Context(), keymanagement.CreateVaultRequest{
			CompartmentId: new(testCompartment), DisplayName: new("over"),
			VaultType: keymanagement.CreateVaultDetailsVaultTypeDefault,
		})
		assertLimit(t, err)
	})

	t.Run("key versions per vault", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		v := e.mkVault(t, "versions")
		m := e.mgmt(t, v)
		k := e.mkKey(t, m, "aes", aes256(), "")

		for range 99 {
			_, err := m.CreateKeyVersion(t.Context(), keymanagement.CreateKeyVersionRequest{KeyId: k.Id})
			require.NoError(t, err)
		}

		_, err := m.CreateKeyVersion(t.Context(), keymanagement.CreateKeyVersionRequest{KeyId: k.Id})
		assertLimit(t, err)

		_, err = m.CreateKey(t.Context(), keymanagement.CreateKeyRequest{
			CompartmentId: new(testCompartment), DisplayName: new("sw"), KeyShape: aes256(),
			ProtectionMode: keymanagement.CreateKeyDetailsProtectionModeSoftware,
		})
		require.NoError(t, err, "software and hardware versions are counted separately")
	})

	t.Run("asymmetric versions count twice", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		v := e.mkVault(t, "asym")
		m := e.mgmt(t, v)
		k := e.mkKey(t, m, "rsa", shape(keymanagement.KeyShapeAlgorithmRsa, 256, ""), "")

		for range 49 {
			_, err := m.CreateKeyVersion(t.Context(), keymanagement.CreateKeyVersionRequest{KeyId: k.Id})
			require.NoError(t, err)
		}

		_, err := m.CreateKeyVersion(t.Context(), keymanagement.CreateKeyVersionRequest{KeyId: k.Id})
		assertLimit(t, err)
	})

	t.Run("secret versions per secret", func(t *testing.T) {
		t.Parallel()

		se := newSecretEnv(t)
		s := se.create(t, "versions", "v0", nil)

		for i := 1; i < 30; i++ {
			se.update(t, s.Id, vault.UpdateSecretDetails{SecretContent: content(fmt.Sprintf("v%d", i))})
		}

		_, err := se.e.vsecret.UpdateSecret(t.Context(), vault.UpdateSecretRequest{
			SecretId: s.Id, SecretContent: content("one too many"),
		})
		assertLimit(t, err)
	})

	t.Run("secrets per region", func(t *testing.T) {
		t.Parallel()

		se := newSecretEnv(t)
		se.e.backend.Sweep()

		for i := range 5000 {
			_, _, err := se.e.backend.CreateSecret(oci.CreateSecretIn{
				CompartmentID: testCompartment, VaultID: *se.v.Id, KeyID: *se.key.Id,
				SecretName: fmt.Sprintf("s%d", i), SecretContent: &oci.SecretContent{Content: "dg=="},
			})
			require.NoError(t, err)
		}

		_, _, err := se.e.backend.CreateSecret(oci.CreateSecretIn{
			CompartmentID: testCompartment, VaultID: *se.v.Id, KeyID: *se.key.Id,
			SecretName: "over", SecretContent: &oci.SecretContent{Content: "dg=="},
		})
		require.ErrorContains(t, err, "LimitExceeded")
	})
}
