package azurekeyvault_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurekeyvault"
)

func TestBackend_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := azurekeyvault.NewInMemoryBackend(true)
	base := "https://localhost:10004"

	sec, err := b.SetSecret("v", base, "s", &azurekeyvault.SetSecretRequest{Value: new("val")})
	require.NoError(t, err)

	key, err := b.CreateKey("v", base, "k", &azurekeyvault.CreateKeyRequest{Kty: "EC"})
	require.NoError(t, err)

	_, err = b.CreateCertificate("v", base, "c", &azurekeyvault.CreateCertRequest{Policy: &azurekeyvault.CertPolicy{
		X509Props: &azurekeyvault.X509Props{Subject: "CN=persist"},
	}})
	require.NoError(t, err)

	_, err = b.SetIssuer("v", base, "iss", &azurekeyvault.Issuer{Provider: "P"})
	require.NoError(t, err)

	_, err = b.SetContacts("v", base, &azurekeyvault.Contacts{Contacts: []azurekeyvault.Contact{{Email: "a@b.c"}}})
	require.NoError(t, err)

	_, err = b.DeleteSecret("v", base, "s")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())

	restored := azurekeyvault.NewInMemoryBackend(true)
	require.NoError(t, restored.Restore(t.Context(), snap))

	gotDeleted, err := restored.GetDeletedSecret("v", base, "s")
	require.NoError(t, err)
	assert.Equal(t, "val", *gotDeleted.Value)
	assert.Equal(t, *sec.Value, *gotDeleted.Value)

	gotKey, err := restored.GetKey("v", base, "k", "")
	require.NoError(t, err)
	assert.Equal(t, key.Key.X, gotKey.Key.X)

	gotCert, err := restored.GetCertificate("v", base, "c", "")
	require.NoError(t, err)
	assert.NotEmpty(t, gotCert.Cer)

	_, err = restored.GetIssuer("v", base, "iss")
	require.NoError(t, err)

	ct, err := restored.GetContacts("v", base)
	require.NoError(t, err)
	assert.Len(t, ct.Contacts, 1)

	pol, err := restored.GetRotationPolicy("v", base, "k")
	require.NoError(t, err)
	assert.NotEmpty(t, pol.LifetimeActions)
}

func TestBackend_RestoreRejectsBadSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{"null_vault", `{"vaults":{"v":null},"version":1}`, true},
		{"old_version_discarded", `{"vaults":{"v":{"name":"v"}},"version":99}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurekeyvault.NewInMemoryBackend(true)
			err := b.Restore(t.Context(), []byte(tt.data))

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.False(t, b.VaultExists("v"))
		})
	}
}

func TestBackend_SoftDeleteExpiryAndProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		protect      bool
		wantPurgeErr bool
	}{
		{"purgeable", false, false},
		{"protected", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := azurekeyvault.NewInMemoryBackend(false)
			b.CreateVault("v", azurekeyvault.VaultOptions{RetentionDays: 7, PurgeProtection: tt.protect})

			now := time.Unix(1_800_000_000, 0)
			azurekeyvault.SetClock(b, func() time.Time { return now })

			_, err := b.SetSecret("v", "", "s", &azurekeyvault.SetSecretRequest{Value: new("x")})
			require.NoError(t, err)

			del, err := b.DeleteSecret("v", "", "s")
			require.NoError(t, err)
			assert.Equal(t, now.Unix()+7*86400, *del.ScheduledPurgeDate)

			err = b.PurgeSecret("v", "s")
			assert.Equal(t, tt.wantPurgeErr, err != nil)

			now = now.Add(8 * 24 * time.Hour)

			_, err = b.GetDeletedSecret("v", "", "s")
			require.Error(t, err, "scheduled purge date passed")

			_, err = b.SetSecret("v", "", "s", &azurekeyvault.SetSecretRequest{Value: new("again")})
			require.NoError(t, err, "name reusable after retention")
		})
	}
}

func TestBackend_UnknownVault(t *testing.T) {
	t.Parallel()

	b := azurekeyvault.NewInMemoryBackend(false)
	_, err := b.SetSecret("missing", "", "s", &azurekeyvault.SetSecretRequest{Value: new("x")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "VaultNotFound")

	b.CreateVault("missing", azurekeyvault.VaultOptions{})
	_, err = b.SetSecret("MISSING", "", "s", &azurekeyvault.SetSecretRequest{Value: new("x")})
	require.NoError(t, err, "vault names are case-insensitive")

	b.DeleteVault("missing")
	assert.False(t, b.VaultExists("missing"))
}

func TestKeyOperation_SymmetricRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		alg   string
		value string
		bits  int
	}{
		{"A128GCM", "hello", 128},
		{"A256GCM", "hello", 256},
		{"A192CBCPAD", "hello world!", 192},
		{"A256KW", "0123456789abcdef", 256},
	}

	for _, tt := range tests {
		t.Run(tt.alg, func(t *testing.T) {
			t.Parallel()

			b := azurekeyvault.NewInMemoryBackend(true)
			_, err := b.CreateKey("v", "", "oct", &azurekeyvault.CreateKeyRequest{Kty: "oct-HSM", KeySize: tt.bits})
			require.NoError(t, err)

			enc, err := b.KeyOperation("v", "", "oct", "", "encrypt", &azurekeyvault.KeyOperationRequest{
				Alg: tt.alg, Value: azurekeyvault.B64URL([]byte(tt.value)),
			})
			require.NoError(t, err)

			res, ok := enc.(azurekeyvault.KeyOperationResult)
			require.True(t, ok)

			dec, err := b.KeyOperation("v", "", "oct", "", "decrypt", &azurekeyvault.KeyOperationRequest{
				Alg: tt.alg, Value: res.Value, IV: res.IV, Tag: res.Tag,
			})
			require.NoError(t, err)

			got, ok := dec.(azurekeyvault.KeyOperationResult)
			require.True(t, ok)
			assert.Equal(t, azurekeyvault.B64URL([]byte(tt.value)), got.Value)
		})
	}
}
