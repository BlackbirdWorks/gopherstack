package ocivault_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/keymanagement"
	"github.com/oracle/oci-go-sdk/v65/vault"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

func TestCreateRetryToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, se *secretEnv, name string) (string, error)
		name   string
	}{
		{
			name: "vault",
			create: func(t *testing.T, se *secretEnv, name string) (string, error) {
				t.Helper()

				r, err := se.e.vaults.CreateVault(t.Context(), keymanagement.CreateVaultRequest{
					OpcRetryToken: new("tok"),
					CompartmentId: new(testCompartment), DisplayName: new(name),
					VaultType: keymanagement.CreateVaultDetailsVaultTypeDefault,
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
		},
		{
			name: "key",
			create: func(t *testing.T, se *secretEnv, name string) (string, error) {
				t.Helper()

				r, err := se.m.CreateKey(t.Context(), keymanagement.CreateKeyRequest{
					OpcRetryToken: new("tok"),
					CompartmentId: new(testCompartment), DisplayName: new(name), KeyShape: aes256(),
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
		},
		{
			name: "secret",
			create: func(t *testing.T, se *secretEnv, name string) (string, error) {
				t.Helper()

				r, err := se.e.vsecret.CreateSecret(t.Context(), vault.CreateSecretRequest{
					OpcRetryToken: new("tok"),
					CompartmentId: new(testCompartment), VaultId: se.v.Id, KeyId: se.key.Id,
					SecretName: new(name), SecretContent: content("v1"),
				})
				if err != nil {
					return "", err
				}

				return *r.Id, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" replay", func(t *testing.T) {
			t.Parallel()

			se := newSecretEnv(t)
			id1, err := tt.create(t, se, "rt")
			require.NoError(t, err)

			id2, err := tt.create(t, se, "rt")
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
		})

		t.Run(tt.name+" restored", func(t *testing.T) {
			t.Parallel()

			se := newSecretEnv(t)
			id1, err := tt.create(t, se, "rt")
			require.NoError(t, err)

			fresh := newEnv(t, envOpts{})
			require.NoError(t, fresh.backend.Restore(t.Context(), se.e.backend.Snapshot(t.Context())))

			again := &secretEnv{e: fresh, v: se.v, key: se.key, m: fresh.mgmt(t, se.v)}
			id2, err := tt.create(t, again, "rt")
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
		})

		t.Run(tt.name+" expired", func(t *testing.T) {
			t.Parallel()

			se := newSecretEnv(t)
			id1, err := tt.create(t, se, "rt")
			require.NoError(t, err)

			se.e.clock.Advance(25 * time.Hour)

			id2, err := tt.create(t, se, "rt2")
			require.NoError(t, err)
			assert.NotEqual(t, id1, id2)
		})
	}
}

func TestTagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		free   map[string]string
		defd   map[string]map[string]any
		name   string
		code   string
		status int
	}{
		{
			name: "valid", free: map[string]string{"a": "b"},
			defd: map[string]map[string]any{"Ops": {"Cost": "1"}}, status: http.StatusOK,
		},
		{
			name: "long value", free: map[string]string{"a": strings.Repeat("x", 257)},
			status: http.StatusBadRequest, code: "InvalidParameter",
		},
		{
			name: "unknown namespace", defd: map[string]map[string]any{"Nope": {"k": "v"}},
			status: http.StatusNotFound, code: "NotAuthorizedOrNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se := newSecretEnv(t)
			se.e.backend.SetTagLookup(func(ns, key string) (ocitags.Def, bool) {
				return ocitags.Def{}, strings.EqualFold(ns, "ops") && strings.EqualFold(key, "cost")
			})

			sec := se.create(t, "base", "v1", nil)

			check := func(err error) {
				t.Helper()

				if tt.code == "" {
					require.NoError(t, err)

					return
				}

				assert.Equal(t, tt.code, svcErr(t, err).GetCode())
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
			}

			_, err := se.e.vaults.CreateVault(t.Context(), keymanagement.CreateVaultRequest{
				CompartmentId: new(testCompartment), DisplayName: new("tagged"),
				VaultType:    keymanagement.CreateVaultDetailsVaultTypeDefault,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = se.m.CreateKey(t.Context(), keymanagement.CreateKeyRequest{
				CompartmentId: new(testCompartment), DisplayName: new("taggedkey"), KeyShape: aes256(),
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = se.e.vsecret.CreateSecret(t.Context(), vault.CreateSecretRequest{
				CompartmentId: new(testCompartment), VaultId: se.v.Id, KeyId: se.key.Id,
				SecretName: new("taggedsecret"), SecretContent: content("v1"),
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = se.e.vaults.UpdateVault(t.Context(), keymanagement.UpdateVaultRequest{
				VaultId:      se.v.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = se.m.UpdateKey(t.Context(), keymanagement.UpdateKeyRequest{
				KeyId:        se.key.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = se.e.vsecret.UpdateSecret(t.Context(), vault.UpdateSecretRequest{
				SecretId:     sec.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)
		})
	}
}
