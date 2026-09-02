package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"

	"time"
)

// TestKMSBackendRetireGrantAllPaths verifies all RetireGrant branches.
func TestKMSBackendRetireGrantAllPaths(t *testing.T) {
	t.Parallel()

	t.Run("by_grant_id", func(t *testing.T) {
		t.Parallel()

		b := kms.NewInMemoryBackend()
		keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
		require.NoError(t, err)

		grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
			KeyID:            keyOut.KeyMetadata.KeyID,
			GranteePrincipal: "arn:aws:iam::123:role/test",
			Operations:       []string{"Decrypt"},
		})
		require.NoError(t, err)

		require.NoError(t, b.RetireGrant(context.Background(), &kms.RetireGrantInput{
			GrantID: grantOut.GrantID,
		}))

		// Retiring again should fail
		err = b.RetireGrant(context.Background(), &kms.RetireGrantInput{GrantID: grantOut.GrantID})
		require.ErrorIs(t, err, kms.ErrGrantNotFound)
	})

	t.Run("by_grant_id_with_key", func(t *testing.T) {
		t.Parallel()

		b := kms.NewInMemoryBackend()
		keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
		require.NoError(t, err)

		grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
			KeyID:            keyOut.KeyMetadata.KeyID,
			GranteePrincipal: "arn:aws:iam::123:role/test",
			Operations:       []string{"Decrypt"},
		})
		require.NoError(t, err)

		require.NoError(t, b.RetireGrant(context.Background(), &kms.RetireGrantInput{
			GrantID: grantOut.GrantID,
			KeyID:   keyOut.KeyMetadata.KeyID,
		}))
	})

	t.Run("empty_grant_id_returns_error", func(t *testing.T) {
		t.Parallel()

		b := kms.NewInMemoryBackend()
		err := b.RetireGrant(context.Background(), &kms.RetireGrantInput{GrantID: ""})
		require.ErrorIs(t, err, kms.ErrGrantNotFound)
	})

	t.Run("wrong_key_id_returns_error", func(t *testing.T) {
		t.Parallel()

		b := kms.NewInMemoryBackend()
		key1, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
		require.NoError(t, err)
		key2, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
		require.NoError(t, err)

		grantOut, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
			KeyID:            key1.KeyMetadata.KeyID,
			GranteePrincipal: "arn:aws:iam::123:role/test",
			Operations:       []string{"Decrypt"},
		})
		require.NoError(t, err)

		err = b.RetireGrant(context.Background(), &kms.RetireGrantInput{
			GrantID: grantOut.GrantID,
			KeyID:   key2.KeyMetadata.KeyID,
		})
		require.ErrorIs(t, err, kms.ErrGrantNotFound)
	})
}

// TestKMSBackendListGrantsPagination verifies Limit/Marker pagination for ListGrants.
func TestKMSBackendListGrantsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		marker         string
		wantCount      int
		limit          int32
		wantTruncated  bool
		wantNextMarker bool
	}{
		{
			name:           "first_page",
			limit:          2,
			wantCount:      2,
			wantTruncated:  true,
			wantNextMarker: true,
		},
		{
			name:           "second_page_via_marker",
			limit:          2,
			marker:         "2",
			wantCount:      2,
			wantTruncated:  true,
			wantNextMarker: true,
		},
		{
			name:           "last_page",
			limit:          2,
			marker:         "4",
			wantCount:      1,
			wantTruncated:  false,
			wantNextMarker: false,
		},
		{
			name:      "marker_beyond_end",
			limit:     2,
			marker:    "100",
			wantCount: 0,
		},
		{
			name:           "invalid_marker_treated_as_start",
			limit:          2,
			marker:         "not-a-number",
			wantCount:      2,
			wantTruncated:  true,
			wantNextMarker: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			for range 5 {
				_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
					KeyID:            key.KeyMetadata.KeyID,
					GranteePrincipal: "arn:aws:iam::000000000000:role/r",
					Operations:       []string{"Decrypt"},
				})
				require.NoError(t, err)
			}

			lim := tt.limit
			out, err := b.ListGrants(context.Background(), &kms.ListGrantsInput{
				KeyID:  key.KeyMetadata.KeyID,
				Limit:  &lim,
				Marker: tt.marker,
			})
			require.NoError(t, err)
			assert.Len(t, out.Grants, tt.wantCount)
			assert.Equal(t, tt.wantTruncated, out.Truncated)

			if tt.wantNextMarker {
				assert.NotEmpty(t, out.NextMarker)
			} else {
				assert.Empty(t, out.NextMarker)
			}
		})
	}
}

// TestKMSBackendListRetirableGrantsPagination verifies filtering by RetiringPrincipal and pagination.
func TestKMSBackendListRetirableGrantsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		retiringPrincipal string
		marker            string
		wantCount         int
		limit             int32
		wantTruncated     bool
	}{
		{
			name:              "filters_by_retiring_principal",
			retiringPrincipal: "arn:aws:iam::000000000000:role/retiring",
			limit:             10,
			wantCount:         3,
		},
		{
			name:              "no_match",
			retiringPrincipal: "arn:aws:iam::000000000000:role/nobody",
			limit:             10,
			wantCount:         0,
		},
		{
			name:              "pagination_first_page",
			retiringPrincipal: "arn:aws:iam::000000000000:role/retiring",
			limit:             2,
			wantCount:         2,
			wantTruncated:     true,
		},
		{
			name:              "pagination_second_page",
			retiringPrincipal: "arn:aws:iam::000000000000:role/retiring",
			limit:             2,
			marker:            "2",
			wantCount:         1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			for range 3 {
				_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
					KeyID:             key.KeyMetadata.KeyID,
					GranteePrincipal:  "arn:aws:iam::000000000000:role/grantee",
					RetiringPrincipal: "arn:aws:iam::000000000000:role/retiring",
					Operations:        []string{"Decrypt"},
				})
				require.NoError(t, err)
			}

			// Grant with a different retiring principal — must not appear in results.
			_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
				KeyID:             key.KeyMetadata.KeyID,
				GranteePrincipal:  "arn:aws:iam::000000000000:role/grantee",
				RetiringPrincipal: "arn:aws:iam::000000000000:role/other",
				Operations:        []string{"Decrypt"},
			})
			require.NoError(t, err)

			lim := tt.limit
			out, err := b.ListRetirableGrants(context.Background(), &kms.ListRetirableGrantsInput{
				RetiringPrincipal: tt.retiringPrincipal,
				Limit:             &lim,
				Marker:            tt.marker,
			})
			require.NoError(t, err)
			assert.Len(t, out.Grants, tt.wantCount)
			assert.Equal(t, tt.wantTruncated, out.Truncated)
		})
	}
}

// TestKMSBackendListGrants_DefaultLimit_Is50 verifies the documented default
// page size when Limit is omitted: aws-sdk-go-v2/service/kms's
// ListGrantsInput.Limit doc comment says "If you do not include a value, it
// defaults to 50" (max 100) -- distinct from ListKeys/ListKeyPolicies/
// ListKeyRotations, whose documented default is 100.
func TestKMSBackendListGrants_DefaultLimit_Is50(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	for range 51 {
		_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
			KeyID:            key.KeyMetadata.KeyID,
			GranteePrincipal: "arn:aws:iam::000000000000:role/r",
			Operations:       []string{"Decrypt"},
		})
		require.NoError(t, err)
	}

	out, err := b.ListGrants(context.Background(), &kms.ListGrantsInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.Len(t, out.Grants, 50)
	assert.True(t, out.Truncated)
	assert.Equal(t, "50", out.NextMarker)
}

// TestKMSBackendListRetirableGrants_DefaultLimit_Is50 mirrors
// TestKMSBackendListGrants_DefaultLimit_Is50 for ListRetirableGrants, which
// documents the identical "defaults to 50" Limit semantics.
func TestKMSBackendListRetirableGrants_DefaultLimit_Is50(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	const retiringPrincipal = "arn:aws:iam::000000000000:role/retiring"

	for range 51 {
		_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
			KeyID:             key.KeyMetadata.KeyID,
			GranteePrincipal:  "arn:aws:iam::000000000000:role/grantee",
			RetiringPrincipal: retiringPrincipal,
			Operations:        []string{"Decrypt"},
		})
		require.NoError(t, err)
	}

	out, err := b.ListRetirableGrants(context.Background(), &kms.ListRetirableGrantsInput{
		RetiringPrincipal: retiringPrincipal,
	})
	require.NoError(t, err)
	assert.Len(t, out.Grants, 50)
	assert.True(t, out.Truncated)
	assert.Equal(t, "50", out.NextMarker)
}

func TestCreateGrant_PendingDeletion_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *kms.InMemoryBackend, keyID string)
		name  string
		state string
	}{
		{
			name:  "PendingDeletion",
			state: kms.KeyStatePendingDeletion,
			setup: func(t *testing.T, b *kms.InMemoryBackend, keyID string) {
				t.Helper()
				_, err := b.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
					KeyID:               keyID,
					PendingWindowInDays: 7,
				})
				require.NoError(t, err)
			},
		},
		{
			name:  "PendingImport",
			state: kms.KeyStatePendingImport,
			setup: func(t *testing.T, _ *kms.InMemoryBackend, _ string) {
				t.Helper()
				// EXTERNAL-origin keys start in PendingImport — use that key.
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := ops2NewBackend(t)

			var keyID string
			if tc.state == kms.KeyStatePendingImport {
				out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: "EXTERNAL"})
				require.NoError(t, err)
				keyID = out.KeyMetadata.KeyID
			} else {
				keyID = ops2MustCreateSymKey(t, b)
				tc.setup(t, b, keyID)
			}

			_, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
				KeyID:            keyID,
				GranteePrincipal: "arn:aws:iam::123456789012:role/TestRole",
				Operations:       []string{"Decrypt"},
			})
			require.Error(t, err)
			assert.ErrorIs(t, err, kms.ErrKeyInvalidState,
				"expected KMSInvalidStateException for key in %s", tc.state)
		})
	}
}

func TestCreateGrant_Disabled_Allowed(t *testing.T) {
	t.Parallel()
	// AWS allows CreateGrant on Disabled keys (only PendingDeletion/PendingImport are blocked).
	b := ops2NewBackend(t)
	keyID := ops2MustCreateSymKey(t, b)

	require.NoError(t, b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: keyID}))

	_, err := b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::123456789012:role/TestRole",
		Operations:       []string{"Decrypt"},
	})
	assert.NoError(t, err, "CreateGrant on Disabled key should succeed")
}

// Test_GrantTokenValidation_NonContextCryptoOps verifies that Sign, Verify,
// GetPublicKey, GenerateMac, VerifyMac, and DeriveSharedSecret each accept a valid
// grant token, reject a bogus one with InvalidGrantTokenException, and behave
// normally with no token at all. Before this fix, GrantTokens was not even a field
// on these input structs, so a caller-supplied token was silently discarded and
// never validated (a disguised stub: the grant system models these operations as
// grantable, but grant validity was never enforced for them).
func TestGrantTokenValidation_NonContextCryptoOps(t *testing.T) {
	t.Parallel()

	type grantScenario struct {
		wantErr error
		name    string
		token   string
	}

	scenarios := []grantScenario{
		{name: "no_grant_token_succeeds", token: "none"},
		{name: "valid_grant_token_succeeds", token: "valid"},
		{name: "bogus_grant_token_rejected", token: "bogus", wantErr: kms.ErrInvalidGrantToken},
	}

	type opCase struct {
		run  func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error
		name string
	}

	opCases := []opCase{
		{
			name: "Sign",
			run: func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error {
				t.Helper()

				keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageSignVerify,
					KeySpec:  "ECC_NIST_P256",
				})
				require.NoError(t, err)

				_, err = b.Sign(ctx, &kms.SignInput{
					KeyID:            keyOut.KeyMetadata.KeyID,
					Message:          []byte("sign me"),
					SigningAlgorithm: "ECDSA_SHA_256",
					GrantTokens:      tokens,
				})

				return err
			},
		},
		{
			name: "Verify",
			run: func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error {
				t.Helper()

				keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageSignVerify,
					KeySpec:  "ECC_NIST_P256",
				})
				require.NoError(t, err)

				signOut, err := b.Sign(ctx, &kms.SignInput{
					KeyID:            keyOut.KeyMetadata.KeyID,
					Message:          []byte("verify me"),
					SigningAlgorithm: "ECDSA_SHA_256",
				})
				require.NoError(t, err)

				_, err = b.Verify(ctx, &kms.VerifyInput{
					KeyID:            keyOut.KeyMetadata.KeyID,
					Message:          []byte("verify me"),
					Signature:        signOut.Signature,
					SigningAlgorithm: "ECDSA_SHA_256",
					GrantTokens:      tokens,
				})

				return err
			},
		},
		{
			name: "GetPublicKey",
			run: func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error {
				t.Helper()

				keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageSignVerify,
					KeySpec:  "ECC_NIST_P256",
				})
				require.NoError(t, err)

				_, err = b.GetPublicKey(ctx, &kms.GetPublicKeyInput{
					KeyID:       keyOut.KeyMetadata.KeyID,
					GrantTokens: tokens,
				})

				return err
			},
		},
		{
			name: "GenerateMac",
			run: func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error {
				t.Helper()

				keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageGenerateMac,
					KeySpec:  "HMAC_256",
				})
				require.NoError(t, err)

				_, err = b.GenerateMac(ctx, &kms.GenerateMacInput{
					KeyID:        keyOut.KeyMetadata.KeyID,
					Message:      []byte("mac me"),
					MacAlgorithm: "HMAC_SHA_256",
					GrantTokens:  tokens,
				})

				return err
			},
		},
		{
			name: "VerifyMac",
			run: func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error {
				t.Helper()

				keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageGenerateMac,
					KeySpec:  "HMAC_256",
				})
				require.NoError(t, err)

				macOut, err := b.GenerateMac(ctx, &kms.GenerateMacInput{
					KeyID:        keyOut.KeyMetadata.KeyID,
					Message:      []byte("mac me"),
					MacAlgorithm: "HMAC_SHA_256",
				})
				require.NoError(t, err)

				_, err = b.VerifyMac(ctx, &kms.VerifyMacInput{
					KeyID:        keyOut.KeyMetadata.KeyID,
					Message:      []byte("mac me"),
					MacAlgorithm: "HMAC_SHA_256",
					Mac:          macOut.Mac,
					GrantTokens:  tokens,
				})

				return err
			},
		},
		{
			name: "DeriveSharedSecret",
			run: func(t *testing.T, b *kms.InMemoryBackend, ctx context.Context, tokens []string) error {
				t.Helper()

				aliceOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageKeyAgreement,
					KeySpec:  "ECC_NIST_P256",
				})
				require.NoError(t, err)

				bobOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{
					KeyUsage: kms.KeyUsageKeyAgreement,
					KeySpec:  "ECC_NIST_P256",
				})
				require.NoError(t, err)

				bobPub, err := b.GetPublicKey(ctx, &kms.GetPublicKeyInput{KeyID: bobOut.KeyMetadata.KeyID})
				require.NoError(t, err)

				_, err = b.DeriveSharedSecret(ctx, &kms.DeriveSharedSecretInput{
					KeyID:                 aliceOut.KeyMetadata.KeyID,
					KeyAgreementAlgorithm: "ECDH",
					PublicKey:             bobPub.PublicKey,
					GrantTokens:           tokens,
				})

				return err
			},
		},
	}

	for _, op := range opCases {
		for _, sc := range scenarios {
			t.Run(op.name+"/"+sc.name, func(t *testing.T) {
				t.Parallel()

				ctx := context.Background()
				b := kms.NewInMemoryBackend()

				var tokens []string

				switch sc.token {
				case "valid":
					grantKeyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
					require.NoError(t, err)

					grantOut, err := b.CreateGrant(ctx, &kms.CreateGrantInput{
						KeyID:            grantKeyOut.KeyMetadata.KeyID,
						GranteePrincipal: "arn:aws:iam::000000000000:role/test",
						Operations: []string{
							"Sign", "Verify", "GetPublicKey", "GenerateMac", "VerifyMac", "DeriveSharedSecret",
						},
					})
					require.NoError(t, err)
					tokens = []string{grantOut.GrantToken}
				case "bogus":
					tokens = []string{"not-a-real-grant-token"}
				}

				err := op.run(t, b, ctx, tokens)

				if sc.wantErr != nil {
					require.Error(t, err)
					assert.ErrorIs(t, err, sc.wantErr, "op %s: got %v, want %v", op.name, err, sc.wantErr)

					return
				}

				require.NoError(t, err, "op %s", op.name)
			})
		}
	}
}

// Test_KMS_Janitor_PurgeKey_CleansGrantByKeyIndex verifies that permanently purging
// a key past its ScheduleKeyDeletion window also drops the key's grantsByKey
// secondary-index submap. Before the fix, purgeKey deleted the key's grants from
// the canonical grants map and grantsByToken, but left the (now permanently
// unreachable) grantsByKey[region][keyID] submap allocated forever — a real memory
// leak for any long-running instance that creates and deletes many keys with grants.
func TestKMS_Janitor_PurgeKey_CleansGrantByKeyIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := kms.NewInMemoryBackend()

	keyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	_, err = b.CreateGrant(ctx, &kms.CreateGrantInput{
		KeyID:            keyID,
		GranteePrincipal: "arn:aws:iam::000000000000:role/test",
		Operations:       []string{"Encrypt", "Decrypt"},
	})
	require.NoError(t, err)

	require.Equal(t, 1, kms.GrantsByKeyCount(b, kms.MockRegion, keyID), "grant index should be populated before purge")

	_, err = b.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{KeyID: keyID, PendingWindowInDays: 7})
	require.NoError(t, err)

	// Backdate the deletion so the janitor purges it on the next sweep.
	b.SetDeletionDateForTest(keyID, time.Now().Add(-time.Second))

	j := kms.NewJanitor(b, time.Minute)
	j.SweepOnce(ctx)

	_, err = b.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: keyID})
	require.Error(t, err, "key should be permanently purged")

	assert.Equal(t, 0, kms.GrantsByKeyCount(b, kms.MockRegion, keyID),
		"grantsByKey submap must be dropped, not merely emptied, after a permanent key purge")
}

func TestCreateGrantValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   kms.CreateGrantInput
		wantErr bool
	}{
		{
			name: "valid",
			input: kms.CreateGrantInput{
				GranteePrincipal: "arn:aws:iam::123456789012:user/alice",
				Operations:       []string{"Decrypt"},
			},
			wantErr: false,
		},
		{
			name: "empty_principal",
			input: kms.CreateGrantInput{
				GranteePrincipal: "",
				Operations:       []string{"Decrypt"},
			},
			wantErr: true,
		},
		{
			name: "whitespace_principal",
			input: kms.CreateGrantInput{
				GranteePrincipal: "   ",
				Operations:       []string{"Decrypt"},
			},
			wantErr: true,
		},
		{
			name: "empty_operations",
			input: kms.CreateGrantInput{
				GranteePrincipal: "arn:aws:iam::123456789012:user/bob",
				Operations:       []string{},
			},
			wantErr: true,
		},
		{
			name: "nil_operations",
			input: kms.CreateGrantInput{
				GranteePrincipal: "arn:aws:iam::123456789012:user/bob",
				Operations:       nil,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			tt.input.KeyID = key.KeyMetadata.KeyID

			_, grantErr := b.CreateGrant(context.Background(), &tt.input)

			if tt.wantErr {
				require.Error(t, grantErr)
				require.ErrorIs(t, grantErr, kms.ErrValidation)
			} else {
				require.NoError(t, grantErr)
			}
		})
	}
}
