package kms_test

// Parity sweep 3 (bd: gopherstack-42s) fixes verified here:
//   - Sign, Verify, GetPublicKey, GenerateMac, VerifyMac, and DeriveSharedSecret silently
//     dropped GrantTokens on the wire (no struct field) and never validated them, even
//     though all six are valid CreateGrant operations. A bogus/expired grant token was
//     accepted instead of rejected with InvalidGrantTokenException.
//   - GenerateDataKeyPair (and its WithoutPlaintext variant) accepted GrantTokens and
//     EncryptionContext on the wire but never enforced the grant's EncryptionContext
//     constraints, unlike Encrypt/Decrypt/GenerateDataKey/ReEncrypt.
//   - An unrecognized KeySpec/KeyPairSpec (CreateKey, GenerateDataKeyPair) classified as
//     500 InternalServiceError instead of 400 ValidationException, because the sentinel
//     error was never wrapped with ErrValidation.
//   - The KMS janitor's purgeKey permanently deleted a key without cleaning up its
//     grantsByKey secondary-index submap, leaking memory for the life of the process.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// Test_GrantTokenValidation_NonContextCryptoOps verifies that Sign, Verify,
// GetPublicKey, GenerateMac, VerifyMac, and DeriveSharedSecret each accept a valid
// grant token, reject a bogus one with InvalidGrantTokenException, and behave
// normally with no token at all. Before this fix, GrantTokens was not even a field
// on these input structs, so a caller-supplied token was silently discarded and
// never validated (a disguised stub: the grant system models these operations as
// grantable, but grant validity was never enforced for them).
func Test_GrantTokenValidation_NonContextCryptoOps(t *testing.T) {
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

// Test_GenerateDataKeyPair_GrantTokenConstraints verifies that GenerateDataKeyPair
// (and its WithoutPlaintext sibling) enforce a grant's EncryptionContextEquals
// constraint against the caller-supplied EncryptionContext, mirroring the behavior
// already proven for Encrypt/Decrypt/GenerateDataKey.
func Test_GenerateDataKeyPair_GrantTokenConstraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr       error
		callerContext map[string]string
		name          string
		withoutPT     bool
	}{
		{
			name:          "matching_context_succeeds",
			callerContext: map[string]string{"purpose": "test"},
		},
		{
			name:          "mismatched_context_rejected",
			callerContext: map[string]string{"purpose": "wrong"},
			wantErr:       kms.ErrKeyInvalidState,
		},
		{
			name:          "matching_context_succeeds_without_plaintext",
			callerContext: map[string]string{"purpose": "test"},
			withoutPT:     true,
		},
		{
			name:          "mismatched_context_rejected_without_plaintext",
			callerContext: map[string]string{"purpose": "wrong"},
			withoutPT:     true,
			wantErr:       kms.ErrKeyInvalidState,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := kms.NewInMemoryBackend()

			wrapKeyOut, err := b.CreateKey(ctx, &kms.CreateKeyInput{})
			require.NoError(t, err)

			grantOut, err := b.CreateGrant(ctx, &kms.CreateGrantInput{
				KeyID:            wrapKeyOut.KeyMetadata.KeyID,
				GranteePrincipal: "arn:aws:iam::000000000000:role/test",
				Operations:       []string{"GenerateDataKeyPair", "GenerateDataKeyPairWithoutPlaintext"},
				Constraints: &kms.GrantConstraints{
					EncryptionContextEquals: map[string]string{"purpose": "test"},
				},
			})
			require.NoError(t, err)

			var opErr error

			if tt.withoutPT {
				_, opErr = b.GenerateDataKeyPairWithoutPlaintext(ctx, &kms.GenerateDataKeyPairWithoutPlaintextInput{
					KeyID:             wrapKeyOut.KeyMetadata.KeyID,
					KeyPairSpec:       "ECC_NIST_P256",
					EncryptionContext: tt.callerContext,
					GrantTokens:       []string{grantOut.GrantToken},
				})
			} else {
				_, opErr = b.GenerateDataKeyPair(ctx, &kms.GenerateDataKeyPairInput{
					KeyID:             wrapKeyOut.KeyMetadata.KeyID,
					KeyPairSpec:       "ECC_NIST_P256",
					EncryptionContext: tt.callerContext,
					GrantTokens:       []string{grantOut.GrantToken},
				})
			}

			if tt.wantErr != nil {
				require.Error(t, opErr)
				assert.ErrorIs(t, opErr, tt.wantErr, "got %v, want %v", opErr, tt.wantErr)

				return
			}

			require.NoError(t, opErr)
		})
	}
}

// Test_KMS_InvalidKeySpec_Returns400ValidationException drives the full HTTP handler
// path to confirm an unrecognized KeySpec/KeyPairSpec surfaces as a 400
// ValidationException, not a 500 InternalServiceError. Before the fix,
// generateKeyMaterial's "unsupported key spec" sentinel was never wrapped with
// ErrValidation, so classifyKMSError fell through to its default 500 branch.
func Test_KMS_InvalidKeySpec_Returns400ValidationException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildBody    func(t *testing.T, wrapKeyID string) (target, body string)
		name         string
		needsWrapKey bool
	}{
		{
			name: "CreateKey_bogus_KeySpec",
			buildBody: func(t *testing.T, _ string) (string, string) {
				t.Helper()

				return "TrentService.CreateKey", `{"KeySpec":"BOGUS_SPEC"}`
			},
		},
		{
			name:         "GenerateDataKeyPair_bogus_KeyPairSpec",
			needsWrapKey: true,
			buildBody: func(t *testing.T, wrapKeyID string) (string, string) {
				t.Helper()

				body, err := json.Marshal(map[string]string{
					"KeyId":       wrapKeyID,
					"KeyPairSpec": "BOGUS_SPEC",
				})
				require.NoError(t, err)

				return "TrentService.GenerateDataKeyPair", string(body)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := kms.NewInMemoryBackend()
			h := kms.NewHandler(backend)

			var wrapKeyID string

			if tt.needsWrapKey {
				out, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})
				require.NoError(t, err)
				wrapKeyID = out.KeyMetadata.KeyID
			}

			target, body := tt.buildBody(t, wrapKeyID)

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			req.Header.Set("X-Amz-Target", target)

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			require.NoError(t, h.Handler()(c))
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp kms.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "ValidationException", errResp.Type)
		})
	}
}

// Test_KMS_Janitor_PurgeKey_CleansGrantByKeyIndex verifies that permanently purging
// a key past its ScheduleKeyDeletion window also drops the key's grantsByKey
// secondary-index submap. Before the fix, purgeKey deleted the key's grants from
// the canonical grants map and grantsByToken, but left the (now permanently
// unreachable) grantsByKey[region][keyID] submap allocated forever — a real memory
// leak for any long-running instance that creates and deletes many keys with grants.
func Test_KMS_Janitor_PurgeKey_CleansGrantByKeyIndex(t *testing.T) {
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

// Test_KMS_ErrorClassification_MissingTableEntries drives the full HTTP handler path
// to confirm two sentinels that were absent from kmsErrorTable now classify correctly
// instead of falling through to the generic 500 branch:
//   - ErrExpiredKeyMaterial (raised by checkKeyMaterialExpiry when an imported key's
//     material has passed its ValidTo) must surface as the real AWS
//     ExpiredImportTokenException (400, client fault), not a 500.
//   - ErrKeyMaterialUnavailable (raised when key material is absent, e.g. after
//     restoring an old-format snapshot that never persisted key material) must surface
//     as the real AWS KeyUnavailableException (500, server fault — matches the real
//     SDK's ErrorFault: Server for this exception), not the made-up "InternalServiceError"
//     type string that classifyKMSError's default branch used to emit (not a real KMS
//     exception name at all).
func Test_KMS_ErrorClassification_MissingTableEntries(t *testing.T) {
	t.Parallel()

	t.Run("expired_import_material", func(t *testing.T) {
		t.Parallel()

		e := echo.New()
		backend := kms.NewInMemoryBackend()
		h := kms.NewHandler(backend)

		keyOut, err := backend.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
		require.NoError(t, err)
		keyID := keyOut.KeyMetadata.KeyID

		mat := make([]byte, 32)
		for i := range mat {
			mat[i] = byte(i + 1)
		}
		pastTS := float64(time.Now().Add(-time.Hour).UnixNano()) / 1e9
		require.NoError(t, backend.ImportKeyMaterial(context.Background(), &kms.ImportKeyMaterialInput{
			KeyID:       keyID,
			KeyMaterial: mat,
			ValidTo:     pastTS,
		}))

		body, err := json.Marshal(kms.EncryptInput{KeyID: keyID, Plaintext: []byte("test")})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(body)))
		req.Header.Set("X-Amz-Target", "TrentService.Encrypt")
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(e.NewContext(req, rec)))

		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp kms.ErrorResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "ExpiredImportTokenException", errResp.Type)
	})

	t.Run("key_material_unavailable_after_restore", func(t *testing.T) {
		t.Parallel()

		orig := kms.NewInMemoryBackend()
		keyOut, err := orig.CreateKey(context.Background(), &kms.CreateKeyInput{})
		require.NoError(t, err)
		keyID := keyOut.KeyMetadata.KeyID

		snap := orig.Snapshot(t.Context())
		require.NotEmpty(t, snap)
		stripped := strings.ReplaceAll(string(snap), `"key_materials":`, `"_key_materials":`)

		restored := kms.NewInMemoryBackend()
		require.NoError(t, restored.Restore(t.Context(), []byte(stripped)))

		e := echo.New()
		h := kms.NewHandler(restored)

		body, err := json.Marshal(kms.EncryptInput{KeyID: keyID, Plaintext: []byte("test")})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(body)))
		req.Header.Set("X-Amz-Target", "TrentService.Encrypt")
		rec := httptest.NewRecorder()
		require.NoError(t, h.Handler()(e.NewContext(req, rec)))

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var errResp kms.ErrorResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "KeyUnavailableException", errResp.Type)
	})
}
