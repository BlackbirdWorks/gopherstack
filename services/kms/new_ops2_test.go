package kms_test

import (
	"context"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// postKMSOp is a helper that sends a KMS operation request and returns the response.
func postKMSOp(t *testing.T, h *kms.Handler, operation, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "TrentService."+operation)
	c := e.NewContext(req, rec)

	_ = h.Handler()(c)

	return rec
}

// TestCustomKeyStore_CRUD verifies Create/Describe/Connect/Disconnect/Delete lifecycle.
func TestCustomKeyStore_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		steps   func(t *testing.T, b *kms.InMemoryBackend)
		name    string
		wantErr bool
	}{
		{
			name: "create_describe_connect_disconnect_delete",
			steps: func(t *testing.T, b *kms.InMemoryBackend) {
				t.Helper()

				// Create
				out, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
					CustomKeyStoreName: "test-store",
				})
				require.NoError(t, err)
				assert.NotEmpty(t, out.CustomKeyStoreID)
				storeID := out.CustomKeyStoreID

				// Describe – should find it
				desc, err := b.DescribeCustomKeyStores(context.Background(), &kms.DescribeCustomKeyStoresInput{
					CustomKeyStoreID: storeID,
				})
				require.NoError(t, err)
				require.Len(t, desc.CustomKeyStores, 1)
				assert.Equal(t, kms.ConnectionStateDisconnected, desc.CustomKeyStores[0].ConnectionState)

				// Connect
				require.NoError(t, b.ConnectCustomKeyStore(context.Background(), &kms.ConnectCustomKeyStoreInput{
					CustomKeyStoreID: storeID,
				}))

				// Describe – should be CONNECTED
				desc2, err := b.DescribeCustomKeyStores(context.Background(), &kms.DescribeCustomKeyStoresInput{
					CustomKeyStoreID: storeID,
				})
				require.NoError(t, err)
				assert.Equal(t, kms.ConnectionStateConnected, desc2.CustomKeyStores[0].ConnectionState)

				// Disconnect
				require.NoError(t, b.DisconnectCustomKeyStore(context.Background(), &kms.DisconnectCustomKeyStoreInput{
					CustomKeyStoreID: storeID,
				}))

				// Delete
				require.NoError(t, b.DeleteCustomKeyStore(context.Background(), &kms.DeleteCustomKeyStoreInput{
					CustomKeyStoreID: storeID,
				}))

				// Describe – should be empty
				desc3, err := b.DescribeCustomKeyStores(context.Background(), &kms.DescribeCustomKeyStoresInput{})
				require.NoError(t, err)
				assert.Empty(t, desc3.CustomKeyStores)
			},
		},
		{
			name: "delete_connected_returns_error",
			steps: func(t *testing.T, b *kms.InMemoryBackend) {
				t.Helper()

				out, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
					CustomKeyStoreName: "connected-store",
				})
				require.NoError(t, err)

				require.NoError(t, b.ConnectCustomKeyStore(context.Background(), &kms.ConnectCustomKeyStoreInput{
					CustomKeyStoreID: out.CustomKeyStoreID,
				}))

				err = b.DeleteCustomKeyStore(context.Background(), &kms.DeleteCustomKeyStoreInput{
					CustomKeyStoreID: out.CustomKeyStoreID,
				})
				require.Error(t, err)
			},
		},
		{
			name: "duplicate_name_returns_error",
			steps: func(t *testing.T, b *kms.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
					CustomKeyStoreName: "dup-store",
				})
				require.NoError(t, err)

				_, err = b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
					CustomKeyStoreName: "dup-store",
				})
				require.Error(t, err)
			},
		},
		{
			name: "connect_not_found_returns_error",
			steps: func(t *testing.T, b *kms.InMemoryBackend) {
				t.Helper()

				err := b.ConnectCustomKeyStore(context.Background(), &kms.ConnectCustomKeyStoreInput{
					CustomKeyStoreID: "nonexistent-id",
				})
				require.Error(t, err)
			},
		},
		{
			name: "disconnect_already_disconnected_returns_error",
			steps: func(t *testing.T, b *kms.InMemoryBackend) {
				t.Helper()

				out, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
					CustomKeyStoreName: "already-disconnected",
				})
				require.NoError(t, err)

				err = b.DisconnectCustomKeyStore(context.Background(), &kms.DisconnectCustomKeyStoreInput{
					CustomKeyStoreID: out.CustomKeyStoreID,
				})
				require.Error(t, err)
			},
		},
		{
			name: "describe_by_name",
			steps: func(t *testing.T, b *kms.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
					CustomKeyStoreName: "named-store",
				})
				require.NoError(t, err)

				desc, err := b.DescribeCustomKeyStores(context.Background(), &kms.DescribeCustomKeyStoresInput{
					CustomKeyStoreName: "named-store",
				})
				require.NoError(t, err)
				require.Len(t, desc.CustomKeyStores, 1)
				assert.Equal(t, "named-store", desc.CustomKeyStores[0].CustomKeyStoreName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			tt.steps(t, b)
		})
	}
}

// TestDeriveSharedSecret verifies ECDH key agreement between two KEY_AGREEMENT ECC keys.
func TestDeriveSharedSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keySpec string
		wantErr bool
	}{
		{name: "ecc_p256", keySpec: "ECC_NIST_P256", wantErr: false},
		{name: "ecc_p384", keySpec: "ECC_NIST_P384", wantErr: false},
		{name: "ecc_p521", keySpec: "ECC_NIST_P521", wantErr: false},
		{name: "wrong_usage_symmetric", keySpec: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()

			if tt.wantErr {
				// Use a symmetric (ENCRYPT_DECRYPT) key – should fail DeriveSharedSecret.
				symKey, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				require.NoError(t, err)

				// We also need a peer ECC key for its public key.
				peerB := kms.NewInMemoryBackend()
				peerOut, err := peerB.CreateKey(context.Background(), &kms.CreateKeyInput{
					KeySpec:  "ECC_NIST_P256",
					KeyUsage: kms.KeyUsageKeyAgreement,
				})
				require.NoError(t, err)

				pubOut, err := peerB.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: peerOut.KeyMetadata.KeyID})
				require.NoError(t, err)

				_, err = b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
					KeyID:                 symKey.KeyMetadata.KeyID,
					KeyAgreementAlgorithm: "ECDH",
					PublicKey:             pubOut.PublicKey,
				})
				require.Error(t, err)

				return
			}

			// Create a KEY_AGREEMENT ECC key in backend A.
			keyA, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
				KeySpec:  tt.keySpec,
				KeyUsage: kms.KeyUsageKeyAgreement,
			})
			require.NoError(t, err)

			// Create a KEY_AGREEMENT ECC key in backend B (peer).
			bPeer := kms.NewInMemoryBackend()
			keyB, err := bPeer.CreateKey(context.Background(), &kms.CreateKeyInput{
				KeySpec:  tt.keySpec,
				KeyUsage: kms.KeyUsageKeyAgreement,
			})
			require.NoError(t, err)

			// Get public keys.
			pubA, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: keyA.KeyMetadata.KeyID})
			require.NoError(t, err)

			pubB, err := bPeer.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: keyB.KeyMetadata.KeyID})
			require.NoError(t, err)

			// Derive shared secret from A's perspective (using B's public key).
			outA, err := b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
				KeyID:                 keyA.KeyMetadata.KeyID,
				KeyAgreementAlgorithm: "ECDH",
				PublicKey:             pubB.PublicKey,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, outA.SharedSecret)
			assert.Equal(t, "ECDH", outA.KeyAgreementAlgorithm)

			// Derive shared secret from B's perspective (using A's public key).
			outB, err := bPeer.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
				KeyID:                 keyB.KeyMetadata.KeyID,
				KeyAgreementAlgorithm: "ECDH",
				PublicKey:             pubA.PublicKey,
			})
			require.NoError(t, err)

			// Both sides should yield the same shared secret (ECDH property).
			assert.Equal(t, outA.SharedSecret, outB.SharedSecret)
		})
	}
}

// TestGenerateDataKeyPair verifies key pair generation with plaintext private key.
func TestGenerateDataKeyPair(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		keyPairSpec string
		wantErr     bool
	}{
		{name: "rsa_2048", keyPairSpec: "RSA_2048", wantErr: false},
		{name: "rsa_3072", keyPairSpec: "RSA_3072", wantErr: false},
		{name: "rsa_4096", keyPairSpec: "RSA_4096", wantErr: false},
		{name: "ecc_p256", keyPairSpec: "ECC_NIST_P256", wantErr: false},
		{name: "ecc_p384", keyPairSpec: "ECC_NIST_P384", wantErr: false},
		{name: "ecc_p521", keyPairSpec: "ECC_NIST_P521", wantErr: false},
		{name: "invalid_spec", keyPairSpec: "INVALID", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()

			// Create a symmetric wrapping key.
			wrapKey, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			out, err := b.GenerateDataKeyPair(context.Background(), &kms.GenerateDataKeyPairInput{
				KeyID:       wrapKey.KeyMetadata.KeyID,
				KeyPairSpec: tt.keyPairSpec,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, out.PublicKey)
			assert.NotEmpty(t, out.PrivateKeyPlaintext)
			assert.NotEmpty(t, out.PrivateKeyCiphertextBlob)
			assert.Equal(t, tt.keyPairSpec, out.KeyPairSpec)
			assert.NotEmpty(t, out.KeyID)

			// Verify the encrypted private key can be decrypted back to the plaintext.
			decOut, err := b.Decrypt(context.Background(), &kms.DecryptInput{
				CiphertextBlob: out.PrivateKeyCiphertextBlob,
			})
			require.NoError(t, err)
			assert.Equal(t, out.PrivateKeyPlaintext, decOut.Plaintext)
		})
	}
}

// TestGenerateDataKeyPairWithoutPlaintext verifies key pair generation without plaintext private key.
func TestGenerateDataKeyPairWithoutPlaintext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		keyPairSpec string
		wantErr     bool
	}{
		{name: "rsa_2048", keyPairSpec: "RSA_2048", wantErr: false},
		{name: "ecc_p256", keyPairSpec: "ECC_NIST_P256", wantErr: false},
		{name: "invalid_spec", keyPairSpec: "INVALID", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()

			wrapKey, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			out, err := b.GenerateDataKeyPairWithoutPlaintext(context.Background(), &kms.GenerateDataKeyPairWithoutPlaintextInput{
				KeyID:       wrapKey.KeyMetadata.KeyID,
				KeyPairSpec: tt.keyPairSpec,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, out.PublicKey)
			assert.NotEmpty(t, out.PrivateKeyCiphertextBlob)
			assert.Equal(t, tt.keyPairSpec, out.KeyPairSpec)
		})
	}
}

// TestGenerateMac verifies HMAC computation with HMAC KMS keys.
func TestGenerateMac(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		keySpec      string
		macAlgorithm string
		wantErr      bool
	}{
		{name: "hmac_256", keySpec: "HMAC_256", macAlgorithm: "HMAC_SHA_256", wantErr: false},
		{name: "hmac_384", keySpec: "HMAC_384", macAlgorithm: "HMAC_SHA_384", wantErr: false},
		{name: "hmac_512", keySpec: "HMAC_512", macAlgorithm: "HMAC_SHA_512", wantErr: false},
		{name: "wrong_usage_symmetric", keySpec: "SYMMETRIC_DEFAULT", macAlgorithm: "HMAC_SHA_256", wantErr: true},
		{name: "unsupported_algorithm", keySpec: "HMAC_256", macAlgorithm: "UNKNOWN_ALG", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()

			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{KeySpec: tt.keySpec})
			require.NoError(t, err)

			out, err := b.GenerateMac(context.Background(), &kms.GenerateMacInput{
				KeyID:        key.KeyMetadata.KeyID,
				MacAlgorithm: tt.macAlgorithm,
				Message:      []byte("hello world"),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, out.Mac)
			assert.Equal(t, tt.macAlgorithm, out.MacAlgorithm)
			assert.NotEmpty(t, out.KeyID)

			// Verify determinism: same message with same key produces same MAC.
			out2, err := b.GenerateMac(context.Background(), &kms.GenerateMacInput{
				KeyID:        key.KeyMetadata.KeyID,
				MacAlgorithm: tt.macAlgorithm,
				Message:      []byte("hello world"),
			})
			require.NoError(t, err)
			assert.Equal(t, out.Mac, out2.Mac)

			// Different message produces different MAC.
			out3, err := b.GenerateMac(context.Background(), &kms.GenerateMacInput{
				KeyID:        key.KeyMetadata.KeyID,
				MacAlgorithm: tt.macAlgorithm,
				Message:      []byte("different message"),
			})
			require.NoError(t, err)
			assert.NotEqual(t, out.Mac, out3.Mac)
		})
	}
}

// TestGenerateRandom verifies random byte generation.
func TestGenerateRandom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantLen       int
		numberOfBytes int32
		wantErr       bool
	}{
		{name: "default_32_bytes", wantLen: 32},
		{name: "explicit_64_bytes", numberOfBytes: 64, wantLen: 64},
		{name: "max_1024_bytes", numberOfBytes: 1024, wantLen: 1024},
		{name: "zero_returns_error", numberOfBytes: -1, wantErr: true},
		{name: "over_max_returns_error", numberOfBytes: 1025, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()

			input := &kms.GenerateRandomInput{}
			if tt.numberOfBytes > 0 {
				n := tt.numberOfBytes
				input.NumberOfBytes = &n
			} else if tt.numberOfBytes != 0 {
				// Negative means explicitly pass zero to trigger validation error.
				zero := int32(0)
				input.NumberOfBytes = &zero
			}

			out, err := b.GenerateRandom(context.Background(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Plaintext, tt.wantLen)

			// Two calls should produce different random bytes.
			out2, err := b.GenerateRandom(context.Background(), input)
			require.NoError(t, err)
			assert.NotEqual(t, out.Plaintext, out2.Plaintext)
		})
	}
}

// TestNewOpsHandler verifies the HTTP handler dispatches all 10 new operations correctly.
func TestNewOpsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn        func(*testing.T, *kms.InMemoryBackend) string
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		operation      string
		name           string
		body           string
		expectedStatus int
	}{
		{
			name:           "CreateCustomKeyStore",
			operation:      "CreateCustomKeyStore",
			body:           `{"CustomKeyStoreName":"test-store"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.CreateCustomKeyStoreOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.CustomKeyStoreID)
			},
		},
		{
			name:      "DescribeCustomKeyStores_empty",
			operation: "DescribeCustomKeyStores",
			body:      `{}`,
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				_, _ = b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{CustomKeyStoreName: "s1"})

				return ""
			},
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.DescribeCustomKeyStoresOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.CustomKeyStores, 1)
			},
		},
		{
			name:           "DeleteCustomKeyStore_not_found",
			operation:      "DeleteCustomKeyStore",
			body:           `{"CustomKeyStoreId":"nonexistent"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:      "ConnectCustomKeyStore",
			operation: "ConnectCustomKeyStore",
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				out, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
					CustomKeyStoreName: "c-store",
				})
				require.NoError(t, err)

				return out.CustomKeyStoreID
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:      "DisconnectCustomKeyStore",
			operation: "DisconnectCustomKeyStore",
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				out, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
					CustomKeyStoreName: "d-store",
				})
				require.NoError(t, err)
				_ = b.ConnectCustomKeyStore(context.Background(), &kms.ConnectCustomKeyStoreInput{
					CustomKeyStoreID: out.CustomKeyStoreID,
				})

				return out.CustomKeyStoreID
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "GenerateRandom",
			operation:      "GenerateRandom",
			body:           `{"NumberOfBytes":32}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.GenerateRandomOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.Plaintext, 32)
			},
		},
		{
			name:           "GenerateRandom_over_limit",
			operation:      "GenerateRandom",
			body:           `{"NumberOfBytes":2048}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "GenerateMac_success",
			operation:      "GenerateMac",
			expectedStatus: http.StatusOK,
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{KeySpec: "HMAC_256"})
				require.NoError(t, err)

				return key.KeyMetadata.KeyID
			},
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.GenerateMacOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.Mac)
			},
		},
		{
			name:           "GenerateDataKeyPair_success",
			operation:      "GenerateDataKeyPair",
			expectedStatus: http.StatusOK,
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				require.NoError(t, err)

				return key.KeyMetadata.KeyID
			},
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.GenerateDataKeyPairOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.PublicKey)
				assert.NotEmpty(t, out.PrivateKeyPlaintext)
			},
		},
		{
			name:           "GenerateDataKeyPairWithoutPlaintext_success",
			operation:      "GenerateDataKeyPairWithoutPlaintext",
			expectedStatus: http.StatusOK,
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				require.NoError(t, err)

				return key.KeyMetadata.KeyID
			},
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.GenerateDataKeyPairWithoutPlaintextOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.PublicKey)
				assert.NotEmpty(t, out.PrivateKeyCiphertextBlob)
			},
		},
		{
			name:           "DeriveSharedSecret_success",
			operation:      "DeriveSharedSecret",
			expectedStatus: http.StatusOK,
			setupFn: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
					KeySpec:  "ECC_NIST_P256",
					KeyUsage: kms.KeyUsageKeyAgreement,
				})
				require.NoError(t, err)

				return key.KeyMetadata.KeyID
			},
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out kms.DeriveSharedSecretOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.SharedSecret)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := kms.NewInMemoryBackend()
			h := kms.NewHandler(backend)

			var resourceID string
			if tt.setupFn != nil {
				resourceID = tt.setupFn(t, backend)
			}

			body := tt.body
			if body == "" {
				// Build body from resourceID if available.
				body = buildBodyFromResourceID(tt.operation, resourceID)
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "TrentService."+tt.operation)
			c := e.NewContext(req, rec)

			_ = h.Handler()(c)

			assert.Equal(t, tt.expectedStatus, rec.Code, "body: %s", rec.Body.String())
			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// buildBodyFromResourceID constructs a JSON body for an operation using the given resource ID.
func buildBodyFromResourceID(operation, resourceID string) string {
	if resourceID == "" {
		return `{}`
	}

	switch operation {
	case "ConnectCustomKeyStore", "DeleteCustomKeyStore", "DisconnectCustomKeyStore":
		body, _ := json.Marshal(map[string]string{"CustomKeyStoreId": resourceID})

		return string(body)
	case "GenerateMac":
		body, _ := json.Marshal(map[string]any{
			"KeyId":        resourceID,
			"MacAlgorithm": "HMAC_SHA_256",
			"Message":      []byte("test message"),
		})

		return string(body)
	case "GenerateDataKeyPair":
		body, _ := json.Marshal(map[string]any{
			"KeyId":       resourceID,
			"KeyPairSpec": "ECC_NIST_P256",
		})

		return string(body)
	case "GenerateDataKeyPairWithoutPlaintext":
		body, _ := json.Marshal(map[string]any{
			"KeyId":       resourceID,
			"KeyPairSpec": "ECC_NIST_P256",
		})

		return string(body)
	case "DeriveSharedSecret":
		// Need a peer public key; generate one in a fresh backend.
		peerBackend := kms.NewInMemoryBackend()
		peerKey, _ := peerBackend.CreateKey(context.Background(), &kms.CreateKeyInput{
			KeySpec:  "ECC_NIST_P256",
			KeyUsage: kms.KeyUsageKeyAgreement,
		})
		pubOut, _ := peerBackend.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{
			KeyID: peerKey.KeyMetadata.KeyID,
		})
		body, _ := json.Marshal(map[string]any{
			"KeyId":                 resourceID,
			"KeyAgreementAlgorithm": "ECDH",
			"PublicKey":             pubOut.PublicKey,
		})

		return string(body)
	}

	return `{}`
}

// TestCustomKeyStorePersistence verifies that custom key stores survive a snapshot/restore cycle.
func TestCustomKeyStorePersistence(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()

	out, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
		CustomKeyStoreName: "persist-store",
	})
	require.NoError(t, err)

	require.NoError(t, b.ConnectCustomKeyStore(context.Background(), &kms.ConnectCustomKeyStoreInput{
		CustomKeyStoreID: out.CustomKeyStoreID,
	}))

	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	b2 := kms.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	desc, err := b2.DescribeCustomKeyStores(context.Background(), &kms.DescribeCustomKeyStoresInput{
		CustomKeyStoreID: out.CustomKeyStoreID,
	})
	require.NoError(t, err)
	require.Len(t, desc.CustomKeyStores, 1)
	assert.Equal(t, kms.ConnectionStateConnected, desc.CustomKeyStores[0].ConnectionState)
	assert.Equal(t, "persist-store", desc.CustomKeyStores[0].CustomKeyStoreName)
}

// TestCreateKey_HMACSpec verifies that HMAC key specs are accepted by CreateKey.
func TestCreateKey_HMACSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keySpec  string
		keyUsage string
	}{
		{name: "hmac_256_default_usage", keySpec: "HMAC_256"},
		{name: "hmac_384_default_usage", keySpec: "HMAC_384"},
		{name: "hmac_512_default_usage", keySpec: "HMAC_512"},
		{name: "hmac_256_explicit_usage", keySpec: "HMAC_256", keyUsage: kms.KeyUsageGenerateMac},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
				KeySpec:  tt.keySpec,
				KeyUsage: tt.keyUsage,
			})
			require.NoError(t, err)
			assert.Equal(t, kms.KeyUsageGenerateMac, out.KeyMetadata.KeyUsage)
			assert.Equal(t, tt.keySpec, out.KeyMetadata.KeySpec)
		})
	}
}

// TestCreateKey_KeyAgreementSpec verifies that ECC KEY_AGREEMENT keys are accepted by CreateKey.
func TestCreateKey_KeyAgreementSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keySpec  string
		keyUsage string
	}{
		{name: "ecc_p256_key_agreement", keySpec: "ECC_NIST_P256", keyUsage: kms.KeyUsageKeyAgreement},
		{name: "ecc_p384_key_agreement", keySpec: "ECC_NIST_P384", keyUsage: kms.KeyUsageKeyAgreement},
		{name: "ecc_p521_key_agreement", keySpec: "ECC_NIST_P521", keyUsage: kms.KeyUsageKeyAgreement},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
				KeySpec:  tt.keySpec,
				KeyUsage: tt.keyUsage,
			})
			require.NoError(t, err)
			assert.Equal(t, kms.KeyUsageKeyAgreement, out.KeyMetadata.KeyUsage)
			assert.Equal(t, tt.keySpec, out.KeyMetadata.KeySpec)
		})
	}
}

// TestGetSupportedOperations_NewOps verifies the new operations appear in GetSupportedOperations.
func TestGetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	h := kms.NewHandler(b)
	ops := h.GetSupportedOperations()

	newOps := []string{
		"ConnectCustomKeyStore",
		"CreateCustomKeyStore",
		"DeleteCustomKeyStore",
		"DeriveSharedSecret",
		"DescribeCustomKeyStores",
		"DisconnectCustomKeyStore",
		"GetParametersForImport",
		"GenerateDataKeyPair",
		"GenerateDataKeyPairWithoutPlaintext",
		"GenerateMac",
		"GenerateRandom",
		"ListKeyPolicies",
		"ListKeyRotations",
		"ReplicateKey",
		"RotateKeyOnDemand",
		"UpdateCustomKeyStore",
		"UpdateKeyDescription",
		"UpdatePrimaryRegion",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op, "expected %s in GetSupportedOperations", op)
	}
}

// TestNewOpsHandlerReset verifies that custom key stores are cleared after Reset.
func TestNewOpsHandlerReset(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	h := kms.NewHandler(b)

	_, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
		CustomKeyStoreName: "to-be-reset",
	})
	require.NoError(t, err)

	h.Reset()

	e := echo.New()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "TrentService.DescribeCustomKeyStores")
	c := e.NewContext(req, rec)

	_ = h.Handler()(c)

	require.Equal(t, http.StatusOK, rec.Code)

	var out kms.DescribeCustomKeyStoresOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.CustomKeyStores)
}

// TestGenerateDataKeyPair_WrongKeyUsage verifies that a non-ENCRYPT_DECRYPT wrapping key returns an error.
func TestGenerateDataKeyPair_WrongKeyUsage(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()

	// Create a SIGN_VERIFY key – should not be usable as a wrapping key.
	signKey, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec: "RSA_2048",
	})
	require.NoError(t, err)

	_, err = b.GenerateDataKeyPair(context.Background(), &kms.GenerateDataKeyPairInput{
		KeyID:       signKey.KeyMetadata.KeyID,
		KeyPairSpec: "ECC_NIST_P256",
	})
	require.Error(t, err)
}

// TestGenerateMac_DisabledKey verifies that GenerateMac fails for a disabled key.
func TestGenerateMac_DisabledKey(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()

	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{KeySpec: "HMAC_256"})
	require.NoError(t, err)

	require.NoError(t, b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: key.KeyMetadata.KeyID}))

	_, err = b.GenerateMac(context.Background(), &kms.GenerateMacInput{
		KeyID:        key.KeyMetadata.KeyID,
		MacAlgorithm: "HMAC_SHA_256",
		Message:      []byte("test"),
	})
	require.Error(t, err)
}

// TestCustomKeyStoreHandler_ViaHTTP verifies the full HTTP round-trip for all custom key store ops.
func TestCustomKeyStoreHandler_ViaHTTP(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	h := kms.NewHandler(b)

	// Create via HTTP.
	rec := postKMSOp(t, h, "CreateCustomKeyStore", `{"CustomKeyStoreName":"http-store"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut kms.CreateCustomKeyStoreOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	storeID := createOut.CustomKeyStoreID

	// Connect via HTTP.
	connectBody, _ := json.Marshal(map[string]string{"CustomKeyStoreId": storeID})
	rec = postKMSOp(t, h, "ConnectCustomKeyStore", string(connectBody))
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe via HTTP – should be CONNECTED.
	descBody, _ := json.Marshal(map[string]string{"CustomKeyStoreId": storeID})
	rec = postKMSOp(t, h, "DescribeCustomKeyStores", string(descBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var descOut kms.DescribeCustomKeyStoresOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descOut))
	require.Len(t, descOut.CustomKeyStores, 1)
	assert.Equal(t, kms.ConnectionStateConnected, descOut.CustomKeyStores[0].ConnectionState)

	// Disconnect via HTTP.
	rec = postKMSOp(t, h, "DisconnectCustomKeyStore", string(connectBody))
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete via HTTP.
	deleteBody, _ := json.Marshal(map[string]string{"CustomKeyStoreId": storeID})
	rec = postKMSOp(t, h, "DeleteCustomKeyStore", string(deleteBody))
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestKMSBackendNewMaintenanceOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *kms.InMemoryBackend)
		name string
	}{
		{
			name: "GetParametersForImport",
			run: func(t *testing.T, b *kms.InMemoryBackend) {
				t.Helper()

				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
				require.NoError(t, err)

				out, err := b.GetParametersForImport(context.Background(), &kms.GetParametersForImportInput{KeyID: key.KeyMetadata.KeyID})
				require.NoError(t, err)
				assert.Equal(t, key.KeyMetadata.KeyID, out.KeyID)
				assert.NotEmpty(t, out.ImportToken)
				assert.NotEmpty(t, out.PublicKey)
			},
		},
		{
			name: "ListKeyPolicies",
			run: func(t *testing.T, b *kms.InMemoryBackend) {
				t.Helper()

				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				require.NoError(t, err)

				out, err := b.ListKeyPolicies(context.Background(), &kms.ListKeyPoliciesInput{KeyID: key.KeyMetadata.KeyID})
				require.NoError(t, err)
				assert.Equal(t, []string{"default"}, out.PolicyNames)
			},
		},
		{
			name: "ListKeyRotations_after_on_demand_rotation",
			run: func(t *testing.T, b *kms.InMemoryBackend) {
				t.Helper()

				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				require.NoError(t, err)

				_, err = b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: key.KeyMetadata.KeyID})
				require.NoError(t, err)

				out, err := b.ListKeyRotations(context.Background(), &kms.ListKeyRotationsInput{KeyID: key.KeyMetadata.KeyID})
				require.NoError(t, err)
				require.Len(t, out.Rotations, 1)
				assert.Positive(t, out.Rotations[0].RotationDate)
			},
		},
		{
			name: "ReplicateKey",
			run: func(t *testing.T, b *kms.InMemoryBackend) {
				t.Helper()

				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{MultiRegion: true})
				require.NoError(t, err)

				out, err := b.ReplicateKey(context.Background(), &kms.ReplicateKeyInput{
					KeyID:         key.KeyMetadata.KeyID,
					ReplicaRegion: "us-west-2",
				})
				require.NoError(t, err)
				assert.True(t, out.ReplicaKeyMetadata.MultiRegion)
				assert.Contains(t, out.ReplicaKeyMetadata.Arn, ":us-west-2:")
			},
		},
		{
			name: "UpdateCustomKeyStore",
			run: func(t *testing.T, b *kms.InMemoryBackend) {
				t.Helper()

				created, err := b.CreateCustomKeyStore(context.Background(), 
					&kms.CreateCustomKeyStoreInput{CustomKeyStoreName: "before-name"},
				)
				require.NoError(t, err)

				err = b.UpdateCustomKeyStore(context.Background(), &kms.UpdateCustomKeyStoreInput{
					CustomKeyStoreID:      created.CustomKeyStoreID,
					NewCustomKeyStoreName: "after-name",
				})
				require.NoError(t, err)

				desc, err := b.DescribeCustomKeyStores(context.Background(), &kms.DescribeCustomKeyStoresInput{
					CustomKeyStoreID: created.CustomKeyStoreID,
				})
				require.NoError(t, err)
				require.Len(t, desc.CustomKeyStores, 1)
				assert.Equal(t, "after-name", desc.CustomKeyStores[0].CustomKeyStoreName)
			},
		},
		{
			name: "UpdateKeyDescription",
			run: func(t *testing.T, b *kms.InMemoryBackend) {
				t.Helper()

				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				require.NoError(t, err)

				err = b.UpdateKeyDescription(context.Background(), &kms.UpdateKeyDescriptionInput{
					KeyID:       key.KeyMetadata.KeyID,
					Description: "updated description",
				})
				require.NoError(t, err)

				desc, err := b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: key.KeyMetadata.KeyID})
				require.NoError(t, err)
				assert.Equal(t, "updated description", desc.KeyMetadata.Description)
			},
		},
		{
			name: "UpdatePrimaryRegion",
			run: func(t *testing.T, b *kms.InMemoryBackend) {
				t.Helper()

				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{MultiRegion: true})
				require.NoError(t, err)

				err = b.UpdatePrimaryRegion(context.Background(), &kms.UpdatePrimaryRegionInput{
					KeyID:         key.KeyMetadata.KeyID,
					PrimaryRegion: "eu-west-1",
				})
				require.NoError(t, err)

				replica, err := b.ReplicateKey(context.Background(), &kms.ReplicateKeyInput{
					KeyID:         key.KeyMetadata.KeyID,
					ReplicaRegion: "us-west-2",
				})
				require.NoError(t, err)
				assert.True(t, replica.ReplicaKeyMetadata.MultiRegion)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, kms.NewInMemoryBackend())
		})
	}
}

func TestKMSHandlerNewMaintenanceOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, b *kms.InMemoryBackend) string
		name       string
		action     string
		wantField  string
		wantStatus int
	}{
		{
			name:   "GetParametersForImport",
			action: "GetParametersForImport",
			setup: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
				require.NoError(t, err)

				return `{"KeyId":"` + key.KeyMetadata.KeyID + `"}`
			},
			wantStatus: http.StatusOK,
			wantField:  "ImportToken",
		},
		{
			name:   "ListKeyPolicies",
			action: "ListKeyPolicies",
			setup: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				require.NoError(t, err)

				return `{"KeyId":"` + key.KeyMetadata.KeyID + `"}`
			},
			wantStatus: http.StatusOK,
			wantField:  "PolicyNames",
		},
		{
			name:   "RotateKeyOnDemand",
			action: "RotateKeyOnDemand",
			setup: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				require.NoError(t, err)

				return `{"KeyId":"` + key.KeyMetadata.KeyID + `"}`
			},
			wantStatus: http.StatusOK,
			wantField:  "KeyId",
		},
		{
			name:   "UpdateKeyDescription",
			action: "UpdateKeyDescription",
			setup: func(t *testing.T, b *kms.InMemoryBackend) string {
				t.Helper()
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				require.NoError(t, err)

				return `{"KeyId":"` + key.KeyMetadata.KeyID + `","Description":"handler update"}`
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			h := kms.NewHandler(b)
			body := tt.setup(t, b)

			rec := postKMSOp(t, h, tt.action, body)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField == "" {
				return
			}

			var payload map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
			_, ok := payload[tt.wantField]
			assert.True(t, ok)
		})
	}
}
