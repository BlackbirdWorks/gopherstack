package kms_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// sendKMSOp is a helper that posts a KMS JSON request to the handler and returns the response recorder.
func sendKMSOp(t *testing.T, h *kms.Handler, operation, body string) *httptest.ResponseRecorder {
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

// newTestBackend creates a fresh InMemoryBackend using default account/region.
func newTestBackend() *kms.InMemoryBackend {
	return kms.NewInMemoryBackend()
}

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &kms.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, kms.ErrNilAppContext)
}

func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	assert.Equal(t, 1, kms.KeyCount(b))

	b.CreateAlias(context.Background(), &kms.CreateAliasInput{
		AliasName:   "alias/test-reset",
		TargetKeyID: key.KeyMetadata.KeyID,
	})
	assert.Equal(t, 1, kms.AliasCount(b))

	b.Reset()
	assert.Equal(t, 0, kms.KeyCount(b))
	assert.Equal(t, 0, kms.AliasCount(b))
	assert.Equal(t, 0, kms.GrantCount(b))
	assert.Equal(t, 0, kms.CustomKeyStoreCount(b))
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	// Create a key via the handler.
	rec := sendKMSOp(t, h, "CreateKey", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, kms.KeyCount(b))

	h.Reset()
	assert.Equal(t, 0, kms.KeyCount(b))
}

func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)
	assert.Positive(t, kms.HandlerOpsLen(h), "handler should have pre-built dispatch ops")
}

func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	// Verify sorted.
	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(
			t,
			ops[i-1],
			ops[i],
			"GetSupportedOperations should be sorted; found %q > %q",
			ops[i-1],
			ops[i],
		)
	}

	// Spot-check expected operations.
	for _, expected := range []string{
		"CreateKey", "Encrypt", "Decrypt", "VerifyMac", "GenerateMac",
		"ConnectCustomKeyStore", "DeriveSharedSecret",
	} {
		assert.Contains(t, ops, expected)
	}
}

func TestRefinement1_ErrValidation_Mapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *kms.InMemoryBackend) string
		name  string
		op    string
	}{
		{
			name: "ScheduleKeyDeletion_too_few_days",
			op:   "ScheduleKeyDeletion",
			setup: func(b *kms.InMemoryBackend) string {
				key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
				if err != nil {
					return `{}`
				}
				body, _ := json.Marshal(map[string]any{"KeyId": key.KeyMetadata.KeyID, "PendingWindowInDays": 3})

				return string(body)
			},
		},
		{
			name:  "GenerateRandom_zero_bytes",
			op:    "GenerateRandom",
			setup: func(_ *kms.InMemoryBackend) string { return `{"NumberOfBytes":0}` },
		},
		{
			name:  "GenerateRandom_over_limit",
			op:    "GenerateRandom",
			setup: func(_ *kms.InMemoryBackend) string { return `{"NumberOfBytes":2048}` },
		},
		{
			name:  "CreateCustomKeyStore_empty_name",
			op:    "CreateCustomKeyStore",
			setup: func(_ *kms.InMemoryBackend) string { return `{"CustomKeyStoreName":""}` },
		},
		{
			name: "GenerateMac_empty_algorithm",
			op:   "GenerateMac",
			setup: func(_ *kms.InMemoryBackend) string {
				return `{"KeyId":"nonexistent","MacAlgorithm":"","Message":"dGVzdA=="}`
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			h := kms.NewHandler(b)
			body := tt.setup(b)
			rec := sendKMSOp(t, h, tt.op, body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "ValidationException")
		})
	}
}

func TestRefinement1_ErrCustomKeyStoreNotFound_Mapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
		body string
	}{
		{
			name: "ConnectCustomKeyStore_not_found",
			op:   "ConnectCustomKeyStore",
			body: `{"CustomKeyStoreId":"nonexistent"}`,
		},
		{
			name: "DisconnectCustomKeyStore_not_found",
			op:   "DisconnectCustomKeyStore",
			body: `{"CustomKeyStoreId":"nonexistent"}`,
		},
		{
			name: "DeleteCustomKeyStore_not_found",
			op:   "DeleteCustomKeyStore",
			body: `{"CustomKeyStoreId":"nonexistent"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			h := kms.NewHandler(b)
			rec := sendKMSOp(t, h, tt.op, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "CustomKeyStoreNotFoundException")
		})
	}
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	assert.Equal(t, 0, kms.KeyCount(b))
	assert.Equal(t, 0, kms.AliasCount(b))
	assert.Equal(t, 0, kms.GrantCount(b))
	assert.Equal(t, 0, kms.CustomKeyStoreCount(b))

	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	assert.Equal(t, 1, kms.KeyCount(b))

	err = b.CreateAlias(context.Background(), &kms.CreateAliasInput{
		AliasName:   "alias/test-count",
		TargetKeyID: key.KeyMetadata.KeyID,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, kms.AliasCount(b))

	_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            key.KeyMetadata.KeyID,
		GranteePrincipal: "arn:aws:iam::123456789012:user/alice",
		Operations:       []string{"Encrypt"},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, kms.GrantCount(b))

	_, err = b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
		CustomKeyStoreName: "test-store",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, kms.CustomKeyStoreCount(b))
}

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	assert.Equal(t, 0, kms.KeyCount(b))

	// AddKeyInternal
	testKey := &kms.Key{
		KeyID:    "test-key-id-1234-1234-1234-123456789012",
		Arn:      "arn:aws:kms:us-east-1:000000000000:key/test-key-id-1234-1234-1234-123456789012",
		KeyState: kms.KeyStateEnabled,
		KeyUsage: kms.KeyUsageEncryptDecrypt,
		KeySpec:  "SYMMETRIC_DEFAULT",
		Enabled:  true,
	}
	b.AddKeyInternal(testKey, nil)
	assert.Equal(t, 1, kms.KeyCount(b))

	// AddCustomKeyStoreInternal
	testStore := &kms.CustomKeyStore{
		CustomKeyStoreID:   "test-ks-id",
		CustomKeyStoreName: "seeded-store",
		ConnectionState:    kms.ConnectionStateDisconnected,
		CustomKeyStoreType: "AWS_CLOUDHSM",
	}
	b.AddCustomKeyStoreInternal(testStore)
	assert.Equal(t, 1, kms.CustomKeyStoreCount(b))

	// Verify describe finds it.
	out, err := b.DescribeCustomKeyStores(context.Background(), &kms.DescribeCustomKeyStoresInput{
		CustomKeyStoreID: "test-ks-id",
	})
	require.NoError(t, err)
	require.Len(t, out.CustomKeyStores, 1)
	assert.Equal(t, "seeded-store", out.CustomKeyStores[0].CustomKeyStoreName)
}

func TestRefinement1_KeyMetadata_Enabled(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	assert.True(t, out.KeyMetadata.Enabled, "newly created key should be Enabled=true in metadata")

	desc, err := b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: out.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.True(t, desc.KeyMetadata.Enabled)

	require.NoError(t, b.DisableKey(context.Background(), &kms.DisableKeyInput{KeyID: out.KeyMetadata.KeyID}))

	desc2, err := b.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: out.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.False(t, desc2.KeyMetadata.Enabled)
}

func TestRefinement1_KeyMetadata_MacAlgorithms(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{KeySpec: "HMAC_256"})
	require.NoError(t, err)
	assert.Equal(t, []string{"HMAC_SHA_256"}, out.KeyMetadata.MacAlgorithms)
	assert.Empty(t, out.KeyMetadata.EncryptionAlgorithms)
	assert.Empty(t, out.KeyMetadata.SigningAlgorithms)
}

func TestRefinement1_KeyMetadata_CustomerMasterKeySpec(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	assert.Equal(t, out.KeyMetadata.KeySpec, out.KeyMetadata.CustomerMasterKeySpec)
}

func TestRefinement1_KeyMetadata_RSA_ENCRYPT_DECRYPT(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "RSA_2048",
		KeyUsage: kms.KeyUsageEncryptDecrypt,
	})
	require.NoError(t, err)
	assert.Equal(t, kms.KeyUsageEncryptDecrypt, out.KeyMetadata.KeyUsage)
	assert.Contains(t, out.KeyMetadata.EncryptionAlgorithms, "RSAES_OAEP_SHA_256")
}

func TestRefinement1_GetPublicKey_KeyAgreement(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "ECC_NIST_P256",
		KeyUsage: kms.KeyUsageKeyAgreement,
	})
	require.NoError(t, err)

	pub, err := b.GetPublicKey(context.Background(), &kms.GetPublicKeyInput{KeyID: out.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.NotEmpty(t, pub.PublicKey)
	assert.Equal(t, kms.KeyUsageKeyAgreement, pub.KeyUsage)
	assert.Equal(t, []string{"ECDH"}, pub.KeyAgreementAlgorithms)
	assert.Empty(t, pub.SigningAlgorithms)
}

func TestRefinement1_RSA_OAEP_EncryptDecrypt(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "RSA_2048",
		KeyUsage: kms.KeyUsageEncryptDecrypt,
	})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	plaintext := []byte("hello RSA-OAEP world")
	encOut, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     keyID,
		Plaintext: plaintext,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, encOut.CiphertextBlob)

	decOut, err := b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: encOut.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, plaintext, decOut.Plaintext)
}

func TestRefinement1_VerifyMac_Success(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{KeySpec: "HMAC_256"})
	require.NoError(t, err)
	keyID := out.KeyMetadata.KeyID

	msg := []byte("test message")
	macOut, err := b.GenerateMac(context.Background(), &kms.GenerateMacInput{
		KeyID:        keyID,
		MacAlgorithm: "HMAC_SHA_256",
		Message:      msg,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, macOut.Mac)

	verOut, err := b.VerifyMac(context.Background(), &kms.VerifyMacInput{
		KeyID:        keyID,
		MacAlgorithm: "HMAC_SHA_256",
		Message:      msg,
		Mac:          macOut.Mac,
	})
	require.NoError(t, err)
	assert.True(t, verOut.MacValid)
	assert.Equal(t, "HMAC_SHA_256", verOut.MacAlgorithm)
}

func TestRefinement1_VerifyMac_WrongMac(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{KeySpec: "HMAC_256"})
	require.NoError(t, err)

	_, err = b.VerifyMac(context.Background(), &kms.VerifyMacInput{
		KeyID:        out.KeyMetadata.KeyID,
		MacAlgorithm: "HMAC_SHA_256",
		Message:      []byte("message"),
		Mac:          []byte("wrong-mac"),
	})
	require.ErrorIs(t, err, kms.ErrInvalidSignature)
}

func TestRefinement1_VerifyMac_EmptyAlgorithm(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.VerifyMac(context.Background(), &kms.VerifyMacInput{
		KeyID:        "any",
		MacAlgorithm: "",
		Message:      []byte("x"),
		Mac:          []byte("y"),
	})
	require.ErrorIs(t, err, kms.ErrValidation)
}

func TestRefinement1_VerifyMac_Handler(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{KeySpec: "HMAC_256"})
	require.NoError(t, err)

	msg := []byte("hello")
	macOut, err := b.GenerateMac(context.Background(), &kms.GenerateMacInput{
		KeyID:        key.KeyMetadata.KeyID,
		MacAlgorithm: "HMAC_SHA_256",
		Message:      msg,
	})
	require.NoError(t, err)

	bodyBytes, _ := json.Marshal(map[string]any{
		"KeyId":        key.KeyMetadata.KeyID,
		"MacAlgorithm": "HMAC_SHA_256",
		"Message":      msg,
		"Mac":          macOut.Mac,
	})
	rec := sendKMSOp(t, h, "VerifyMac", string(bodyBytes))
	require.Equal(t, http.StatusOK, rec.Code)

	var verOut kms.VerifyMacOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &verOut))
	assert.True(t, verOut.MacValid)
}

func TestRefinement1_DeriveSharedSecret_InvalidAlgorithm(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	out, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "ECC_NIST_P256",
		KeyUsage: kms.KeyUsageKeyAgreement,
	})
	require.NoError(t, err)

	_, err = b.DeriveSharedSecret(context.Background(), &kms.DeriveSharedSecretInput{
		KeyID:                 out.KeyMetadata.KeyID,
		KeyAgreementAlgorithm: "RSA",
		PublicKey:             []byte("fake"),
	})
	require.ErrorIs(t, err, kms.ErrValidation)
}

func TestRefinement1_GenerateDataKeyPair_EmptySpec(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	_, err = b.GenerateDataKeyPair(context.Background(), &kms.GenerateDataKeyPairInput{
		KeyID:       key.KeyMetadata.KeyID,
		KeyPairSpec: "",
	})
	require.ErrorIs(t, err, kms.ErrValidation)
}

func TestRefinement1_CreateCustomKeyStore_EmptyName(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
		CustomKeyStoreName: "",
	})
	require.ErrorIs(t, err, kms.ErrValidation)
}

func TestRefinement1_CreateCustomKeyStore_InvalidType(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	_, err := b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
		CustomKeyStoreName: "my-store",
		CustomKeyStoreType: "INVALID_TYPE",
	})
	require.ErrorIs(t, err, kms.ErrValidation)
}

func TestRefinement1_ConnectCustomKeyStore_EmptyID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	err := b.ConnectCustomKeyStore(context.Background(), &kms.ConnectCustomKeyStoreInput{CustomKeyStoreID: ""})
	require.ErrorIs(t, err, kms.ErrValidation)
}

func TestRefinement1_DisconnectCustomKeyStore_EmptyID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	err := b.DisconnectCustomKeyStore(context.Background(), &kms.DisconnectCustomKeyStoreInput{CustomKeyStoreID: ""})
	require.ErrorIs(t, err, kms.ErrValidation)
}

func TestRefinement1_DeleteCustomKeyStore_EmptyID(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	err := b.DeleteCustomKeyStore(context.Background(), &kms.DeleteCustomKeyStoreInput{CustomKeyStoreID: ""})
	require.ErrorIs(t, err, kms.ErrValidation)
}

func TestRefinement1_ErrCustomKeyStoreNotFound_Sentinel(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	err := b.ConnectCustomKeyStore(
		context.Background(),
		&kms.ConnectCustomKeyStoreInput{CustomKeyStoreID: "no-such-id"},
	)
	require.ErrorIs(t, err, kms.ErrCustomKeyStoreNotFound)
}

func TestRefinement1_ScheduleKeyDeletion_UsesErrValidation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	_, err = b.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
		KeyID:               key.KeyMetadata.KeyID,
		PendingWindowInDays: 3,
	})
	require.ErrorIs(t, err, kms.ErrValidation)
}

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := newTestBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	desc, err := b2.DescribeKey(context.Background(), &kms.DescribeKeyInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.Equal(t, key.KeyMetadata.KeyID, desc.KeyMetadata.KeyID)
	assert.True(t, desc.KeyMetadata.Enabled)

	// Encryption should still work after restore.
	encOut, err := b2.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("after restore"),
	})
	require.NoError(t, err)

	decOut, err := b2.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: encOut.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("after restore"), decOut.Plaintext)
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	for range 3 {
		_, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
		require.NoError(t, err)
		b.Reset()
		assert.Equal(t, 0, kms.KeyCount(b))
	}
}

func TestRefinement1_GenerateRandom_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		n       *int32
		name    string
		wantErr bool
	}{
		{name: "valid_32", n: func() *int32 {
			v := int32(32)

			return &v
		}()},
		{name: "valid_1024", n: func() *int32 {
			v := int32(1024)

			return &v
		}()},
		{name: "zero", n: func() *int32 {
			v := int32(0)

			return &v
		}(), wantErr: true},
		{name: "negative", n: func() *int32 {
			v := int32(-1)

			return &v
		}(), wantErr: true},
		{name: "over_max", n: func() *int32 {
			v := int32(1025)

			return &v
		}(), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			out, err := b.GenerateRandom(context.Background(), &kms.GenerateRandomInput{NumberOfBytes: tt.n})

			if tt.wantErr {
				require.ErrorIs(t, err, kms.ErrValidation)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Plaintext, int(*tt.n))
		})
	}
}
