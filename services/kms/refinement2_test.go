package kms_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// ---------------------------------------------------------------------------
// Encrypt / Decrypt EncryptionAlgorithm field
// ---------------------------------------------------------------------------

func TestEncryptionAlgorithmField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keySpec  string
		keyUsage string
		wantAlgo string
	}{
		{
			name:     "symmetric_default",
			keySpec:  "SYMMETRIC_DEFAULT",
			wantAlgo: "SYMMETRIC_DEFAULT",
		},
		{
			name:     "rsa_2048",
			keySpec:  "RSA_2048",
			keyUsage: "ENCRYPT_DECRYPT",
			wantAlgo: "RSAES_OAEP_SHA_256",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			keyOut, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
				KeySpec:  tt.keySpec,
				KeyUsage: tt.keyUsage,
			})
			require.NoError(t, err)

			encOut, err := b.Encrypt(context.Background(), &kms.EncryptInput{
				KeyID:     keyOut.KeyMetadata.KeyID,
				Plaintext: []byte("hello"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantAlgo, encOut.EncryptionAlgorithm)

			decOut, err := b.Decrypt(context.Background(), &kms.DecryptInput{
				CiphertextBlob: encOut.CiphertextBlob,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantAlgo, decOut.EncryptionAlgorithm)
		})
	}
}

func TestReEncryptAlgorithmFields(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()

	src, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	dst, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: src.KeyMetadata.KeyID, Plaintext: []byte("data")})
	require.NoError(t, err)

	out, err := b.ReEncrypt(context.Background(), &kms.ReEncryptInput{
		CiphertextBlob:   enc.CiphertextBlob,
		DestinationKeyID: dst.KeyMetadata.KeyID,
	})
	require.NoError(t, err)
	assert.Equal(t, "SYMMETRIC_DEFAULT", out.SourceEncryptionAlgorithm)
	assert.Equal(t, "SYMMETRIC_DEFAULT", out.DestinationEncryptionAlgorithm)
}

// ---------------------------------------------------------------------------
// Decrypt: KeyID hint validation
// ---------------------------------------------------------------------------

func TestDecryptKeyIDHint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hintFn  func(correctID, otherID string) string
		name    string
		wantErr bool
	}{
		{
			name:    "matching_hint",
			hintFn:  func(correctID, _ string) string { return correctID },
			wantErr: false,
		},
		{
			name:    "wrong_hint",
			hintFn:  func(_, otherID string) string { return otherID },
			wantErr: true,
		},
		{
			name:    "no_hint",
			hintFn:  func(_, _ string) string { return "" },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			k1, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)
			k2, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
				KeyID:     k1.KeyMetadata.KeyID,
				Plaintext: []byte("secret"),
			})
			require.NoError(t, err)

			hint := tt.hintFn(k1.KeyMetadata.KeyID, k2.KeyMetadata.KeyID)

			_, decErr := b.Decrypt(context.Background(), &kms.DecryptInput{
				CiphertextBlob: enc.CiphertextBlob,
				KeyID:          hint,
			})

			if tt.wantErr {
				require.Error(t, decErr)
			} else {
				require.NoError(t, decErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CreateGrant: required field validation
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// UpdateKeyDescription: max length validation
// ---------------------------------------------------------------------------

func TestUpdateKeyDescriptionMaxLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		descLen int
		wantErr bool
	}{
		{name: "at_limit", descLen: 8192, wantErr: false},
		{name: "under_limit", descLen: 100, wantErr: false},
		{name: "over_limit", descLen: 8193, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			desc := strings.Repeat("x", tt.descLen)
			updateErr := b.UpdateKeyDescription(context.Background(), &kms.UpdateKeyDescriptionInput{
				KeyID:       key.KeyMetadata.KeyID,
				Description: desc,
			})

			if tt.wantErr {
				require.Error(t, updateErr)
				require.ErrorIs(t, updateErr, kms.ErrValidation)
			} else {
				require.NoError(t, updateErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CreateKey: description max length + HMAC no MultiRegion + tag count
// ---------------------------------------------------------------------------

func TestCreateKeyValidations(t *testing.T) {
	t.Parallel()

	makeTags := func(n int) []kms.Tag {
		tags := make([]kms.Tag, n)
		for i := range tags {
			tags[i] = kms.Tag{TagKey: "k", TagValue: "v"}
		}

		return tags
	}

	tests := []struct {
		name    string
		input   kms.CreateKeyInput
		wantErr bool
	}{
		{
			name:    "desc_too_long",
			input:   kms.CreateKeyInput{Description: strings.Repeat("a", 8193)},
			wantErr: true,
		},
		{
			name:    "desc_at_limit",
			input:   kms.CreateKeyInput{Description: strings.Repeat("a", 8192)},
			wantErr: false,
		},
		{
			name: "hmac_multiregion",
			input: kms.CreateKeyInput{
				KeySpec:     "HMAC_256",
				KeyUsage:    "GENERATE_VERIFY_MAC",
				MultiRegion: true,
			},
			wantErr: true,
		},
		{
			name:    "too_many_tags",
			input:   kms.CreateKeyInput{Tags: makeTags(51)},
			wantErr: true,
		},
		{
			name:    "exactly_50_tags",
			input:   kms.CreateKeyInput{Tags: makeTags(50)},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			_, err := b.CreateKey(context.Background(), &tt.input)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CreateAlias: name length and whitespace validation
// ---------------------------------------------------------------------------

func TestCreateAliasValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		aliasName string
		wantErr   bool
	}{
		{name: "valid", aliasName: "alias/my-key", wantErr: false},
		{name: "no_prefix", aliasName: "my-key", wantErr: true},
		{name: "aws_reserved", aliasName: "alias/aws/s3", wantErr: true},
		{
			name:      "too_long",
			aliasName: "alias/" + strings.Repeat("a", 251),
			wantErr:   true,
		},
		{
			name:      "at_max_length",
			aliasName: "alias/" + strings.Repeat("a", 250),
			wantErr:   false,
		},
		{name: "with_space", aliasName: "alias/my key", wantErr: true},
		{name: "with_tab", aliasName: "alias/my\tkey", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			createErr := b.CreateAlias(context.Background(), &kms.CreateAliasInput{
				AliasName:   tt.aliasName,
				TargetKeyID: key.KeyMetadata.KeyID,
			})

			if tt.wantErr {
				require.Error(t, createErr)
			} else {
				require.NoError(t, createErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ReplicateKey: source must be MultiRegion
// ---------------------------------------------------------------------------

func TestReplicateKeyRequiresMultiRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		multiRegion bool
		wantErr     bool
	}{
		{name: "multi_region", multiRegion: true, wantErr: false},
		{name: "single_region", multiRegion: false, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{MultiRegion: tt.multiRegion})
			require.NoError(t, err)

			_, replicaErr := b.ReplicateKey(context.Background(), &kms.ReplicateKeyInput{
				KeyID:         key.KeyMetadata.KeyID,
				ReplicaRegion: "us-west-2",
			})

			if tt.wantErr {
				require.Error(t, replicaErr)
				require.ErrorIs(t, replicaErr, kms.ErrUnsupportedOrigin)
			} else {
				require.NoError(t, replicaErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// RotateKeyOnDemand: symmetric + AWS_KMS only
// ---------------------------------------------------------------------------

func TestRotateKeyOnDemandValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   kms.CreateKeyInput
		wantErr bool
	}{
		{
			name:    "symmetric_aws_kms",
			input:   kms.CreateKeyInput{},
			wantErr: false,
		},
		{
			name: "asymmetric_rsa",
			input: kms.CreateKeyInput{
				KeySpec:  "RSA_2048",
				KeyUsage: "SIGN_VERIFY",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &tt.input)
			require.NoError(t, err)

			_, rotErr := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{
				KeyID: key.KeyMetadata.KeyID,
			})

			if tt.wantErr {
				require.Error(t, rotErr)
			} else {
				require.NoError(t, rotErr)
			}
		})
	}
}

func TestRotateKeyOnDemandExternalOriginRejected(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Origin: kms.KeyOriginExternal})
	require.NoError(t, err)

	// Import material so key transitions to Enabled state.
	err = b.ImportKeyMaterial(context.Background(), &kms.ImportKeyMaterialInput{
		KeyID:       key.KeyMetadata.KeyID,
		KeyMaterial: make([]byte, 32),
	})
	require.NoError(t, err)

	_, rotErr := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: key.KeyMetadata.KeyID})
	require.Error(t, rotErr)
	require.ErrorIs(t, rotErr, kms.ErrUnsupportedOrigin)
}

// ---------------------------------------------------------------------------
// GetKeyRotationStatus: OnDemandRotationStartDate
// ---------------------------------------------------------------------------

func TestGetKeyRotationStatusOnDemandDate(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	// Before on-demand rotation: field should be absent.
	status, err := b.GetKeyRotationStatus(context.Background(), &kms.GetKeyRotationStatusInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.Zero(t, status.OnDemandRotationStartDate)

	// Trigger on-demand rotation.
	_, err = b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)

	// After on-demand rotation: date should be populated.
	status2, err := b.GetKeyRotationStatus(context.Background(), &kms.GetKeyRotationStatusInput{KeyID: key.KeyMetadata.KeyID})
	require.NoError(t, err)
	assert.NotZero(t, status2.OnDemandRotationStartDate)
}

// ---------------------------------------------------------------------------
// ListKeyRotations: RotationType and KeyId fields
// ---------------------------------------------------------------------------

func TestListKeyRotationsFields(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	keyID := key.KeyMetadata.KeyID

	// Rotation via EnableKeyRotation (AWS_KMS rotation type).
	err = b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID})
	require.NoError(t, err)

	// Rotation via RotateKeyOnDemand (IMPORTED rotation type).
	_, err = b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
	require.NoError(t, err)

	out, err := b.ListKeyRotations(context.Background(), &kms.ListKeyRotationsInput{KeyID: keyID})
	require.NoError(t, err)
	require.NotEmpty(t, out.Rotations)

	for _, r := range out.Rotations {
		assert.NotEmpty(t, r.KeyID)
		assert.NotEmpty(t, r.RotationType)
		assert.NotZero(t, r.RotationDate)
	}
}

// TestListKeyRotationsTypedRecords verifies that rotations are stored with their
// correct types (AWS_KMS for scheduled, IMPORTED for on-demand) using the new
// typed RotationRecord storage.
func TestListKeyRotationsTypedRecords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(b *kms.InMemoryBackend, keyID string) error
		wantTypes []string
	}{
		{
			name: "enable_rotation_creates_no_immediate_record",
			setup: func(b *kms.InMemoryBackend, keyID string) error {
				return b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID})
			},
			wantTypes: []string{},
		},
		{
			name: "on_demand_rotation_is_imported",
			setup: func(b *kms.InMemoryBackend, keyID string) error {
				_, err := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})

				return err
			},
			wantTypes: []string{"IMPORTED"},
		},
		{
			name: "enable_then_on_demand_rotation_has_one_record",
			setup: func(b *kms.InMemoryBackend, keyID string) error {
				if err := b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID}); err != nil {
					return err
				}
				_, err := b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})

				return err
			},
			wantTypes: []string{"IMPORTED"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			keyID := key.KeyMetadata.KeyID

			require.NoError(t, tt.setup(b, keyID))

			out, err := b.ListKeyRotations(context.Background(), &kms.ListKeyRotationsInput{KeyID: keyID})
			require.NoError(t, err)
			require.Len(t, out.Rotations, len(tt.wantTypes))

			for i, wantType := range tt.wantTypes {
				assert.Equal(t, wantType, out.Rotations[i].RotationType)
				assert.Equal(t, keyID, out.Rotations[i].KeyID)
				assert.NotZero(t, out.Rotations[i].RotationDate)
			}
		})
	}
}

// TestListKeyRotationsLegacyFallback verifies that RotateKeyOnDemand creates an IMPORTED
// rotation record. EnableKeyRotation no longer causes an immediate rotation; only on-demand
// rotation creates a record.
func TestListKeyRotationsLegacyFallback(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	keyID := key.KeyMetadata.KeyID

	// Enabling rotation does not create an immediate rotation record.
	require.NoError(t, b.EnableKeyRotation(context.Background(), &kms.EnableKeyRotationInput{KeyID: keyID}))

	// On-demand rotation creates a record with type IMPORTED.
	_, err = b.RotateKeyOnDemand(context.Background(), &kms.RotateKeyOnDemandInput{KeyID: keyID})
	require.NoError(t, err)

	// List rotations - should have exactly 1 record (on-demand only).
	out, err := b.ListKeyRotations(context.Background(), &kms.ListKeyRotationsInput{KeyID: keyID})
	require.NoError(t, err)
	require.Len(t, out.Rotations, 1)
	assert.Equal(t, "IMPORTED", out.Rotations[0].RotationType)
}

func TestCancelKeyDeletionOutput(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	keyID := key.KeyMetadata.KeyID

	_, err = b.ScheduleKeyDeletion(context.Background(), &kms.ScheduleKeyDeletionInput{
		KeyID:               keyID,
		PendingWindowInDays: 7,
	})
	require.NoError(t, err)

	out, err := b.CancelKeyDeletion(context.Background(), &kms.CancelKeyDeletionInput{KeyID: keyID})
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, keyID, out.KeyID)
	assert.Equal(t, kms.KeyStateDisabled, out.KeyState)
}

// ---------------------------------------------------------------------------
// Handler: CancelKeyDeletion returns structured output via HTTP
// ---------------------------------------------------------------------------

func TestHandlerCancelKeyDeletionReturnsBody(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	keyRec := sendKMSOp(t, h, "CreateKey", `{}`)
	require.Equal(t, http.StatusOK, keyRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(keyRec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	sendKMSOp(t, h, "ScheduleKeyDeletion", `{"KeyId":"`+keyID+`","PendingWindowInDays":7}`)

	rec := sendKMSOp(t, h, "CancelKeyDeletion", `{"KeyId":"`+keyID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, keyID, out["KeyId"])
	assert.Equal(t, kms.KeyStateDisabled, out["KeyState"])
}

// ---------------------------------------------------------------------------
// Handler: TagResource validates key existence
// ---------------------------------------------------------------------------

func TestHandlerTagResourceKeyMustExist(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	rec := sendKMSOp(t, h, "TagResource", `{"KeyId":"nonexistent","Tags":[{"TagKey":"k","TagValue":"v"}]}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerUntagResourceKeyMustExist(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	rec := sendKMSOp(t, h, "UntagResource", `{"KeyId":"nonexistent","TagKeys":["k"]}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerListResourceTagsKeyMustExist(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	rec := sendKMSOp(t, h, "ListResourceTags", `{"KeyId":"nonexistent"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Handler: TagResource enforces max 50 tag limit
// ---------------------------------------------------------------------------

func TestHandlerTagResourceMaxTags(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	// Create a key.
	rec := sendKMSOp(t, h, "CreateKey", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	// Build a tag list with 51 unique entries.
	var sb strings.Builder

	sb.WriteString(`{"KeyId":"` + keyID + `","Tags":[`)

	for i := range 51 {
		if i > 0 {
			sb.WriteString(",")
		}

		sb.WriteString(`{"TagKey":"uniquekey` + strings.Repeat("x", i+1) + `","TagValue":"v"}`)
	}

	sb.WriteString(`]}`)

	rec2 := sendKMSOp(t, h, "TagResource", sb.String())
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ---------------------------------------------------------------------------
// Handler: EncryptionAlgorithm in HTTP Encrypt response
// ---------------------------------------------------------------------------

func TestHandlerEncryptReturnsAlgorithm(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	keyRec := sendKMSOp(t, h, "CreateKey", `{}`)
	require.Equal(t, http.StatusOK, keyRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(keyRec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	plaintext := base64.StdEncoding.EncodeToString([]byte("test-plaintext"))
	encRec := sendKMSOp(t, h, "Encrypt", `{"KeyId":"`+keyID+`","Plaintext":"`+plaintext+`"}`)
	require.Equal(t, http.StatusOK, encRec.Code)

	var encOut map[string]any
	require.NoError(t, json.Unmarshal(encRec.Body.Bytes(), &encOut))
	assert.Equal(t, "SYMMETRIC_DEFAULT", encOut["EncryptionAlgorithm"])
}

// ---------------------------------------------------------------------------
// Handler: DescribeKey returns correct field for HMAC MultiRegion rejection
// ---------------------------------------------------------------------------

func TestHandlerCreateKeyHMACMultiRegionRejected(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	rec := sendKMSOp(t, h, "CreateKey", `{"KeySpec":"HMAC_256","KeyUsage":"GENERATE_VERIFY_MAC","MultiRegion":true}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Handler: ReplicateKey requires MultiRegion source via HTTP
// ---------------------------------------------------------------------------

func TestHandlerReplicateKeyRequiresMultiRegion(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	// Create non-MultiRegion key.
	keyRec := sendKMSOp(t, h, "CreateKey", `{}`)
	require.Equal(t, http.StatusOK, keyRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(keyRec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	rec := sendKMSOp(t, h, "ReplicateKey", `{"KeyId":"`+keyID+`","ReplicaRegion":"us-west-2"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Handler: CreateAlias whitespace rejected via HTTP
// ---------------------------------------------------------------------------

func TestHandlerCreateAliasWhitespaceRejected(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	keyRec := sendKMSOp(t, h, "CreateKey", `{}`)
	require.Equal(t, http.StatusOK, keyRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(keyRec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	rec := sendKMSOp(t, h, "CreateAlias", `{"AliasName":"alias/my bad alias","TargetKeyId":"`+keyID+`"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Handler: UpdateKeyDescription max length rejected via HTTP
// ---------------------------------------------------------------------------

func TestHandlerUpdateKeyDescriptionMaxLengthRejected(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	keyRec := sendKMSOp(t, h, "CreateKey", `{}`)
	require.Equal(t, http.StatusOK, keyRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(keyRec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	longDesc := strings.Repeat("x", 8193)
	rec := sendKMSOp(t, h, "UpdateKeyDescription", `{"KeyId":"`+keyID+`","Description":"`+longDesc+`"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Handler: ReplicateKey copies source tags to replica
// ---------------------------------------------------------------------------

func TestHandlerReplicateKeyCopiesTags(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	keyRec := sendKMSOp(t, h, "CreateKey", `{"MultiRegion":true}`)
	require.Equal(t, http.StatusOK, keyRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(keyRec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	tagRec := sendKMSOp(t, h, "TagResource", `{"KeyId":"`+keyID+`","Tags":[{"TagKey":"env","TagValue":"prod"}]}`)
	require.Equal(t, http.StatusOK, tagRec.Code)

	repRec := sendKMSOp(t, h, "ReplicateKey", `{"KeyId":"`+keyID+`","ReplicaRegion":"eu-west-1"}`)
	require.Equal(t, http.StatusOK, repRec.Code)

	var repOut map[string]any
	require.NoError(t, json.Unmarshal(repRec.Body.Bytes(), &repOut))
	replicaID := repOut["ReplicaKeyMetadata"].(map[string]any)["KeyId"].(string)

	tagListRec := sendKMSOp(t, h, "ListResourceTags", `{"KeyId":"`+replicaID+`"}`)
	require.Equal(t, http.StatusOK, tagListRec.Code)

	var tagListOut map[string]any
	require.NoError(t, json.Unmarshal(tagListRec.Body.Bytes(), &tagListOut))
	tags := tagListOut["Tags"].([]any)
	require.NotEmpty(t, tags)

	found := false
	for _, tag := range tags {
		tagMap := tag.(map[string]any)
		if tagMap["TagKey"] == "env" && tagMap["TagValue"] == "prod" {
			found = true
		}
	}

	assert.True(t, found, "expected env=prod tag to be copied to replica")
}

// ---------------------------------------------------------------------------
// Handler: CreateKey with Tags applies them immediately
// ---------------------------------------------------------------------------

func TestHandlerCreateKeyWithTagsApplied(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	rec := sendKMSOp(t, h, "CreateKey", `{"Tags":[{"TagKey":"team","TagValue":"platform"}]}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	tagListRec := sendKMSOp(t, h, "ListResourceTags", `{"KeyId":"`+keyID+`"}`)
	require.Equal(t, http.StatusOK, tagListRec.Code)

	var tagListOut map[string]any
	require.NoError(t, json.Unmarshal(tagListRec.Body.Bytes(), &tagListOut))
	tags := tagListOut["Tags"].([]any)
	require.NotEmpty(t, tags)

	found := false
	for _, tag := range tags {
		tagMap := tag.(map[string]any)
		if tagMap["TagKey"] == "team" && tagMap["TagValue"] == "platform" {
			found = true
		}
	}

	assert.True(t, found, "expected team=platform tag to be present after CreateKey")
}

// ---------------------------------------------------------------------------
// Handler: ListKeyRotations via HTTP returns RotationType
// ---------------------------------------------------------------------------

func TestHandlerListKeyRotationsViaHTTP(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	keyRec := sendKMSOp(t, h, "CreateKey", `{}`)
	require.Equal(t, http.StatusOK, keyRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(keyRec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	sendKMSOp(t, h, "RotateKeyOnDemand", `{"KeyId":"`+keyID+`"}`)

	rec := sendKMSOp(t, h, "ListKeyRotations", `{"KeyId":"`+keyID+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	rotations := out["Rotations"].([]any)
	require.NotEmpty(t, rotations)

	first := rotations[0].(map[string]any)
	assert.NotEmpty(t, first["RotationType"])
	assert.NotEmpty(t, first["KeyId"])
}

// ---------------------------------------------------------------------------
// RSA ENCRYPT_DECRYPT key round-trip
// ---------------------------------------------------------------------------

func TestRSAEncryptDecryptRoundTrip(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "RSA_2048",
		KeyUsage: "ENCRYPT_DECRYPT",
	})
	require.NoError(t, err)

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("rsa secret"),
	})
	require.NoError(t, err)
	assert.Equal(t, "RSAES_OAEP_SHA_256", enc.EncryptionAlgorithm)

	dec, err := b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("rsa secret"), dec.Plaintext)
	assert.Equal(t, "RSAES_OAEP_SHA_256", dec.EncryptionAlgorithm)
}

// ---------------------------------------------------------------------------
// Handler: UpdatePrimaryRegion via HTTP
// ---------------------------------------------------------------------------

func TestHandlerUpdatePrimaryRegionViaHTTP(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	keyRec := sendKMSOp(t, h, "CreateKey", `{"MultiRegion":true}`)
	require.Equal(t, http.StatusOK, keyRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(keyRec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	rec := sendKMSOp(t, h, "UpdatePrimaryRegion", `{"KeyId":"`+keyID+`","PrimaryRegion":"eu-central-1"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// ECDSA sign/verify with SHA-384 and SHA-512 algorithms
// ---------------------------------------------------------------------------

func TestECDSASignVerifyAllAlgorithms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		keySpec   string
		algorithm string
	}{
		{name: "p384_sha384", keySpec: "ECC_NIST_P384", algorithm: "ECDSA_SHA_384"},
		{name: "p521_sha512", keySpec: "ECC_NIST_P521", algorithm: "ECDSA_SHA_512"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{KeySpec: tt.keySpec, KeyUsage: "SIGN_VERIFY"})
			require.NoError(t, err)

			signOut, err := b.Sign(context.Background(), &kms.SignInput{
				KeyID:            key.KeyMetadata.KeyID,
				Message:          []byte("message"),
				SigningAlgorithm: tt.algorithm,
			})
			require.NoError(t, err)

			verOut, err := b.Verify(context.Background(), &kms.VerifyInput{
				KeyID:            key.KeyMetadata.KeyID,
				Message:          []byte("message"),
				Signature:        signOut.Signature,
				SigningAlgorithm: tt.algorithm,
			})
			require.NoError(t, err)
			assert.True(t, verOut.SignatureValid)
		})
	}
}

// ---------------------------------------------------------------------------
// Handler: ReplicateKey with explicit input tags overrides source tags
// ---------------------------------------------------------------------------

func TestHandlerReplicateKeyWithInputTags(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	h := kms.NewHandler(b)

	keyRec := sendKMSOp(t, h, "CreateKey", `{"MultiRegion":true,"Tags":[{"TagKey":"src","TagValue":"yes"}]}`)
	require.Equal(t, http.StatusOK, keyRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(keyRec.Body.Bytes(), &createOut))
	keyID := createOut["KeyMetadata"].(map[string]any)["KeyId"].(string)

	// Replicate with explicit tags that should overlay the source tags.
	repRec := sendKMSOp(t, h, "ReplicateKey",
		`{"KeyId":"`+keyID+`","ReplicaRegion":"ap-northeast-1","Tags":[{"TagKey":"extra","TagValue":"1"}]}`)
	require.Equal(t, http.StatusOK, repRec.Code)

	var repOut map[string]any
	require.NoError(t, json.Unmarshal(repRec.Body.Bytes(), &repOut))
	replicaID := repOut["ReplicaKeyMetadata"].(map[string]any)["KeyId"].(string)

	tagListRec := sendKMSOp(t, h, "ListResourceTags", `{"KeyId":"`+replicaID+`"}`)
	require.Equal(t, http.StatusOK, tagListRec.Code)

	var tagListOut map[string]any
	require.NoError(t, json.Unmarshal(tagListRec.Body.Bytes(), &tagListOut))
	tags := tagListOut["Tags"].([]any)

	keys := make(map[string]string)
	for _, tag := range tags {
		tm := tag.(map[string]any)
		keys[tm["TagKey"].(string)] = tm["TagValue"].(string)
	}

	assert.Equal(t, "yes", keys["src"], "expected source tag copied")
	assert.Equal(t, "1", keys["extra"], "expected input tag applied")
}

// ---------------------------------------------------------------------------
// ARN-based key lookup
// ---------------------------------------------------------------------------

func TestDecryptViaARNHint(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("arn decrypt"),
	})
	require.NoError(t, err)

	// Use the ARN as the KeyID hint.
	dec, err := b.Decrypt(context.Background(), &kms.DecryptInput{
		CiphertextBlob: enc.CiphertextBlob,
		KeyID:          key.KeyMetadata.Arn,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("arn decrypt"), dec.Plaintext)
}

// ---------------------------------------------------------------------------
// VerifyMac - covers VerifyMac backend path
// ---------------------------------------------------------------------------

func TestVerifyMacRoundTrip(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "HMAC_256",
		KeyUsage: "GENERATE_VERIFY_MAC",
	})
	require.NoError(t, err)

	macOut, err := b.GenerateMac(context.Background(), &kms.GenerateMacInput{
		KeyID:        key.KeyMetadata.KeyID,
		MacAlgorithm: "HMAC_SHA_256",
		Message:      []byte("msg"),
	})
	require.NoError(t, err)

	verOut, err := b.VerifyMac(context.Background(), &kms.VerifyMacInput{
		KeyID:        key.KeyMetadata.KeyID,
		MacAlgorithm: "HMAC_SHA_256",
		Message:      []byte("msg"),
		Mac:          macOut.Mac,
	})
	require.NoError(t, err)
	assert.True(t, verOut.MacValid)

	// Wrong message should fail.
	_, verErr := b.VerifyMac(context.Background(), &kms.VerifyMacInput{
		KeyID:        key.KeyMetadata.KeyID,
		MacAlgorithm: "HMAC_SHA_256",
		Message:      []byte("wrong"),
		Mac:          macOut.Mac,
	})
	require.Error(t, verErr)
}

// ---------------------------------------------------------------------------
// GenerateDataKeyWithoutPlaintext with NumberOfBytes
// ---------------------------------------------------------------------------

func TestGenerateDataKeyWithoutPlaintextNumberOfBytes(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	numBytes := int32(64)
	out, err := b.GenerateDataKeyWithoutPlaintext(context.Background(), &kms.GenerateDataKeyWithoutPlaintextInput{
		KeyID:         key.KeyMetadata.KeyID,
		NumberOfBytes: &numBytes,
	})
	require.NoError(t, err)
	assert.NotNil(t, out.CiphertextBlob)
}

// ---------------------------------------------------------------------------
// RSA PKCS1-v1_5 sign/verify (covers RSASSA_PKCS1_V1_5 branches)
// ---------------------------------------------------------------------------

func TestRSAPKCS1SignVerify(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{
		KeySpec:  "RSA_2048",
		KeyUsage: "SIGN_VERIFY",
	})
	require.NoError(t, err)

	signOut, err := b.Sign(context.Background(), &kms.SignInput{
		KeyID:            key.KeyMetadata.KeyID,
		Message:          []byte("data"),
		SigningAlgorithm: "RSASSA_PKCS1_V1_5_SHA_256",
	})
	require.NoError(t, err)

	verOut, err := b.Verify(context.Background(), &kms.VerifyInput{
		KeyID:            key.KeyMetadata.KeyID,
		Message:          []byte("data"),
		Signature:        signOut.Signature,
		SigningAlgorithm: "RSASSA_PKCS1_V1_5_SHA_256",
	})
	require.NoError(t, err)
	assert.True(t, verOut.SignatureValid)
}

// ---------------------------------------------------------------------------
// Alias-via-ARN resolution
// ---------------------------------------------------------------------------

func TestEncryptViaAliasARN(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	err = b.CreateAlias(context.Background(), &kms.CreateAliasInput{
		AliasName:   "alias/my-arn-test",
		TargetKeyID: key.KeyMetadata.KeyID,
	})
	require.NoError(t, err)

	// Build an ARN for the alias.
	aliasARN := "arn:aws:kms:us-east-1:000000000000:alias/my-arn-test"

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     aliasARN,
		Plaintext: []byte("alias-arn"),
	})
	require.NoError(t, err)
	require.NotNil(t, enc)
}

// ---------------------------------------------------------------------------
// Snapshot + Restore round-trip coverage
// ---------------------------------------------------------------------------

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{Description: "snap-test"})
	require.NoError(t, err)

	enc, err := b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     key.KeyMetadata.KeyID,
		Plaintext: []byte("before-snap"),
	})
	require.NoError(t, err)

	snap := b.Snapshot()

	// Create a new backend and restore into it.
	b2 := kms.NewInMemoryBackend()
	err = b2.Restore(snap)
	require.NoError(t, err)

	// Decryption must succeed on the restored backend.
	dec, err := b2.Decrypt(&kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob})
	require.NoError(t, err)
	assert.Equal(t, []byte("before-snap"), dec.Plaintext)
}

// ---------------------------------------------------------------------------
// Handler: Reset clears cached aliases (clearResolutionCache)
// ---------------------------------------------------------------------------

func TestHandlerResetClearsAliasCache(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	h := kms.NewHandler(b)

	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	err = b.CreateAlias(context.Background(), &kms.CreateAliasInput{
		AliasName:   "alias/to-be-cleared",
		TargetKeyID: key.KeyMetadata.KeyID,
	})
	require.NoError(t, err)

	// Resolve alias to populate the cache.
	_, err = b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     "alias/to-be-cleared",
		Plaintext: []byte("pre-reset"),
	})
	require.NoError(t, err)

	// Reset clears everything including the resolution cache.
	h.Reset()

	// After reset the alias should no longer exist.
	_, err = b.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     "alias/to-be-cleared",
		Plaintext: []byte("post-reset"),
	})
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// RSA PSS sign/verify covers RSASSA_PSS branches in signWithKeyMaterial
// ---------------------------------------------------------------------------

func TestRSAPSSSignVerifyAllSizes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keySpec string
		algo    string
	}{
		{name: "rsa3072_pss_sha384", keySpec: "RSA_3072", algo: "RSASSA_PSS_SHA_384"},
		{name: "rsa4096_pss_sha512", keySpec: "RSA_4096", algo: "RSASSA_PSS_SHA_512"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{KeySpec: tt.keySpec, KeyUsage: "SIGN_VERIFY"})
			require.NoError(t, err)

			msg := []byte("test message for " + tt.name)
			signOut, err := b.Sign(context.Background(), &kms.SignInput{
				KeyID:            key.KeyMetadata.KeyID,
				Message:          msg,
				SigningAlgorithm: tt.algo,
			})
			require.NoError(t, err)

			verOut, err := b.Verify(context.Background(), &kms.VerifyInput{
				KeyID:            key.KeyMetadata.KeyID,
				Message:          msg,
				Signature:        signOut.Signature,
				SigningAlgorithm: tt.algo,
			})
			require.NoError(t, err)
			assert.True(t, verOut.SignatureValid)
		})
	}
}
