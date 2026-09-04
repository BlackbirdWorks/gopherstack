package kms_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestWireErrorTypes_NotValidationException verifies that call sites remapped away
// from the sentinel ErrValidation (gopherstack-e3yu: "ValidationException" names no
// type in any KMS operation's deserializeOpError) now emit the wire __type/status
// their reaching operation actually recognizes.
func TestWireErrorTypes_NotValidationException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *kms.Handler) string
		name       string
		action     string
		wantType   string
		wantStatus int
	}{
		{
			name:   "delete custom key store not found",
			action: "DeleteCustomKeyStore",
			setup: func(t *testing.T, _ *kms.Handler) string {
				t.Helper()

				return mustJSON(t, kms.DeleteCustomKeyStoreInput{CustomKeyStoreID: ""})
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "CustomKeyStoreNotFoundException",
		},
		{
			name:   "connect custom key store not found",
			action: "ConnectCustomKeyStore",
			setup: func(t *testing.T, _ *kms.Handler) string {
				t.Helper()

				return mustJSON(t, kms.ConnectCustomKeyStoreInput{CustomKeyStoreID: ""})
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "CustomKeyStoreNotFoundException",
		},
		{
			name:   "disconnect custom key store not found",
			action: "DisconnectCustomKeyStore",
			setup: func(t *testing.T, _ *kms.Handler) string {
				t.Helper()

				return mustJSON(t, kms.DisconnectCustomKeyStoreInput{CustomKeyStoreID: ""})
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "CustomKeyStoreNotFoundException",
		},
		{
			name:   "update custom key store not found",
			action: "UpdateCustomKeyStore",
			setup: func(t *testing.T, _ *kms.Handler) string {
				t.Helper()

				return mustJSON(t, kms.UpdateCustomKeyStoreInput{CustomKeyStoreID: "  "})
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "CustomKeyStoreNotFoundException",
		},
		{
			name:   "rotate key on demand daily limit exceeded",
			action: "RotateKeyOnDemand",
			setup: func(t *testing.T, h *kms.Handler) string {
				t.Helper()

				keyID := ab2MustCreateKeyExternal(t, h, false)
				for range 10 {
					_, err := h.Backend.RotateKeyOnDemand(
						context.Background(),
						&kms.RotateKeyOnDemandInput{KeyID: keyID},
					)
					require.NoError(t, err)
				}

				return mustJSON(t, kms.RotateKeyOnDemandInput{KeyID: keyID})
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "LimitExceededException",
		},
		{
			name:   "create key too many tags",
			action: "CreateKey",
			setup: func(t *testing.T, _ *kms.Handler) string {
				t.Helper()

				return mustJSON(t, kms.CreateKeyInput{Tags: manyTags(t, 51)})
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "LimitExceededException",
		},
		{
			name:   "create key unsupported key spec",
			action: "CreateKey",
			setup: func(t *testing.T, _ *kms.Handler) string {
				t.Helper()

				return mustJSON(t, kms.CreateKeyInput{KeySpec: "BOGUS_SPEC"})
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "UnsupportedOperationException",
		},
		{
			name:   "generate data key pair empty key pair spec",
			action: "GenerateDataKeyPair",
			setup: func(t *testing.T, _ *kms.Handler) string {
				t.Helper()

				return mustJSON(t, kms.GenerateDataKeyPairInput{KeyID: "irrelevant"})
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "UnsupportedOperationException",
		},
		{
			name:   "get parameters for import invalid wrapping algorithm",
			action: "GetParametersForImport",
			setup: func(t *testing.T, _ *kms.Handler) string {
				t.Helper()

				return mustJSON(t, kms.GetParametersForImportInput{
					KeyID:             "irrelevant",
					WrappingAlgorithm: "RSAES_MADE_UP",
				})
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "UnsupportedOperationException",
		},
		{
			name:   "get parameters for import invalid wrapping key spec",
			action: "GetParametersForImport",
			setup: func(t *testing.T, _ *kms.Handler) string {
				t.Helper()

				return mustJSON(t, kms.GetParametersForImportInput{
					KeyID:           "irrelevant",
					WrappingKeySpec: "RSA_512",
				})
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "UnsupportedOperationException",
		},
		{
			name:   "import key material without wrapping key on record",
			action: "ImportKeyMaterial",
			setup: func(t *testing.T, h *kms.Handler) string {
				t.Helper()

				keyID := ab2MustCreateKeyExternal(t, h, true)

				return mustJSON(t, kms.ImportKeyMaterialInput{
					KeyID:       keyID,
					KeyMaterial: make([]byte, 256), // >= minRSAWrappedMaterialBytes, no GetParametersForImport call
				})
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "InvalidImportTokenException",
		},
		{
			name:   "get key last usage alias not supported",
			action: "GetKeyLastUsage",
			setup: func(t *testing.T, _ *kms.Handler) string {
				t.Helper()

				return mustJSON(t, kms.GetKeyLastUsageInput{KeyID: "alias/whatever"})
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "NotFoundException",
		},
		{
			name:   "tag resource reserved aws prefix",
			action: "TagResource",
			setup: func(t *testing.T, h *kms.Handler) string {
				t.Helper()

				keyID := ab2MustCreateKeyExternal(t, h, false)

				return `{"KeyId":"` + keyID + `","Tags":[{"TagKey":"aws:reserved","TagValue":"v"}]}`
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "TagException",
		},
		{
			name:   "tag resource too many tags",
			action: "TagResource",
			setup: func(t *testing.T, h *kms.Handler) string {
				t.Helper()

				keyID := ab2MustCreateKeyExternal(t, h, false)
				tags := make([]string, 0, 51)
				for i := range 51 {
					tags = append(tags, `{"TagKey":"k`+strconv.Itoa(i)+`","TagValue":"v"}`)
				}

				return `{"KeyId":"` + keyID + `","Tags":[` + strings.Join(tags, ",") + `]}`
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "LimitExceededException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := ab2NewHandler(t)
			body := tc.setup(t, h)

			rec := doKMSRequest(t, h, tc.action, body)

			require.Equal(t, tc.wantStatus, rec.Code)

			var resp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tc.wantType, resp.Type)
			assert.NotEqual(t, "ValidationException", resp.Type)
		})
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()

	b, err := json.Marshal(v)
	require.NoError(t, err)

	return string(b)
}

func manyTags(t *testing.T, n int) []kms.Tag {
	t.Helper()

	tags := make([]kms.Tag, 0, n)
	for i := range n {
		tags = append(tags, kms.Tag{TagKey: "k" + strconv.Itoa(i), TagValue: "v"})
	}

	return tags
}

// ab2MustCreateKeyExternal creates a key via the handler's backend, either a normal
// AWS_KMS-origin Enabled symmetric key (external=false) or an EXTERNAL-origin
// PendingImport symmetric key awaiting ImportKeyMaterial (external=true).
func ab2MustCreateKeyExternal(t *testing.T, h *kms.Handler, external bool) string {
	t.Helper()

	input := &kms.CreateKeyInput{}
	if external {
		input.Origin = kms.KeyOriginExternal
	}

	out, err := h.Backend.CreateKey(context.Background(), input)
	require.NoError(t, err)

	return out.KeyMetadata.KeyID
}
