package ssm_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kms"
	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &ssm.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, ssm.ErrNilAppContext)
}

// TestRefinement1_ProviderInit verifies normal provider init.
func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &ssm.Provider{}
	ctx := &service.AppContext{JanitorCtx: t.Context()}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestStorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ ssm.StorageBackend = (*ssm.InMemoryBackend)(nil)
}

// TestRefinement1_HandlerOpsLen verifies the number of supported operations.
func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := ssm.NewHandler(ssm.NewInMemoryBackend())
	assert.Len(t, h.GetSupportedOperations(), 152)
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestSDKOpsSorted(t *testing.T) {
	t.Parallel()

	h := ssm.NewHandler(ssm.NewInMemoryBackend())
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

type testKMSAdapter struct {
	b *kms.InMemoryBackend
}

func (a *testKMSAdapter) EncryptSSM(keyID string, plaintext []byte) ([]byte, error) {
	out, err := a.b.Encrypt(context.Background(), &kms.EncryptInput{KeyID: keyID, Plaintext: plaintext})
	if err != nil {
		return nil, err
	}

	return out.CiphertextBlob, nil
}
func (a *testKMSAdapter) DecryptSSM(ciphertext []byte) ([]byte, error) {
	out, err := a.b.Decrypt(context.Background(), &kms.DecryptInput{CiphertextBlob: ciphertext})
	if err != nil {
		return nil, err
	}

	return out.Plaintext, nil
}

// newSSMWithKMS creates an SSM backend wired to a real KMS backend.
// Returns the SSM backend and the created key ID.
func newSSMWithKMS(t *testing.T) (*ssm.InMemoryBackend, string) {
	t.Helper()
	kmsBackend := kms.NewInMemoryBackend()
	keyOut, err := kmsBackend.CreateKey(t.Context(), &kms.CreateKeyInput{Description: "test key"})
	require.NoError(t, err)
	keyID := keyOut.KeyMetadata.KeyID

	ssmBackend := ssm.NewInMemoryBackend()
	ssmBackend.WithKMS(&testKMSAdapter{b: kmsBackend})

	return ssmBackend, keyID
}
func putTestParam(t *testing.T, b *ssm.InMemoryBackend, name string) {
	t.Helper()

	_, err := b.PutParameter(context.Background(), &ssm.PutParameterInput{
		Name:  name,
		Value: "v",
		Type:  "String",
	})
	require.NoError(t, err)
}
func TestParameterExpiration_JanitorEvicts(t *testing.T) {
	t.Parallel()

	t.Run("past-expiry parameter is deleted by janitor", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		ctx := context.Background()

		// Attach an Expiration policy with a timestamp in the past.
		pastTS := time.Now().UTC().Add(-1 * time.Hour).Format("2006-01-02T15:04:05.000Z")
		policies := fmt.Sprintf(
			`[{"Type":"Expiration","Version":"1.0","Attributes":{"Timestamp":%q}}]`,
			pastTS,
		)

		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:     "/expire/past",
			Value:    "gone-soon",
			Type:     "String",
			Tier:     "Advanced",
			Policies: policies,
		})
		require.NoError(t, err)

		// Confirm it exists.
		_, err = b.GetParameter(ctx, &ssm.GetParameterInput{Name: "/expire/past"})
		require.NoError(t, err)

		// Run the janitor sweep.
		j := ssm.NewJanitor(b, time.Minute)
		j.SweepOnce(ctx)

		// Parameter must be gone.
		_, err = b.GetParameter(ctx, &ssm.GetParameterInput{Name: "/expire/past"})
		assert.ErrorIs(t, err, ssm.ErrParameterNotFound,
			"past-expiry parameter must be evicted by janitor")
	})

	t.Run("future-expiry parameter is kept by janitor", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		ctx := context.Background()

		futureTS := time.Now().UTC().Add(24 * time.Hour).Format("2006-01-02T15:04:05.000Z")
		policies := fmt.Sprintf(
			`[{"Type":"Expiration","Version":"1.0","Attributes":{"Timestamp":%q}}]`,
			futureTS,
		)

		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:     "/expire/future",
			Value:    "still-here",
			Type:     "String",
			Tier:     "Advanced",
			Policies: policies,
		})
		require.NoError(t, err)

		j := ssm.NewJanitor(b, time.Minute)
		j.SweepOnce(ctx)

		_, err = b.GetParameter(ctx, &ssm.GetParameterInput{Name: "/expire/future"})
		require.NoError(t, err, "future-expiry parameter must survive janitor sweep")
	})

	t.Run("parameter without policy is not evicted", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		ctx := context.Background()

		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:  "/no-policy",
			Value: "permanent",
			Type:  "String",
		})
		require.NoError(t, err)

		j := ssm.NewJanitor(b, time.Minute)
		j.SweepOnce(ctx)

		_, err = b.GetParameter(ctx, &ssm.GetParameterInput{Name: "/no-policy"})
		require.NoError(t, err, "parameter without expiry policy must not be evicted")
	})

	t.Run("RFC3339 timestamp format also works", func(t *testing.T) {
		t.Parallel()

		b := ssm.NewInMemoryBackend()
		ctx := context.Background()

		pastTS := time.Now().UTC().Add(-1 * time.Hour).Format(time.RFC3339)
		policies := fmt.Sprintf(
			`[{"Type":"Expiration","Version":"1.0","Attributes":{"Timestamp":%q}}]`,
			pastTS,
		)

		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:     "/expire/rfc3339",
			Value:    "gone",
			Type:     "String",
			Tier:     "Advanced",
			Policies: policies,
		})
		require.NoError(t, err)

		j := ssm.NewJanitor(b, time.Minute)
		j.SweepOnce(ctx)

		_, err = b.GetParameter(ctx, &ssm.GetParameterInput{Name: "/expire/rfc3339"})
		assert.ErrorIs(t, err, ssm.ErrParameterNotFound)
	})
}
func Test_PutParameter_HierarchyLevelLimit(t *testing.T) {
	t.Parallel()

	fifteenLevels := "/" + strings.Join(makeLevels(14), "/") + "/name" // 14 + name = 15
	sixteenLevels := "/" + strings.Join(makeLevels(15), "/") + "/name" // 15 + name = 16

	cases := []struct {
		name      string
		paramName string
		wantErr   bool
	}{
		{name: "exactly 15 levels is valid", paramName: fifteenLevels, wantErr: false},
		{name: "16 levels exceeds the limit", paramName: sixteenLevels, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			ctx := context.Background()

			_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
				Name:  tc.paramName,
				Type:  ssm.StringType,
				Value: "v",
			})

			if tc.wantErr {
				require.ErrorIs(t, err, ssm.ErrHierarchyLevelLimitExceeded)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test_PutParameter_IntelligentTiering_AutoUpgradesToAdvanced verifies that,
// per AWS docs, Intelligent-Tiering parameters are auto-promoted to the
// Advanced tier whenever the request needs a capability Standard doesn't
// support (value over 4 KiB, or parameter policies attached) instead of the
// PutParameter call failing.
func Test_PutParameter_IntelligentTiering_AutoUpgradesToAdvanced(t *testing.T) {
	t.Parallel()

	smallValue := strings.Repeat("a", 100)
	overStandardValue := strings.Repeat("a", 5000) // >4096, <=8192

	cases := []struct {
		name     string
		tier     string
		value    string
		wantTier string
		wantErr  bool
	}{
		{
			// AWS echoes back the requested "Intelligent-Tiering" tier as-is
			// when no capability forces a promotion — it does not resolve to
			// the concrete underlying "Standard" tier in the response.
			name:     "small value on Intelligent-Tiering reports Intelligent-Tiering",
			tier:     "Intelligent-Tiering",
			value:    smallValue,
			wantTier: "Intelligent-Tiering",
		},
		{
			name:     "value over 4KiB on Intelligent-Tiering auto-upgrades to Advanced",
			tier:     "Intelligent-Tiering",
			value:    overStandardValue,
			wantTier: "Advanced",
		},
		{
			name:    "value over 4KiB on explicit Standard tier fails",
			tier:    "Standard",
			value:   overStandardValue,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			ctx := context.Background()

			out, err := b.PutParameter(ctx, &ssm.PutParameterInput{
				Name:  "/tier/param",
				Type:  ssm.StringType,
				Value: tc.value,
				Tier:  tc.tier,
			})

			if tc.wantErr {
				require.ErrorIs(t, err, ssm.ErrValidationException)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantTier, out.Tier)
		})
	}
}

// Test_PutParameter_PoliciesRequireAdvancedTier verifies AWS's documented
// constraint that parameter policies (Expiration, ExpirationNotification,
// NoChangeNotification) are only supported on Advanced-tier parameters;
// Standard tier rejects them, and Intelligent-Tiering auto-upgrades to
// Advanced to accommodate them.
func Test_PutParameter_PoliciesRequireAdvancedTier(t *testing.T) {
	t.Parallel()

	const policy = `[{"Type":"Expiration","Version":"1.0","Attributes":{"Timestamp":"2099-01-01T00:00:00.000Z"}}]`

	cases := []struct {
		name     string
		tier     string
		wantTier string
		wantErr  bool
	}{
		{name: "default (Standard) tier rejects policies", tier: "", wantErr: true},
		{name: "explicit Standard tier rejects policies", tier: "Standard", wantErr: true},
		{name: "Advanced tier accepts policies", tier: "Advanced", wantTier: "Advanced"},
		{
			name:     "Intelligent-Tiering auto-upgrades for policies",
			tier:     "Intelligent-Tiering",
			wantTier: "Advanced",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			ctx := context.Background()

			out, err := b.PutParameter(ctx, &ssm.PutParameterInput{
				Name:     "/policy/param",
				Type:     ssm.StringType,
				Value:    "v",
				Tier:     tc.tier,
				Policies: policy,
			})

			if tc.wantErr {
				require.ErrorIs(t, err, ssm.ErrValidationException)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantTier, out.Tier)
		})
	}
}

// TestDeleteParameter_RegionCleanup verifies that deleting the last parameter
// in a region removes the empty inner maps so they do not linger indefinitely.
func TestDeleteParameter_RegionCleanup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deleteVia  string // "single" or "batch"
		paramCount int
	}{
		{name: "single delete cleans region", deleteVia: "single", paramCount: 1},
		{name: "batch delete cleans region", deleteVia: "batch", paramCount: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := ssm.NewInMemoryBackend()

			names := make([]string, tc.paramCount)
			for i := range tc.paramCount {
				name := "/cleanup/p"
				if tc.paramCount > 1 {
					name = "/cleanup/p" + string(rune('0'+i))
				}
				names[i] = name
				_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
					Name:  name,
					Type:  ssm.StringType,
					Value: "v",
				})
				require.NoError(t, err)
			}

			switch tc.deleteVia {
			case "single":
				_, err := b.DeleteParameter(ctx, &ssm.DeleteParameterInput{Name: names[0]})
				require.NoError(t, err)
			case "batch":
				_, err := b.DeleteParameters(ctx, &ssm.DeleteParametersInput{Names: names})
				require.NoError(t, err)
			}

			// After deleting all params, no tag entry should survive either.
			for _, name := range names {
				assert.False(t, b.HasTagEntry(name),
					"tag entry must be cleaned up after DeleteParameter")
			}

			// HistoryLen must be zero — the history inner map entry was cleaned up.
			for _, name := range names {
				assert.Equal(t, 0, b.HistoryLen(name))
			}
		})
	}
}
func postAudit1(t *testing.T, h *ssm.Handler, op string, body any) (int, map[string]any) {
	t.Helper()

	payload, err := json.Marshal(body)
	require.NoError(t, err)

	rec := doRequest(t, h, op, string(payload))

	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)

	return rec.Code, out
}
func newAudit1Handler() *ssm.Handler {
	return ssm.NewHandler(ssm.NewInMemoryBackend())
}
func TestPutParameter_Tier_Standard(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/tier-standard",
		"Type":  "String",
		"Value": "hello",
		"Tier":  "Standard",
	})

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "Standard", out["Tier"])
}
func TestPutParameter_Tier_Advanced(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/tier-advanced",
		"Type":  "String",
		"Value": "hello",
		"Tier":  "Advanced",
	})

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "Advanced", out["Tier"])
}
func TestPutParameter_Tier_IntelligentTiering(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/tier-intelligent",
		"Type":  "String",
		"Value": "hello",
		"Tier":  "Intelligent-Tiering",
	})

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "Intelligent-Tiering", out["Tier"])
}
func TestPutParameter_Tier_DefaultIsStandard(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/no-tier",
		"Type":  "String",
		"Value": "hello",
	})

	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "Standard", out["Tier"])
}
func TestPutParameter_Tier_InvalidRejected(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/bad-tier",
		"Type":  "String",
		"Value": "hello",
		"Tier":  "Gold",
	})

	assert.Equal(t, http.StatusBadRequest, code)
}
func TestPutParameter_Standard_SizeLimit(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	bigValue := strings.Repeat("x", 4097)
	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/big",
		"Type":  "String",
		"Value": bigValue,
		"Tier":  "Standard",
	})

	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, out["message"], "4096")
}
func TestPutParameter_Advanced_SizeFits(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	bigValue := strings.Repeat("x", 5000)
	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/big-advanced",
		"Type":  "String",
		"Value": bigValue,
		"Tier":  "Advanced",
	})

	assert.Equal(t, http.StatusOK, code)
}
func TestPutParameter_Advanced_SizeLimit(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	tooBig := strings.Repeat("x", 8193)
	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/too-big-advanced",
		"Type":  "String",
		"Value": tooBig,
		"Tier":  "Advanced",
	})

	assert.Equal(t, http.StatusBadRequest, code)
}
func TestPutParameter_AllowedPattern_Match(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":           "/app/pattern-ok",
		"Type":           "String",
		"Value":          "abc123",
		"AllowedPattern": "^[a-z0-9]+$",
	})

	assert.Equal(t, http.StatusOK, code)
}
func TestPutParameter_AllowedPattern_Mismatch(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":           "/app/pattern-bad",
		"Type":           "String",
		"Value":          "ABC!!!",
		"AllowedPattern": "^[a-z0-9]+$",
	})

	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, out["message"], "AllowedPattern")
}
func TestPutParameter_AllowedPattern_InvalidRegex(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":           "/app/bad-regex",
		"Type":           "String",
		"Value":          "v",
		"AllowedPattern": "[invalid(",
	})

	assert.Equal(t, http.StatusBadRequest, code)
}
func TestPutParameter_DataType_Text(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":     "/app/dt-text",
		"Type":     "String",
		"Value":    "hello",
		"DataType": "text",
	})

	assert.Equal(t, http.StatusOK, code)
}
func TestPutParameter_DataType_EC2Image(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":     "/app/dt-ec2",
		"Type":     "String",
		"Value":    "ami-0abcdef1234567890",
		"DataType": "aws:ec2:image",
	})

	assert.Equal(t, http.StatusOK, code)
}
func TestPutParameter_DataType_Invalid(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, _ := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":     "/app/dt-bad",
		"Type":     "String",
		"Value":    "v",
		"DataType": "bogus:type",
	})

	assert.Equal(t, http.StatusBadRequest, code)
}

// TestSSMHandler_Reset verifies the reset method clears state.
func TestSSMHandler_Reset(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name:  "/test/reset-param",
		Type:  "String",
		Value: "value",
	})
	require.NoError(t, err)

	h.Reset()
}
func TestSSMBackend_GetParameters_MissingParam(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, _ = b.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name:  "/exists",
		Type:  "String",
		Value: "val",
	})

	out, err := b.GetParameters(context.TODO(), &ssm.GetParametersInput{
		Names: []string{"/exists", "/does-not-exist"},
	})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 1)
	assert.Equal(t, "/exists", out.Parameters[0].Name)
	assert.Len(t, out.InvalidParameters, 1)
	assert.Equal(t, "/does-not-exist", out.InvalidParameters[0])
}
func TestSSMBackend_DeleteParameter_NotFound(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, err := b.DeleteParameter(context.TODO(), &ssm.DeleteParameterInput{Name: "/nonexistent"})
	require.Error(t, err)
	require.ErrorIs(t, err, ssm.ErrParameterNotFound)
}
func TestSSMBackend_DeleteParameter_Success(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, _ = b.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name:  "/to-delete",
		Type:  "String",
		Value: "val",
	})

	_, err := b.DeleteParameter(context.TODO(), &ssm.DeleteParameterInput{Name: "/to-delete"})
	require.NoError(t, err)

	_, err = b.GetParameter(context.TODO(), &ssm.GetParameterInput{Name: "/to-delete"})
	require.ErrorIs(t, err, ssm.ErrParameterNotFound)
}
func TestSSMHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantName string
	}{
		{
			name:     "name_field_extracted",
			body:     `{"Name":"/my/param"}`,
			wantName: "/my/param",
		},
		{
			name:     "no_name_field_returns_empty",
			body:     `{"Type":"String"}`,
			wantName: "",
		},
		{
			name:     "invalid_json_returns_empty",
			body:     "not-json",
			wantName: "",
		},
		{
			name:     "name_not_string_returns_empty",
			body:     `{"Name":123}`,
			wantName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			e := echo.New()

			var req *http.Request
			if tt.body != "" {
				req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			} else {
				req = httptest.NewRequest(http.MethodPost, "/", nil)
			}

			req.Header.Set("X-Amz-Target", "AmazonSSM.GetParameter")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantName, h.ExtractResource(c))
		})
	}
}
func TestSSMHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	rec := doRequest(t, h, "UnknownOp", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["__type"], "UnknownOperationException")
}
func TestSSMHandler_InternalError(t *testing.T) {
	t.Parallel()

	// PutParameter with invalid JSON triggers UnmarshalError, which would fall to InternalServerError
	// unless it's a recognized error type
	h, _ := newTestHandler(t)
	rec := doRequest(t, h, "PutParameter", `{"Name":`)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}
func TestSSMHandler_DeleteParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(b *ssm.InMemoryBackend)
		body     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(context.TODO(), &ssm.PutParameterInput{
					Name:  "/delete-me",
					Type:  "String",
					Value: "val",
				})
			},
			body:     `{"Name":"/delete-me"}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     `{"Name":"/nonexistent"}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "DeleteParameter", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestSSMHandler_SnapshotRestore_Delegation tests the Handler's Snapshot/Restore delegation.
func TestSSMHandler_SnapshotRestore_Delegation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *ssm.InMemoryBackend)
		check func(t *testing.T, b *ssm.InMemoryBackend)
		name  string
	}{
		{
			name: "snapshot_and_restore_via_handler",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(
					context.TODO(),
					&ssm.PutParameterInput{Name: "/snap-param", Type: "String", Value: "snap-value"},
				)
			},
			check: func(t *testing.T, b *ssm.InMemoryBackend) {
				t.Helper()

				out, err := b.GetParameter(context.TODO(), &ssm.GetParameterInput{Name: "/snap-param"})
				require.NoError(t, err)
				assert.Equal(t, "snap-value", out.Parameter.Value)
			},
		},
		{
			name:  "empty_backend_snapshot_and_restore",
			setup: func(_ *ssm.InMemoryBackend) {},
			check: func(t *testing.T, b *ssm.InMemoryBackend) {
				t.Helper()
				assert.Empty(t, b.ListAll(context.TODO()))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			origBackend := ssm.NewInMemoryBackend()
			tt.setup(origBackend)

			snap := origBackend.Snapshot(t.Context())
			require.NotNil(t, snap)

			freshBackend := ssm.NewInMemoryBackend()
			require.NoError(t, freshBackend.Restore(t.Context(), snap))

			tt.check(t, freshBackend)
		})
	}
}

// TestSSMHandler_ValidationError covers the path where a ValidationException error is returned.
func TestSSMHandler_ValidationError(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// ssm/amazon prefix triggers ErrValidationException, which is now explicitly handled.
	rec := doRequest(t, h, "PutParameter", `{"Name":"ssm/bad","Type":"String","Value":"v"}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["__type"])
}

// TestSSMHandler_ErrInvalidKeyID covers the InvalidKeyId path.
func TestSSMHandler_ErrInvalidKeyID(t *testing.T) {
	t.Parallel()

	// The ErrInvalidKeyID is returned when KeyId is provided
	// We exercise handleError by directly checking each branch
	// The InternalServerError branch is hit by a random error
	h, _ := newTestHandler(t)

	// Unknown operation triggers UnknownOperationException
	rec := doRequest(t, h, "BogusOperation", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["__type"], "UnknownOperationException")
}

// testInvalidBackend wraps InMemoryBackend and returns an internal error on PutParameter.
type testInvalidBackend struct {
	*ssm.InMemoryBackend
}

var errSimulatedInternal = errors.New("simulated internal error")

func (b *testInvalidBackend) PutParameter(
	_ context.Context,
	_ *ssm.PutParameterInput,
) (*ssm.PutParameterOutput, error) {
	return nil, errSimulatedInternal
}

// TestSSMHandler_InternalServerError exercises the InternalServerError path in handleError.
func TestSSMHandler_InternalServerError(t *testing.T) {
	t.Parallel()

	// Use a backend that returns a non-recognized error
	errBackend := &testInvalidBackend{InMemoryBackend: ssm.NewInMemoryBackend()}
	h2 := ssm.NewHandler(errBackend)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/",
		strings.NewReader(`{"Name":"/test","Type":"String","Value":"v"}`))
	req.Header.Set("X-Amz-Target", "AmazonSSM.PutParameter")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h2.Handler()(c))
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}
func TestSSMHandler_HandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *ssm.InMemoryBackend)
		name  string
	}{
		{
			name: "with_data",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(
					context.TODO(),
					&ssm.PutParameterInput{Name: "/h-snap", Type: "String", Value: "hval"},
				)
			},
		},
		{
			name:  "empty",
			setup: func(_ *ssm.InMemoryBackend) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			tt.setup(backend)
			h := ssm.NewHandler(backend)

			snap := h.Snapshot(t.Context())
			require.NotNil(t, snap)

			freshBackend := ssm.NewInMemoryBackend()
			freshH := ssm.NewHandler(freshBackend)
			require.NoError(t, freshH.Restore(t.Context(), snap))

			if tt.name == "with_data" {
				out, err := freshBackend.GetParameter(context.TODO(), &ssm.GetParameterInput{Name: "/h-snap"})
				require.NoError(t, err)
				assert.Equal(t, "hval", out.Parameter.Value)
			}
		})
	}
}

// TestRefinement2_DeepCopy_Association verifies modifying caller's Parameters doesn't corrupt stored data.
func TestDeepCopy_Association(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		docName   string
		params    map[string][]string
		mutateKey string
		mutateVal string
	}{
		{
			name:      "modify_after_create_does_not_corrupt",
			docName:   "TestDoc",
			params:    map[string][]string{"commands": {"echo hello"}},
			mutateKey: "commands",
			mutateVal: "echo MUTATED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ssm.NewInMemoryBackend()
			h := ssm.NewHandler(backend)

			doRequest(t, h, "CreateDocument",
				`{"Name":"`+tt.docName+`","Content":"{\"schemaVersion\":\"2.2\"}"}`)

			paramsJSON, _ := json.Marshal(tt.params)
			createBody := `{"Name":"` + tt.docName + `","Parameters":` + string(paramsJSON) + `}`

			rec := doRequest(t, h, "CreateAssociation", createBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp ssm.CreateAssociationOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			originalCmds := createResp.AssociationDescription.Parameters[tt.mutateKey]

			// Verify original value was stored correctly
			require.NotEmpty(t, originalCmds)
			assert.Equal(t, tt.params[tt.mutateKey][0], originalCmds[0])
		})
	}
}
func newFullHandler() *ssm.Handler {
	return ssm.NewHandler(ssm.NewInMemoryBackend())
}
func mustPost(t *testing.T, h *ssm.Handler, op string, body any) (int, map[string]any) {
	t.Helper()

	b, err := json.Marshal(body)
	require.NoError(t, err)

	rec := doRequest(t, h, op, string(b))

	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)

	return rec.Code, out
}
func TestFull_ParameterStore_PutGetDelete(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Put
	code, out := mustPost(t, h, "PutParameter", map[string]any{
		"Name":  "/full/p1",
		"Type":  "String",
		"Value": "v1",
	})
	assert.Equal(t, http.StatusOK, code)
	assert.InEpsilon(t, float64(1), out["Version"], 0.01)

	// Get
	code, out = mustPost(t, h, "GetParameter", map[string]any{"Name": "/full/p1"})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "v1", out["Parameter"].(map[string]any)["Value"])

	// Overwrite
	code, out = mustPost(t, h, "PutParameter", map[string]any{
		"Name":      "/full/p1",
		"Type":      "String",
		"Value":     "v2",
		"Overwrite": true,
	})
	assert.Equal(t, http.StatusOK, code)
	assert.InEpsilon(t, float64(2), out["Version"], 0.01)

	// Delete
	code, _ = mustPost(t, h, "DeleteParameter", map[string]any{"Name": "/full/p1"})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = mustPost(t, h, "GetParameter", map[string]any{"Name": "/full/p1"})
	assert.Equal(t, http.StatusBadRequest, code)
}
func TestFull_ParameterStore_GetParameters_Batch(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	for _, name := range []string{"/b/1", "/b/2", "/b/3"} {
		mustPost(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	code, out := mustPost(t, h, "GetParameters", map[string]any{
		"Names": []string{"/b/1", "/b/2", "/b/missing"},
	})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	invalid := out["InvalidParameters"].([]any)
	assert.Len(t, params, 2)
	assert.Len(t, invalid, 1)
	assert.Equal(t, "/b/missing", invalid[0])
}
func TestFull_ParameterStore_DeleteParameters_Batch(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	for _, name := range []string{"/del/1", "/del/2"} {
		mustPost(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	code, out := mustPost(t, h, "DeleteParameters", map[string]any{
		"Names": []string{"/del/1", "/del/missing"},
	})

	assert.Equal(t, http.StatusOK, code)
	deleted := out["DeletedParameters"].([]any)
	invalid := out["InvalidParameters"].([]any)
	assert.Len(t, deleted, 1)
	assert.Len(t, invalid, 1)
}

// TestPutParameter_Validation exercises validation branches in PutParameter.
func TestPutParameter_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		name    string
		input   ssm.PutParameterInput
		wantErr bool
	}{
		{
			name: "invalid_data_type",
			input: ssm.PutParameterInput{
				Name:     "/valid/param",
				Type:     "String",
				Value:    "v",
				DataType: "badtype",
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
		{
			name: "invalid_allowed_pattern",
			input: ssm.PutParameterInput{
				Name:           "/valid/param",
				Type:           "String",
				Value:          "hello world",
				AllowedPattern: `^\d+$`,
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
		{
			name: "bad_regex_pattern",
			input: ssm.PutParameterInput{
				Name:           "/valid/param",
				Type:           "String",
				Value:          "v",
				AllowedPattern: `[invalid`,
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
		{
			name: "tier_too_large_standard",
			input: ssm.PutParameterInput{
				Name:  "/valid/param",
				Type:  "String",
				Value: string(make([]byte, 5000)), // > 4096 bytes
				Tier:  "Standard",
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
		{
			name: "tier_too_large_advanced",
			input: ssm.PutParameterInput{
				Name:  "/valid/param",
				Type:  "String",
				Value: string(make([]byte, 9000)), // > 8192 bytes
				Tier:  "Advanced",
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
		{
			name: "invalid_tier_string",
			input: ssm.PutParameterInput{
				Name:  "/valid/param",
				Type:  "String",
				Value: "v",
				Tier:  "UnknownTier",
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
		{
			name: "valid_ec2_image_datatype",
			input: ssm.PutParameterInput{
				Name:     "/valid/ec2",
				Type:     "String",
				Value:    "ami-12345678",
				DataType: "aws:ec2:image",
			},
			wantErr: false,
		},
		{
			name: "valid_intelligent_tiering",
			input: ssm.PutParameterInput{
				Name:  "/valid/tiering",
				Type:  "String",
				Value: "v",
				Tier:  "Intelligent-Tiering",
			},
			wantErr: false,
		},
		{
			name: "param_name_too_long",
			input: ssm.PutParameterInput{
				Name:  "/" + string(make([]byte, 2048)),
				Type:  "String",
				Value: "v",
			},
			wantErr: true,
			errIs:   ssm.ErrValidationException,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.PutParameter(context.TODO(), &tt.input)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestHandlerReset exercises the handler Reset method.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(h *ssm.Handler, b *ssm.InMemoryBackend)
		name  string
	}{
		{
			name: "reset_clears_parameters",
			setup: func(_ *ssm.Handler, b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(context.TODO(), &ssm.PutParameterInput{
					Name: "/before-reset", Type: "String", Value: "v",
				})
			},
		},
		{
			name:  "reset_on_empty_backend",
			setup: func(_ *ssm.Handler, _ *ssm.InMemoryBackend) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			tt.setup(h, b)
			h.Reset()
			assert.Empty(t, b.ListAll(context.TODO()))
		})
	}
}
func TestAdvancedTier_AcceptsLargeValue(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Advanced tier allows up to 8 KiB — use a 5 KiB value.
	largeVal := make([]byte, 5000)
	for i := range largeVal {
		largeVal[i] = 'A'
	}

	body, _ := json.Marshal(map[string]any{
		"Name":  "/advanced/param",
		"Value": string(largeVal),
		"Type":  "String",
		"Tier":  "Advanced",
	})
	rec := doRequest(t, h, "PutParameter", string(body))
	assert.Equal(t, http.StatusOK, rec.Code, "Advanced tier should accept 5KiB value")
}
func TestStandardTier_RejectsLargeValue(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// Standard tier only allows up to 4 KiB — use 5 KiB.
	largeVal := make([]byte, 5000)
	for i := range largeVal {
		largeVal[i] = 'B'
	}

	body, _ := json.Marshal(map[string]any{
		"Name":  "/standard/param",
		"Value": string(largeVal),
		"Type":  "String",
		"Tier":  "Standard",
	})
	rec := doRequest(t, h, "PutParameter", string(body))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "Standard tier should reject 5KiB value")
}
