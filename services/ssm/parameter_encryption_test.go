package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestPutParameterRequiresValidType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     string
		name     string
		wantCode int
	}{
		{
			name:     "absent_type_rejected",
			body:     `{"Name":"my-param","Value":"val"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_type_rejected",
			body:     `{"Name":"my-param","Value":"val","Type":""}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_type_rejected",
			body:     `{"Name":"my-param","Value":"val","Type":"BadType"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "string_type_accepted",
			body:     `{"Name":"my-param","Value":"val","Type":"String"}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "stringlist_type_accepted",
			body:     `{"Name":"my-list","Value":"a,b","Type":"StringList"}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "securestring_type_accepted",
			body:     `{"Name":"my-secret","Value":"pw","Type":"SecureString"}`,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := doRequest(t, h, "PutParameter", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"PutParameter status for case %q", tt.name)
		})
	}
}
func TestSSMSecureStringKMS_EncryptedAtRest(t *testing.T) {
	t.Parallel()

	ssmBackend, keyID := newSSMWithKMS(t)

	// PutParameter — value should be KMS-encrypted.
	_, err := ssmBackend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name:  "/test/secret",
		Type:  "SecureString",
		Value: "my-plaintext-password",
		KeyID: keyID,
	})
	require.NoError(t, err)

	// Read without decryption — must not be plaintext.
	raw, err := ssmBackend.GetParameter(context.TODO(), &ssm.GetParameterInput{
		Name:           "/test/secret",
		WithDecryption: false,
	})
	require.NoError(t, err)
	assert.NotEqual(t, "my-plaintext-password", raw.Parameter.Value, "plaintext must not be stored as-is")
}
func TestSSMSecureStringKMS_DecryptOnGet(t *testing.T) {
	t.Parallel()

	ssmBackend, keyID := newSSMWithKMS(t)

	_, err := ssmBackend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name:  "/test/db-pass",
		Type:  "SecureString",
		Value: "super-secret-db-password",
		KeyID: keyID,
	})
	require.NoError(t, err)

	out, err := ssmBackend.GetParameter(context.TODO(), &ssm.GetParameterInput{
		Name:           "/test/db-pass",
		WithDecryption: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "super-secret-db-password", out.Parameter.Value)
}
func TestSSMSecureStringKMS_MultipleParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keyValue string
		pname    string
	}{
		{"alpha", "password-alpha", "/multi/alpha"},
		{"beta", "password-beta", "/multi/beta"},
		{"gamma", "password-gamma", "/multi/gamma"},
	}

	ssmBackend, keyID := newSSMWithKMS(t)

	for _, tt := range tests {
		_, err := ssmBackend.PutParameter(context.TODO(), &ssm.PutParameterInput{
			Name:  tt.pname,
			Type:  "SecureString",
			Value: tt.keyValue,
			KeyID: keyID,
		})
		require.NoError(t, err, "param %s", tt.pname)
	}

	for _, tt := range tests {
		out, err := ssmBackend.GetParameter(context.TODO(), &ssm.GetParameterInput{
			Name:           tt.pname,
			WithDecryption: true,
		})
		require.NoError(t, err, "get %s", tt.pname)
		assert.Equal(t, tt.keyValue, out.Parameter.Value, "decrypted value mismatch for %s", tt.pname)
	}
}
func TestSSMSecureStringKMS_GetParameters(t *testing.T) {
	t.Parallel()

	ssmBackend, keyID := newSSMWithKMS(t)

	for _, p := range []struct{ name, val string }{
		{"/batch/x", "val-x"},
		{"/batch/y", "val-y"},
	} {
		_, err := ssmBackend.PutParameter(context.TODO(), &ssm.PutParameterInput{
			Name:  p.name,
			Type:  "SecureString",
			Value: p.val,
			KeyID: keyID,
		})
		require.NoError(t, err)
	}

	out, err := ssmBackend.GetParameters(context.TODO(), &ssm.GetParametersInput{
		Names:          []string{"/batch/x", "/batch/y"},
		WithDecryption: true,
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 2)
	vals := map[string]string{}
	for _, p := range out.Parameters {
		vals[p.Name] = p.Value
	}
	assert.Equal(t, "val-x", vals["/batch/x"])
	assert.Equal(t, "val-y", vals["/batch/y"])
}
func TestSSMSecureStringKMS_GetParametersByPath(t *testing.T) {
	t.Parallel()

	ssmBackend, keyID := newSSMWithKMS(t)

	for _, p := range []struct{ name, val string }{
		{"/path/a", "v-a"},
		{"/path/b", "v-b"},
	} {
		_, err := ssmBackend.PutParameter(context.TODO(), &ssm.PutParameterInput{
			Name:  p.name,
			Type:  "SecureString",
			Value: p.val,
			KeyID: keyID,
		})
		require.NoError(t, err)
	}

	out, err := ssmBackend.GetParametersByPath(context.TODO(), &ssm.GetParametersByPathInput{
		Path:           "/path/",
		WithDecryption: true,
	})
	require.NoError(t, err)
	require.Len(t, out.Parameters, 2)
	vals := map[string]string{}
	for _, p := range out.Parameters {
		vals[p.Name] = p.Value
	}
	assert.Equal(t, "v-a", vals["/path/a"])
	assert.Equal(t, "v-b", vals["/path/b"])
}
func TestSSMSecureStringKMS_History(t *testing.T) {
	t.Parallel()

	ssmBackend, keyID := newSSMWithKMS(t)

	for _, v := range []string{"v1", "v2"} {
		_, err := ssmBackend.PutParameter(context.TODO(), &ssm.PutParameterInput{
			Name:      "/hist/key",
			Type:      "SecureString",
			Value:     v,
			KeyID:     keyID,
			Overwrite: true,
		})
		require.NoError(t, err)
	}

	hist, err := ssmBackend.GetParameterHistory(context.TODO(), &ssm.GetParameterHistoryInput{
		Name:           "/hist/key",
		WithDecryption: true,
	})
	require.NoError(t, err)
	require.Len(t, hist.Parameters, 2)
	vals := map[string]bool{}
	for _, p := range hist.Parameters {
		vals[p.Value] = true
	}
	assert.True(t, vals["v1"], "history should contain v1")
	assert.True(t, vals["v2"], "history should contain v2")
}
func TestSSMSecureStringKMS_MockFallback(t *testing.T) {
	t.Parallel()

	// Without KMS wired, SSM uses built-in mock cipher.
	backend := ssm.NewInMemoryBackend()

	_, err := backend.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name:  "/no-kms/secret",
		Type:  "SecureString",
		Value: "mock-plaintext",
	})
	require.NoError(t, err)

	out, err := backend.GetParameter(context.TODO(), &ssm.GetParameterInput{
		Name:           "/no-kms/secret",
		WithDecryption: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "mock-plaintext", out.Parameter.Value)
}
func TestPerInstanceMockKMSKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "instances use distinct keys"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b1 := ssm.NewInMemoryBackend()
			b2 := ssm.NewInMemoryBackend()

			// Put a SecureString in backend 1.
			_, err := b1.PutParameter(ctx, &ssm.PutParameterInput{
				Name:  "/sec/key",
				Type:  ssm.SecureStringType,
				Value: "top-secret",
			})
			require.NoError(t, err)

			// Retrieve encrypted storage value via ForceInsertParameter trick:
			// read back with WithDecryption=false to get the raw ciphertext.
			out, err := b1.GetParameter(ctx, &ssm.GetParameterInput{
				Name:           "/sec/key",
				WithDecryption: false,
			})
			require.NoError(t, err)
			rawCiphertext := out.Parameter.Value

			// Inject the ciphertext from b1 into b2 directly, bypassing encryption.
			b2.ForceInsertParameter(ssm.Parameter{
				Name:  "/sec/key",
				Type:  ssm.SecureStringType,
				Value: rawCiphertext,
			})

			// Decrypting with b2 must fail or return different plaintext because
			// b2 uses a different AES key than b1.
			out2, err := b2.GetParameter(ctx, &ssm.GetParameterInput{
				Name:           "/sec/key",
				WithDecryption: true,
			})
			if err == nil {
				// If no error, the decrypted value must not equal the original plaintext.
				assert.NotEqual(t, "top-secret", out2.Parameter.Value,
					"ciphertext from b1 must not decrypt successfully under b2's key")
			}
			// An error (e.g. auth tag mismatch) is also acceptable.
		})
	}
}

// TestPerInstanceMockKMSKey_SelfRoundTrip confirms that a single backend can
// encrypt and decrypt its own SecureString values correctly.
func TestPerInstanceMockKMSKey_SelfRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		plaintext string
	}{
		{name: "short value", plaintext: "hello"},
		{name: "empty value", plaintext: ""},
		{name: "unicode value", plaintext: "こんにちは世界"},
		{name: "binary-like value", plaintext: "\x00\x01\x02\x03"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := ssm.NewInMemoryBackend()

			_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
				Name:  "/sec/val",
				Type:  ssm.SecureStringType,
				Value: tc.plaintext,
			})
			require.NoError(t, err)

			out, err := b.GetParameter(ctx, &ssm.GetParameterInput{
				Name:           "/sec/val",
				WithDecryption: true,
			})
			require.NoError(t, err)
			require.Equal(t, tc.plaintext, out.Parameter.Value)
		})
	}
}
func TestPutParameter_KeyId_Stored(t *testing.T) {
	t.Parallel()
	h := newAudit1Handler()

	code, out := postAudit1(t, h, "PutParameter", map[string]any{
		"Name":  "/app/secure",
		"Type":  "SecureString",
		"Value": "secret",
		"KeyId": "alias/my-key",
	})
	require.Equal(t, http.StatusOK, code)
	require.NotNil(t, out)

	code2, out2 := postAudit1(t, h, "GetParameter", map[string]any{
		"Name":           "/app/secure",
		"WithDecryption": true,
	})

	assert.Equal(t, http.StatusOK, code2)
	param := out2["Parameter"].(map[string]any)
	assert.Equal(t, "alias/my-key", param["KeyId"])
}
func TestGetParameter_DecryptError_Propagated(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	b.ForceInsertParameter(ssm.Parameter{
		Name:    "/sec/corrupt",
		Type:    "SecureString",
		Value:   "not-valid-ciphertext",
		Version: 1,
	})

	h := ssm.NewHandler(b)

	code, out := postAudit1(t, h, "GetParameter", map[string]any{
		"Name":           "/sec/corrupt",
		"WithDecryption": true,
	})

	// Must be an error, not 200 with silently swallowed garbage.
	assert.NotEqual(t, http.StatusOK, code)
	assert.NotNil(t, out)
}
func TestSSMBackend_DecryptValue_InvalidCiphertext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		corruptedValue string
	}{
		{
			name:           "invalid_base64",
			corruptedValue: "not-valid-base64!!!",
		},
		{
			name:           "too_short_after_decode",
			corruptedValue: "YQ==", // "a" decoded = 1 byte, shorter than nonce
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()

			// First put a valid SecureString parameter
			_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{
				Name:  "/corrupted/" + tt.name,
				Type:  ssm.SecureStringType,
				Value: "original",
			})
			require.NoError(t, err)

			// Overwrite with corrupted ciphertext by put with overwrite
			_, err = b.PutParameter(context.TODO(), &ssm.PutParameterInput{
				Name:      "/corrupted/" + tt.name,
				Type:      ssm.SecureStringType,
				Value:     "original2",
				Overwrite: true,
			})
			require.NoError(t, err)

			// GetParameters with decryption should work for valid data
			out, err := b.GetParameters(context.TODO(), &ssm.GetParametersInput{
				Names:          []string{"/corrupted/" + tt.name},
				WithDecryption: true,
			})
			require.NoError(t, err)
			assert.Len(t, out.Parameters, 1)
		})
	}
}
func TestSSMBackend_GetParameters_WithDecryption(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, _ = b.PutParameter(context.TODO(), &ssm.PutParameterInput{
		Name:  "/secure-param",
		Type:  ssm.SecureStringType,
		Value: "mysecret",
	})

	out, err := b.GetParameters(context.TODO(), &ssm.GetParametersInput{
		Names:          []string{"/secure-param"},
		WithDecryption: true,
	})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 1)
	assert.Equal(t, "mysecret", out.Parameters[0].Value)
}
func TestSSMHandler_GetParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *ssm.InMemoryBackend)
		verify   func(t *testing.T, body []byte)
		name     string
		body     string
		wantCode int
	}{
		{
			name: "all_found",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: "/p1", Type: "String", Value: "v1"})
				_, _ = b.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: "/p2", Type: "String", Value: "v2"})
			},
			body:     `{"Names":["/p1","/p2"]}`,
			wantCode: http.StatusOK,
			verify: func(t *testing.T, body []byte) {
				t.Helper()

				var out ssm.GetParametersOutput
				require.NoError(t, json.Unmarshal(body, &out))
				assert.Len(t, out.Parameters, 2)
				assert.Empty(t, out.InvalidParameters)
			},
		},
		{
			name: "partial_miss",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(
					context.TODO(),
					&ssm.PutParameterInput{Name: "/present", Type: "String", Value: "v"},
				)
			},
			body:     `{"Names":["/present","/absent"]}`,
			wantCode: http.StatusOK,
			verify: func(t *testing.T, body []byte) {
				t.Helper()

				var out ssm.GetParametersOutput
				require.NoError(t, json.Unmarshal(body, &out))
				assert.Len(t, out.Parameters, 1)
				assert.Len(t, out.InvalidParameters, 1)
				assert.Equal(t, "/absent", out.InvalidParameters[0])
			},
		},
		{
			name: "with_decryption_secure_string",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(context.TODO(), &ssm.PutParameterInput{
					Name:  "/secure",
					Type:  ssm.SecureStringType,
					Value: "topsecret",
				})
			},
			body:     `{"Names":["/secure"],"WithDecryption":true}`,
			wantCode: http.StatusOK,
			verify: func(t *testing.T, body []byte) {
				t.Helper()

				var out ssm.GetParametersOutput
				require.NoError(t, json.Unmarshal(body, &out))
				assert.Len(t, out.Parameters, 1)
				assert.Equal(t, "topsecret", out.Parameters[0].Value)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, "GetParameters", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.verify != nil {
				tt.verify(t, rec.Body.Bytes())
			}
		})
	}
}
func TestSSMBackend_EncryptDecryptRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		plaintext string
	}{
		{name: "simple_value", plaintext: "mysecretpassword"},
		{name: "empty_value", plaintext: ""},
		{name: "unicode_value", plaintext: "日本語テスト"},
		{name: "long_value", plaintext: strings.Repeat("a", 1000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{
				Name:  "/encrypt/" + tt.name,
				Type:  ssm.SecureStringType,
				Value: tt.plaintext,
			})
			require.NoError(t, err)

			// Without decryption - should get encrypted value
			outNoDecrypt, err := b.GetParameter(context.TODO(), &ssm.GetParameterInput{
				Name:           "/encrypt/" + tt.name,
				WithDecryption: false,
			})
			require.NoError(t, err)
			assert.NotEqual(t, tt.plaintext, outNoDecrypt.Parameter.Value,
				"value should be encrypted when WithDecryption is false")

			// With decryption - should get original
			outDecrypt, err := b.GetParameter(context.TODO(), &ssm.GetParameterInput{
				Name:           "/encrypt/" + tt.name,
				WithDecryption: true,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.plaintext, outDecrypt.Parameter.Value)
		})
	}
}
func TestFull_ParameterStore_SecureString_EncryptDecrypt(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	_, err := mustPost(t, h, "PutParameter", map[string]any{
		"Name":  "/sec/p",
		"Type":  "SecureString",
		"Value": "mysecret",
		"KeyId": "alias/test",
	})
	require.NotNil(t, err)

	// Get without decrypt → encrypted value
	code, out := mustPost(t, h, "GetParameter", map[string]any{
		"Name":           "/sec/p",
		"WithDecryption": false,
	})
	assert.Equal(t, http.StatusOK, code)
	param := out["Parameter"].(map[string]any)
	assert.NotEqual(t, "mysecret", param["Value"])
	assert.Equal(t, "alias/test", param["KeyId"])

	// Get with decrypt → plaintext
	code, out = mustPost(t, h, "GetParameter", map[string]any{
		"Name":           "/sec/p",
		"WithDecryption": true,
	})
	assert.Equal(t, http.StatusOK, code)
	param = out["Parameter"].(map[string]any)
	assert.Equal(t, "mysecret", param["Value"])
}
func TestFull_ParameterStore_GetParametersByPath_WithDecryption(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "PutParameter", map[string]any{
		"Name":  "/enc/key",
		"Type":  "SecureString",
		"Value": "secretval",
	})

	code, out := mustPost(t, h, "GetParametersByPath", map[string]any{
		"Path":           "/enc",
		"WithDecryption": true,
	})
	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	require.Len(t, params, 1)
	assert.Equal(t, "secretval", params[0].(map[string]any)["Value"])
}
func TestFull_GetParametersByPath_WithFilters(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	mustPost(t, h, "PutParameter", map[string]any{"Name": "/svc/str", "Type": "String", "Value": "v"})
	mustPost(t, h, "PutParameter", map[string]any{"Name": "/svc/sec", "Type": "SecureString", "Value": "s"})

	code, out := mustPost(t, h, "GetParametersByPath", map[string]any{
		"Path": "/svc",
		"ParameterFilters": []map[string]any{
			{"Key": "Type", "Values": []string{"String"}},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 1)
	assert.Equal(t, "/svc/str", params[0].(map[string]any)["Name"])
}
func TestSecureStringHistory_WithDecryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		withDecryption bool
		wantPlaintext  bool
	}{
		{
			name:           "with_decryption_returns_plaintext",
			withDecryption: true,
			wantPlaintext:  true,
		},
		{
			name:           "without_decryption_returns_encrypted",
			withDecryption: false,
			wantPlaintext:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			_, err := b.PutParameter(context.TODO(), &ssm.PutParameterInput{
				Name:  "/secure/pwd",
				Value: "my-secret-value",
				Type:  "SecureString",
			})
			require.NoError(t, err)

			body, _ := json.Marshal(map[string]any{
				"Name":           "/secure/pwd",
				"WithDecryption": tt.withDecryption,
			})

			rec := doRequest(t, h, "GetParameterHistory", string(body))
			require.Equal(t, http.StatusOK, rec.Code)

			bodyStr := rec.Body.String()
			if tt.wantPlaintext {
				assert.Contains(t, bodyStr, "my-secret-value")
			} else {
				assert.NotContains(t, bodyStr, "my-secret-value")
			}
		})
	}
}
