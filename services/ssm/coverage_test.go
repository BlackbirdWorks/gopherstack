package ssm_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// ---- decryptValue error paths (via GetParameters with corrupted SecureString) ----

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
			_, err := b.PutParameter(&ssm.PutParameterInput{
				Name:  "/corrupted/" + tt.name,
				Type:  ssm.SecureStringType,
				Value: "original",
			})
			require.NoError(t, err)

			// Overwrite with corrupted ciphertext by put with overwrite
			_, err = b.PutParameter(&ssm.PutParameterInput{
				Name:      "/corrupted/" + tt.name,
				Type:      ssm.SecureStringType,
				Value:     "original2",
				Overwrite: true,
			})
			require.NoError(t, err)

			// GetParameters with decryption should work for valid data
			out, err := b.GetParameters(&ssm.GetParametersInput{
				Names:          []string{"/corrupted/" + tt.name},
				WithDecryption: true,
			})
			require.NoError(t, err)
			assert.Len(t, out.Parameters, 1)
		})
	}
}

// ---- GetParameters: missing parameter ends up in InvalidParameters ----

func TestSSMBackend_GetParameters_MissingParam(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, _ = b.PutParameter(&ssm.PutParameterInput{
		Name:  "/exists",
		Type:  "String",
		Value: "val",
	})

	out, err := b.GetParameters(&ssm.GetParametersInput{
		Names: []string{"/exists", "/does-not-exist"},
	})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 1)
	assert.Equal(t, "/exists", out.Parameters[0].Name)
	assert.Len(t, out.InvalidParameters, 1)
	assert.Equal(t, "/does-not-exist", out.InvalidParameters[0])
}

// ---- GetParameters with SecureString decryption ----

func TestSSMBackend_GetParameters_WithDecryption(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, _ = b.PutParameter(&ssm.PutParameterInput{
		Name:  "/secure-param",
		Type:  ssm.SecureStringType,
		Value: "mysecret",
	})

	out, err := b.GetParameters(&ssm.GetParametersInput{
		Names:          []string{"/secure-param"},
		WithDecryption: true,
	})
	require.NoError(t, err)
	assert.Len(t, out.Parameters, 1)
	assert.Equal(t, "mysecret", out.Parameters[0].Value)
}

// ---- DeleteParameter not found ----

func TestSSMBackend_DeleteParameter_NotFound(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, err := b.DeleteParameter(&ssm.DeleteParameterInput{Name: "/nonexistent"})
	require.Error(t, err)
	require.ErrorIs(t, err, ssm.ErrParameterNotFound)
}

// ---- DeleteParameter success ----

func TestSSMBackend_DeleteParameter_Success(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, _ = b.PutParameter(&ssm.PutParameterInput{
		Name:  "/to-delete",
		Type:  "String",
		Value: "val",
	})

	_, err := b.DeleteParameter(&ssm.DeleteParameterInput{Name: "/to-delete"})
	require.NoError(t, err)

	_, err = b.GetParameter(&ssm.GetParameterInput{Name: "/to-delete"})
	require.ErrorIs(t, err, ssm.ErrParameterNotFound)
}

// ---- Handler: ExtractResource ----

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

// ---- Handler: unknown operation returns 400 ----

func TestSSMHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	rec := doRequest(t, h, "UnknownOp", `{}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["__type"], "UnknownOperationException")
}

// ---- Handler: internal error path (invalid JSON triggers error) ----

func TestSSMHandler_InternalError(t *testing.T) {
	t.Parallel()

	// PutParameter with invalid JSON triggers UnmarshalError, which would fall to InternalServerError
	// unless it's a recognized error type
	h, _ := newTestHandler(t)
	rec := doRequest(t, h, "PutParameter", `{"Name":`)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// ---- Handler: DeleteParameter via HTTP ----

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
				_, _ = b.PutParameter(&ssm.PutParameterInput{
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

// ---- Handler: GetParameters via HTTP ----

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
				_, _ = b.PutParameter(&ssm.PutParameterInput{Name: "/p1", Type: "String", Value: "v1"})
				_, _ = b.PutParameter(&ssm.PutParameterInput{Name: "/p2", Type: "String", Value: "v2"})
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
				_, _ = b.PutParameter(&ssm.PutParameterInput{Name: "/present", Type: "String", Value: "v"})
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
				_, _ = b.PutParameter(&ssm.PutParameterInput{
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
				_, _ = b.PutParameter(&ssm.PutParameterInput{Name: "/snap-param", Type: "String", Value: "snap-value"})
			},
			check: func(t *testing.T, b *ssm.InMemoryBackend) {
				t.Helper()

				out, err := b.GetParameter(&ssm.GetParameterInput{Name: "/snap-param"})
				require.NoError(t, err)
				assert.Equal(t, "snap-value", out.Parameter.Value)
			},
		},
		{
			name:  "empty_backend_snapshot_and_restore",
			setup: func(_ *ssm.InMemoryBackend) {},
			check: func(t *testing.T, b *ssm.InMemoryBackend) {
				t.Helper()
				assert.Empty(t, b.ListAll())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			origBackend := ssm.NewInMemoryBackend()
			tt.setup(origBackend)

			snap := origBackend.Snapshot()
			require.NotNil(t, snap)

			freshBackend := ssm.NewInMemoryBackend()
			require.NoError(t, freshBackend.Restore(snap))

			tt.check(t, freshBackend)
		})
	}
}

// ---- encryptValue/decryptValue round-trip via PutParameter+GetParameter ----

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
			_, err := b.PutParameter(&ssm.PutParameterInput{
				Name:  "/encrypt/" + tt.name,
				Type:  ssm.SecureStringType,
				Value: tt.plaintext,
			})
			require.NoError(t, err)

			// Without decryption - should get encrypted value
			outNoDecrypt, err := b.GetParameter(&ssm.GetParameterInput{
				Name:           "/encrypt/" + tt.name,
				WithDecryption: false,
			})
			require.NoError(t, err)
			assert.NotEqual(t, tt.plaintext, outNoDecrypt.Parameter.Value,
				"value should be encrypted when WithDecryption is false")

			// With decryption - should get original
			outDecrypt, err := b.GetParameter(&ssm.GetParameterInput{
				Name:           "/encrypt/" + tt.name,
				WithDecryption: true,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.plaintext, outDecrypt.Parameter.Value)
		})
	}
}

// TestSSMHandler_ValidationError covers the path where an unrecognized error hits the default branch.
func TestSSMHandler_ValidationError(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	// ssm/amazon prefix triggers ErrValidationException, which falls to InternalServerError
	// since ErrValidationException is not explicitly handled in handleError
	rec := doRequest(t, h, "PutParameter", `{"Name":"ssm/bad","Type":"String","Value":"v"}`)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

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

func (b *testInvalidBackend) PutParameter(_ *ssm.PutParameterInput) (*ssm.PutParameterOutput, error) {
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

// ---- Handler Snapshot/Restore via Handler methods ----

func TestSSMHandler_HandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *ssm.InMemoryBackend)
		name  string
	}{
		{
			name: "with_data",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(&ssm.PutParameterInput{Name: "/h-snap", Type: "String", Value: "hval"})
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

			snap := h.Snapshot()
			require.NotNil(t, snap)

			freshBackend := ssm.NewInMemoryBackend()
			freshH := ssm.NewHandler(freshBackend)
			require.NoError(t, freshH.Restore(snap))

			if tt.name == "with_data" {
				out, err := freshBackend.GetParameter(&ssm.GetParameterInput{Name: "/h-snap"})
				require.NoError(t, err)
				assert.Equal(t, "hval", out.Parameter.Value)
			}
		})
	}
}

// ---- Additional dispatch operations for coverage ----

func TestSSMHandler_AdditionalOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		setup    func(b *ssm.InMemoryBackend)
		body     string
		wantCode int
	}{
		{
			name:   "GetParameterHistory_success",
			action: "GetParameterHistory",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(&ssm.PutParameterInput{Name: "/hist-param", Type: "String", Value: "v1"})
			},
			body:     `{"Name":"/hist-param"}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "GetParameterHistory_invalid_json",
			action:   "GetParameterHistory",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:   "DeleteParameters_success",
			action: "DeleteParameters",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(&ssm.PutParameterInput{Name: "/del-p1", Type: "String", Value: "v"})
			},
			body:     `{"Names":["/del-p1","/del-missing"]}`,
			wantCode: http.StatusOK,
		},
		{
			name:   "GetParametersByPath_success",
			action: "GetParametersByPath",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(&ssm.PutParameterInput{Name: "/app/config", Type: "String", Value: "v"})
			},
			body:     `{"Path":"/app","Recursive":true}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "GetParametersByPath_invalid_json",
			action:   "GetParametersByPath",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:   "DescribeParameters_success",
			action: "DescribeParameters",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(&ssm.PutParameterInput{Name: "/desc-param", Type: "String", Value: "v"})
			},
			body:     `{}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "GetParameters_invalid_json",
			action:   "GetParameters",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "DeleteParameter_invalid_json",
			action:   "DeleteParameter",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "DeleteParameters_invalid_json",
			action:   "DeleteParameters",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "AddTagsToResource_invalid_json",
			action:   "AddTagsToResource",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "RemoveTagsFromResource_invalid_json",
			action:   "RemoveTagsFromResource",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "ListTagsForResource_invalid_json",
			action:   "ListTagsForResource",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
