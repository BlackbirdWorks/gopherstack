package secretsmanager_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// GetRandomPassword constraints comprehensive (consolidated into a single
// table-driven test).
// ---------------------------------------------------------------------------

func TestGetRandomPassword_Constraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   *secretsmanager.GetRandomPasswordInput
		checkFn func(t *testing.T, pw string)
		name    string
		wantLen int
		wantErr bool
	}{
		{
			name:    "default",
			input:   &secretsmanager.GetRandomPasswordInput{},
			wantLen: 32,
		},
		{
			name: "custom_length",
			input: &secretsmanager.GetRandomPasswordInput{
				PasswordLength: ptr64(20),
			},
			wantLen: 20,
		},
		{
			name: "length_too_short",
			input: &secretsmanager.GetRandomPasswordInput{
				PasswordLength: ptr64(0),
			},
			wantErr: true,
		},
		{
			name: "length_too_long",
			input: &secretsmanager.GetRandomPasswordInput{
				PasswordLength: ptr64(4097),
			},
			wantErr: true,
		},
		{
			name: "exclude_numbers",
			input: &secretsmanager.GetRandomPasswordInput{
				ExcludeNumbers: true,
			},
			checkFn: func(t *testing.T, pw string) {
				t.Helper()
				for _, c := range pw {
					assert.False(t, c >= '0' && c <= '9', "password must not contain digits")
				}
			},
		},
		{
			name: "exclude_punctuation",
			input: &secretsmanager.GetRandomPasswordInput{
				ExcludePunctuation: true,
			},
			checkFn: func(t *testing.T, pw string) {
				t.Helper()
				assert.NotEmpty(t, pw)
			},
		},
		{
			name: "exclude_uppercase",
			input: &secretsmanager.GetRandomPasswordInput{
				ExcludeUppercase: true,
			},
			checkFn: func(t *testing.T, pw string) {
				t.Helper()
				for _, c := range pw {
					assert.False(t, c >= 'A' && c <= 'Z', "password must not contain uppercase")
				}
			},
		},
		{
			name: "exclude_lowercase",
			input: &secretsmanager.GetRandomPasswordInput{
				ExcludeLowercase: true,
			},
			checkFn: func(t *testing.T, pw string) {
				t.Helper()
				for _, c := range pw {
					assert.False(t, c >= 'a' && c <= 'z', "password must not contain lowercase")
				}
			},
		},
		{
			name: "include_space",
			input: &secretsmanager.GetRandomPasswordInput{
				PasswordLength: ptr64(200),
				IncludeSpace:   true,
			},
			wantLen: 200,
		},
		{
			name: "exclude_all_chars",
			input: &secretsmanager.GetRandomPasswordInput{
				ExcludeNumbers:     true,
				ExcludePunctuation: true,
				ExcludeUppercase:   true,
				ExcludeLowercase:   true,
			},
			wantErr: true,
		},
		{
			name: "require_each_included_type",
			input: &secretsmanager.GetRandomPasswordInput{
				PasswordLength:          ptr64(20),
				RequireEachIncludedType: true,
			},
			checkFn: func(t *testing.T, pw string) {
				t.Helper()
				hasLower, hasUpper, hasDigit, hasPunct := false, false, false, false
				for _, c := range pw {
					switch {
					case c >= 'a' && c <= 'z':
						hasLower = true
					case c >= 'A' && c <= 'Z':
						hasUpper = true
					case c >= '0' && c <= '9':
						hasDigit = true
					default:
						hasPunct = true
					}
				}
				assert.True(t, hasLower, "must include lowercase")
				assert.True(t, hasUpper, "must include uppercase")
				assert.True(t, hasDigit, "must include digit")
				assert.True(t, hasPunct, "must include punctuation")
			},
		},
		{
			name: "exclude_chars",
			input: &secretsmanager.GetRandomPasswordInput{
				ExcludeCharacters: "aeiouAEIOU",
			},
			checkFn: func(t *testing.T, pw string) {
				t.Helper()
				for _, c := range pw {
					assert.NotContains(t, "aeiouAEIOU", string(c), "excluded chars must not appear")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			out, err := b.GetRandomPassword(tt.input)

			if tt.wantErr {
				require.ErrorIs(t, err, secretsmanager.ErrInvalidPasswordParameters)

				return
			}

			require.NoError(t, err)

			if tt.wantLen > 0 {
				assert.Len(t, out.RandomPassword, tt.wantLen)
			}

			if tt.checkFn != nil {
				tt.checkFn(t, out.RandomPassword)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetRandomPassword backend + HTTP dispatch
// ---------------------------------------------------------------------------

// TestGetRandomPassword_Backend verifies the GetRandomPassword backend method.
func TestGetRandomPassword_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*secretsmanager.GetRandomPasswordInput)
		checkCharsFn func(t *testing.T, pw string)
		name         string
		wantLength   int64
		wantErr      bool
	}{
		{
			name:       "default_length",
			wantLength: 32,
		},
		{
			name: "custom_length",
			setup: func(in *secretsmanager.GetRandomPasswordInput) {
				l := int64(16)
				in.PasswordLength = &l
			},
			wantLength: 16,
		},
		{
			name: "exclude_numbers",
			setup: func(in *secretsmanager.GetRandomPasswordInput) {
				in.ExcludeNumbers = true
			},
			wantLength: 32,
			checkCharsFn: func(t *testing.T, pw string) {
				t.Helper()
				for _, ch := range pw {
					assert.NotContains(t, "0123456789", string(ch), "password should not contain digits")
				}
			},
		},
		{
			name: "exclude_uppercase",
			setup: func(in *secretsmanager.GetRandomPasswordInput) {
				in.ExcludeUppercase = true
			},
			wantLength: 32,
			checkCharsFn: func(t *testing.T, pw string) {
				t.Helper()
				for _, ch := range pw {
					assert.NotContains(
						t,
						"ABCDEFGHIJKLMNOPQRSTUVWXYZ",
						string(ch),
						"password should not contain uppercase",
					)
				}
			},
		},
		{
			name: "exclude_lowercase",
			setup: func(in *secretsmanager.GetRandomPasswordInput) {
				in.ExcludeLowercase = true
			},
			wantLength: 32,
			checkCharsFn: func(t *testing.T, pw string) {
				t.Helper()
				for _, ch := range pw {
					assert.NotContains(
						t,
						"abcdefghijklmnopqrstuvwxyz",
						string(ch),
						"password should not contain lowercase",
					)
				}
			},
		},
		{
			name: "include_space",
			setup: func(in *secretsmanager.GetRandomPasswordInput) {
				l := int64(200)
				in.PasswordLength = &l
				in.IncludeSpace = true
			},
			wantLength: 200,
		},
		{
			name: "exclude_specific_chars",
			setup: func(in *secretsmanager.GetRandomPasswordInput) {
				in.ExcludeCharacters = "abc"
			},
			wantLength: 32,
			checkCharsFn: func(t *testing.T, pw string) {
				t.Helper()
				for _, ch := range pw {
					assert.NotContains(t, "abc", string(ch), "password should not contain excluded chars")
				}
			},
		},
		{
			name: "require_each_included_type",
			setup: func(in *secretsmanager.GetRandomPasswordInput) {
				l := int64(32)
				in.PasswordLength = &l
				in.RequireEachIncludedType = true
			},
			wantLength: 32,
			checkCharsFn: func(t *testing.T, pw string) {
				t.Helper()
				hasLower := strings.ContainsAny(pw, "abcdefghijklmnopqrstuvwxyz")
				hasUpper := strings.ContainsAny(pw, "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
				hasDigit := strings.ContainsAny(pw, "0123456789")
				hasPunct := strings.ContainsAny(pw, "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")
				assert.True(t, hasLower, "password should contain a lowercase letter")
				assert.True(t, hasUpper, "password should contain an uppercase letter")
				assert.True(t, hasDigit, "password should contain a digit")
				assert.True(t, hasPunct, "password should contain a punctuation character")
			},
		},
		{
			name: "require_each_included_type_length_too_short",
			setup: func(in *secretsmanager.GetRandomPasswordInput) {
				l := int64(3)
				in.PasswordLength = &l
				in.RequireEachIncludedType = true
			},
			wantErr: true,
		},
		{
			name: "empty_charset",
			setup: func(in *secretsmanager.GetRandomPasswordInput) {
				in.ExcludeLowercase = true
				in.ExcludeUppercase = true
				in.ExcludeNumbers = true
				in.ExcludePunctuation = true
			},
			wantErr: true,
		},
		{
			name: "length_too_small",
			setup: func(in *secretsmanager.GetRandomPasswordInput) {
				l := int64(0)
				in.PasswordLength = &l
			},
			wantErr: true,
		},
		{
			name: "length_too_large",
			setup: func(in *secretsmanager.GetRandomPasswordInput) {
				l := int64(5000)
				in.PasswordLength = &l
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()

			input := &secretsmanager.GetRandomPasswordInput{}
			if tt.setup != nil {
				tt.setup(input)
			}

			out, err := backend.GetRandomPassword(input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out)
			assert.Len(t, []rune(out.RandomPassword), int(tt.wantLength))

			if tt.checkCharsFn != nil {
				tt.checkCharsFn(t, out.RandomPassword)
			}
		})
	}
}

// TestGetRandomPassword_Handler verifies GetRandomPassword via HTTP dispatch.
func TestGetRandomPassword_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		name           string
		body           string
		expectedStatus int
	}{
		{
			name:           "default",
			body:           `{}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.GetRandomPasswordOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, []rune(out.RandomPassword), 32)
			},
		},
		{
			name:           "custom_length",
			body:           `{"PasswordLength":20}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.GetRandomPasswordOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, []rune(out.RandomPassword), 20)
			},
		},
		{
			name:           "invalid_length_zero",
			body:           `{"PasswordLength":0}`,
			expectedStatus: http.StatusBadRequest,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var errResp secretsmanager.ErrorResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "InvalidParameterException", errResp.Type)
			},
		},
		{
			name:           "exclude_numbers",
			body:           `{"PasswordLength":50,"ExcludeNumbers":true}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.GetRandomPasswordOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, []rune(out.RandomPassword), 50)
				for _, ch := range out.RandomPassword {
					assert.NotContains(t, "0123456789", string(ch))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			backend := secretsmanager.NewInMemoryBackend()
			h := secretsmanager.NewHandler(backend)

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("X-Amz-Target", "secretsmanager.GetRandomPassword")
			rec := httptest.NewRecorder()

			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetRandomPassword optimized randomRunes.
// Uses doR1Request defined in helpers_test.go (same package).
// ---------------------------------------------------------------------------

// TestGetRandomPassword_RandomRunesOptimized verifies password generation still works
// with the optimized randomRunes.
func TestGetRandomPassword_RandomRunesOptimized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		minLen int
	}{
		{
			name:   "default_length",
			body:   `{}`,
			minLen: 32,
		},
		{
			name:   "custom_length_64",
			body:   `{"PasswordLength":64}`,
			minLen: 64,
		},
		{
			name:   "custom_length_128",
			body:   `{"PasswordLength":128}`,
			minLen: 128,
		},
		{
			name:   "no_special_chars",
			body:   `{"ExcludePunctuation":true,"PasswordLength":20}`,
			minLen: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			h := secretsmanager.NewHandler(b)
			rec := doR1Request(t, h, "secretsmanager.GetRandomPassword", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				RandomPassword string `json:"RandomPassword"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.GreaterOrEqual(t, len(out.RandomPassword), tt.minLen)
		})
	}
}
