package ssm_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings" // Added for strings.NewReader
	"testing"

	"github.com/blackbirdworks/gopherstack/ssm"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// TestInMemoryBackend verifies the logic of the in-memory SSM storage.
func TestInMemoryBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, ssm.StorageBackend)
		name string
	}{
		{
			run: func(t *testing.T, backend ssm.StorageBackend) {
				t.Helper()
				putIn := &ssm.PutParameterInput{
					Name:        "db-password",
					Type:        "SecureString",
					Value:       "supersecret",
					Description: "The DB password",
				}
				putOut, err := backend.PutParameter(putIn)
				require.NoError(t, err)
				assert.Equal(t, int64(1), putOut.Version)

				// Get with decryption enabled
				getOut, err := backend.GetParameter(&ssm.GetParameterInput{
					Name:           "db-password",
					WithDecryption: true,
				})
				require.NoError(t, err)
				assert.Equal(t, "supersecret", getOut.Parameter.Value)
				assert.Equal(t, int64(1), getOut.Parameter.Version)
			},
			name: "PutAndGet",
		},
		{
			run: func(t *testing.T, backend ssm.StorageBackend) {
				t.Helper()
				_, _ = backend.PutParameter(&ssm.PutParameterInput{
					Name:  "db-password",
					Type:  "SecureString",
					Value: "supersecret",
				})

				input2 := &ssm.PutParameterInput{
					Name: "db-password", Type: "String", Value: "{}", Overwrite: false,
				}
				_, duplicateErr := backend.PutParameter(input2)
				require.ErrorIs(t, duplicateErr, ssm.ErrParameterAlreadyExists)
			},
			name: "DuplicateKeyError",
		},
		{
			run: func(t *testing.T, backend ssm.StorageBackend) {
				t.Helper()
				_, _ = backend.PutParameter(&ssm.PutParameterInput{
					Name:  "db-password",
					Type:  "SecureString",
					Value: "supersecret",
				})

				putInOverwrite := &ssm.PutParameterInput{
					Name:      "db-password",
					Type:      "String",
					Value:     "newsecret",
					Overwrite: true,
				}
				putOut, err := backend.PutParameter(putInOverwrite)
				require.NoError(t, err)
				assert.Equal(t, int64(2), putOut.Version)

				getOut, err := backend.GetParameter(&ssm.GetParameterInput{Name: "db-password"})
				require.NoError(t, err)
				assert.Equal(t, "newsecret", getOut.Parameter.Value)
				assert.Equal(t, int64(2), getOut.Parameter.Version)
			},
			name: "Overwrite",
		},
		{
			run: func(t *testing.T, backend ssm.StorageBackend) {
				t.Helper()
				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "db-password", Type: "String", Value: "pwd"})
				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "api-key", Type: "String", Value: "123"})

				getParamsOut, err := backend.GetParameters(&ssm.GetParametersInput{
					Names: []string{"db-password", "api-key", "missing-key"},
				})
				require.NoError(t, err)
				assert.Len(t, getParamsOut.Parameters, 2)
				assert.Len(t, getParamsOut.InvalidParameters, 1)
				assert.Equal(t, "missing-key", getParamsOut.InvalidParameters[0])
			},
			name: "GetParameters",
		},
		{
			run: func(t *testing.T, backend ssm.StorageBackend) {
				t.Helper()
				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "api-key", Type: "String", Value: "123"})
				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "db-password", Type: "String", Value: "pwd"})

				all := backend.ListAll()
				assert.Len(t, all, 2)
				assert.Equal(t, "api-key", all[0].Name)
				assert.Equal(t, "db-password", all[1].Name)
			},
			name: "ListAll",
		},
		{
			run: func(t *testing.T, backend ssm.StorageBackend) {
				t.Helper()
				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "api-key", Type: "String", Value: "123"})
				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "db-password", Type: "String", Value: "pwd"})

				backend.DeleteParameter(&ssm.DeleteParameterInput{Name: "api-key"})
				backend.DeleteParameter(&ssm.DeleteParameterInput{Name: "db-password"})
				assert.Empty(t, backend.ListAll())
			},
			name: "DeleteAll",
		},
		{
			run: func(t *testing.T, backend ssm.StorageBackend) {
				t.Helper()
				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "key1", Type: "String", Value: "v1"})

				delOut, err := backend.DeleteParameters(
					&ssm.DeleteParametersInput{
						Names: []string{"db-password", "key1", "missing"},
					},
				)
				require.NoError(t, err)
				assert.Len(t, delOut.DeletedParameters, 1)
				assert.Len(t, delOut.InvalidParameters, 2)
				assert.Empty(t, backend.ListAll())
			},
			name: "DeleteParameters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := ssm.NewInMemoryBackend()
			tt.run(t, backend)
		})
	}
}

func TestSSMHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		expectedCheck  func(*testing.T, *httptest.ResponseRecorder)
		method         string
		target         string
		body           string
		name           string
		expectedStatus int
	}{
		{
			expectedCheck: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var resp ssm.GetParameterOutput
				err := json.Unmarshal(rec.Body.Bytes(), &resp)
				require.NoError(t, err)
				assert.Equal(t, "test-value", resp.Parameter.Value)
			},
			expectedStatus: http.StatusOK,
			method:         http.MethodPost,
			target:         "AmazonSSM.GetParameter",
			body:           `{"Name":"test-param"}`,
			name:           "GetParameter",
		},
		{
			expectedCheck: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var errResp ssm.ErrorResponse
				json.Unmarshal(rec.Body.Bytes(), &errResp)
				assert.Equal(t, "UnknownOperationException", errResp.Type)
			},
			expectedStatus: http.StatusBadRequest,
			method:         http.MethodPost,
			target:         "AmazonSSM.FakeAction",
			body:           `{}`,
			name:           "UnknownAction",
		},
		{
			expectedCheck: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "Missing X-Amz-Target")
			},
			expectedStatus: http.StatusBadRequest,
			method:         http.MethodPost,
			target:         "",
			body:           `{}`,
			name:           "MissingTarget",
		},
		{
			expectedCheck: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var ops []string
				json.Unmarshal(rec.Body.Bytes(), &ops)
				assert.Contains(t, ops, "GetParameter")
			},
			expectedStatus: http.StatusOK,
			method:         http.MethodGet,
			target:         "",
			body:           ``,
			name:           "GetSupportedOperations",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			log := logger.NewLogger(slog.LevelDebug)
			backend := ssm.NewInMemoryBackend()
			handler := ssm.NewHandler(backend, log)

			backend.PutParameter(&ssm.PutParameterInput{
				Name:  "test-param",
				Type:  "String",
				Value: "test-value",
			})

			var req *http.Request
			if tt.body != "" {
				req = httptest.NewRequest(tt.method, "/", strings.NewReader(tt.body))
			} else {
				req = httptest.NewRequest(tt.method, "/", nil)
			}

			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := handler.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.expectedCheck != nil {
				tt.expectedCheck(t, rec)
			}
		})
	}
}

// TestParameterHistory verifies the parameter versioning and history functionality.
func TestParameterHistory(t *testing.T) {
	t.Parallel()

	t.Run("GetHistoryInitialVersion", func(t *testing.T) {
		t.Parallel()
		backend := ssm.NewInMemoryBackend()

		// Create initial parameter (using String type to avoid encryption)
		putIn := &ssm.PutParameterInput{
			Name:        "api-key",
			Type:        "String",
			Value:       "key-v1",
			Description: "API key",
		}
		putOut, err := backend.PutParameter(putIn)
		require.NoError(t, err)
		assert.Equal(t, int64(1), putOut.Version)

		// Get history for initial version
		historyOut, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{
			Name: "api-key",
		})
		require.NoError(t, err)
		require.Len(t, historyOut.Parameters, 1)
		assert.Equal(t, int64(1), historyOut.Parameters[0].Version)
		assert.Equal(t, "key-v1", historyOut.Parameters[0].Value)
	})

	t.Run("GetHistoryMultipleVersions", func(t *testing.T) {
		t.Parallel()
		backend := ssm.NewInMemoryBackend()

		// Create and update parameter multiple times
		_, _ = backend.PutParameter(&ssm.PutParameterInput{
			Name:  "counter",
			Type:  "String",
			Value: "1",
		})

		_, _ = backend.PutParameter(&ssm.PutParameterInput{
			Name:      "counter",
			Type:      "String",
			Value:     "2",
			Overwrite: true,
		})

		_, _ = backend.PutParameter(&ssm.PutParameterInput{
			Name:      "counter",
			Type:      "String",
			Value:     "3",
			Overwrite: true,
		})

		// Get history - should have all 3 versions
		historyOut, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{
			Name: "counter",
		})
		require.NoError(t, err)
		require.Len(t, historyOut.Parameters, 3)

		// History should be in reverse order (newest first)
		assert.Equal(t, int64(3), historyOut.Parameters[0].Version)
		assert.Equal(t, "3", historyOut.Parameters[0].Value)

		assert.Equal(t, int64(2), historyOut.Parameters[1].Version)
		assert.Equal(t, "2", historyOut.Parameters[1].Value)

		assert.Equal(t, int64(1), historyOut.Parameters[2].Version)
		assert.Equal(t, "1", historyOut.Parameters[2].Value)
	})

	t.Run("GetHistoryParameterNotFound", func(t *testing.T) {
		t.Parallel()
		backend := ssm.NewInMemoryBackend()

		// Try to get history for non-existent parameter
		_, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{
			Name: "nonexistent",
		})
		require.Error(t, err)
		assert.Equal(t, ssm.ErrParameterNotFound, err)
	})

	t.Run("GetHistoryWithMaxResults", func(t *testing.T) {
		t.Parallel()
		backend := ssm.NewInMemoryBackend()

		// Create multiple versions
		for i := 1; i <= 5; i++ {
			overwrite := i > 1
			_, _ = backend.PutParameter(&ssm.PutParameterInput{
				Name:      "paginated-param",
				Type:      "String",
				Value:     "value-" + string(rune(i+'0'-1)),
				Overwrite: overwrite,
			})
		}

		// Get history with MaxResults
		maxResults := int64(2)
		historyOut, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{
			Name:       "paginated-param",
			MaxResults: &maxResults,
		})
		require.NoError(t, err)
		require.Len(t, historyOut.Parameters, 2)

		// Should return the latest 2 versions
		assert.Equal(t, int64(5), historyOut.Parameters[0].Version)
		assert.Equal(t, int64(4), historyOut.Parameters[1].Version)
	})

	t.Run("GetHistoryTypeChanges", func(t *testing.T) {
		t.Parallel()
		backend := ssm.NewInMemoryBackend()

		// Create parameter
		_, _ = backend.PutParameter(&ssm.PutParameterInput{
			Name:  "type-change",
			Type:  "String",
			Value: "string-value",
		})

		// Update with different type
		_, _ = backend.PutParameter(&ssm.PutParameterInput{
			Name:      "type-change",
			Type:      "SecureString",
			Value:     "secure-value",
			Overwrite: true,
		})

		historyOut, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{
			Name: "type-change",
		})
		require.NoError(t, err)
		require.Len(t, historyOut.Parameters, 2)

		// Check that both types are stored correctly
		assert.Equal(t, "SecureString", historyOut.Parameters[0].Type)
		assert.Equal(t, "String", historyOut.Parameters[1].Type)
	})
}

// TestSecureString verifies the SecureString encryption/decryption functionality.
func TestSecureString(t *testing.T) {
	t.Parallel()

	t.Run("PutSecureStringEncryption", func(t *testing.T) {
		t.Parallel()
		backend := ssm.NewInMemoryBackend()

		// Put a SecureString parameter
		_, err := backend.PutParameter(&ssm.PutParameterInput{
			Name:  "db-password",
			Type:  "SecureString",
			Value: "super-secret-password",
		})
		require.NoError(t, err)

		// Get without decryption - should be encrypted
		output, err := backend.GetParameter(&ssm.GetParameterInput{
			Name:           "db-password",
			WithDecryption: false,
		})
		require.NoError(t, err)
		assert.Equal(t, "SecureString", output.Parameter.Type)
		assert.NotEqual(t, "super-secret-password", output.Parameter.Value)
		assert.NotEmpty(t, output.Parameter.Value) // Should be encrypted (base64 encoded)
	})

	t.Run("GetSecureStringWithDecryption", func(t *testing.T) {
		t.Parallel()
		backend := ssm.NewInMemoryBackend()

		// Put a SecureString parameter
		_, err := backend.PutParameter(&ssm.PutParameterInput{
			Name:  "db-password",
			Type:  "SecureString",
			Value: "super-secret-password",
		})
		require.NoError(t, err)

		// Get with decryption - should be decrypted
		output, err := backend.GetParameter(&ssm.GetParameterInput{
			Name:           "db-password",
			WithDecryption: true,
		})
		require.NoError(t, err)
		assert.Equal(t, "SecureString", output.Parameter.Type)
		assert.Equal(t, "super-secret-password", output.Parameter.Value)
	})

	t.Run("GetParametersSecureStringDecryption", func(t *testing.T) {
		t.Parallel()
		backend := ssm.NewInMemoryBackend()

		// Put multiple parameters
		_, _ = backend.PutParameter(&ssm.PutParameterInput{
			Name:  "api-key",
			Type:  "SecureString",
			Value: "api-key-value",
		})

		_, _ = backend.PutParameter(&ssm.PutParameterInput{
			Name:  "db-password",
			Type:  "SecureString",
			Value: "db-password-value",
		})

		_, _ = backend.PutParameter(&ssm.PutParameterInput{
			Name:  "environment",
			Type:  "String",
			Value: "production",
		})

		// Get all without decryption
		outputNoDecrypt, err := backend.GetParameters(&ssm.GetParametersInput{
			Names:          []string{"api-key", "db-password", "environment"},
			WithDecryption: false,
		})
		require.NoError(t, err)
		require.Len(t, outputNoDecrypt.Parameters, 3)

		// SecureString values should be encrypted
		for _, param := range outputNoDecrypt.Parameters {
			if param.Type == "SecureString" {
				assert.NotContains(t, param.Value, "-value")
			}
		}

		// Get all with decryption
		outputWithDecrypt, err := backend.GetParameters(&ssm.GetParametersInput{
			Names:          []string{"api-key", "db-password", "environment"},
			WithDecryption: true,
		})
		require.NoError(t, err)
		require.Len(t, outputWithDecrypt.Parameters, 3)

		// Verify decrypted values
		for _, param := range outputWithDecrypt.Parameters {
			switch param.Name {
			case "api-key":
				assert.Equal(t, "api-key-value", param.Value)
			case "db-password":
				assert.Equal(t, "db-password-value", param.Value)
			case "environment":
				assert.Equal(t, "production", param.Value)
			}
		}
	})

	t.Run("SecureStringHistoryEncryption", func(t *testing.T) {
		t.Parallel()
		backend := ssm.NewInMemoryBackend()

		// Create SecureString parameter
		_, _ = backend.PutParameter(&ssm.PutParameterInput{
			Name:  "secret",
			Type:  "SecureString",
			Value: "secret-v1",
		})

		// Update it
		_, _ = backend.PutParameter(&ssm.PutParameterInput{
			Name:      "secret",
			Type:      "SecureString",
			Value:     "secret-v2",
			Overwrite: true,
		})

		// Get history - values should be encrypted
		historyOutput, err := backend.GetParameterHistory(&ssm.GetParameterHistoryInput{
			Name: "secret",
		})
		require.NoError(t, err)
		require.Len(t, historyOutput.Parameters, 2)

		// History should store encrypted values
		for _, histParam := range historyOutput.Parameters {
			assert.Equal(t, "SecureString", histParam.Type)
			assert.NotContains(t, histParam.Value, "secret-v")
		}
	})
}
