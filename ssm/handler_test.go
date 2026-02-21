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

				getOut, err := backend.GetParameter(&ssm.GetParameterInput{Name: "db-password"})
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
