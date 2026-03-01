package ssm_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/ssm"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func TestSSM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "InMemoryBackend",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "Handler",
			run: func(t *testing.T) {
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
							var errResp service.JSONErrorResponse
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
			},
		},
		{
			name: "ParameterHistory",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "SecureString",
			run: func(t *testing.T) {
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
			},
		},
		{
			name: "GetParametersByPath",
			run: func(t *testing.T) {
				t.Run("DirectChildrenOnly", func(t *testing.T) {
					t.Parallel()

					backend := ssm.NewInMemoryBackend()
					for _, name := range []string{"/app/db/host", "/app/db/port", "/app/cache/host", "/app/config"} {
						_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
					}

					out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
						Path:      "/app",
						Recursive: false,
					})
					require.NoError(t, err)
					// Only direct children: /app/config
					assert.Len(t, out.Parameters, 1)
					assert.Equal(t, "/app/config", out.Parameters[0].Name)
				})

				t.Run("Recursive", func(t *testing.T) {
					t.Parallel()

					backend := ssm.NewInMemoryBackend()
					for _, name := range []string{"/app/db/host", "/app/db/port", "/app/cache/host", "/app/config"} {
						_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
					}

					out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
						Path:      "/app",
						Recursive: true,
					})
					require.NoError(t, err)
					assert.Len(t, out.Parameters, 4)
				})

				t.Run("Pagination", func(t *testing.T) {
					t.Parallel()

					backend := ssm.NewInMemoryBackend()
					for i := range 5 {
						name := "/params/key" + string(rune('0'+i))
						_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
					}

					maxRes := int64(2)
					out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
						Path:       "/params",
						Recursive:  true,
						MaxResults: &maxRes,
					})
					require.NoError(t, err)
					assert.Len(t, out.Parameters, 2)
					assert.NotEmpty(t, out.NextToken)

					out2, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
						Path:       "/params",
						Recursive:  true,
						MaxResults: &maxRes,
						NextToken:  out.NextToken,
					})
					require.NoError(t, err)
					assert.Len(t, out2.Parameters, 2)
				})

				t.Run("EmptyPath", func(t *testing.T) {
					t.Parallel()

					backend := ssm.NewInMemoryBackend()
					out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
						Path:      "/nonexistent",
						Recursive: true,
					})
					require.NoError(t, err)
					assert.Empty(t, out.Parameters)
				})

				t.Run("WithDecryption", func(t *testing.T) {
					t.Parallel()

					backend := ssm.NewInMemoryBackend()
					_, _ = backend.PutParameter(&ssm.PutParameterInput{
						Name: "/secrets/key", Type: "SecureString", Value: "plaintext",
					})

					out, err := backend.GetParametersByPath(&ssm.GetParametersByPathInput{
						Path:           "/secrets",
						Recursive:      true,
						WithDecryption: true,
					})
					require.NoError(t, err)
					require.Len(t, out.Parameters, 1)
					assert.Equal(t, "plaintext", out.Parameters[0].Value)
				})
			},
		},
		{
			name: "DescribeParameters",
			run: func(t *testing.T) {
				t.Run("AllParameters", func(t *testing.T) {
					t.Parallel()

					backend := ssm.NewInMemoryBackend()
					for _, p := range []struct{ name, typ string }{
						{"/a", "String"}, {"/b", "SecureString"}, {"/c", "StringList"},
					} {
						_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: p.name, Type: p.typ, Value: "v"})
					}

					out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{})
					require.NoError(t, err)
					assert.Len(t, out.Parameters, 3)
					// Values should not be included
					for _, m := range out.Parameters {
						assert.Empty(t, m.Description) // Description is empty for these test params
					}
				})

				t.Run("FilterByType", func(t *testing.T) {
					t.Parallel()

					backend := ssm.NewInMemoryBackend()
					for _, p := range []struct{ name, typ string }{
						{"/a", "String"}, {"/b", "SecureString"}, {"/c", "String"},
					} {
						_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: p.name, Type: p.typ, Value: "v"})
					}

					out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
						ParameterFilters: []ssm.ParameterFilter{
							{Key: "Type", Option: "Equals", Values: []string{"String"}},
						},
					})
					require.NoError(t, err)
					assert.Len(t, out.Parameters, 2)
				})

				t.Run("FilterByNameBeginsWith", func(t *testing.T) {
					t.Parallel()

					backend := ssm.NewInMemoryBackend()
					for _, name := range []string{"/app/db", "/app/cache", "/other/key"} {
						_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
					}

					out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
						ParameterFilters: []ssm.ParameterFilter{
							{Key: "Name", Option: "BeginsWith", Values: []string{"/app"}},
						},
					})
					require.NoError(t, err)
					assert.Len(t, out.Parameters, 2)
				})

				t.Run("Pagination", func(t *testing.T) {
					t.Parallel()

					backend := ssm.NewInMemoryBackend()
					for i := range 5 {
						_, _ = backend.PutParameter(&ssm.PutParameterInput{
							Name: "/p" + string(rune('0'+i)), Type: "String", Value: "v",
						})
					}

					maxRes := int64(2)
					out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{MaxResults: &maxRes})
					require.NoError(t, err)
					assert.Len(t, out.Parameters, 2)
					assert.NotEmpty(t, out.NextToken)

					// Get remaining pages
					out2, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
						MaxResults: &maxRes, NextToken: out.NextToken,
					})
					require.NoError(t, err)
					assert.Len(t, out2.Parameters, 2)
				})

				t.Run("BeyondEnd", func(t *testing.T) {
					t.Parallel()

					backend := ssm.NewInMemoryBackend()
					out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
						NextToken: "9999",
					})
					require.NoError(t, err)
					assert.Empty(t, out.Parameters)
				})
			},
		},
		{
			name: "HandlerNewOps",
			run: func(t *testing.T) {
				e := echo.New()
				log := logger.NewLogger(slog.LevelDebug)
				backend := ssm.NewInMemoryBackend()
				handler := ssm.NewHandler(backend, log)

				// Seed parameters
				for _, name := range []string{"/app/db", "/app/cache", "/other/key"} {
					_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: name, Type: "String", Value: "v"})
				}

				t.Run("GetParametersByPath", func(t *testing.T) {
					t.Parallel()

					req := httptest.NewRequest(
						http.MethodPost, "/",
						strings.NewReader(`{"Path":"/app","Recursive":true}`),
					)
					req.Header.Set("X-Amz-Target", "AmazonSSM.GetParametersByPath")
					rec := httptest.NewRecorder()
					c := e.NewContext(req, rec)

					require.NoError(t, handler.Handler()(c))
					assert.Equal(t, http.StatusOK, rec.Code)

					var out ssm.GetParametersByPathOutput
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
					assert.Len(t, out.Parameters, 2)
				})

				t.Run("DescribeParameters", func(t *testing.T) {
					t.Parallel()

					req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
					req.Header.Set("X-Amz-Target", "AmazonSSM.DescribeParameters")
					rec := httptest.NewRecorder()
					c := e.NewContext(req, rec)

					require.NoError(t, handler.Handler()(c))
					assert.Equal(t, http.StatusOK, rec.Code)

					var out ssm.DescribeParametersOutput
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
					assert.Len(t, out.Parameters, 3)
				})
			},
		},
		{
			name: "HandlerInterface",
			run: func(t *testing.T) {
				log := logger.NewLogger(slog.LevelDebug)
				backend := ssm.NewInMemoryBackend()
				h := ssm.NewHandler(backend, log)

				assert.Equal(t, "SSM", h.Name())
				assert.Equal(t, 100, h.MatchPriority())

				e := echo.New()

				// ExtractOperation
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Amz-Target", "AmazonSSM.GetParameter")
				c := e.NewContext(req, httptest.NewRecorder())
				assert.Equal(t, "GetParameter", h.ExtractOperation(c))

				// ExtractOperation with no separator
				req2 := httptest.NewRequest(http.MethodPost, "/", nil)
				req2.Header.Set("X-Amz-Target", "AmazonSSMNoSep")
				c2 := e.NewContext(req2, httptest.NewRecorder())
				assert.Equal(t, "Unknown", h.ExtractOperation(c2))

				// ExtractResource with Name
				body := `{"Name":"/my/param"}`
				req3 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
				c3 := e.NewContext(req3, httptest.NewRecorder())
				assert.Equal(t, "/my/param", h.ExtractResource(c3))

				// ExtractResource with no Name
				req4 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
				c4 := e.NewContext(req4, httptest.NewRecorder())
				assert.Empty(t, h.ExtractResource(c4))
			},
		},
		{
			name: "Provider",
			run: func(t *testing.T) {
				p := &ssm.Provider{}
				assert.Equal(t, "SSM", p.Name())

				log := logger.NewLogger(slog.LevelDebug)
				ctx := &service.AppContext{Logger: log}
				svc, err := p.Init(ctx)
				require.NoError(t, err)
				assert.NotNil(t, svc)
			},
		},
		{
			name: "HandlerErrorCases",
			run: func(t *testing.T) {
				tests := []struct {
					target         string
					body           string
					name           string
					expectedErrTyp string
					expectedStatus int
				}{
					{
						name:           "ParameterNotFound",
						target:         "AmazonSSM.GetParameter",
						body:           `{"Name":"/missing/param"}`,
						expectedStatus: http.StatusBadRequest,
						expectedErrTyp: "ParameterNotFound",
					},
					{
						name:           "ParameterAlreadyExists",
						target:         "AmazonSSM.PutParameter",
						body:           `{"Name":"/existing","Type":"String","Value":"v2","Overwrite":false}`,
						expectedStatus: http.StatusBadRequest,
						expectedErrTyp: "ParameterAlreadyExists",
					},
					{
						name:           "InvalidTarget",
						target:         "AmazonSSMNoSep",
						body:           `{}`,
						expectedStatus: http.StatusBadRequest,
					},
				}

				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						t.Parallel()

						e := echo.New()
						log := logger.NewLogger(slog.LevelDebug)
						backend := ssm.NewInMemoryBackend()
						h := ssm.NewHandler(backend, log)

						if tt.name == "ParameterAlreadyExists" {
							_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "/existing", Type: "String", Value: "v1"})
						}

						req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
						req.Header.Set("X-Amz-Target", tt.target)
						rec := httptest.NewRecorder()

						require.NoError(t, h.Handler()(e.NewContext(req, rec)))
						assert.Equal(t, tt.expectedStatus, rec.Code)

						if tt.expectedErrTyp != "" {
							var errResp service.JSONErrorResponse
							require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
							assert.Equal(t, tt.expectedErrTyp, errResp.Type)
						}
					})
				}
			},
		},
		{
			name: "ParamMatchesFilterOptions",
			run: func(t *testing.T) {
				backend := ssm.NewInMemoryBackend()
				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "/app/db/host", Type: "String", Value: "localhost"})
				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "/app/cache/host", Type: "SecureString", Value: "cache"})
				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "/other/key", Type: "String", Value: "v"})

				t.Run("Contains", func(t *testing.T) {
					t.Parallel()

					out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
						ParameterFilters: []ssm.ParameterFilter{
							{Key: "Name", Option: "Contains", Values: []string{"db"}},
						},
					})
					require.NoError(t, err)
					assert.Len(t, out.Parameters, 1)
					assert.Equal(t, "/app/db/host", out.Parameters[0].Name)
				})

				t.Run("UnknownKeyIgnored", func(t *testing.T) {
					t.Parallel()

					out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
						ParameterFilters: []ssm.ParameterFilter{
							{Key: "UnknownKey", Option: "Equals", Values: []string{"anything"}},
						},
					})
					require.NoError(t, err)
					// Unknown filter key matches everything
					assert.Len(t, out.Parameters, 3)
				})

				t.Run("DefaultOptionIsEquals", func(t *testing.T) {
					t.Parallel()

					out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
						ParameterFilters: []ssm.ParameterFilter{
							{Key: "Type", Values: []string{"SecureString"}},
						},
					})
					require.NoError(t, err)
					assert.Len(t, out.Parameters, 1)
				})
			},
		},
		{
			name: "ParseNextTokenBadToken",
			run: func(t *testing.T) {
				backend := ssm.NewInMemoryBackend()
				for i := range 3 {
					_, _ = backend.PutParameter(&ssm.PutParameterInput{
						Name: "/p" + string(rune('0'+i)), Type: "String", Value: "v",
					})
				}

				// A bad token is treated as 0 (start from beginning)
				out, err := backend.DescribeParameters(&ssm.DescribeParametersInput{
					NextToken: "not-a-number",
				})
				require.NoError(t, err)
				assert.Len(t, out.Parameters, 3)
			},
		},
		{
			name: "HandlerGetParametersByPathViaHTTP",
			run: func(t *testing.T) {
				e := echo.New()
				log := logger.NewLogger(slog.LevelDebug)
				backend := ssm.NewInMemoryBackend()
				h := ssm.NewHandler(backend, log)

				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "/svc/a", Type: "String", Value: "1"})
				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "/svc/b", Type: "String", Value: "2"})
				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "/other/c", Type: "String", Value: "3"})

				req := httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader(`{"Path":"/svc","Recursive":true}`))
				req.Header.Set("X-Amz-Target", "AmazonSSM.GetParametersByPath")
				rec := httptest.NewRecorder()
				require.NoError(t, h.Handler()(e.NewContext(req, rec)))
				assert.Equal(t, http.StatusOK, rec.Code)

				var out ssm.GetParametersByPathOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.Parameters, 2)
			},
		},
		{
			name: "HandlerDescribeParametersViaHTTP",
			run: func(t *testing.T) {
				e := echo.New()
				log := logger.NewLogger(slog.LevelDebug)
				backend := ssm.NewInMemoryBackend()
				h := ssm.NewHandler(backend, log)

				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "/a", Type: "String", Value: "1"})
				_, _ = backend.PutParameter(&ssm.PutParameterInput{Name: "/b", Type: "SecureString", Value: "2"})

				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
				req.Header.Set("X-Amz-Target", "AmazonSSM.DescribeParameters")
				rec := httptest.NewRecorder()
				require.NoError(t, h.Handler()(e.NewContext(req, rec)))
				assert.Equal(t, http.StatusOK, rec.Code)

				var out ssm.DescribeParametersOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.Parameters, 2)
			},
		},
		{
			name: "HandlerMethodNotAllowed",
			run: func(t *testing.T) {
				e := echo.New()
				log := logger.NewLogger(slog.LevelDebug)
				backend := ssm.NewInMemoryBackend()
				h := ssm.NewHandler(backend, log)

				req := httptest.NewRequest(http.MethodPut, "/", nil)
				rec := httptest.NewRecorder()
				require.NoError(t, h.Handler()(e.NewContext(req, rec)))
				assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
			},
		},
		{
			name: "ValidateParameterName",
			run: func(t *testing.T) {
				backend := ssm.NewInMemoryBackend()

				tests := []struct {
					name      string
					paramName string
					wantErr   bool
				}{
					{name: "valid path", paramName: "/my/param", wantErr: false},
					{name: "valid simple", paramName: "MyParam", wantErr: false},
					{name: "double slash", paramName: "/my//param", wantErr: true},
					{name: "reserved ssm", paramName: "ssm/something", wantErr: true},
					{name: "reserved aws", paramName: "aws-param", wantErr: true},
					{name: "reserved amazon", paramName: "amazon.param", wantErr: true},
					{name: "invalid char", paramName: "/my param!", wantErr: true},
				}

				for _, tc := range tests {
					t.Run(tc.name, func(t *testing.T) {
						t.Parallel()
						_, err := backend.PutParameter(&ssm.PutParameterInput{
							Name:  tc.paramName,
							Type:  "String",
							Value: "val",
						})
						if tc.wantErr {
							require.Error(t, err)
							assert.ErrorIs(t, err, ssm.ErrValidationException)
						} else {
							require.NoError(t, err)
						}
					})
				}
			},
		},
		{
			name: "TagOperations",
			run: func(t *testing.T) {
				e := echo.New()
				log := logger.NewLogger(slog.LevelDebug)
				backend := ssm.NewInMemoryBackend()
				h := ssm.NewHandler(backend, log)

				_, err := backend.PutParameter(&ssm.PutParameterInput{
					Name:  "my-param",
					Type:  "String",
					Value: "val",
				})
				require.NoError(t, err)

				// AddTagsToResource
				addBody := `{"ResourceType":"Parameter","ResourceId":"my-param",` +
					`"Tags":[{"Key":"env","Value":"prod"},{"Key":"team","Value":"ops"}]}`
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(addBody))
				req.Header.Set("X-Amz-Target", "AmazonSSM.AddTagsToResource")
				rec := httptest.NewRecorder()
				require.NoError(t, h.Handler()(e.NewContext(req, rec)))
				assert.Equal(t, http.StatusOK, rec.Code)

				// ListTagsForResource
				listBody := `{"ResourceType":"Parameter","ResourceId":"my-param"}`
				req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(listBody))
				req2.Header.Set("X-Amz-Target", "AmazonSSM.ListTagsForResource")
				rec2 := httptest.NewRecorder()
				require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
				assert.Equal(t, http.StatusOK, rec2.Code)

				var listOut ssm.ListTagsForResourceOutput
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
				require.Len(t, listOut.TagList, 2)
				assert.Equal(t, "env", listOut.TagList[0].Key)
				assert.Equal(t, "prod", listOut.TagList[0].Value)

				// RemoveTagsFromResource
				removeBody := `{"ResourceType":"Parameter","ResourceId":"my-param","TagKeys":["env"]}`
				req3 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(removeBody))
				req3.Header.Set("X-Amz-Target", "AmazonSSM.RemoveTagsFromResource")
				rec3 := httptest.NewRecorder()
				require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
				assert.Equal(t, http.StatusOK, rec3.Code)

				// Verify only team tag remains
				listOut2, err := backend.ListTagsForResource(&ssm.ListTagsForResourceInput{ResourceID: "my-param"})
				require.NoError(t, err)
				require.Len(t, listOut2.TagList, 1)
				assert.Equal(t, "team", listOut2.TagList[0].Key)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
