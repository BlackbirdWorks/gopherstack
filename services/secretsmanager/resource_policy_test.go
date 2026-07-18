package secretsmanager_test

// resource_policy_test.go consolidates every GetResourcePolicy / PutResourcePolicy /
// DeleteResourcePolicy / ValidateResourcePolicy test that was previously scattered
// across several older test files. Ported verbatim (assertions unchanged).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// validResourcePolicyDoc is a minimal well-formed IAM policy document used across
// the resource-policy tests in this file.
const validResourcePolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
	`"Principal":{"AWS":"arn:aws:iam::123456789012:root"},` +
	`"Action":"secretsmanager:GetSecretValue","Resource":"*"}]}`

// ---------------------------------------------------------------------------
// Resource policy comprehensive
// ---------------------------------------------------------------------------

func TestResourcePolicy_PutGetDelete(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "policy-secret", SecretString: "v"},
	)
	require.NoError(t, err)

	// Put
	_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
		SecretID:       "policy-secret",
		ResourcePolicy: validResourcePolicyDoc,
	})
	require.NoError(t, err)

	// Get
	out, err := b.GetResourcePolicy(
		context.Background(),
		&secretsmanager.GetResourcePolicyInput{SecretID: "policy-secret"},
	)
	require.NoError(t, err)
	assert.JSONEq(t, validResourcePolicyDoc, out.ResourcePolicy)

	// Delete
	_, err = b.DeleteResourcePolicy(
		context.Background(),
		&secretsmanager.DeleteResourcePolicyInput{SecretID: "policy-secret"},
	)
	require.NoError(t, err)

	// Get after delete returns empty
	out2, err := b.GetResourcePolicy(
		context.Background(),
		&secretsmanager.GetResourcePolicyInput{SecretID: "policy-secret"},
	)
	require.NoError(t, err)
	assert.Empty(t, out2.ResourcePolicy)
}

func TestResourcePolicy_EmptyPolicyRejected(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "policy-empty", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
		SecretID:       "policy-empty",
		ResourcePolicy: "",
	})
	require.ErrorIs(t, err, secretsmanager.ErrInvalidParameter)
}

func TestResourcePolicy_NotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.GetResourcePolicy(context.Background(), &secretsmanager.GetResourcePolicyInput{SecretID: "missing"})
	require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)

	_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
		SecretID:       "missing",
		ResourcePolicy: validResourcePolicyDoc,
	})
	require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)

	_, err = b.DeleteResourcePolicy(
		context.Background(),
		&secretsmanager.DeleteResourcePolicyInput{SecretID: "missing"},
	)
	require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
}

func TestResourcePolicy_DeletedSecretRejected(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "policy-del", SecretString: "v"},
	)
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "policy-del"})
	require.NoError(t, err)

	_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
		SecretID:       "policy-del",
		ResourcePolicy: validResourcePolicyDoc,
	})
	require.ErrorIs(t, err, secretsmanager.ErrSecretDeleted)
}

// ---------------------------------------------------------------------------
// ValidateResourcePolicy comprehensive (consolidated into one table-driven test;
// the individual TestAudit_ValidateResourcePolicy_* cases all shared the same
// setup/assert shape).
// ---------------------------------------------------------------------------

func TestValidateResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		name           string
		resourcePolicy string
		secretID       string
		createSecret   bool
		wantPassed     bool
		wantErrsNotNil bool
	}{
		{
			name:           "valid_passes",
			resourcePolicy: validResourcePolicyDoc,
			wantPassed:     true,
		},
		{
			name:           "missing_version",
			resourcePolicy: `{"Statement":[]}`,
			wantPassed:     false,
			wantErrsNotNil: true,
		},
		{
			name:           "missing_statement",
			resourcePolicy: `{"Version":"2012-10-17"}`,
			wantPassed:     false,
		},
		{
			name:           "invalid_json",
			resourcePolicy: `not-json`,
			wantPassed:     false,
		},
		{
			name:           "empty",
			resourcePolicy: "",
			wantErr:        secretsmanager.ErrInvalidParameter,
		},
		{
			name:           "with_secret_id",
			resourcePolicy: validResourcePolicyDoc,
			secretID:       "val-pol-secret",
			createSecret:   true,
			wantPassed:     true,
		},
		{
			name:           "secret_id_not_found",
			resourcePolicy: validResourcePolicyDoc,
			secretID:       "missing",
			wantErr:        secretsmanager.ErrSecretNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()

			if tt.createSecret {
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: tt.secretID, SecretString: "v"},
				)
				require.NoError(t, err)
			}

			out, err := b.ValidateResourcePolicy(context.Background(), &secretsmanager.ValidateResourcePolicyInput{
				SecretID:       tt.secretID,
				ResourcePolicy: tt.resourcePolicy,
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantPassed, out.PolicyValidationPassed)

			if tt.wantErrsNotNil {
				require.NotEmpty(t, out.ValidationErrors)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// GetResourcePolicy on a deleted secret
// ---------------------------------------------------------------------------

// TestGetResourcePolicy_DeletedSecret verifies that calling GetResourcePolicy
// on a soft-deleted secret returns ErrSecretDeleted (not an empty policy response).
// AWS returns InvalidRequestException in this case.
func TestGetResourcePolicy_DeletedSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(*testing.T, *secretsmanager.InMemoryBackend)
		wantFn func(*testing.T, error)
		name   string
	}{
		{
			name: "backend_deleted_returns_error",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "grp-del", SecretString: "v"},
				)
				require.NoError(t, err)
				_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "grp-del"})
				require.NoError(t, err)
			},
			wantFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, secretsmanager.ErrSecretDeleted, "deleted secret must return ErrSecretDeleted")
			},
		},
		{
			name: "backend_active_no_policy_returns_empty",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "grp-active", SecretString: "v"},
				)
				require.NoError(t, err)
			},
			wantFn: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err, "active secret with no policy must not error")
			},
		},
		{
			name: "backend_not_found_returns_error",
			setup: func(t *testing.T, _ *secretsmanager.InMemoryBackend) {
				t.Helper()
			},
			wantFn: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			tt.setup(t, b)

			secretID := map[string]string{
				"backend_deleted_returns_error":          "grp-del",
				"backend_active_no_policy_returns_empty": "grp-active",
				"backend_not_found_returns_error":        "nonexistent",
			}[tt.name]

			_, err := b.GetResourcePolicy(
				context.Background(),
				&secretsmanager.GetResourcePolicyInput{SecretID: secretID},
			)
			tt.wantFn(t, err)
		})
	}
}

// TestGetResourcePolicy_DeletedSecret_HTTP verifies that the HTTP handler
// returns 400 InvalidRequestException for a deleted secret.
func TestGetResourcePolicy_DeletedSecret_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *secretsmanager.InMemoryBackend)
		name           string
		body           string
		expectedType   string
		expectedStatus int
	}{
		{
			name: "deleted_returns_400_InvalidRequestException",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "grp-http-del", SecretString: "v"},
				)
				require.NoError(t, err)
				_, err = b.DeleteSecret(
					context.Background(),
					&secretsmanager.DeleteSecretInput{SecretID: "grp-http-del"},
				)
				require.NoError(t, err)
			},
			body:           `{"SecretId":"grp-http-del"}`,
			expectedStatus: http.StatusBadRequest,
			expectedType:   "InvalidRequestException",
		},
		{
			name:           "not_found_returns_400_ResourceNotFoundException",
			body:           `{"SecretId":"no-such-secret"}`,
			expectedStatus: http.StatusBadRequest,
			expectedType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			h := secretsmanager.NewHandler(b)
			rec := doR1Request(t, h, "secretsmanager.GetResourcePolicy", tt.body)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			var errResp secretsmanager.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.expectedType, errResp.Type)
		})
	}
}

// ---------------------------------------------------------------------------
// PutResourcePolicy / GetResourcePolicy / DeleteResourcePolicy HTTP cycle
// ---------------------------------------------------------------------------

// TestResourcePolicy_Cycle verifies PutResourcePolicy, GetResourcePolicy, and DeleteResourcePolicy.
func TestResourcePolicy_Cycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *secretsmanager.InMemoryBackend)
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		name           string
		target         string
		body           string
		expectedStatus int
	}{
		{
			name: "put_resource_policy",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "policy-secret", SecretString: "v"},
				)
				require.NoError(t, err)
			},
			target:         "secretsmanager.PutResourcePolicy",
			body:           `{"SecretId":"policy-secret","ResourcePolicy":"{\"Version\":\"2012-10-17\",\"Statement\":[]}"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.PutResourcePolicyOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "policy-secret", out.Name)
				assert.NotEmpty(t, out.ARN)
			},
		},
		{
			name: "get_resource_policy_after_put",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "get-policy", SecretString: "v"},
				)
				require.NoError(t, err)
				_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
					SecretID:       "get-policy",
					ResourcePolicy: `{"Version":"2012-10-17","Statement":[]}`,
				})
				require.NoError(t, err)
			},
			target:         "secretsmanager.GetResourcePolicy",
			body:           `{"SecretId":"get-policy"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.GetResourcePolicyOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "get-policy", out.Name)
				assert.Contains(t, out.ResourcePolicy, "2012-10-17")
			},
		},
		{
			name: "delete_resource_policy",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "del-policy", SecretString: "v"},
				)
				require.NoError(t, err)
				_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
					SecretID:       "del-policy",
					ResourcePolicy: `{"Version":"2012-10-17","Statement":[]}`,
				})
				require.NoError(t, err)
			},
			target:         "secretsmanager.DeleteResourcePolicy",
			body:           `{"SecretId":"del-policy"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.DeleteResourcePolicyOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "del-policy", out.Name)
			},
		},
		{
			name:           "get_policy_not_found",
			target:         "secretsmanager.GetResourcePolicy",
			body:           `{"SecretId":"nonexistent"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "put_policy_not_found",
			target:         "secretsmanager.PutResourcePolicy",
			body:           `{"SecretId":"nonexistent","ResourcePolicy":"{}"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "delete_policy_not_found",
			target:         "secretsmanager.DeleteResourcePolicy",
			body:           `{"SecretId":"nonexistent"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "put_bad_json",
			target:         "secretsmanager.PutResourcePolicy",
			body:           `{bad}`,
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:           "get_bad_json",
			target:         "secretsmanager.GetResourcePolicy",
			body:           `{bad}`,
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:           "delete_bad_json",
			target:         "secretsmanager.DeleteResourcePolicy",
			body:           `{bad}`,
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := secretsmanager.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, backend)
			}

			h := secretsmanager.NewHandler(backend)
			rec := doSMRequest(t, h, tt.target, tt.body)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// TestResourcePolicy_BackendEdgeCases ports the resource-policy-specific subtests
// of the original TestNewOpsBackend (the replication/rotation/batchget subtests of
// that function belong to sibling agents).
func TestResourcePolicy_BackendEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("GetResourcePolicy_empty_when_not_set", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "no-policy", SecretString: "v"},
		)
		require.NoError(t, err)

		out, err := b.GetResourcePolicy(
			context.Background(),
			&secretsmanager.GetResourcePolicyInput{SecretID: "no-policy"},
		)
		require.NoError(t, err)
		assert.Empty(t, out.ResourcePolicy)
		assert.Equal(t, "no-policy", out.Name)
	})

	t.Run("PutResourcePolicy_deleted_returns_error", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "put-del-policy", SecretString: "v"},
		)
		require.NoError(t, err)
		_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "put-del-policy"})
		require.NoError(t, err)

		_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
			SecretID:       "put-del-policy",
			ResourcePolicy: "{}",
		})
		require.ErrorIs(t, err, secretsmanager.ErrSecretDeleted)
	})

	t.Run("DeleteResourcePolicy_deleted_returns_error", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "del-del-policy", SecretString: "v"},
		)
		require.NoError(t, err)
		_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "del-del-policy"})
		require.NoError(t, err)

		_, err = b.DeleteResourcePolicy(
			context.Background(),
			&secretsmanager.DeleteResourcePolicyInput{SecretID: "del-del-policy"},
		)
		require.ErrorIs(t, err, secretsmanager.ErrSecretDeleted)
	})
}

// ---------------------------------------------------------------------------
// PutResourcePolicy empty-policy rejection
// ---------------------------------------------------------------------------

// TestPutResourcePolicy_EmptyRejects verifies empty ResourcePolicy returns 400.
func TestPutResourcePolicy_EmptyRejects(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "ep-secret", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
		SecretID:       "ep-secret",
		ResourcePolicy: "",
	})
	require.ErrorIs(t, err, secretsmanager.ErrInvalidParameter)
}

// TestPutResourcePolicy_EmptyHTTP verifies empty policy returns HTTP 400.
func TestPutResourcePolicy_EmptyHTTP(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "ep-http", SecretString: "v"},
	)
	require.NoError(t, err)

	h := secretsmanager.NewHandler(b)
	rec := doR1Request(t, h, "secretsmanager.PutResourcePolicy",
		`{"SecretId":"ep-http","ResourcePolicy":""}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ValidateResourcePolicy via HTTP
// ---------------------------------------------------------------------------

// TestValidateResourcePolicy_HTTP tests the ValidateResourcePolicy operation.
func TestValidateResourcePolicy_HTTP(t *testing.T) {
	t.Parallel()

	// validFullPolicy is a minimal well-formed IAM policy document.
	const validFullPolicy = `{"ResourcePolicy":` +
		`"{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",` +
		`\"Principal\":{\"AWS\":\"arn:aws:iam::123456789012:root\"},` +
		`\"Action\":\"secretsmanager:GetSecretValue\",\"Resource\":\"*\"}]}"}`

	tests := []struct {
		name         string
		body         string
		wantStatus   int
		wantPassed   bool
		wantErrCount int
	}{
		{
			name:         "valid_policy",
			body:         validFullPolicy,
			wantStatus:   http.StatusOK,
			wantPassed:   true,
			wantErrCount: 0,
		},
		{
			name:         "missing_version",
			body:         `{"ResourcePolicy":"{\"Statement\":[]}"}`,
			wantStatus:   http.StatusOK,
			wantPassed:   false,
			wantErrCount: 1,
		},
		{
			name:         "missing_statement",
			body:         `{"ResourcePolicy":"{\"Version\":\"2012-10-17\"}"}`,
			wantStatus:   http.StatusOK,
			wantPassed:   false,
			wantErrCount: 1,
		},
		{
			name:       "invalid_json_policy",
			body:       `{"ResourcePolicy":"not-json"}`,
			wantStatus: http.StatusOK,
			wantPassed: false,
		},
		{
			name:       "empty_policy",
			body:       `{"ResourcePolicy":""}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			h := secretsmanager.NewHandler(b)
			rec := doR1Request(t, h, "secretsmanager.ValidateResourcePolicy", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out secretsmanager.ValidateResourcePolicyOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.wantPassed, out.PolicyValidationPassed)

				if tt.wantErrCount > 0 {
					assert.Len(t, out.ValidationErrors, tt.wantErrCount)
				}
			}
		})
	}
}

// TestValidateResourcePolicy_HTTP_SecretExists verifies secret-scoped validation.
func TestValidateResourcePolicy_HTTP_SecretExists(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	// Create a secret first.
	rec := doR1Request(t, h, "secretsmanager.CreateSecret", `{"Name":"validate-test","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// validPolicy is a minimal well-formed IAM policy JSON string.
	const validPolicy = `{"Version":"2012-10-17","Statement":[{` +
		`"Effect":"Allow","Principal":{"AWS":"*"},` +
		`"Action":"secretsmanager:GetSecretValue","Resource":"*"}]}`
	body, _ := json.Marshal(map[string]string{
		"SecretId":       "validate-test",
		"ResourcePolicy": validPolicy,
	})

	rec = doR1Request(t, h, "secretsmanager.ValidateResourcePolicy", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ValidateResourcePolicyOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.True(t, out.PolicyValidationPassed)
}
