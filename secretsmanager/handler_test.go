package secretsmanager_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/secretsmanager"
)

// TestSecretsManagerBackendCreateSecret verifies secret creation.
func TestSecretsManagerBackendCreateSecret(t *testing.T) {
	t.Parallel()

	t.Run("WithStringValue", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		out, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{
			Name:         "my-secret",
			Description:  "a test secret",
			SecretString: "mysecretvalue",
		})

		require.NoError(t, err)
		assert.NotEmpty(t, out.ARN)
		assert.Equal(t, "my-secret", out.Name)
		assert.NotEmpty(t, out.VersionID)
	})

	t.Run("WithoutValue", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		out, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{
			Name: "empty-secret",
		})

		require.NoError(t, err)
		assert.NotEmpty(t, out.ARN)
		assert.Empty(t, out.VersionID) // no version when no value
	})

	t.Run("DuplicateNameFails", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{Name: "dup-secret"})

		_, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{Name: "dup-secret"})
		require.ErrorIs(t, err, secretsmanager.ErrSecretAlreadyExists)
	})

	t.Run("WithTags", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		out, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{
			Name: "tagged-secret",
			Tags: []secretsmanager.Tag{
				{Key: "env", Value: "test"},
				{Key: "team", Value: "platform"},
			},
		})

		require.NoError(t, err)
		assert.NotEmpty(t, out.ARN)
	})
}

// TestSecretsManagerBackendGetSecretValue verifies getting a secret value.
func TestSecretsManagerBackendGetSecretValue(t *testing.T) {
	t.Parallel()

	t.Run("CurrentVersion", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{
			Name:         "db-password",
			SecretString: "secretpassword",
		})

		out, err := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{
			SecretID: "db-password",
		})

		require.NoError(t, err)
		assert.Equal(t, "secretpassword", out.SecretString)
		assert.Contains(t, out.VersionStages, secretsmanager.StagingLabelCurrent)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		_, err := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretID: "missing"})
		require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
	})

	t.Run("DeletedSecretFails", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{
			Name:         "deleted-secret",
			SecretString: "value",
		})
		_, _ = backend.DeleteSecret(&secretsmanager.DeleteSecretInput{SecretID: "deleted-secret"})

		_, err := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretID: "deleted-secret"})
		require.ErrorIs(t, err, secretsmanager.ErrSecretDeleted)
	})
}

// TestSecretsManagerBackendPutSecretValue verifies adding new versions.
func TestSecretsManagerBackendPutSecretValue(t *testing.T) {
	t.Parallel()

	t.Run("NewVersion", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{
			Name:         "versioned-secret",
			SecretString: "v1",
		})

		out, err := backend.PutSecretValue(&secretsmanager.PutSecretValueInput{
			SecretID:     "versioned-secret",
			SecretString: "v2",
		})

		require.NoError(t, err)
		assert.NotEmpty(t, out.VersionID)
		assert.Contains(t, out.VersionStages, secretsmanager.StagingLabelCurrent)

		// New current value
		curr, _ := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretID: "versioned-secret"})
		assert.Equal(t, "v2", curr.SecretString)

		// Previous value accessible via AWSPREVIOUS
		prev, prevErr := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{
			SecretID:     "versioned-secret",
			VersionStage: secretsmanager.StagingLabelPrevious,
		})
		require.NoError(t, prevErr)
		assert.Equal(t, "v1", prev.SecretString)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		_, err := backend.PutSecretValue(&secretsmanager.PutSecretValueInput{
			SecretID:     "missing",
			SecretString: "value",
		})
		require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
	})
}

// TestSecretsManagerBackendDeleteAndRestore verifies soft-delete and restore.
func TestSecretsManagerBackendDeleteAndRestore(t *testing.T) {
	t.Parallel()

	t.Run("DeleteAndRestore", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{
			Name:         "restorable",
			SecretString: "data",
		})

		delOut, err := backend.DeleteSecret(&secretsmanager.DeleteSecretInput{SecretID: "restorable"})
		require.NoError(t, err)
		assert.NotZero(t, delOut.DeletionDate)

		// Restore
		restOut, err := backend.RestoreSecret(&secretsmanager.RestoreSecretInput{SecretID: "restorable"})
		require.NoError(t, err)
		assert.Equal(t, "restorable", restOut.Name)

		// Can get value again
		_, err = backend.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretID: "restorable"})
		require.NoError(t, err)
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		_, err := backend.DeleteSecret(&secretsmanager.DeleteSecretInput{SecretID: "missing"})
		require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
	})

	t.Run("RestoreNotFound", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		_, err := backend.RestoreSecret(&secretsmanager.RestoreSecretInput{SecretID: "missing"})
		require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
	})
}

// TestSecretsManagerBackendListSecrets verifies listing secrets.
func TestSecretsManagerBackendListSecrets(t *testing.T) {
	t.Parallel()

	t.Run("Basic", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		for _, name := range []string{"alpha", "beta", "gamma"} {
			_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{Name: name})
		}

		out, err := backend.ListSecrets(&secretsmanager.ListSecretsInput{})
		require.NoError(t, err)
		assert.Len(t, out.SecretList, 3)
	})

	t.Run("ExcludesDeleted", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{Name: "active"})
		_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{Name: "deleted"})
		_, _ = backend.DeleteSecret(&secretsmanager.DeleteSecretInput{SecretID: "deleted"})

		out, err := backend.ListSecrets(&secretsmanager.ListSecretsInput{})
		require.NoError(t, err)
		assert.Len(t, out.SecretList, 1)
		assert.Equal(t, "active", out.SecretList[0].Name)
	})

	t.Run("IncludesDeleted", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{Name: "active"})
		_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{Name: "deleted"})
		_, _ = backend.DeleteSecret(&secretsmanager.DeleteSecretInput{SecretID: "deleted"})

		out, err := backend.ListSecrets(&secretsmanager.ListSecretsInput{IncludeDeleted: true})
		require.NoError(t, err)
		assert.Len(t, out.SecretList, 2)
	})

	t.Run("Pagination", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		for _, name := range []string{"a", "b", "c", "d", "e"} {
			_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{Name: name})
		}

		limit := int64(2)
		out, err := backend.ListSecrets(&secretsmanager.ListSecretsInput{MaxResults: &limit})
		require.NoError(t, err)
		assert.Len(t, out.SecretList, 2)
		assert.NotEmpty(t, out.NextToken)

		out2, err := backend.ListSecrets(&secretsmanager.ListSecretsInput{
			MaxResults: &limit,
			NextToken:  out.NextToken,
		})
		require.NoError(t, err)
		assert.Len(t, out2.SecretList, 2)
	})
}

// TestSecretsManagerBackendDescribeSecret verifies describing a secret.
func TestSecretsManagerBackendDescribeSecret(t *testing.T) {
	t.Parallel()

	t.Run("Found", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{
			Name:         "described",
			Description:  "my description",
			SecretString: "value",
			Tags: []secretsmanager.Tag{
				{Key: "env", Value: "prod"},
			},
		})

		out, err := backend.DescribeSecret(&secretsmanager.DescribeSecretInput{SecretID: "described"})
		require.NoError(t, err)
		assert.Equal(t, "described", out.Name)
		assert.Equal(t, "my description", out.Description)
		assert.NotEmpty(t, out.VersionIDsToStages)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, err := backend.DescribeSecret(&secretsmanager.DescribeSecretInput{SecretID: "missing"})
		require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
	})
}

// TestSecretsManagerBackendUpdateSecret verifies updating a secret.
func TestSecretsManagerBackendUpdateSecret(t *testing.T) {
	t.Parallel()

	t.Run("UpdateDescription", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{Name: "updatable", SecretString: "original"})

		out, err := backend.UpdateSecret(&secretsmanager.UpdateSecretInput{
			SecretID:    "updatable",
			Description: "new description",
		})
		require.NoError(t, err)
		assert.Equal(t, "updatable", out.Name)
		assert.Empty(t, out.VersionID) // no new version for description-only update

		desc, _ := backend.DescribeSecret(&secretsmanager.DescribeSecretInput{SecretID: "updatable"})
		assert.Equal(t, "new description", desc.Description)
	})

	t.Run("UpdateValue", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{Name: "with-value", SecretString: "v1"})

		out, err := backend.UpdateSecret(&secretsmanager.UpdateSecretInput{
			SecretID:     "with-value",
			SecretString: "v2",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, out.VersionID)
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, err := backend.UpdateSecret(&secretsmanager.UpdateSecretInput{SecretID: "missing"})
		require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
	})
}

// TestSecretsManagerBackendListAll verifies the ListAll method.
func TestSecretsManagerBackendListAll(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()

	for _, name := range []string{"z-secret", "a-secret", "m-secret"} {
		_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{Name: name})
	}

	all := backend.ListAll()
	require.Len(t, all, 3)
	assert.Equal(t, "a-secret", all[0].Name)
	assert.Equal(t, "m-secret", all[1].Name)
	assert.Equal(t, "z-secret", all[2].Name)
}

// TestSecretsManagerHandler verifies HTTP dispatch.
func TestSecretsManagerHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn        func(*testing.T, secretsmanager.StorageBackend)
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		target         string
		name           string
		body           string
		expectedStatus int
	}{
		{
			name:           "CreateSecret",
			target:         "secretsmanager.CreateSecret",
			body:           `{"Name":"test-secret","SecretString":"my-value"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.CreateSecretOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "test-secret", out.Name)
			},
		},
		{
			name:           "GetSecretValue",
			target:         "secretsmanager.GetSecretValue",
			body:           `{"SecretId":"pre-created"}`,
			expectedStatus: http.StatusOK,
			setupFn: func(t *testing.T, backend secretsmanager.StorageBackend) {
				t.Helper()
				_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{
					Name:         "pre-created",
					SecretString: "the-value",
				})
			},
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.GetSecretValueOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "the-value", out.SecretString)
			},
		},
		{
			name:           "UnknownOperation",
			target:         "secretsmanager.NoSuchOp",
			body:           `{}`,
			expectedStatus: http.StatusBadRequest,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var errResp secretsmanager.ErrorResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "UnknownOperationException", errResp.Type)
			},
		},
		{
			name:           "MissingTarget",
			target:         "",
			body:           `{}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "GetSupportedOps",
			target:         "",
			body:           "",
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var ops []string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ops))
				assert.Contains(t, ops, "CreateSecret")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			log := logger.NewLogger(slog.LevelDebug)
			backend := secretsmanager.NewInMemoryBackend()

			if tt.setupFn != nil {
				tt.setupFn(t, backend)
			}

			h := secretsmanager.NewHandler(backend, log)

			var req *http.Request

			switch {
			case tt.target != "":
				req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
				req.Header.Set("X-Amz-Target", tt.target)
			case tt.body != "":
				req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			default:
				req = httptest.NewRequest(http.MethodGet, "/", nil)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// TestSecretsManagerHandlerFullCycle tests the full CRUD cycle via HTTP.
func TestSecretsManagerHandlerFullCycle(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend, log)

	// CreateSecret
	createReq := httptest.NewRequest(
		http.MethodPost, "/",
		strings.NewReader(`{"Name":"full-cycle","SecretString":"initial-value"}`),
	)
	createReq.Header.Set("X-Amz-Target", "secretsmanager.CreateSecret")
	createRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(createReq, createRec)))
	assert.Equal(t, http.StatusOK, createRec.Code)

	// PutSecretValue
	putReq := httptest.NewRequest(
		http.MethodPost, "/",
		strings.NewReader(`{"SecretId":"full-cycle","SecretString":"updated-value"}`),
	)
	putReq.Header.Set("X-Amz-Target", "secretsmanager.PutSecretValue")
	putRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(putReq, putRec)))
	assert.Equal(t, http.StatusOK, putRec.Code)

	// DescribeSecret
	descReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"SecretId":"full-cycle"}`))
	descReq.Header.Set("X-Amz-Target", "secretsmanager.DescribeSecret")
	descRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(descReq, descRec)))
	assert.Equal(t, http.StatusOK, descRec.Code)

	// ListSecrets
	listReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	listReq.Header.Set("X-Amz-Target", "secretsmanager.ListSecrets")
	listRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(listReq, listRec)))
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listOut secretsmanager.ListSecretsOutput
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	assert.Len(t, listOut.SecretList, 1)

	// UpdateSecret
	updateReq := httptest.NewRequest(
		http.MethodPost, "/",
		strings.NewReader(`{"SecretId":"full-cycle","Description":"new desc"}`),
	)
	updateReq.Header.Set("X-Amz-Target", "secretsmanager.UpdateSecret")
	updateRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(updateReq, updateRec)))
	assert.Equal(t, http.StatusOK, updateRec.Code)

	// DeleteSecret
	deleteReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"SecretId":"full-cycle"}`))
	deleteReq.Header.Set("X-Amz-Target", "secretsmanager.DeleteSecret")
	deleteRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(deleteReq, deleteRec)))
	assert.Equal(t, http.StatusOK, deleteRec.Code)

	// RestoreSecret
	restoreReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"SecretId":"full-cycle"}`))
	restoreReq.Header.Set("X-Amz-Target", "secretsmanager.RestoreSecret")
	restoreRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(restoreReq, restoreRec)))
	assert.Equal(t, http.StatusOK, restoreRec.Code)
}

// TestSecretsManagerHandlerMethodNotAllowed verifies non-POST/GET are rejected.
func TestSecretsManagerHandlerMethodNotAllowed(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend, log)

	req := httptest.NewRequest(http.MethodPut, "/something", nil)
	rec := httptest.NewRecorder()

	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// TestSecretsManagerHandlerRouteMatcher verifies the route matcher.
func TestSecretsManagerHandlerRouteMatcher(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend, logger.NewLogger(slog.LevelDebug))
	matcher := h.RouteMatcher()

	t.Run("MatchesSecretsManager", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("X-Amz-Target", "secretsmanager.CreateSecret")
		c := e.NewContext(req, httptest.NewRecorder())
		assert.True(t, matcher(c))
	})

	t.Run("DoesNotMatchOther", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("X-Amz-Target", "TrentService.CreateKey")
		c := e.NewContext(req, httptest.NewRecorder())
		assert.False(t, matcher(c))
	})
}

// TestSecretsManagerHandlerInvalidTarget verifies a malformed target header.
func TestSecretsManagerHandlerInvalidTarget(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend, log)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set("X-Amz-Target", "secretsmanagerNoSep")
	rec := httptest.NewRecorder()

	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestSecretsManagerBinarySecret verifies binary secret storage.
func TestSecretsManagerBinarySecret(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()

	binaryData := []byte{0x01, 0x02, 0x03, 0xFF}

	_, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{
		Name:         "binary-secret",
		SecretBinary: binaryData,
	})
	require.NoError(t, err)

	out, err := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretID: "binary-secret"})
	require.NoError(t, err)
	assert.Equal(t, binaryData, out.SecretBinary)
	assert.Empty(t, out.SecretString)
}

// TestSecretsManagerVersionByID verifies retrieving a specific version by ID.
func TestSecretsManagerVersionByID(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()

	_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{
		Name:         "versioned",
		SecretString: "v1-value",
	})

	// Get the initial version ID
	current, _ := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretID: "versioned"})
	v1ID := current.VersionID

	// Add v2
	_, _ = backend.PutSecretValue(&secretsmanager.PutSecretValueInput{
		SecretID:     "versioned",
		SecretString: "v2-value",
	})

	// Retrieve v1 by ID
	out, err := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{
		SecretID:  "versioned",
		VersionID: v1ID,
	})
	require.NoError(t, err)
	assert.Equal(t, "v1-value", out.SecretString)
}

// TestSecretsManagerHandlerInterface verifies handler interface methods.
func TestSecretsManagerHandlerInterface(t *testing.T) {
	t.Parallel()

	log := logger.NewLogger(slog.LevelDebug)
	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend, log)

	assert.Equal(t, "SecretsManager", h.Name())
	assert.Equal(t, 95, h.MatchPriority())

	e := echo.New()

	// ExtractOperation
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "secretsmanager.CreateSecret")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "CreateSecret", h.ExtractOperation(c))

	// ExtractOperation with no separator
	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	req2.Header.Set("X-Amz-Target", "secretsmanagerNoSep")
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.Equal(t, "Unknown", h.ExtractOperation(c2))

	// ExtractResource via SecretId
	body := `{"SecretId":"my-secret"}`
	req3 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	c3 := e.NewContext(req3, httptest.NewRecorder())
	assert.Equal(t, "my-secret", h.ExtractResource(c3))

	// ExtractResource via Name
	body2 := `{"Name":"my-name"}`
	req4 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body2))
	c4 := e.NewContext(req4, httptest.NewRecorder())
	assert.Equal(t, "my-name", h.ExtractResource(c4))

	// ExtractResource with no known field
	req5 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	c5 := e.NewContext(req5, httptest.NewRecorder())
	assert.Empty(t, h.ExtractResource(c5))
}

// TestSecretsManagerProvider verifies the Provider.
func TestSecretsManagerProvider(t *testing.T) {
	t.Parallel()

	p := &secretsmanager.Provider{}
	assert.Equal(t, "SecretsManager", p.Name())

	log := logger.NewLogger(slog.LevelDebug)
	ctx := &service.AppContext{Logger: log}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// TestSecretsManagerHandlerErrorCases exercises handleError paths.
func TestSecretsManagerHandlerErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target         string
		body           string
		name           string
		expectedErrTyp string
		expectedStatus int
	}{
		{
			name:           "SecretNotFound",
			target:         "secretsmanager.GetSecretValue",
			body:           `{"SecretId":"does-not-exist"}`,
			expectedStatus: http.StatusBadRequest,
			expectedErrTyp: "ResourceNotFoundException",
		},
		{
			name:           "SecretAlreadyExists",
			target:         "secretsmanager.CreateSecret",
			body:           `{"Name":"dup-secret"}`,
			expectedStatus: http.StatusBadRequest,
			expectedErrTyp: "ResourceExistsException",
		},
		{
			name:           "SecretDeleted",
			target:         "secretsmanager.GetSecretValue",
			body:           `{"SecretId":"deleted-secret"}`,
			expectedStatus: http.StatusBadRequest,
			expectedErrTyp: "InvalidRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			log := logger.NewLogger(slog.LevelDebug)
			backend := secretsmanager.NewInMemoryBackend()
			h := secretsmanager.NewHandler(backend, log)

			if tt.name == "SecretAlreadyExists" {
				_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{Name: "dup-secret"})
			}
			if tt.name == "SecretDeleted" {
				_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{
					Name:         "deleted-secret",
					SecretString: "value",
				})
				_, _ = backend.DeleteSecret(&secretsmanager.DeleteSecretInput{SecretID: "deleted-secret"})
			}

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()

			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.expectedStatus, rec.Code)

			var errResp secretsmanager.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.expectedErrTyp, errResp.Type)
		})
	}
}

// TestSecretsManagerResolveSecretIDARN verifies ARN-based secret resolution.
func TestSecretsManagerResolveSecretIDARN(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()

	// Create a secret and retrieve its ARN
	out, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{
		Name:         "arn-test-secret",
		SecretString: "arn-value",
	})
	require.NoError(t, err)
	arn := out.ARN

	// Get by ARN
	valOut, err := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{
		SecretID: arn,
	})
	require.NoError(t, err)
	assert.Equal(t, "arn-value", valOut.SecretString)
}

// TestSecretsManagerGetSecretValueVersionLabel tests GetSecretValue with a version label.
func TestSecretsManagerGetSecretValueVersionLabel(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()
	_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{
		Name:         "labeled-secret",
		SecretString: "v1",
	})
	_, _ = backend.PutSecretValue(&secretsmanager.PutSecretValueInput{
		SecretID:     "labeled-secret",
		SecretString: "v2",
	})

	// Retrieve AWSPREVIOUS
	out, err := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{
		SecretID:     "labeled-secret",
		VersionStage: secretsmanager.StagingLabelPrevious,
	})
	require.NoError(t, err)
	assert.Equal(t, "v1", out.SecretString)
}

// TestSecretsManagerPutSecretValueLabelRotation tests label rotation in PutSecretValue.
func TestSecretsManagerPutSecretValueLabelRotation(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend, log)

	// Create initial secret
	_, _ = backend.CreateSecret(&secretsmanager.CreateSecretInput{
		Name:         "rotate-test",
		SecretString: "v1",
	})

	// Put v2 via HTTP
	putBody, _ := json.Marshal(map[string]string{
		"SecretId":     "rotate-test",
		"SecretString": "v2",
	})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(putBody)))
	req.Header.Set("X-Amz-Target", "secretsmanager.PutSecretValue")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var putOut secretsmanager.PutSecretValueOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putOut))
	assert.Contains(t, putOut.VersionStages, secretsmanager.StagingLabelCurrent)

	// Current should be v2
	curr, err := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretID: "rotate-test"})
	require.NoError(t, err)
	assert.Equal(t, "v2", curr.SecretString)
}

// TestSecretsManagerTagResource tests tag add/remove operations.
func TestSecretsManagerTagResource(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend, log)

	_, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{
		Name:         "tag-secret",
		SecretString: "value",
	})
	require.NoError(t, err)

	// TagResource via HTTP
	tagBody := `{"SecretId":"tag-secret","Tags":[{"Key":"env","Value":"test"},{"Key":"team","Value":"platform"}]}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tagBody))
	req.Header.Set("X-Amz-Target", "secretsmanager.TagResource")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// DescribeSecret should show tags
	desc, err := backend.DescribeSecret(&secretsmanager.DescribeSecretInput{SecretID: "tag-secret"})
	require.NoError(t, err)
	assert.Equal(t, "test", desc.Tags["env"])
	assert.Equal(t, "platform", desc.Tags["team"])

	// UntagResource via HTTP
	untagBody := `{"SecretId":"tag-secret","TagKeys":["env"]}`
	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(untagBody))
	req2.Header.Set("X-Amz-Target", "secretsmanager.UntagResource")
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	desc2, err := backend.DescribeSecret(&secretsmanager.DescribeSecretInput{SecretID: "tag-secret"})
	require.NoError(t, err)
	assert.NotContains(t, desc2.Tags, "env")
	assert.Equal(t, "platform", desc2.Tags["team"])
}

// TestSecretsManagerRotateSecret tests the rotation stub.
func TestSecretsManagerRotateSecret(t *testing.T) {
	t.Parallel()

	e := echo.New()
	log := logger.NewLogger(slog.LevelDebug)
	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend, log)

	_, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{
		Name:         "rotate-secret",
		SecretString: "original-value",
	})
	require.NoError(t, err)

	rotateBody := `{"SecretId":"rotate-secret"}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(rotateBody))
	req.Header.Set("X-Amz-Target", "secretsmanager.RotateSecret")
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.RotateSecretOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "rotate-secret", out.Name)
	assert.NotEmpty(t, out.VersionID)

	// New version should be AWSCURRENT
	curr, err := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{SecretID: "rotate-secret"})
	require.NoError(t, err)
	assert.Equal(t, out.VersionID, curr.VersionID)
	assert.Equal(t, "original-value", curr.SecretString)
}
