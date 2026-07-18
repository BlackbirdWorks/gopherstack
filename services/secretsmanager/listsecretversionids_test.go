package secretsmanager_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// ListSecretVersionIds comprehensive
// ---------------------------------------------------------------------------

func TestListSecretVersionIds_Basic(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "lvid-basic",
		SecretString:       "v1",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:           "lvid-basic",
		SecretString:       "v2",
		ClientRequestToken: "v2",
	})
	require.NoError(t, err)

	out, err := b.ListSecretVersionIDs(
		context.Background(),
		&secretsmanager.ListSecretVersionIDsInput{SecretID: "lvid-basic"},
	)
	require.NoError(t, err)
	// Only labeled versions by default (v1=AWSPREVIOUS, v2=AWSCURRENT)
	assert.Len(t, out.Versions, 2)
}

func TestListSecretVersionIds_MaxResultsInvalid(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "lvid-mr", SecretString: "v"},
	)
	require.NoError(t, err)

	mr := int64(0)
	_, err = b.ListSecretVersionIDs(context.Background(), &secretsmanager.ListSecretVersionIDsInput{
		SecretID:   "lvid-mr",
		MaxResults: &mr,
	})
	require.ErrorIs(t, err, secretsmanager.ErrInvalidParameter, "MaxResults=0 for ListSecretVersionIds must fail")
}

func TestListSecretVersionIds_IncludeDeprecated(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "lvid-depr",
		SecretString:       "v1",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	// Rotate 3 times to create unlabeled versions
	for i := 2; i <= 4; i++ {
		_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
			SecretID:           "lvid-depr",
			SecretString:       fmt.Sprintf("v%d", i),
			ClientRequestToken: fmt.Sprintf("v%d", i),
		})
		require.NoError(t, err)
	}

	outNormal, err := b.ListSecretVersionIDs(
		context.Background(),
		&secretsmanager.ListSecretVersionIDsInput{SecretID: "lvid-depr"},
	)
	require.NoError(t, err)

	outAll, err := b.ListSecretVersionIDs(context.Background(), &secretsmanager.ListSecretVersionIDsInput{
		SecretID:          "lvid-depr",
		IncludeDeprecated: true,
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(outAll.Versions), len(outNormal.Versions),
		"IncludeDeprecated must return at least as many versions")
}

func TestListSecretVersionIds_SortedNewestFirst(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "lvid-sort",
		SecretString:       "v1",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	time.Sleep(2 * time.Millisecond)

	_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretID:           "lvid-sort",
		SecretString:       "v2",
		ClientRequestToken: "v2",
	})
	require.NoError(t, err)

	out, err := b.ListSecretVersionIDs(
		context.Background(),
		&secretsmanager.ListSecretVersionIDsInput{SecretID: "lvid-sort"},
	)
	require.NoError(t, err)
	require.Len(t, out.Versions, 2)
	// Newest (v2 = AWSCURRENT) should be first
	assert.Equal(t, "v2", out.Versions[0].VersionID)
}

func TestListSecretVersionIds_NotFound(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.ListSecretVersionIDs(
		context.Background(),
		&secretsmanager.ListSecretVersionIDsInput{SecretID: "missing"},
	)
	require.ErrorIs(t, err, secretsmanager.ErrSecretNotFound)
}

func TestListSecretVersionIds_Pagination(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "lvid-pages",
		SecretString:       "v1",
		ClientRequestToken: "v1",
	})
	require.NoError(t, err)

	for i := 2; i <= 5; i++ {
		_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
			SecretID:           "lvid-pages",
			SecretString:       fmt.Sprintf("v%d", i),
			ClientRequestToken: fmt.Sprintf("v%d", i),
		})
		require.NoError(t, err)
	}

	// Use IncludeDeprecated to surface all 5 versions (v1-v3 are unlabeled/deprecated).
	mr := int64(2)
	page1, err := b.ListSecretVersionIDs(context.Background(), &secretsmanager.ListSecretVersionIDsInput{
		SecretID:          "lvid-pages",
		MaxResults:        &mr,
		IncludeDeprecated: true,
	})
	require.NoError(t, err)
	assert.Len(t, page1.Versions, 2)
	assert.NotEmpty(t, page1.NextToken)
}

// ---------------------------------------------------------------------------
// LastAccessedDate in version list entries
//
// NOTE: a near-exact duplicate of TestListSecretVersionIds_IncludeDeprecated above (both
// create a secret, add several versions via PutSecretValue, and assert that
// IncludeDeprecated returns len(withDeprecated) >= len(withoutDeprecated)) has been
// deliberately dropped here to avoid true duplicate coverage.
// ---------------------------------------------------------------------------

func TestListSecretVersionIds_EntryIncludesLastAccessedDate(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackendWithConfig("000000000001", "us-east-1")

	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "with-access", SecretString: "value"},
	)
	require.NoError(t, err)

	_, err = b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{SecretID: "with-access"})
	require.NoError(t, err)

	out, err := b.ListSecretVersionIDs(
		context.Background(),
		&secretsmanager.ListSecretVersionIDsInput{SecretID: "with-access"},
	)
	require.NoError(t, err)
	require.Len(t, out.Versions, 1)
	require.NotNil(t, out.Versions[0].LastAccessedDate)
}

// ---------------------------------------------------------------------------
// Deleted secrets
// ---------------------------------------------------------------------------

// TestListSecretVersionIds_DeletedSecret verifies ListSecretVersionIds works on deleted secrets.
func TestListSecretVersionIds_DeletedSecret(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "del-ver", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "del-ver"})
	require.NoError(t, err)

	out, err := b.ListSecretVersionIDs(
		context.Background(),
		&secretsmanager.ListSecretVersionIDsInput{SecretID: "del-ver"},
	)
	require.NoError(t, err)
	assert.Len(t, out.Versions, 1)
}

// ---------------------------------------------------------------------------
// HTTP dispatch
// ---------------------------------------------------------------------------

// TestListSecretVersionIds_HTTPDispatchTable verifies the ListSecretVersionIds
// operation works via HTTP.
func TestListSecretVersionIds_HTTPDispatchTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *secretsmanager.InMemoryBackend)
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		name           string
		body           string
		expectedStatus int
	}{
		{
			name: "lists_versions",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "lsvi-secret", SecretString: "v1"},
				)
				require.NoError(t, err)
				_, err = b.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
					SecretID:     "lsvi-secret",
					SecretString: "v2",
				})
				require.NoError(t, err)
			},
			body:           `{"SecretId":"lsvi-secret"}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.ListSecretVersionIDsOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "lsvi-secret", out.Name)
				assert.NotEmpty(t, out.Versions)
			},
		},
		{
			name:           "not_found",
			body:           `{"SecretId":"nonexistent"}`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "bad_json",
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
			rec := doSMRequest(t, h, "secretsmanager.ListSecretVersionIds", tt.body)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Backend + HTTP scenarios
// ---------------------------------------------------------------------------

// TestListSecretVersionIds_BackendScenarios verifies ListSecretVersionIDs via backend.
func TestListSecretVersionIds_BackendScenarios(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		setup     func(t *testing.T, b *secretsmanager.InMemoryBackend)
		checkFn   func(t *testing.T, out *secretsmanager.ListSecretVersionIDsOutput)
		name      string
		input     secretsmanager.ListSecretVersionIDsInput
		wantErr   bool
	}{
		{
			name: "returns_labeled_versions",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "lsv-test", SecretString: "v1"},
				)
				require.NoError(t, err)
				_, err = b.PutSecretValue(
					context.Background(),
					&secretsmanager.PutSecretValueInput{SecretID: "lsv-test", SecretString: "v2"},
				)
				require.NoError(t, err)
			},
			input: secretsmanager.ListSecretVersionIDsInput{SecretID: "lsv-test"},
			checkFn: func(t *testing.T, out *secretsmanager.ListSecretVersionIDsOutput) {
				t.Helper()
				assert.Equal(t, "lsv-test", out.Name)
				assert.Len(t, out.Versions, 2)
			},
		},
		{
			name: "include_deprecated_returns_all",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "lsv-depr", SecretString: "v1"},
				)
				require.NoError(t, err)
				_, err = b.PutSecretValue(
					context.Background(),
					&secretsmanager.PutSecretValueInput{SecretID: "lsv-depr", SecretString: "v2"},
				)
				require.NoError(t, err)
				_, err = b.PutSecretValue(
					context.Background(),
					&secretsmanager.PutSecretValueInput{SecretID: "lsv-depr", SecretString: "v3"},
				)
				require.NoError(t, err)
			},
			input: secretsmanager.ListSecretVersionIDsInput{SecretID: "lsv-depr", IncludeDeprecated: true},
			checkFn: func(t *testing.T, out *secretsmanager.ListSecretVersionIDsOutput) {
				t.Helper()
				assert.Len(t, out.Versions, 3)
			},
		},
		{
			name:      "not_found",
			setup:     func(_ *testing.T, _ *secretsmanager.InMemoryBackend) {},
			input:     secretsmanager.ListSecretVersionIDsInput{SecretID: "nonexistent"},
			wantErr:   true,
			wantErrIs: secretsmanager.ErrSecretNotFound,
		},
		{
			name: "pagination",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "lsv-page", SecretString: "v1"},
				)
				require.NoError(t, err)
				_, err = b.PutSecretValue(
					context.Background(),
					&secretsmanager.PutSecretValueInput{SecretID: "lsv-page", SecretString: "v2"},
				)
				require.NoError(t, err)
			},
			input: secretsmanager.ListSecretVersionIDsInput{
				SecretID:          "lsv-page",
				IncludeDeprecated: true,
				MaxResults: func() *int64 {
					v := int64(1)

					return &v
				}(),
			},
			checkFn: func(t *testing.T, out *secretsmanager.ListSecretVersionIDsOutput) {
				t.Helper()
				assert.Len(t, out.Versions, 1)
				assert.NotEmpty(t, out.NextToken)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			tt.setup(t, b)

			out, err := b.ListSecretVersionIDs(context.Background(), &tt.input)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out)

			if tt.checkFn != nil {
				tt.checkFn(t, out)
			}
		})
	}
}

// TestListSecretVersionIds_ViaHandler verifies ListSecretVersionIDs via HTTP dispatch
// using a raw echo request (rather than the doSMRequest/doR1Request helpers).
func TestListSecretVersionIds_ViaHandler(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

	_, err := backend.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "handler-lsv", SecretString: "v1"},
	)
	require.NoError(t, err)
	_, err = backend.PutSecretValue(
		context.Background(),
		&secretsmanager.PutSecretValueInput{SecretID: "handler-lsv", SecretString: "v2"},
	)
	require.NoError(t, err)

	body := `{"SecretId":"handler-lsv"}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "secretsmanager.ListSecretVersionIds")
	rec := httptest.NewRecorder()

	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ListSecretVersionIDsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "handler-lsv", out.Name)
	assert.NotEmpty(t, out.Versions)
}
