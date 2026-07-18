package secretsmanager_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// BatchGetSecretValue comprehensive
// ---------------------------------------------------------------------------

func TestBatchGetSecretValue_MaxResultsTooHigh(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	mr := int32(21)
	_, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{MaxResults: &mr})
	require.ErrorIs(t, err, secretsmanager.ErrInvalidParameter, "BatchGetSecretValue MaxResults>20 must fail")
}

func TestBatchGetSecretValue_MaxResultsHTTP(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.BatchGetSecretValue", `{"MaxResults":25}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatchGetSecretValue_ByIDList(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	for _, name := range []string{"bg-s1", "bg-s2"} {
		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: name, SecretString: name + "-val"},
		)
		require.NoError(t, err)
	}

	out, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{
		SecretIDList: []string{"bg-s1", "bg-s2"},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretValues, 2)
	assert.Empty(t, out.Errors)
}

func TestBatchGetSecretValue_MissingInErrors(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "bg-good", SecretString: "v"},
	)
	require.NoError(t, err)

	out, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{
		SecretIDList: []string{"bg-good", "bg-missing"},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretValues, 1)
	assert.Len(t, out.Errors, 1)
	assert.Equal(t, "bg-missing", out.Errors[0].SecretID)
}

func TestBatchGetSecretValue_ByFilter(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "bg-filter-match",
		SecretString: "v",
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "other-name",
		SecretString: "v",
	})
	require.NoError(t, err)

	out, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{
		Filters: []secretsmanager.BatchGetSecretValueFilter{
			{Key: "name", Values: []string{"bg-filter-match"}},
		},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretValues, 1)
	assert.Equal(t, "bg-filter-match", out.SecretValues[0].Name)
}

// ---------------------------------------------------------------------------
// LastAccessedDate tracking (issue #4)
// ---------------------------------------------------------------------------

// TestBatchGetSecretValue_UpdatesLastAccessedDate verifies that calling
// BatchGetSecretValue updates the LastAccessedDate on each successfully retrieved
// secret and version, matching the behavior of GetSecretValue.
func TestBatchGetSecretValue_UpdatesLastAccessedDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *secretsmanager.InMemoryBackend)
		inputFn func() *secretsmanager.BatchGetSecretValueInput
		checkFn func(*testing.T, *secretsmanager.InMemoryBackend)
		name    string
	}{
		{
			name: "by_id_list_updates_accessed_date",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "bgv-acc-1", SecretString: "val"},
				)
				require.NoError(t, err)
			},
			inputFn: func() *secretsmanager.BatchGetSecretValueInput {
				return &secretsmanager.BatchGetSecretValueInput{SecretIDList: []string{"bgv-acc-1"}}
			},
			checkFn: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				desc, err := b.DescribeSecret(
					context.Background(),
					&secretsmanager.DescribeSecretInput{SecretID: "bgv-acc-1"},
				)
				require.NoError(t, err)
				assert.NotNil(t, desc.LastAccessedDate, "LastAccessedDate must be set after BatchGetSecretValue")
			},
		},
		{
			name: "by_filter_updates_accessed_date",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "bgv-filt-1", SecretString: "val"},
				)
				require.NoError(t, err)
			},
			inputFn: func() *secretsmanager.BatchGetSecretValueInput {
				return &secretsmanager.BatchGetSecretValueInput{}
			},
			checkFn: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				desc, err := b.DescribeSecret(
					context.Background(),
					&secretsmanager.DescribeSecretInput{SecretID: "bgv-filt-1"},
				)
				require.NoError(t, err)
				assert.NotNil(
					t,
					desc.LastAccessedDate,
					"LastAccessedDate must be set after BatchGetSecretValue by filter",
				)
			},
		},
		{
			name: "deleted_secret_in_id_list_does_not_update",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "bgv-del-1", SecretString: "val"},
				)
				require.NoError(t, err)
				_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "bgv-del-1"})
				require.NoError(t, err)
			},
			inputFn: func() *secretsmanager.BatchGetSecretValueInput {
				return &secretsmanager.BatchGetSecretValueInput{SecretIDList: []string{"bgv-del-1"}}
			},
			checkFn: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				desc, err := b.DescribeSecret(
					context.Background(),
					&secretsmanager.DescribeSecretInput{SecretID: "bgv-del-1"},
				)
				require.NoError(t, err)
				assert.Nil(t, desc.LastAccessedDate, "deleted secret must not have LastAccessedDate updated")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			tt.setup(t, b)

			out, err := b.BatchGetSecretValue(context.Background(), tt.inputFn())
			require.NoError(t, err)

			// For the non-deleted cases, verify we got a successful result.
			if tt.name != "deleted_secret_in_id_list_does_not_update" {
				assert.NotEmpty(t, out.SecretValues)
			}

			tt.checkFn(t, b)
		})
	}
}

// ---------------------------------------------------------------------------
// SecretIdList length limit (issue #2)
// ---------------------------------------------------------------------------

// TestBatchGetSecretValue_SecretIDListTooLong verifies that BatchGetSecretValue
// rejects a SecretIdList with more than 20 entries with InvalidParameterException.
// AWS enforces this limit: SecretIdList must not contain more than 20 entries.
func TestBatchGetSecretValue_SecretIDListTooLong(t *testing.T) {
	t.Parallel()

	makeIDList := func(n int) []string {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf("secret-%d", i+1)
		}

		return ids
	}

	tests := []struct {
		name    string
		ids     []string
		wantErr bool
	}{
		{
			name:    "exactly_20_ids_allowed",
			ids:     makeIDList(20),
			wantErr: false,
		},
		{
			name:    "21_ids_rejected",
			ids:     makeIDList(21),
			wantErr: true,
		},
		{
			name:    "far_above_limit_rejected",
			ids:     makeIDList(100),
			wantErr: true,
		},
		{
			name:    "empty_list_allowed",
			ids:     []string{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			_, err := b.BatchGetSecretValue(
				context.Background(),
				&secretsmanager.BatchGetSecretValueInput{SecretIDList: tt.ids},
			)

			if tt.wantErr {
				require.ErrorIs(t, err, secretsmanager.ErrInvalidParameter,
					"SecretIdList > 20 must return ErrInvalidParameter")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBatchGetSecretValue_SecretIDListTooLong_HTTP verifies that the HTTP handler
// returns 400 InvalidParameterException when SecretIdList exceeds 20 entries.
func TestBatchGetSecretValue_SecretIDListTooLong_HTTP(t *testing.T) {
	t.Parallel()

	makeJSONIDList := func(n int) string {
		ids := make([]string, n)
		for i := range ids {
			ids[i] = fmt.Sprintf(`"secret-%d"`, i+1)
		}

		return "[" + strings.Join(ids, ",") + "]"
	}

	tests := []struct {
		name       string
		body       string
		wantType   string
		wantStatus int
	}{
		{
			name:       "21_ids_returns_400_InvalidParameterException",
			body:       `{"SecretIdList":` + makeJSONIDList(21) + `}`,
			wantType:   "InvalidParameterException",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "20_ids_returns_200",
			body:       `{"SecretIdList":` + makeJSONIDList(20) + `}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			h := secretsmanager.NewHandler(b)

			rec := doR1Request(t, h, "secretsmanager.BatchGetSecretValue", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantType != "" {
				var errResp secretsmanager.ErrorResponse
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantType, errResp.Type)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Real-AWS parity cases (ported from handler_parity_test.go)
// ---------------------------------------------------------------------------

// TestBatchGetSecretValue_SecretIdListAndFiltersRejected verifies that providing
// both SecretIdList and Filters returns InvalidParameterException. Real AWS behavior.
func TestBatchGetSecretValue_SecretIdListAndFiltersRejected(t *testing.T) {
	t.Parallel()

	h := newSMHandler()

	rec := doSMRequest(t, h, "secretsmanager.BatchGetSecretValue",
		`{"SecretIdList":["s1"],"Filters":[{"Key":"name","Values":["s"]}]}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"BatchGetSecretValue with both SecretIdList and Filters must return 400; body: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidParameterException", resp["__type"])
}

// TestBatchGetSecretValue_FilterUsesPrefix verifies that BatchGetSecretValue
// filter matching uses prefix (not exact) matching, consistent with ListSecrets.
func TestBatchGetSecretValue_FilterUsesPrefix(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)
	ctx := context.Background()

	_, err := b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: "prefix-match-abc", SecretString: "v"})
	require.NoError(t, err)

	_, err = b.CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: "prefix-match-xyz", SecretString: "v"})
	require.NoError(t, err)

	// Filter by prefix "prefix-match-" — should return both.
	rec := doSMRequest(t, h, "secretsmanager.BatchGetSecretValue",
		`{"Filters":[{"Key":"name","Values":["prefix-match-"]}]}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		SecretValues []map[string]any `json:"SecretValues"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.SecretValues, 2,
		"real AWS: BatchGetSecretValue filter uses prefix matching; body: %s", rec.Body.String())
}

// ---------------------------------------------------------------------------
// HTTP dispatch table
// ---------------------------------------------------------------------------

// TestBatchGetSecretValue_HTTP verifies BatchGetSecretValue via HTTP dispatch.
func TestBatchGetSecretValue_HTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *secretsmanager.InMemoryBackend)
		checkFn        func(*testing.T, *httptest.ResponseRecorder)
		name           string
		body           string
		expectedStatus int
	}{
		{
			name: "by_secret_id_list",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "batch-s1", SecretString: "val1"},
				)
				require.NoError(t, err)
				_, err = b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "batch-s2", SecretString: "val2"},
				)
				require.NoError(t, err)
			},
			body:           `{"SecretIdList":["batch-s1","batch-s2"]}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.BatchGetSecretValueOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.SecretValues, 2)
				assert.Empty(t, out.Errors)
			},
		},
		{
			name: "partial_errors",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "batch-ok", SecretString: "v"},
				)
				require.NoError(t, err)
			},
			body:           `{"SecretIdList":["batch-ok","batch-missing"]}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.BatchGetSecretValueOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.SecretValues, 1)
				assert.Len(t, out.Errors, 1)
				assert.Equal(t, "ResourceNotFoundException", out.Errors[0].ErrorCode)
				assert.Equal(t, "batch-missing", out.Errors[0].SecretID)
			},
		},
		{
			name: "all_secrets_when_no_id_list",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "batch-all-1", SecretString: "a"},
				)
				require.NoError(t, err)
				_, err = b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "batch-all-2", SecretString: "b"},
				)
				require.NoError(t, err)
			},
			body:           `{}`,
			expectedStatus: http.StatusOK,
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var out secretsmanager.BatchGetSecretValueOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.SecretValues, 2)
			},
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
			rec := doSMRequest(t, h, "secretsmanager.BatchGetSecretValue", tt.body)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.checkFn != nil {
				tt.checkFn(t, rec)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Backend edge cases (ported from the original TestNewOpsBackend)
// ---------------------------------------------------------------------------

// TestBatchGetSecretValue_DeletedSecretInIDList verifies that BatchGetSecretValue
// reports a deleted secret referenced by SecretIdList as an InvalidRequestException
// error entry, rather than a successful result.
func TestBatchGetSecretValue_DeletedSecretInIDList(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "del-batch", SecretString: "v"},
	)
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "del-batch"})
	require.NoError(t, err)

	out, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{
		SecretIDList: []string{"del-batch"},
	})
	require.NoError(t, err)
	assert.Empty(t, out.SecretValues)
	require.Len(t, out.Errors, 1)
	assert.Equal(t, "InvalidRequestException", out.Errors[0].ErrorCode)
}

// ---------------------------------------------------------------------------
// Tag-key filter + pagination
// ---------------------------------------------------------------------------

// TestBatchGetSecretValue_FilterTagKey verifies BatchGetSecretValue with a tag-key filter.
func TestBatchGetSecretValue_FilterTagKey(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "batch-tag",
		SecretString: "v",
		Tags:         []secretsmanager.Tag{{Key: "class", Value: "database"}},
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "batch-notag", SecretString: "v"},
	)
	require.NoError(t, err)

	out, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{
		Filters: []secretsmanager.BatchGetSecretValueFilter{{Key: "tag-key", Values: []string{"class"}}},
	})
	require.NoError(t, err)
	require.Len(t, out.SecretValues, 1)
	assert.Equal(t, "batch-tag", out.SecretValues[0].Name)
}

// TestBatchGetSecretValue_Pagination verifies NextToken pagination in filter mode.
func TestBatchGetSecretValue_Pagination(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()

	for i := range 5 {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
			Name:         fmt.Sprintf("pag-%02d", i),
			SecretString: "v",
		})
		require.NoError(t, err)
	}

	maxResults := int32(2)
	out1, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{
		MaxResults: &maxResults,
	})
	require.NoError(t, err)
	require.Len(t, out1.SecretValues, 2)
	assert.NotEmpty(t, out1.NextToken)

	out2, err := b.BatchGetSecretValue(context.Background(), &secretsmanager.BatchGetSecretValueInput{
		MaxResults: &maxResults,
		NextToken:  out1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, out2.SecretValues, 2)
	assert.NotEmpty(t, out2.NextToken)
}
