package secretsmanager_test

// handler_dispatch_test.go consolidates every HTTP routing/dispatch/provider/chaos/
// reset/store-plumbing test that was previously scattered across several older test
// files. Ported verbatim (assertions unchanged); matches handler.go + provider.go +
// store.go.

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// HTTP dispatch
// ---------------------------------------------------------------------------

// TestHandler_Dispatch verifies HTTP dispatch.
func TestHandler_Dispatch(t *testing.T) {
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
				_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
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

			backend := secretsmanager.NewInMemoryBackend()

			if tt.setupFn != nil {
				tt.setupFn(t, backend)
			}

			h := secretsmanager.NewHandler(backend)

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

// TestHandler_FullCycle tests the full CRUD cycle via HTTP.
func TestHandler_FullCycle(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

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

// TestHandler_MethodNotAllowed verifies non-POST/GET are rejected.
func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

	req := httptest.NewRequest(http.MethodPut, "/something", nil)
	rec := httptest.NewRecorder()

	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// TestHandler_RouteMatcher verifies the route matcher.
func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	e := echo.New()
	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)
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

// TestHandler_InvalidTarget verifies a malformed target header.
func TestHandler_InvalidTarget(t *testing.T) {
	t.Parallel()

	e := echo.New()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set("X-Amz-Target", "secretsmanagerNoSep")
	rec := httptest.NewRecorder()

	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_Interface verifies handler interface methods.
func TestHandler_Interface(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

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

// TestProvider_Init verifies the Provider.
func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &secretsmanager.Provider{}
	assert.Equal(t, "SecretsManager", p.Name())

	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// TestHandler_ErrorCases exercises handleError paths.
func TestHandler_ErrorCases(t *testing.T) {
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

			backend := secretsmanager.NewInMemoryBackend()
			h := secretsmanager.NewHandler(backend)

			if tt.name == "SecretAlreadyExists" {
				_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "dup-secret"})
			}
			if tt.name == "SecretDeleted" {
				_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
					Name:         "deleted-secret",
					SecretString: "value",
				})
				_, _ = backend.DeleteSecret(
					context.Background(),
					&secretsmanager.DeleteSecretInput{SecretID: "deleted-secret"},
				)
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

// TestHandler_Chaos verifies ChaosServiceName/Operations/Regions.
func TestHandler_Chaos(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)
	h.DefaultRegion = "us-east-1"

	assert.Equal(t, "secretsmanager", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

// TestHandler_Reset verifies Handler.Reset clears backend state.
func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(backend)

	_, err := backend.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "handler-reset", SecretString: "val"},
	)
	require.NoError(t, err)
	assert.Len(t, backend.ListAll(), 1)

	h.Reset()
	assert.Empty(t, backend.ListAll())
}

// TestBackend_Reset verifies the Reset method clears all state.
func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()

	_, err := backend.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "to-be-reset", SecretString: "val"},
	)
	require.NoError(t, err)
	assert.Len(t, backend.ListAll(), 1)

	backend.Reset()
	assert.Empty(t, backend.ListAll())
}

// TestResolveSecretIDByARN verifies ARN-based secret resolution.
func TestResolveSecretIDByARN(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()

	// Create a secret and retrieve its ARN
	out, err := backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "arn-test-secret",
		SecretString: "arn-value",
	})
	require.NoError(t, err)
	arn := out.ARN

	// Get by ARN
	valOut, err := backend.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
		SecretID: arn,
	})
	require.NoError(t, err)
	assert.Equal(t, "arn-value", valOut.SecretString)
}

// TestListAll verifies the ListAll method.
func TestListAll(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()

	for _, name := range []string{"z-secret", "a-secret", "m-secret"} {
		_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name})
	}

	all := backend.ListAll()
	require.Len(t, all, 3)
	assert.Equal(t, "a-secret", all[0].Name)
	assert.Equal(t, "m-secret", all[1].Name)
	assert.Equal(t, "z-secret", all[2].Name)
}

// ---------------------------------------------------------------------------
// Backend/handler store plumbing
// ---------------------------------------------------------------------------

// TestHandler_OpsLen verifies all ops are registered in the dispatch table.
func TestHandler_OpsLen(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	assert.Equal(t, len(h.GetSupportedOperations()), secretsmanager.HandlerOpsLen(h))
}

// TestBackend_AccountIDAndRegion verifies AccountID() and Region() return configured values.
func TestBackend_AccountIDAndRegion(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackendWithConfig("123456789012", "eu-west-1")
	assert.Equal(t, "123456789012", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

// TestProvider_ErrNilAppContext verifies provider.Init(nil) returns ErrNilAppContext.
func TestProvider_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &secretsmanager.Provider{}
	_, err := p.Init(nil)

	require.ErrorIs(t, err, secretsmanager.ErrNilAppContext)
}

// TestBackend_SecretCount verifies SecretCount tracks secrets correctly.
func TestBackend_SecretCount(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	require.Equal(t, 0, secretsmanager.SecretCount(b))

	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "a", SecretString: "v"})
	require.NoError(t, err)
	assert.Equal(t, 1, secretsmanager.SecretCount(b))

	_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "b", SecretString: "v"})
	require.NoError(t, err)
	assert.Equal(t, 2, secretsmanager.SecretCount(b))
}

// TestBackend_ResourcePolicyCount verifies ResourcePolicyCount tracks policies.
func TestBackend_ResourcePolicyCount(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "pol-secret", SecretString: "v"},
	)
	require.NoError(t, err)
	require.Equal(t, 0, secretsmanager.ResourcePolicyCount(b))

	_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
		SecretID:       "pol-secret",
		ResourcePolicy: `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, secretsmanager.ResourcePolicyCount(b))

	_, err = b.DeleteResourcePolicy(
		context.Background(),
		&secretsmanager.DeleteResourcePolicyInput{SecretID: "pol-secret"},
	)
	require.NoError(t, err)
	assert.Equal(t, 0, secretsmanager.ResourcePolicyCount(b))
}

// TestBackend_ReplicationConfigCount verifies ReplicationConfigCount tracks replicas.
func TestBackend_ReplicationConfigCount(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "rep-cnt", SecretString: "v"},
	)
	require.NoError(t, err)
	require.Equal(t, 0, secretsmanager.ReplicationConfigCount(b))

	_, err = b.ReplicateSecretToRegions(context.Background(), &secretsmanager.ReplicateSecretToRegionsInput{
		SecretID:          "rep-cnt",
		AddReplicaRegions: []secretsmanager.ReplicaRegion{{Region: "us-west-2"}},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, secretsmanager.ReplicationConfigCount(b))

	_, err = b.StopReplicationToReplica(
		context.Background(),
		&secretsmanager.StopReplicationToReplicaInput{SecretID: "rep-cnt"},
	)
	require.NoError(t, err)
	assert.Equal(t, 0, secretsmanager.ReplicationConfigCount(b))
}

// TestBackend_AddSecretInternal verifies AddSecretInternal seeds correctly.
func TestBackend_AddSecretInternal(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	b.AddSecretInternal(&secretsmanager.Secret{
		ARN:  "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-seed-AABBCC",
		Name: "my-seed",
	})
	assert.Equal(t, 1, secretsmanager.SecretCount(b))

	got, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "my-seed"})
	require.NoError(t, err)
	assert.Equal(t, "my-seed", got.Name)
}

// TestBackend_GenerateVersionID verifies generated version IDs are UUID-formatted.
func TestBackend_GenerateVersionID(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "uuid-ver", SecretString: "v"},
	)
	require.NoError(t, err)

	_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
		SecretID:          "uuid-ver",
		RotationLambdaARN: testLambdaARN,
	})
	require.NoError(t, err)

	desc, err := b.DescribeSecret(context.Background(), &secretsmanager.DescribeSecretInput{SecretID: "uuid-ver"})
	require.NoError(t, err)

	uuidRE := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	for id := range desc.VersionIDsToStages {
		assert.True(t, uuidRE.MatchString(id), "version ID %q is not a valid v4 UUID", id)
	}
}

// TestBackend_ResetCleansAllMaps verifies Reset clears secrets + policies + replications.
func TestBackend_ResetCleansAllMaps(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "reset-s", SecretString: "v"},
	)
	require.NoError(t, err)
	_, err = b.PutResourcePolicy(context.Background(), &secretsmanager.PutResourcePolicyInput{
		SecretID:       "reset-s",
		ResourcePolicy: `{"Version":"2012-10-17","Statement":[]}`,
	})
	require.NoError(t, err)
	_, err = b.ReplicateSecretToRegions(context.Background(), &secretsmanager.ReplicateSecretToRegionsInput{
		SecretID:          "reset-s",
		AddReplicaRegions: []secretsmanager.ReplicaRegion{{Region: "us-west-2"}},
	})
	require.NoError(t, err)

	b.Reset()

	assert.Equal(t, 0, secretsmanager.SecretCount(b))
	assert.Equal(t, 0, secretsmanager.ResourcePolicyCount(b))
	assert.Equal(t, 0, secretsmanager.ReplicationConfigCount(b))
}

// TestBackend_RestoreEnsuresNonNilMaps verifies ensureNonNilMaps protects Restore from nil maps.
func TestBackend_RestoreEnsuresNonNilMaps(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	// Restore with minimal valid JSON that has no map keys.
	err := b.Restore(t.Context(), []byte(`{"accountID":"acct","region":"us-east-1"}`))
	require.NoError(t, err)
	// Should be able to create secrets without panics.
	_, err = b.CreateSecret(
		context.Background(),
		&secretsmanager.CreateSecretInput{Name: "post-restore", SecretString: "v"},
	)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// X-Amzn-Errortype header
// ---------------------------------------------------------------------------

// TestHandler_ErrorResponseAmznErrortypeHeader verifies that the X-Amzn-Errortype
// response header is set on error responses and matches the body __type, across
// the distinct AWS error categories. Real AWS (awsJson1_1) always emits this
// header and SDKs read it to construct the typed exception.
func TestHandler_ErrorResponseAmznErrortypeHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed     func(t *testing.T, b *secretsmanager.InMemoryBackend)
		name     string
		action   string
		body     string
		wantType string
	}{
		{
			name:     "not_found",
			action:   "secretsmanager.GetSecretValue",
			body:     `{"SecretId":"does-not-exist"}`,
			wantType: "ResourceNotFoundException",
		},
		{
			name:     "invalid_name",
			action:   "secretsmanager.CreateSecret",
			body:     `{"Name":"bad name with spaces!","SecretString":"v"}`,
			wantType: "InvalidParameterException",
		},
		{
			name:   "already_exists",
			action: "secretsmanager.CreateSecret",
			body:   `{"Name":"dup-secret","SecretString":"v"}`,
			seed: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
					Name:         "dup-secret",
					SecretString: "v",
				})
				require.NoError(t, err)
			},
			wantType: "ResourceExistsException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			h := secretsmanager.NewHandler(b)

			if tt.seed != nil {
				tt.seed(t, b)
			}

			rec := doR1Request(t, h, tt.action, tt.body)

			require.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)

			var errResp secretsmanager.ErrorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantType, errResp.Type, "body __type")
			assert.Equal(t, tt.wantType, rec.Header().Get("X-Amzn-Errortype"),
				"X-Amzn-Errortype header must equal body __type")
		})
	}
}
