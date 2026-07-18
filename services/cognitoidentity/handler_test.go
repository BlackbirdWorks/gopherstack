package cognitoidentity_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidentity"
)

func TestHandler_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")
	h := cognitoidentity.NewHandler(b, "us-east-1")

	_, err := b.CreateIdentityPool(
		context.Background(),
		"handler-persist-pool",
		true,
		false,
		"",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := cognitoidentity.NewHandler(b2, "us-east-1")

	require.NoError(t, h2.Restore(t.Context(), snap))

	pools, _ := b2.ListIdentityPools(context.Background(), 0, "")
	assert.Len(t, pools, 1)
	assert.Equal(t, "handler-persist-pool", pools[0].IdentityPoolName)
}

func newTestHandler(t *testing.T) *cognitoidentity.Handler {
	t.Helper()

	backend := cognitoidentity.NewInMemoryBackend("000000000000", "us-east-1")

	return cognitoidentity.NewHandler(backend, "us-east-1")
}

func doCognitoIdentityRequest(
	t *testing.T,
	h *cognitoidentity.Handler,
	action string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityService."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "CognitoIdentity", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"CreateIdentityPool", "DeleteIdentityPool", "DescribeIdentityPool",
		"ListIdentityPools", "UpdateIdentityPool",
		"GetId", "GetCredentialsForIdentity", "GetOpenIdToken",
		"SetIdentityPoolRoles", "GetIdentityPoolRoles",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matching_target",
			target: "AWSCognitoIdentityService.CreateIdentityPool",
			want:   true,
		},
		{
			name:   "non_matching_target",
			target: "AWSCognitoIdentityProviderService.CreateUserPool",
			want:   false,
		},
		{
			name:   "empty_target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			matcher := h.RouteMatcher()
			got := matcher(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_ChaosProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "cognito-identity", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoIdentityRequest(t, h, "NonExistentAction", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "valid_action",
			target: "AWSCognitoIdentityService.CreateIdentityPool",
			want:   "CreateIdentityPool",
		},
		{
			name:   "empty_target",
			target: "",
			want:   "Unknown",
		},
		{
			name:   "no_prefix",
			target: "SomeOtherService.SomeAction",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := h.ExtractOperation(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "with_identity_pool_id",
			body: `{"IdentityPoolId":"us-east-1:abc123"}`,
			want: "us-east-1:abc123",
		},
		{
			name: "with_identity_id",
			body: `{"IdentityId":"us-east-1:ident456"}`,
			want: "us-east-1:ident456",
		},
		{
			name: "empty_body",
			body: `{}`,
			want: "",
		},
		{
			name: "invalid_json",
			body: `not-json`,
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(tt.body))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			got := h.ExtractResource(c)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_InvalidJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`not-valid-json`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityService.CreateIdentityPool")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetSupportedOperations_IncludesExtendedOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	newOps := []string{
		"DeleteIdentities",
		"DescribeIdentity",
		"GetOpenIdTokenForDeveloperIdentity",
		"GetPrincipalTagAttributeMap",
		"ListIdentities",
		"ListTagsForResource",
		"LookupDeveloperIdentity",
		"MergeDeveloperIdentities",
		"SetPrincipalTagAttributeMap",
		"TagResource",
		"UnlinkDeveloperIdentity",
		"UnlinkIdentity",
		"UntagResource",
	}

	for _, op := range newOps {
		assert.Contains(t, ops, op)
	}
}

func TestReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a pool.
	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "reset-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Verify it exists.
	assert.Equal(t, 1, h.Backend.PoolCount())

	// Reset.
	h.Reset()

	// Should be empty.
	assert.Equal(t, 0, h.Backend.PoolCount())
	assert.Equal(t, 0, h.Backend.IdentityCount())
}

func TestNonNilSlices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// ListIdentityPools with no pools should return [] not null.
	listPoolsRec := doCognitoIdentityRequest(t, h, "ListIdentityPools", map[string]any{
		"MaxResults": 0,
	})
	require.Equal(t, http.StatusOK, listPoolsRec.Code)

	var listPoolsOut map[string]any
	require.NoError(t, json.Unmarshal(listPoolsRec.Body.Bytes(), &listPoolsOut))
	pools, poolsOK := listPoolsOut["IdentityPools"].([]any)
	require.True(t, poolsOK, "IdentityPools should be a non-null array")
	assert.Empty(t, pools)

	// Create a pool then ListIdentities with no identities returns [] not null.
	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "non-nil-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))

	listIDsRec := doCognitoIdentityRequest(t, h, "ListIdentities", map[string]any{
		"IdentityPoolId": created["IdentityPoolId"],
		"MaxResults":     10,
	})
	require.Equal(t, http.StatusOK, listIDsRec.Code)

	var listIDsOut map[string]any
	require.NoError(t, json.Unmarshal(listIDsRec.Body.Bytes(), &listIDsOut))
	identities, idsOK := listIDsOut["Identities"].([]any)
	require.True(t, idsOK, "Identities should be a non-null array")
	assert.Empty(t, identities)
}

func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Seed a pool directly.
	pool := &cognitoidentity.IdentityPool{
		IdentityPoolID:                 "us-east-1:seed-pool-id",
		IdentityPoolName:               "seeded-pool",
		ARN:                            "arn:aws:cognito-identity:us-east-1:000000000000:identitypool/us-east-1:seed-pool-id",
		AllowUnauthenticatedIdentities: true,
	}
	h.Backend.AddPoolInternal(pool)

	assert.Equal(t, 1, h.Backend.PoolCount())

	// Verify it's accessible.
	descRec := doCognitoIdentityRequest(t, h, "DescribeIdentityPool", map[string]any{
		"IdentityPoolId": "us-east-1:seed-pool-id",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, "seeded-pool", descOut["IdentityPoolName"])

	// Seed an identity.
	identity := &cognitoidentity.Identity{
		IdentityID:     "us-east-1:seed-identity-id",
		IdentityPoolID: "us-east-1:seed-pool-id",
	}
	h.Backend.AddIdentityInternal(identity)

	assert.Equal(t, 1, h.Backend.IdentityCount())
}

func TestCountHelpers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, 0, h.Backend.PoolCount())
	assert.Equal(t, 0, h.Backend.IdentityCount())
	assert.Equal(t, 0, h.Backend.PrincipalTagCount())

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "count-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	assert.Equal(t, 1, h.Backend.PoolCount())

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)

	doCognitoIdentityRequest(t, h, "GetId", map[string]any{
		"AccountId":      "000000000000",
		"IdentityPoolId": poolID,
	})

	assert.Equal(t, 1, h.Backend.IdentityCount())

	doCognitoIdentityRequest(t, h, "SetPrincipalTagAttributeMap", map[string]any{
		"IdentityPoolId":       poolID,
		"IdentityProviderName": "cognito-idp.us-east-1.amazonaws.com/us-east-1_Test",
		"UseDefaults":          false,
	})

	assert.Equal(t, 1, h.Backend.PrincipalTagCount())
}

func TestOpsPreBuiltAndIndependent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// The ops map should be pre-built - verify by counting supported operations.
	ops := h.GetSupportedOperations()
	assert.GreaterOrEqual(t, len(ops), 21)

	// Two handler instances should each dispatch correctly (no shared mutable state).
	h2 := newTestHandler(t)
	rec1 := doCognitoIdentityRequest(t, h, "ListIdentityPools", map[string]any{"MaxResults": 0})
	rec2 := doCognitoIdentityRequest(t, h2, "ListIdentityPools", map[string]any{"MaxResults": 0})

	assert.Equal(t, http.StatusOK, rec1.Code)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestPersistenceRoundTripWithPrincipalTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "persist-pt-pool",
		"AllowUnauthenticatedIdentities": true,
		"IdentityPoolTags":               map[string]string{"env": "prod"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	poolID := created["IdentityPoolId"].(string)

	setRec := doCognitoIdentityRequest(t, h, "SetPrincipalTagAttributeMap", map[string]any{
		"IdentityPoolId":       poolID,
		"IdentityProviderName": "cognito-idp.us-east-1.amazonaws.com/us-east-1_P",
		"UseDefaults":          false,
		"PrincipalTags":        map[string]string{"sub": "user_id"},
	})
	require.Equal(t, http.StatusOK, setRec.Code)

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, h2.Backend.PoolCount())
	assert.Equal(t, 1, h2.Backend.PrincipalTagCount())

	getRec := doCognitoIdentityRequest(t, h2, "GetPrincipalTagAttributeMap", map[string]any{
		"IdentityPoolId":       poolID,
		"IdentityProviderName": "cognito-idp.us-east-1.amazonaws.com/us-east-1_P",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))

	tags, _ := getOut["PrincipalTags"].(map[string]any)
	assert.Equal(t, "user_id", tags["sub"])
}

func TestMultipleResetCycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for range 3 {
		doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
			"IdentityPoolName":               "reset-cycle-pool",
			"AllowUnauthenticatedIdentities": true,
		})
	}

	// First pool created; second fails due to name conflict.
	assert.Equal(t, 1, h.Backend.PoolCount())

	h.Reset()
	assert.Equal(t, 0, h.Backend.PoolCount())

	// Can create again after reset.
	rec := doCognitoIdentityRequest(t, h, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "reset-cycle-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, h.Backend.PoolCount())
}
