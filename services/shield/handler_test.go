package shield_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/shield"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

// eipARN returns a test EIP allocation ARN.
func eipARN(id string) string {
	return "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-" + id
}

// albARN returns a test Application Load Balancer ARN.
func albARN(id string) string {
	return "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/" + id + "/abc123"
}

// cfARN returns a test CloudFront distribution ARN.
func cfARN(id string) string {
	return "arn:aws:cloudfront::000000000000:distribution/" + id
}

// r53ARN returns a test Route 53 hosted zone ARN.
func r53ARN(id string) string {
	return "arn:aws:route53:::hostedzone/" + id
}

func newTestHandler(t *testing.T) *shield.Handler {
	t.Helper()

	return shield.NewHandler(shield.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doShieldRequest(
	t *testing.T,
	h *shield.Handler,
	target string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSShield_20160616."+target)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// opaqueOffset decodes a base64url opaque token and returns the embedded offset.
func opaqueOffset(t *testing.T, token string) int {
	t.Helper()

	raw, err := base64.RawURLEncoding.DecodeString(token)
	require.NoError(t, err, "token must be valid base64url")

	var obj struct {
		O int `json:"o"`
	}

	require.NoError(t, json.Unmarshal(raw, &obj), "token must be JSON with 'o' key")

	return obj.O
}

func newSubscribedHandler(t *testing.T) (*shield.Handler, *shield.InMemoryBackend) {
	t.Helper()

	b := shield.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddSubscriptionInternal()

	return shield.NewHandler(b), b
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Shield", h.Name())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{name: "matching target", target: "AWSShield_20160616.GetSubscriptionState", want: true},
		{name: "non-matching target", target: "SageMaker.ListModels", want: false},
		{name: "empty target", target: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AWSShield_20160616.CreateProtection")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "CreateProtection", h.ExtractResource(c))
}

func TestProvider_InitAndName(t *testing.T) {
	t.Parallel()

	p := &shield.Provider{}
	assert.Equal(t, "Shield", p.Name())

	h, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, h)
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateSubscription")
	assert.Contains(t, ops, "DescribeSubscription")
	assert.Contains(t, ops, "GetSubscriptionState")
	assert.Contains(t, ops, "CreateProtection")
	assert.Contains(t, ops, "DescribeProtection")
	assert.Contains(t, ops, "DeleteProtection")
	assert.Contains(t, ops, "ListProtections")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "ListTagsForResource")
	assert.Contains(t, ops, "UntagResource")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "shield", h.ChaosServiceName())
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doShieldRequest(t, h, "UnknownOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
	assert.Equal(t, "shield", h.ChaosServiceName())
}

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

// TestHandler_GetSupportedOperationsNewOps verifies new ops are in supported list.
func TestHandler_GetSupportedOperationsIncludesAllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{
		"AssociateDRTLogBucket",
		"AssociateDRTRole",
		"AssociateHealthCheck",
		"AssociateProactiveEngagementDetails",
		"CreateProtectionGroup",
		"DeleteProtectionGroup",
		"DeleteSubscription",
		"DescribeAttack",
		"DescribeAttackStatistics",
		"DescribeDRTAccess",
	} {
		assert.Contains(t, ops, op, "missing op: %s", op)
	}
}

// TestRefinement1_HandlerOpsLen verifies 37 operations are supported.
func TestHandler_OpsLen(t *testing.T) {
	t.Parallel()

	h := shield.NewHandler(shield.NewInMemoryBackend("000000000000", "us-east-1"))
	assert.Equal(t, 37, shield.HandlerOpsLen(h))
}

// TestRefinement1_AccountID verifies AccountID returns the configured value.
func TestBackend_AccountID(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, "000000000000", b.AccountID())
}

// TestRefinement1_Region verifies Region returns the configured value.
func TestBackend_RegionDefault(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, "us-east-1", b.Region())
}

// TestRefinement1_ErrNilAppContext verifies the nil guard in provider.
func TestProvider_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &shield.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, shield.ErrNilAppContext)
}

// TestRefinement1_Reset verifies Reset clears all state.
func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddProtectionInternal("my-prot", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-123")
	assert.Equal(t, 1, shield.ProtectionCount(b))

	b.Reset()
	assert.Equal(t, 0, shield.ProtectionCount(b))
}

// TestRefinement1_HandlerReset verifies Handler.Reset delegates to backend.
func TestHandler_ResetDelegates(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	h := shield.NewHandler(b)
	b.AddProtectionInternal("my-prot", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-123")

	h.Reset()
	assert.Equal(t, 0, shield.ProtectionCount(b))
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestStorageBackendInterfaceCompiles(t *testing.T) {
	t.Parallel()

	var _ shield.StorageBackend = (*shield.InMemoryBackend)(nil)
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestHandler_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	h := shield.NewHandler(shield.NewInMemoryBackend("000000000000", "us-east-1"))
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

// TestRefinement1_ProviderInit tests Provider Init with valid context.
func TestProvider_InitReturnsRegisterable(t *testing.T) {
	t.Parallel()

	p := &shield.Provider{}
	reg, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestAudit_Gap23_ResetClearsAllState verifies Reset clears all fields.
func TestInMemoryBackend_ResetClearsAllState(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	b.AddProtectionInternal("prot", eipARN("1"))
	b.AddAttackInternal("atk-1", eipARN("1"))

	b.Reset()

	assert.Equal(t, 0, shield.ProtectionCount(b))
	assert.Equal(t, 0, shield.AttackCount(b))
	assert.Equal(t, "INACTIVE", b.GetSubscriptionState())
}

// --- Gap 24: ListResourcesInProtectionGroup BY_RESOURCE_TYPE ---
