package shield_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wireErrorType decodes the __type field from an error response body.
func wireErrorType(t *testing.T, body []byte) string {
	t.Helper()

	var env struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(body, &env))

	return env.Type
}

// TestHandler_ErrorWireType_SubscriptionRequired verifies that an operation requiring an active
// Shield Advanced subscription reports the real Shield "InvalidOperationException" __type, not
// "ResourceAlreadyExistsException". ErrSubscriptionRequired wraps awserr.ErrConflict internally
// for backward-compatible errors.Is matching, which previously caused handleError's generic
// ErrConflict rule to shadow it and misreport the wire type.
func TestHandler_ErrorWireType_SubscriptionRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doShieldRequest(t, h, "CreateProtection", map[string]any{
		"Name":        "prot",
		"ResourceArn": eipARN("1"),
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "InvalidOperationException", wireErrorType(t, rec.Body.Bytes()))
}

// TestHandler_ErrorWireType_InvalidPaginationToken verifies a malformed NextToken reports the
// real Shield "InvalidPaginationTokenException" __type at 400, not a 500 InternalErrorException.
func TestHandler_ErrorWireType_InvalidPaginationToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.NoError(t, h.Backend.CreateSubscription())

	rec := doShieldRequest(t, h, "ListProtections", map[string]any{
		"NextToken": "not-valid-base64url!!!",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "InvalidPaginationTokenException", wireErrorType(t, rec.Body.Bytes()))
}

// TestHandler_ErrorWireType_LimitsExceeded verifies a quota violation reports the real Shield
// "LimitsExceededException" __type.
func TestHandler_ErrorWireType_LimitsExceeded(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.NoError(t, h.Backend.CreateSubscription())

	const maxPerType = 100

	for i := range maxPerType {
		rec := doShieldRequest(t, h, "CreateProtection", map[string]any{
			"Name":        fmt.Sprintf("prot-%d", i),
			"ResourceArn": eipARN(strconv.Itoa(i)),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doShieldRequest(t, h, "CreateProtection", map[string]any{
		"Name":        "one-too-many",
		"ResourceArn": "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-over-the-limit",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "LimitsExceededException", wireErrorType(t, rec.Body.Bytes()))
}

// TestHandler_ErrorWireType_NoAssociatedRole verifies AssociateDRTLogBucket without a prior
// AssociateDRTRole reports the real Shield "NoAssociatedRoleException" __type.
func TestHandler_ErrorWireType_NoAssociatedRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.NoError(t, h.Backend.CreateSubscription())

	rec := doShieldRequest(t, h, "AssociateDRTLogBucket", map[string]any{"LogBucket": "my-bucket"})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "NoAssociatedRoleException", wireErrorType(t, rec.Body.Bytes()))
}
