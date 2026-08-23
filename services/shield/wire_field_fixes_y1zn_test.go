package shield_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// TestDescribeProtectionGroup_NoCreationTimeKey covers gopherstack-y1zn.
// protectionGroupToMap emitted "CreationTime"; types.ProtectionGroup
// (shield@v1.37.4 types/types.go) has no such member.
func TestDescribeProtectionGroup_NoCreationTimeKey(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	pg, err := b.CreateProtectionGroup("y1zn-group", shield.AggregationSum, shield.PatternAll, "", nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DescribeProtectionGroup", map[string]any{
		"ProtectionGroupId": pg.ID,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, `"CreationTime"`,
		"types.ProtectionGroup has no CreationTime member")
}

// TestDescribeSubscription_NoMaxProtectionsKey covers gopherstack-y1zn.
// subscriptionLimits emitted "MaxProtections" inside ProtectionLimits;
// types.ProtectionLimits (shield@v1.37.4 types/types.go) declares only
// ProtectedResourceTypeLimits.
func TestDescribeSubscription_NoMaxProtectionsKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.NoError(t, h.Backend.CreateSubscription())

	rec := doShieldRequest(t, h, "DescribeSubscription", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, `"MaxProtections"`,
		"types.ProtectionLimits has no MaxProtections member")
	assert.Contains(t, body, `"ProtectedResourceTypeLimits"`,
		"types.ProtectionLimits's real member is ProtectedResourceTypeLimits")
}
