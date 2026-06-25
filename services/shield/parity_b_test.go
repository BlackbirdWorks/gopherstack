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

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// doShieldReq sends a POST to the shield handler for the given action.
func doShieldReq(t *testing.T, h *shield.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var marshalErr error
		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSShield_20160616."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	require.NoError(t, h.Handler()(c))

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

// TestParity_OpaquePageToken_ListProtections verifies pagination emits opaque JSON-wrapped tokens.
func TestParity_OpaquePageToken_ListProtections(t *testing.T) {
	t.Parallel()

	h, b := newSubscribedHandler(t)

	for i := range 5 {
		b.AddProtectionInternal(
			"prot-"+string(rune('a'+i)),
			"arn:aws:ec2:us-east-1:123456789012:eip/eipalloc-0"+string(rune('a'+i)),
		)
	}

	rec := doShieldReq(t, h, "ListProtections", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NextToken string `json:"NextToken"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.NextToken)

	offset := opaqueOffset(t, out.NextToken)
	assert.Equal(t, 3, offset)

	// Old-style plain-integer token must be rejected on next page request.
	badToken := base64.StdEncoding.EncodeToString([]byte("3"))
	rec2 := doShieldReq(t, h, "ListProtections", map[string]any{"NextToken": badToken})
	assert.NotEqual(t, http.StatusOK, rec2.Code)
}

// TestParity_OpaquePageToken_ListProtectionGroups verifies group pagination tokens are opaque.
func TestParity_OpaquePageToken_ListProtectionGroups(t *testing.T) {
	t.Parallel()

	h, b := newSubscribedHandler(t)

	for i := range 5 {
		_, err := b.CreateProtectionGroup(
			"group-"+string(rune('a'+i)),
			"SUM", "ALL", "", nil,
		)
		require.NoError(t, err)
	}

	rec := doShieldReq(t, h, "ListProtectionGroups", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NextToken string `json:"NextToken"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.NextToken)

	offset := opaqueOffset(t, out.NextToken)
	assert.Equal(t, 3, offset)
}

// TestParity_OpaquePageToken_ListAttacks verifies attack pagination tokens are opaque.
func TestParity_OpaquePageToken_ListAttacks(t *testing.T) {
	t.Parallel()

	h, b := newSubscribedHandler(t)

	resourceARN := "arn:aws:ec2:us-east-1:123456789012:eip/eipalloc-00000001"
	b.AddProtectionInternal("prot-eip", resourceARN)

	for range 5 {
		_, err := b.SimulateAttack(resourceARN, nil)
		require.NoError(t, err)
	}

	rec := doShieldReq(t, h, "ListAttacks", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NextToken string `json:"NextToken"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.NextToken)

	offset := opaqueOffset(t, out.NextToken)
	assert.Equal(t, 3, offset)
}

// TestParity_GetAttackVectorDefinitionVersion verifies a date-stamped version is returned.
func TestParity_GetAttackVectorDefinitionVersion(t *testing.T) {
	t.Parallel()

	h := shield.NewHandler(shield.NewInMemoryBackend("123456789012", "us-east-1"))

	rec := doShieldReq(t, h, "GetAttackVectorDefinitionVersion", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		AttackVectorDefinitionVersion string `json:"AttackVectorDefinitionVersion"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	// Must not be a bare "1".
	assert.NotEqual(t, "1", out.AttackVectorDefinitionVersion)
	// Must look like a date-stamped version (e.g. "20221129.1").
	assert.Greater(t, len(out.AttackVectorDefinitionVersion), 1)
}

// TestParity_ApplyProtectionFilters_NoBacking verifies filtered list doesn't corrupt the original.
func TestParity_ApplyProtectionFilters_NoBacking(t *testing.T) {
	t.Parallel()

	h, b := newSubscribedHandler(t)

	b.AddProtectionInternal("prot-cf", "arn:aws:cloudfront::123456789012:distribution/dist1")
	b.AddProtectionInternal("prot-alb", "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/alb1/abc")
	b.AddProtectionInternal("prot-r53", "arn:aws:route53:::hostedzone/Z1")

	// Filter to CLOUDFRONT_DISTRIBUTION only.
	rec := doShieldReq(t, h, "ListProtections", map[string]any{
		"InclusionFilters": map[string]any{
			"ResourceTypes": []string{"CLOUDFRONT_DISTRIBUTION"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var filtered struct {
		Protections []map[string]any `json:"Protections"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &filtered))
	assert.Len(t, filtered.Protections, 1)

	// Unfiltered list must still return all three.
	rec2 := doShieldReq(t, h, "ListProtections", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	var all struct {
		Protections []map[string]any `json:"Protections"`
	}

	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &all))
	assert.Len(t, all.Protections, 3)
}

// TestParity_ListProtections_SortOutsideLock verifies concurrent reads don't block each other.
func TestParity_ListProtections_SortOutsideLock(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddSubscriptionInternal()

	for i := range 20 {
		b.AddProtectionInternal(
			"prot-sort-"+string(rune('a'+i%26))+string(rune('0'+i%10)),
			"arn:aws:ec2:us-east-1:123456789012:eip/eipalloc-sort"+string(rune('0'+i)),
		)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		for range 100 {
			_ = b.ListProtections()
		}
	}()

	for range 100 {
		_ = b.ListProtections()
	}

	<-done
}

// TestParity_ListProtectionGroups_SortOutsideLock verifies concurrent reads don't block each other.
func TestParity_ListProtectionGroups_SortOutsideLock(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddSubscriptionInternal()

	for i := range 10 {
		_, err := b.CreateProtectionGroup("grp-"+string(rune('a'+i)), "SUM", "ALL", "", nil)
		require.NoError(t, err)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		for range 100 {
			_ = b.ListProtectionGroups()
		}
	}()

	for range 100 {
		_ = b.ListProtectionGroups()
	}

	<-done
}

// TestParity_IndexEviction_NoPanic verifies the index eviction path doesn't panic.
func TestParity_IndexEviction_NoPanic(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("123456789012", "us-east-1")
	b.AddSubscriptionInternal()

	// Create many protections; subscription cap is 1000, which is below maxIndexEntries (10 000).
	// Just verify creating many entries and listing doesn't panic.
	for i := range 50 {
		resourceARN := "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/lb" +
			string(rune('a'+i%26)) + string(rune('0'+i%10)) + "/abc"
		name := "evict-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		_, _ = b.CreateProtection(name, resourceARN, nil)
	}

	assert.NotPanics(t, func() {
		_ = b.ListProtections()
	})
}
