package kms_test

// isolation_test.go — verifies that KMS region isolation works correctly:
// keys and aliases created in one region are not visible in another.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// kmsCall makes a KMS HTTP request with the given action, body, and region header.
func kmsCall(t *testing.T, h *kms.Handler, e *echo.Echo, action, region, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "TrentService."+action)
	req.Header.Set("X-Amz-Region", region)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

// TestRegionIsolation_AliasInTwoRegions verifies that the same alias name can
// exist independently in two different regions.
func TestRegionIsolation_AliasInTwoRegions(t *testing.T) {
	t.Parallel()

	e := echo.New()
	b := kms.NewInMemoryBackend()
	h := kms.NewHandler(b)

	// Create a key in us-east-1.
	rec := kmsCall(t, h, e, "CreateKey", "us-east-1", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var east1Key kms.CreateKeyOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &east1Key))
	east1KeyID := east1Key.KeyMetadata.KeyID

	// Create alias "alias/my-key" in us-east-1.
	aliasBody, _ := json.Marshal(map[string]string{
		"AliasName":   "alias/my-key",
		"TargetKeyId": east1KeyID,
	})
	rec = kmsCall(t, h, e, "CreateAlias", "us-east-1", string(aliasBody))
	require.Equal(t, http.StatusOK, rec.Code)

	// Create a key in us-west-2.
	rec = kmsCall(t, h, e, "CreateKey", "us-west-2", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var west2Key kms.CreateKeyOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &west2Key))
	west2KeyID := west2Key.KeyMetadata.KeyID

	// Create same alias "alias/my-key" in us-west-2 — should succeed (different region).
	aliasBody2, _ := json.Marshal(map[string]string{
		"AliasName":   "alias/my-key",
		"TargetKeyId": west2KeyID,
	})
	rec = kmsCall(t, h, e, "CreateAlias", "us-west-2", string(aliasBody2))
	assert.Equal(t, http.StatusOK, rec.Code, "same alias name should succeed in a different region")
}

// TestRegionIsolation_ListKeys_RegionScoped verifies that ListKeys in one region
// does not return keys created in another region.
func TestRegionIsolation_ListKeys_RegionScoped(t *testing.T) {
	t.Parallel()

	e := echo.New()
	b := kms.NewInMemoryBackend()
	h := kms.NewHandler(b)

	// Create 3 keys in us-east-1.
	for range 3 {
		rec := kmsCall(t, h, e, "CreateKey", "us-east-1", `{}`)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Create 2 keys in us-west-2.
	for range 2 {
		rec := kmsCall(t, h, e, "CreateKey", "us-west-2", `{}`)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// ListKeys in us-east-1 should return only 3 keys.
	rec := kmsCall(t, h, e, "ListKeys", "us-east-1", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var east1List kms.ListKeysOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &east1List))
	assert.Len(t, east1List.Keys, 3, "us-east-1 should have 3 keys")

	// ListKeys in us-west-2 should return only 2 keys.
	rec = kmsCall(t, h, e, "ListKeys", "us-west-2", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var west2List kms.ListKeysOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &west2List))
	assert.Len(t, west2List.Keys, 2, "us-west-2 should have 2 keys")
}

// TestRegionIsolation_DescribeKey_CrossRegion verifies that an alias-based key
// lookup from a different region returns not found, and that using the key ARN
// (which embeds the region) succeeds from any region context.
func TestRegionIsolation_DescribeKey_CrossRegion(t *testing.T) {
	t.Parallel()

	e := echo.New()
	b := kms.NewInMemoryBackend()
	h := kms.NewHandler(b)

	// Create a key and alias in us-east-1.
	rec := kmsCall(t, h, e, "CreateKey", "us-east-1", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var east1Key kms.CreateKeyOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &east1Key))
	east1KeyARN := east1Key.KeyMetadata.Arn

	aliasBody, _ := json.Marshal(map[string]string{
		"AliasName":   "alias/region-test-key",
		"TargetKeyId": east1Key.KeyMetadata.KeyID,
	})
	rec = kmsCall(t, h, e, "CreateAlias", "us-east-1", string(aliasBody))
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeKey via alias in us-east-1 should succeed.
	descBody, _ := json.Marshal(map[string]string{"KeyId": "alias/region-test-key"})
	rec = kmsCall(t, h, e, "DescribeKey", "us-east-1", string(descBody))
	assert.Equal(t, http.StatusOK, rec.Code, "DescribeKey in same region via alias should succeed")

	// DescribeKey via alias from us-west-2 should fail — aliases are region-scoped.
	rec = kmsCall(t, h, e, "DescribeKey", "us-west-2", string(descBody))
	assert.Equal(t, http.StatusBadRequest, rec.Code, "DescribeKey via alias from wrong region should fail")

	// DescribeKey via ARN from us-west-2 should succeed — ARN encodes the region.
	arnBody, _ := json.Marshal(map[string]string{"KeyId": east1KeyARN})
	rec = kmsCall(t, h, e, "DescribeKey", "us-west-2", string(arnBody))
	assert.Equal(t, http.StatusOK, rec.Code, "DescribeKey via ARN should succeed from any region")
}

// TestRegionIsolation_AliasNotVisible_CrossRegion verifies that an alias
// created in us-east-1 is not visible when listing aliases from us-west-2.
func TestRegionIsolation_AliasNotVisible_CrossRegion(t *testing.T) {
	t.Parallel()

	e := echo.New()
	b := kms.NewInMemoryBackend()
	h := kms.NewHandler(b)

	// Create a key and alias in us-east-1.
	rec := kmsCall(t, h, e, "CreateKey", "us-east-1", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var keyOut kms.CreateKeyOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &keyOut))

	aliasBody, _ := json.Marshal(map[string]string{
		"AliasName":   "alias/east-only",
		"TargetKeyId": keyOut.KeyMetadata.KeyID,
	})
	rec = kmsCall(t, h, e, "CreateAlias", "us-east-1", string(aliasBody))
	require.Equal(t, http.StatusOK, rec.Code)

	// ListAliases in us-west-2 should not see "alias/east-only".
	rec = kmsCall(t, h, e, "ListAliases", "us-west-2", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var aliasList kms.ListAliasesOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &aliasList))

	for _, a := range aliasList.Aliases {
		assert.NotEqual(t, "alias/east-only", a.AliasName,
			"alias created in us-east-1 must not appear in us-west-2 ListAliases")
	}
}
