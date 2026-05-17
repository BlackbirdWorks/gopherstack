package wafv2_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

// ---- Name pattern validation -------------------------------------------------

func TestValidation_ResourceNamePattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		aclName    string
		wantStatus int
	}{
		{name: "valid_simple", aclName: "my-acl", wantStatus: http.StatusOK},
		{name: "valid_with_underscores", aclName: "My_ACL_01", wantStatus: http.StatusOK},
		{name: "valid_alphanumeric", aclName: "MyACL123", wantStatus: http.StatusOK},
		{name: "invalid_starts_with_dash", aclName: "-invalid", wantStatus: http.StatusBadRequest},
		{name: "invalid_has_space", aclName: "my acl", wantStatus: http.StatusBadRequest},
		{name: "invalid_has_dot", aclName: "my.acl", wantStatus: http.StatusBadRequest},
		{name: "invalid_has_slash", aclName: "my/acl", wantStatus: http.StatusBadRequest},
		{name: "invalid_starts_with_underscore", aclName: "_myacl", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
				"Name":          tt.aclName,
				"Scope":         "REGIONAL",
				"DefaultAction": map[string]any{"Allow": map[string]any{}},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "aclName=%q body=%s", tt.aclName, rec.Body.String())
		})
	}
}

// ---- Description max-length validation ---------------------------------------

func TestValidation_DescriptionMaxLength(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Exactly 256 characters — should succeed.
	desc256 := strings.Repeat("a", 256)
	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-desc-ok",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"Description":   desc256,
	})
	assert.Equal(t, http.StatusOK, rec.Code, "256-char description should be accepted: %s", rec.Body.String())

	// 257 characters — should fail.
	desc257 := strings.Repeat("a", 257)
	rec = doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-desc-too-long",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"Description":   desc257,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "257-char description should be rejected: %s", rec.Body.String())
}

// ---- Rule Name uniqueness enforcement ----------------------------------------

func TestValidation_RuleNameUniqueness(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	dupRules := []map[string]any{
		{
			"Name":     "dupe-rule",
			"Priority": 1,
			"Statement": map[string]any{
				"IPSetReferenceStatement": map[string]any{
					"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/x/abc",
				},
			},
			"Action": map[string]any{"Allow": map[string]any{}},
			"VisibilityConfig": map[string]any{
				"MetricName":               "dupe-rule",
				"SampledRequestsEnabled":   false,
				"CloudWatchMetricsEnabled": false,
			},
		},
		{
			"Name":     "dupe-rule", // same name → should fail
			"Priority": 2,
			"Statement": map[string]any{
				"IPSetReferenceStatement": map[string]any{
					"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/x/abc",
				},
			},
			"Action": map[string]any{"Block": map[string]any{}},
			"VisibilityConfig": map[string]any{
				"MetricName":               "dupe-rule",
				"SampledRequestsEnabled":   false,
				"CloudWatchMetricsEnabled": false,
			},
		},
	}

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "acl-dup-rule-name",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"Rules":         dupRules,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "duplicate rule names should be rejected")

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFInvalidParameterException", errResp["__type"])
}

// ---- Rule Priority range enforcement ----------------------------------------

func TestValidation_RulePriorityRange(t *testing.T) {
	t.Parallel()

	makeRule := func(name string, priority int) map[string]any {
		return map[string]any{
			"Name":     name,
			"Priority": priority,
			"Statement": map[string]any{
				"IPSetReferenceStatement": map[string]any{
					"ARN": "arn:aws:wafv2:us-east-1:000000000000:regional/ipset/x/abc",
				},
			},
			"Action": map[string]any{"Allow": map[string]any{}},
			"VisibilityConfig": map[string]any{
				"MetricName":               name,
				"SampledRequestsEnabled":   false,
				"CloudWatchMetricsEnabled": false,
			},
		}
	}

	tests := []struct {
		name       string
		priority   int
		wantStatus int
	}{
		{name: "priority_0", priority: 0, wantStatus: http.StatusOK},
		{name: "priority_1000", priority: 1000, wantStatus: http.StatusOK},
		{name: "priority_1001", priority: 1001, wantStatus: http.StatusBadRequest},
		{name: "priority_negative", priority: -1, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
				"Name":          "acl-priority-" + tt.name,
				"Scope":         "REGIONAL",
				"DefaultAction": map[string]any{"Allow": map[string]any{}},
				"Rules":         []map[string]any{makeRule("r", tt.priority)},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "priority=%d body=%s", tt.priority, rec.Body.String())
		})
	}
}

// ---- Scope mismatch on GetWebACL / GetIPSet ----------------------------------

func TestValidation_ScopeMismatch(t *testing.T) {
	t.Parallel()

	t.Run("WebACL_regional_get_with_cloudfront_scope", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		// Create a REGIONAL WebACL, then attempt to retrieve it as CLOUDFRONT.
		id, _ := createWebACLHelper(t, h, "regional-acl", "REGIONAL")

		rec := doWafv2Request(t, h, "GetWebACL", map[string]any{
			"Id":    id,
			"Scope": "CLOUDFRONT",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code, "scope mismatch should return 400")

		var errResp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
	})

	t.Run("WebACL_cloudfront_scope_create_and_get", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		// Create a CLOUDFRONT WebACL — uses the scope param with a non-REGIONAL value.
		id, _ := createWebACLHelper(t, h, "cf-acl", "CLOUDFRONT")
		require.NotEmpty(t, id)

		// Retrieve with matching scope — should succeed.
		rec := doWafv2Request(t, h, "GetWebACL", map[string]any{
			"Id":    id,
			"Scope": "CLOUDFRONT",
		})
		assert.Equal(t, http.StatusOK, rec.Code, "CLOUDFRONT WebACL should be retrievable with CLOUDFRONT scope")

		// Retrieve with wrong scope — should fail.
		rec = doWafv2Request(t, h, "GetWebACL", map[string]any{
			"Id":    id,
			"Scope": "REGIONAL",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code, "CLOUDFRONT WebACL with REGIONAL scope should fail")
	})

	t.Run("IPSet_scope_mismatch", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		id, _ := createIPSetHelper2(t, h, "my-ipset", "REGIONAL", "IPV4", nil)

		rec := doWafv2Request(t, h, "GetIPSet", map[string]any{
			"Id":    id,
			"Scope": "CLOUDFRONT",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code, "scope mismatch should return 400")
	})
}

// ---- DisassociateWebACL idempotency -----------------------------------------

func TestDisassociateWebACL_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Call DisassociateWebACL on a resource that was never associated.
	// AWS returns success (no-op).
	rec := doWafv2Request(t, h, "DisassociateWebACL", map[string]any{
		"ResourceArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/test/abc",
	})
	assert.Equal(t, http.StatusOK, rec.Code, "DisassociateWebACL should be idempotent: %s", rec.Body.String())

	// Call again to confirm repeated calls also succeed.
	rec = doWafv2Request(t, h, "DisassociateWebACL", map[string]any{
		"ResourceArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/test/abc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- DisassociateWebACL removes the association ------------------------------

func TestDisassociateWebACL_RemovesAssociation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a WebACL and associate it with a resource.
	_, webACLARN := createWebACLHelper(t, h, "assoc-acl", "REGIONAL")
	resourceARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"

	rec := doWafv2Request(t, h, "AssociateWebACL", map[string]any{
		"WebACLArn":   webACLARN,
		"ResourceArn": resourceARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify association exists.
	rec = doWafv2Request(t, h, "GetWebACLForResource", map[string]any{"ResourceArn": resourceARN})
	require.Equal(t, http.StatusOK, rec.Code)

	// Disassociate.
	rec = doWafv2Request(t, h, "DisassociateWebACL", map[string]any{"ResourceArn": resourceARN})
	require.Equal(t, http.StatusOK, rec.Code)

	// Now GetWebACLForResource should return not found.
	rec = doWafv2Request(t, h, "GetWebACLForResource", map[string]any{"ResourceArn": resourceARN})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "after disassociation, resource should have no WebACL")
}

// ---- DeleteWebACL fails when associated to a resource ------------------------

func TestDeleteWebACL_FailsWhenAssociated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	id, webACLARN := createWebACLHelper(t, h, "protected-acl", "REGIONAL")
	resourceARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"

	rec := doWafv2Request(t, h, "AssociateWebACL", map[string]any{
		"WebACLArn":   webACLARN,
		"ResourceArn": resourceARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Attempt delete while associated — should fail with WAFAssociatedItemException.
	rec = doWafv2Request(t, h, "DeleteWebACL", map[string]any{
		"Id":    id,
		"Scope": "REGIONAL",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFAssociatedItemException", errResp["__type"])

	// After disassociation, delete should succeed.
	rec = doWafv2Request(t, h, "DisassociateWebACL", map[string]any{"ResourceArn": resourceARN})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doWafv2Request(t, h, "DeleteWebACL", map[string]any{
		"Id":    id,
		"Scope": "REGIONAL",
	})
	assert.Equal(t, http.StatusOK, rec.Code, "delete after disassociation should succeed: %s", rec.Body.String())
}

// ---- UpdateIPSet with empty addresses (clear) --------------------------------

func TestUpdateIPSet_ClearAddresses(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	id, lockToken := createIPSetHelper2(t, h, "my-ipset", "REGIONAL", "IPV4", []string{"10.0.0.0/8"})

	// Update with empty addresses list should clear addresses (not be ignored).
	rec := doWafv2Request(t, h, "UpdateIPSet", map[string]any{
		"Id":        id,
		"LockToken": lockToken,
		"Addresses": []string{},
	})
	require.Equal(t, http.StatusOK, rec.Code, "UpdateIPSet with empty addresses: %s", rec.Body.String())

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	newLockToken := updateResp["NextLockToken"].(string)

	// Verify addresses were cleared.
	rec = doWafv2Request(t, h, "GetIPSet", map[string]any{"Id": id, "Scope": "REGIONAL"})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	ipSet := getResp["IPSet"].(map[string]any)
	addresses, _ := ipSet["Addresses"].([]any)
	assert.Empty(t, addresses, "addresses should be empty after clearing")

	_ = newLockToken
}

// ---- AssociateWebACL is idempotent (second call replaces) --------------------

func TestAssociateWebACL_ReplaceExisting(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, aclARN1 := createWebACLHelper(t, h, "acl-one", "REGIONAL")
	_, aclARN2 := createWebACLHelper(t, h, "acl-two", "REGIONAL")
	resourceARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"

	// Associate first ACL.
	rec := doWafv2Request(t, h, "AssociateWebACL", map[string]any{
		"WebACLArn":   aclARN1,
		"ResourceArn": resourceARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Associate second ACL — should replace, not duplicate.
	rec = doWafv2Request(t, h, "AssociateWebACL", map[string]any{
		"WebACLArn":   aclARN2,
		"ResourceArn": resourceARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Only one association should remain, pointing to acl-two.
	rec = doWafv2Request(t, h, "GetWebACLForResource", map[string]any{"ResourceArn": resourceARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	webACL := resp["WebACL"].(map[string]any)
	assert.Equal(t, "acl-two", webACL["Name"])
}

// ---- CLOUDFRONT scope ARN uses empty region ---------------------------------

func TestARN_CloudFrontScopeHasEmptyRegion(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("123456789012", "us-east-1")

	// CLOUDFRONT ARN should NOT contain the region in the ARN.
	arnStr := b.WebACLARN("my-acl", "abc123", "CLOUDFRONT")
	assert.Contains(t, arnStr, "global/webacl/my-acl/abc123", "CLOUDFRONT ARN resource path")
	// Region segment should be empty: arn:aws:wafv2::123456789012:global/...
	assert.NotContains(t, arnStr, "us-east-1", "CLOUDFRONT ARN must not include region")

	// REGIONAL ARN should include the region.
	arnRegional := b.WebACLARN("my-acl", "abc123", "REGIONAL")
	assert.Contains(t, arnRegional, "us-east-1", "REGIONAL ARN must include region")
	assert.Contains(t, arnRegional, "regional/webacl/my-acl/abc123")
}

// ---- ListLoggingConfigurations returns stored configs -----------------------

func TestListLoggingConfigurations_ReturnsStoredConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	arn1 := "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/acl-a/id1"
	arn2 := "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/acl-b/id2"

	for _, a := range []string{arn1, arn2} {
		rec := doWafv2Request(t, h, "PutLoggingConfiguration", map[string]any{
			"LoggingConfiguration": map[string]any{
				"ResourceArn":           a,
				"LogDestinationConfigs": []string{"arn:aws:s3:::my-log-bucket"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doWafv2Request(t, h, "ListLoggingConfigurations", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	configs, _ := resp["LoggingConfigurations"].([]any)
	assert.Len(t, configs, 2, "ListLoggingConfigurations should return 2 stored configs")
}

// ---- GetWebACLForResource returns not-found when no association exists -------

func TestGetWebACLForResource_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "GetWebACLForResource", map[string]any{
		"ResourceArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/no-acl/xyz",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFNonexistentItemException", errResp["__type"])
}

// ---- CheckCapacity handles nil / empty rules gracefully ---------------------

func TestCheckCapacity_NilRules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// nil rules → capacity 0.
	rec := doWafv2Request(t, h, "CheckCapacity", map[string]any{
		"Scope": "REGIONAL",
		"Rules": nil,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	capacity, _ := resp["ConsumedCapacity"].(float64)
	assert.InDelta(t, float64(0), capacity, 0, "nil rules capacity should be 0")

	// Empty rules → capacity 0.
	rec = doWafv2Request(t, h, "CheckCapacity", map[string]any{
		"Scope": "REGIONAL",
		"Rules": []any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	capacity, _ = resp["ConsumedCapacity"].(float64)
	assert.InDelta(t, float64(0), capacity, 0, "empty rules capacity should be 0")
}

// ---- CreateWebACL duplicate Name+Scope returns WAFDuplicateItemException ----

func TestCreateWebACL_DuplicateNameScope(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createWebACLHelper(t, h, "dup-acl", "REGIONAL")

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "dup-acl",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "WAFDuplicateItemException", errResp["__type"])
}

// ---- LockToken changes on each UpdateWebACL ---------------------------------

func TestLockToken_RotatesOnUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// Use createWebACLWithRules (REGIONAL scope) which returns (id, lockToken).
	id, token1 := createWebACLWithRules(t, h, "lock-rotation", "REGIONAL")

	rec := doWafv2Request(t, h, "UpdateWebACL", map[string]any{
		"Id":          id,
		"LockToken":   token1,
		"Description": "updated",
	})
	require.Equal(t, http.StatusOK, rec.Code, "update with correct token: %s", rec.Body.String())

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	token2 := updateResp["NextLockToken"].(string)

	assert.NotEmpty(t, token2)
	assert.NotEqual(t, token1, token2, "LockToken must rotate on every successful update")
}

// ---- IPSet name pattern validation ------------------------------------------

func TestValidation_IPSetNamePattern(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateIPSet", map[string]any{
		"Name":             "my.invalid.ipset",
		"Scope":            "REGIONAL",
		"IPAddressVersion": "IPV4",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "IPSet name with dots should be rejected")
}

// ---- RuleGroup name pattern validation --------------------------------------

func TestValidation_RuleGroupNamePattern(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateRuleGroup", map[string]any{
		"Name":     "invalid name!",
		"Scope":    "REGIONAL",
		"Capacity": 10,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "RuleGroup name with spaces/special chars should be rejected")
}
